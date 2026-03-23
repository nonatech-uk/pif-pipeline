"""Scanner watcher — monitors a drop folder for new files via inotify."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

from watchdog.events import FileSystemEventHandler, FileClosedEvent, FileCreatedEvent
from watchdog.observers import Observer

from pipeline.ingest.base import SourceWatcher
from pipeline.models import Envelope

log = logging.getLogger(__name__)


class _Handler(FileSystemEventHandler):
    """Collects file paths into an asyncio queue."""

    def __init__(self, queue: asyncio.Queue[Path], loop: asyncio.AbstractEventLoop) -> None:
        self._queue = queue
        self._loop = loop

    def on_closed(self, event: FileClosedEvent) -> None:
        if not event.is_directory:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, Path(event.src_path))

    def on_created(self, event: FileCreatedEvent) -> None:
        # Fallback for filesystems that don't emit close events
        if not event.is_directory:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, Path(event.src_path))


class ScannerWatcher(SourceWatcher):
    """Watches a drop folder for new files using inotify."""

    source_type = "scanner"

    def __init__(self, drop_folder: Path) -> None:
        self._drop_folder = drop_folder
        self._drop_folder.mkdir(parents=True, exist_ok=True)
        self._debounce_ms = 500
        self._seen: set[Path] = set()

    async def watch(self) -> AsyncGenerator[Envelope, None]:
        queue: asyncio.Queue[Path] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        handler = _Handler(queue, loop)

        observer = Observer()
        observer.schedule(handler, str(self._drop_folder), recursive=False)
        observer.start()
        log.info("Scanner watching %s", self._drop_folder)

        try:
            while True:
                path = await queue.get()

                # Deduplicate — both on_created and on_closed may fire
                if path in self._seen:
                    continue
                self._seen.add(path)

                # Debounce: wait for file to be fully written
                await asyncio.sleep(self._debounce_ms / 1000)

                if not path.exists():
                    self._seen.discard(path)
                    continue

                try:
                    raw_bytes = path.read_bytes()
                    if not raw_bytes:
                        log.warning("Empty file ignored: %s", path)
                        self._seen.discard(path)
                        continue

                    envelope = self._build_envelope(
                        raw_bytes,
                        source_type=self.source_type,
                        source_path=str(path),
                        file_name=path.name,
                    )
                    log.info("Scanner ingested: %s (%s, %d bytes)", path.name, envelope.media_type, envelope.file_size)
                    yield envelope
                except Exception:
                    log.exception("Failed to process %s", path)
                    self._seen.discard(path)
        finally:
            observer.stop()
            observer.join()
