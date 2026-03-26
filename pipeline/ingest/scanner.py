"""Scanner watcher — monitors a drop folder for new files via periodic directory scan."""

from __future__ import annotations

import asyncio
import logging
import shutil
from collections.abc import AsyncGenerator
from pathlib import Path

from pipeline.ingest.base import SourceWatcher
from pipeline.models import Envelope

log = logging.getLogger(__name__)


class ScannerWatcher(SourceWatcher):
    """Watches a drop folder by periodically listing files."""

    source_type = "scanner"

    def __init__(
        self,
        drop_folder: Path,
        processed_folder: Path | None = None,
        poll_interval: int = 10,
    ) -> None:
        self._drop_folder = drop_folder
        self._drop_folder.mkdir(parents=True, exist_ok=True)
        self._processed_folder = processed_folder
        if self._processed_folder:
            self._processed_folder.mkdir(parents=True, exist_ok=True)
        self._poll_interval = poll_interval
        self._seen: set[str] = set()

    async def watch(self) -> AsyncGenerator[Envelope, None]:
        log.info("Scanner watching %s (polling every %ds)", self._drop_folder, self._poll_interval)

        while True:
            try:
                for path in sorted(self._drop_folder.iterdir()):
                    if path.is_dir() or path.name.startswith("."):
                        continue
                    if path.name in self._seen:
                        continue

                    # Wait briefly for file to be fully written
                    await asyncio.sleep(0.5)

                    if not path.exists():
                        continue

                    try:
                        raw_bytes = path.read_bytes()
                        if not raw_bytes:
                            log.warning("Empty file ignored: %s", path)
                            continue

                        envelope = self._build_envelope(
                            raw_bytes,
                            source_type=self.source_type,
                            source_path=str(path),
                            file_name=path.name,
                        )
                        log.info("Scanner ingested: %s (%s, %d bytes)", path.name, envelope.media_type, envelope.file_size)
                        yield envelope

                        self._seen.add(path.name)

                        # Move to processed folder
                        if self._processed_folder:
                            dest = self._processed_folder / path.name
                            try:
                                shutil.move(str(path), str(dest))
                                log.info("Moved to processed: %s", path.name)
                            except Exception:
                                log.exception("Failed to move %s to processed", path.name)
                    except Exception:
                        log.exception("Failed to process %s", path)
            except Exception:
                log.exception("Scanner poll error")

            await asyncio.sleep(self._poll_interval)
