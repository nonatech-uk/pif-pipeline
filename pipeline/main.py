"""Pipeline entrypoint — gathers all source watchers and processes Envelopes."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from pipeline.config import load_settings
from pipeline.ingest.scanner import ScannerWatcher
from pipeline.ingest.immich import ImmichWatcher, router as immich_router
from pipeline.ingest.email import EmailWatcher
from pipeline.models import Envelope

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("pipeline")


async def process_envelope(envelope: Envelope) -> None:
    """Process a single envelope through the pipeline.

    Currently just logs — classification, rules, and actions are wired in later phases.
    """
    log.info(
        "ENVELOPE  id=%s  source=%s  type=%s  size=%s  sha256=%s  file=%s  exif=%s",
        envelope.id[:8],
        envelope.source_type,
        envelope.media_type,
        envelope.file_size,
        (envelope.file_sha256 or "")[:12],
        envelope.file_name,
        "yes" if envelope.exif else "no",
    )
    if envelope.exif:
        e = envelope.exif
        log.info(
            "  EXIF  gps=(%s, %s)  taken=%s  camera=%s %s",
            e.gps_lat, e.gps_lng, e.taken_at, e.camera_make, e.camera_model,
        )


async def run_watcher(watcher, name: str) -> None:
    """Run a single watcher, processing each envelope it yields."""
    log.info("Starting watcher: %s", name)
    try:
        async for envelope in watcher.watch():
            await process_envelope(envelope)
    except asyncio.CancelledError:
        log.info("Watcher %s cancelled", name)
    except Exception:
        log.exception("Watcher %s crashed", name)


def create_app() -> FastAPI:
    """Create the FastAPI app with webhook routes."""
    app = FastAPI(title="Pipeline Ingest")
    app.include_router(immich_router)
    return app


async def main() -> None:
    settings = load_settings()
    log.info("Pipeline starting — project root: %s", settings.project_root)

    # Build watchers
    watchers: list[tuple[str, object]] = []

    # Scanner watcher — always active
    drop_folder = settings.resolve_path(settings.paths.drop_folder)
    watchers.append(("scanner", ScannerWatcher(drop_folder)))

    # Immich watcher — needs API key
    if settings.immich_api_key:
        watchers.append(("immich", ImmichWatcher(
            immich_url=settings.services.immich_url,
            api_key=settings.immich_api_key,
        )))
    else:
        log.warning("IMMICH_API_KEY not set — Immich watcher disabled")

    # Email watcher — needs IMAP credentials
    if settings.services.imap_user and settings.services.imap_password:
        watchers.append(("email", EmailWatcher(
            host=settings.services.imap_host,
            port=settings.services.imap_port,
            user=settings.services.imap_user,
            password=settings.services.imap_password,
        )))
    else:
        log.warning("IMAP credentials not set — Email watcher disabled")

    # Start FastAPI server for webhooks (Immich)
    app = create_app()
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning")
    server = uvicorn.Server(config)

    tasks = [
        asyncio.create_task(server.serve(), name="uvicorn"),
    ]
    for name, watcher in watchers:
        tasks.append(asyncio.create_task(run_watcher(watcher, name), name=name))

    log.info("Pipeline running — %d watchers active", len(watchers))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
