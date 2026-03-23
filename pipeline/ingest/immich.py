"""Immich watcher — receives asset.created webhooks and fetches the original asset."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator

import httpx
from fastapi import APIRouter, Request

from pipeline.ingest.base import SourceWatcher
from pipeline.models import Envelope, ExifData

log = logging.getLogger(__name__)

router = APIRouter()

# Module-level queue — the webhook handler pushes, the watcher pulls
_webhook_queue: asyncio.Queue[dict] = asyncio.Queue()


@router.post("/webhook/immich")
async def immich_webhook(request: Request) -> dict:
    """Receive an Immich asset.created webhook."""
    payload = await request.json()
    log.info("Immich webhook received: %s", payload.get("type", "unknown"))

    if payload.get("type") == "asset.created" or "asset" in payload:
        await _webhook_queue.put(payload)

    return {"ok": True}


class ImmichWatcher(SourceWatcher):
    """Watches for Immich asset.created webhooks and fetches original bytes."""

    source_type = "camera"

    def __init__(self, immich_url: str, api_key: str) -> None:
        self._base_url = immich_url.rstrip("/")
        self._api_key = api_key

    async def watch(self) -> AsyncGenerator[Envelope, None]:
        log.info("Immich watcher listening for webhooks")
        while True:
            payload = await _webhook_queue.get()

            try:
                asset = payload.get("asset", payload)
                asset_id = asset.get("id")
                if not asset_id:
                    log.warning("Immich webhook missing asset ID: %s", payload)
                    continue

                envelope = await self._fetch_asset(asset_id, asset)
                if envelope:
                    log.info("Immich ingested asset %s (%s)", asset_id, envelope.media_type)
                    yield envelope
            except Exception:
                log.exception("Failed to process Immich webhook")

    async def _fetch_asset(self, asset_id: str, metadata: dict) -> Envelope | None:
        """Fetch original asset bytes and build an Envelope."""
        headers = {"x-api-key": self._api_key}

        async with httpx.AsyncClient(timeout=60) as client:
            # Fetch original file bytes
            resp = await client.get(
                f"{self._base_url}/api/assets/{asset_id}/original",
                headers=headers,
            )
            if resp.status_code != 200:
                log.error("Failed to fetch asset %s: HTTP %d", asset_id, resp.status_code)
                return None

            raw_bytes = resp.content

        # Build EXIF from Immich metadata (already parsed)
        exif = _exif_from_immich(metadata)

        envelope = self._build_envelope(
            raw_bytes,
            source_type=self.source_type,
            source_path=f"immich://{asset_id}",
            file_name=metadata.get("originalFileName", metadata.get("originalPath", "").split("/")[-1]),
        )
        # Override EXIF with Immich's richer metadata if available
        if exif:
            envelope.exif = exif

        return envelope


def _exif_from_immich(metadata: dict) -> ExifData | None:
    """Extract EXIF data from Immich's asset metadata."""
    exif_info = metadata.get("exifInfo", {})
    if not exif_info:
        return None

    from datetime import datetime, UTC

    lat = exif_info.get("latitude")
    lng = exif_info.get("longitude")
    taken_str = exif_info.get("dateTimeOriginal")
    taken_at = None
    if taken_str:
        try:
            taken_at = datetime.fromisoformat(taken_str.replace("Z", "+00:00"))
        except ValueError:
            pass

    return ExifData(
        gps_lat=lat,
        gps_lng=lng,
        taken_at=taken_at,
        year=taken_at.year if taken_at else None,
        camera_make=exif_info.get("make"),
        camera_model=exif_info.get("model"),
    )
