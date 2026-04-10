"""Immich watcher — polls for new assets via the search API."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from datetime import datetime, UTC

import httpx

from pipeline.db import get_pool
from pipeline.ingest.base import SourceWatcher
from pipeline.models import Envelope, ExifData

log = logging.getLogger(__name__)

_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS immich_processed (
    asset_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class ImmichWatcher(SourceWatcher):
    """Polls Immich for new assets, deduplicating via asset ID in the DB."""

    source_type = "camera"

    def __init__(
        self,
        immich_url: str,
        api_key: str,
        poll_interval: int = 60,
    ) -> None:
        self._base_url = immich_url.rstrip("/")
        self._api_key = api_key
        self._poll_interval = poll_interval
        self._headers = {"x-api-key": api_key, "Content-Type": "application/json"}

    async def _ensure_table(self) -> None:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.execute(_TABLE_SCHEMA)

    async def _is_processed(self, asset_id: str) -> bool:
        pool = get_pool()
        return await pool.fetchval(
            "SELECT 1 FROM immich_processed WHERE asset_id = $1", asset_id,
        ) is not None

    async def _mark_processed(self, asset_id: str) -> None:
        pool = get_pool()
        await pool.execute(
            "INSERT INTO immich_processed (asset_id) VALUES ($1) ON CONFLICT DO NOTHING",
            asset_id,
        )

    async def watch(self) -> AsyncGenerator[Envelope, None]:
        await self._ensure_table()
        log.info(
            "Immich watcher polling %s every %ds",
            self._base_url, self._poll_interval,
        )

        import os
        hc_uuid = os.environ.get("HC_PIPELINE_IMMICH", "")
        hc_url = f"https://hc.mees.st/ping/{hc_uuid}" if hc_uuid else ""

        while True:
            try:
                for envelope in await self._poll():
                    yield envelope
                if hc_url:
                    async with httpx.AsyncClient(timeout=10) as hc:
                        await hc.get(hc_url)
            except Exception:
                log.exception("Immich poll error, retrying in %ds", self._poll_interval)
                if hc_url:
                    try:
                        async with httpx.AsyncClient(timeout=10) as hc:
                            await hc.get(f"{hc_url}/fail")
                    except Exception:
                        pass

            await asyncio.sleep(self._poll_interval)

    async def _poll(self) -> list[Envelope]:
        """Fetch recent assets from Immich and process new ones."""
        envelopes: list[Envelope] = []

        async with httpx.AsyncClient(timeout=30) as client:
            # Search for recent assets (last 24h to catch up after restarts)
            resp = await client.post(
                f"{self._base_url}/api/search/metadata",
                headers=self._headers,
                json={"order": "desc", "size": 50},
            )
            if resp.status_code != 200:
                log.error("Immich search failed: HTTP %d", resp.status_code)
                return envelopes

            data = resp.json()
            items = data.get("assets", {}).get("items", [])

        # Filter to new assets
        new_assets = []
        for asset in items:
            asset_id = asset.get("id")
            if not asset_id:
                continue
            if not await self._is_processed(asset_id):
                new_assets.append(asset)

        if not new_assets:
            return envelopes

        log.info("Found %d new Immich asset(s) to process", len(new_assets))

        for asset in new_assets:
            asset_id = asset["id"]
            try:
                envelope = await self._fetch_asset(asset_id, asset)
                if envelope:
                    envelopes.append(envelope)
                await self._mark_processed(asset_id)
                log.info("Processed Immich asset %s (%s)", asset_id[:12], asset.get("originalFileName"))
            except Exception:
                log.exception("Failed to process Immich asset %s", asset_id)

        return envelopes

    async def _fetch_asset(self, asset_id: str, metadata: dict) -> Envelope | None:
        """Fetch original asset bytes and build an Envelope."""
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(
                f"{self._base_url}/api/assets/{asset_id}/original",
                headers={"x-api-key": self._api_key},
            )
            if resp.status_code != 200:
                log.error("Failed to fetch asset %s: HTTP %d", asset_id, resp.status_code)
                return None

            raw_bytes = resp.content

        exif = _exif_from_immich(metadata)

        envelope = self._build_envelope(
            raw_bytes,
            source_type=self.source_type,
            source_path=f"immich://{asset_id}",
            file_name=metadata.get("originalFileName", ""),
        )
        if exif:
            envelope.exif = exif

        return envelope


def _exif_from_immich(metadata: dict) -> ExifData | None:
    """Extract EXIF data from Immich's asset metadata."""
    exif_info = metadata.get("exifInfo", {})
    if not exif_info:
        return None

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
