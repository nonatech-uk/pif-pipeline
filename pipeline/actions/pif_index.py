"""PIF photo index seeder — writes skeleton rows to pif.photo_index for enrichment."""

from __future__ import annotations

import logging
from typing import Any

import asyncpg
import httpx

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class PifIndexHandler(ActionHandler):
    """Seed pif.photo_index with metadata so the PIF enricher can add descriptions."""

    name = "pif_index"

    def __init__(self, pif_db_url: str, immich_url: str, immich_api_key: str) -> None:
        self._pif_db_url = pif_db_url
        self._immich_url = immich_url.rstrip("/")
        self._immich_api_key = immich_api_key
        self._pool: asyncpg.Pool | None = None

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._pif_db_url, min_size=1, max_size=2)
        return self._pool

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        asset_id = _extract_asset_id(envelope)
        if not asset_id:
            return ActionResult(ok=False, destination=self.name, reason="No Immich asset ID")

        try:
            # Fetch people and location from Immich in one call
            people, location_name = await self._fetch_immich_metadata(asset_id)

            # Build row data from envelope EXIF
            taken_at = envelope.exif.taken_at if envelope.exif else None
            latitude = envelope.exif.gps_lat if envelope.exif else None
            longitude = envelope.exif.gps_lng if envelope.exif else None

            pool = await self._get_pool()
            await pool.execute(
                """
                INSERT INTO photo_index
                    (immich_asset_id, taken_at, latitude, longitude,
                     location_name, people, album_names, indexed_at)
                VALUES ($1, $2, $3, $4, $5, $6, '{}', now())
                ON CONFLICT (immich_asset_id) DO NOTHING
                """,
                asset_id, taken_at, latitude, longitude,
                location_name, people or [],
            )

            log.info("PIF index seeded for asset %s (people=%s, location=%s)",
                     asset_id[:12], people, location_name)
            return ActionResult(ok=True, destination=self.name, ref=asset_id)

        except Exception as e:
            log.exception("PIF index seed failed for asset %s", asset_id[:12] if asset_id else "?")
            return ActionResult(ok=False, destination=self.name, reason=str(e), retryable=True)

    async def _fetch_immich_metadata(self, asset_id: str) -> tuple[list[str], str | None]:
        """Fetch people names and reverse-geocoded location from Immich."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{self._immich_url}/api/assets/{asset_id}",
                headers={"x-api-key": self._immich_api_key},
            )
            if resp.status_code != 200:
                return [], None

            data = resp.json()

            # People
            people = []
            for person in data.get("people", []):
                name = person.get("name", "").strip()
                if name:
                    people.append(name)

            # Location from EXIF
            exif = data.get("exifInfo", {})
            parts = [p for p in [exif.get("city"), exif.get("state"), exif.get("country")] if p]
            location_name = ", ".join(parts) or None

            return people, location_name


def _extract_asset_id(envelope: Envelope) -> str | None:
    if envelope.source_path and envelope.source_path.startswith("immich://"):
        return envelope.source_path.split("//", 1)[1]
    return None
