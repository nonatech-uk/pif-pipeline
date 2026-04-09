"""Immich album action handler — creates albums and adds assets."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class ImmichAlbumHandler(ActionHandler):
    """Create an Immich album if missing and add the asset to it."""

    name = "immich_album"

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._album_cache: dict[str, str] = {}  # name → album ID

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        rendered = self._render_params(params.get("params", params), envelope)
        album_name = rendered.get("album_name", "Pipeline")
        create_if_missing = rendered.get("create_if_missing", True)

        # Extract asset ID from immich source path
        asset_id = _extract_asset_id(envelope)
        if not asset_id:
            return ActionResult(ok=False, destination=self.name, reason="No Immich asset ID")

        headers = {"x-api-key": self._api_key}

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Find or create album
                album_id = await self._find_album(client, headers, album_name)
                if not album_id and create_if_missing:
                    album_id = await self._create_album(client, headers, album_name)
                if not album_id:
                    return ActionResult(ok=False, destination=self.name, reason=f"Album '{album_name}' not found")

                # Add asset to album
                resp = await client.put(
                    f"{self._base_url}/api/albums/{album_id}/assets",
                    headers=headers,
                    json={"ids": [asset_id]},
                )

                if resp.status_code not in (200, 201):
                    log.error("Immich album error: HTTP %d — %s", resp.status_code, resp.text[:200])
                    return ActionResult(ok=False, destination=self.name, reason=f"HTTP {resp.status_code}")

                log.info("Immich album: added %s to '%s'", asset_id[:8], album_name)

                # Archive asset to remove from timeline/Recents
                if rendered.get("archive", False):
                    archive_resp = await client.put(
                        f"{self._base_url}/api/assets",
                        headers=headers,
                        json={"ids": [asset_id], "isArchived": True},
                    )
                    if archive_resp.status_code in (200, 204):
                        log.info("Immich archive: archived %s", asset_id[:8])
                    else:
                        log.warning("Immich archive failed: HTTP %d — %s", archive_resp.status_code, archive_resp.text[:200])

            return ActionResult(ok=True, destination=self.name, ref=album_id)
        except httpx.HTTPError as e:
            log.error("Immich connection error: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e), retryable=True)

    async def _find_album(self, client: httpx.AsyncClient, headers: dict, name: str) -> str | None:
        if name in self._album_cache:
            return self._album_cache[name]

        resp = await client.get(f"{self._base_url}/api/albums", headers=headers)
        if resp.status_code != 200:
            return None

        for album in resp.json():
            if album.get("albumName") == name:
                self._album_cache[name] = album["id"]
                return album["id"]
        return None

    async def _create_album(self, client: httpx.AsyncClient, headers: dict, name: str) -> str | None:
        resp = await client.post(
            f"{self._base_url}/api/albums",
            headers=headers,
            json={"albumName": name},
        )
        if resp.status_code in (200, 201):
            album_id = resp.json()["id"]
            self._album_cache[name] = album_id
            log.info("Immich album created: '%s' (%s)", name, album_id[:8])
            return album_id
        return None


def _extract_asset_id(envelope: Envelope) -> str | None:
    """Extract Immich asset ID from the source path (immich://<id>)."""
    if envelope.source_path and envelope.source_path.startswith("immich://"):
        return envelope.source_path.split("//", 1)[1]
    return None
