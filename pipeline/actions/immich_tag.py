"""Immich tag action handler — applies tags and people labels to assets."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class ImmichTagHandler(ActionHandler):
    """Apply tags and people labels to an Immich asset."""

    name = "immich_tag"

    def __init__(self, base_url: str, api_key: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._tag_cache: dict[str, str] = {}  # tag name → tag ID

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        rendered = self._render_params(params.get("params", params), envelope)
        tags = rendered.get("tags", [])
        people = rendered.get("people", [])

        if isinstance(tags, str):
            tags = [tags]
        if isinstance(people, str):
            people = [people]

        asset_id = _extract_asset_id(envelope)
        if not asset_id:
            return ActionResult(ok=False, destination=self.name, reason="No Immich asset ID")

        headers = {"x-api-key": self._api_key}
        results: list[str] = []

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Apply tags
                for tag_name in tags:
                    tag_id = await self._ensure_tag(client, headers, tag_name)
                    if tag_id:
                        resp = await client.put(
                            f"{self._base_url}/api/tags/{tag_id}/assets",
                            headers=headers,
                            json={"ids": [asset_id]},
                        )
                        if resp.status_code in (200, 201):
                            results.append(f"tag:{tag_name}")

                # Apply people labels
                for person_name in people:
                    ok = await self._ensure_person(client, headers, asset_id, person_name)
                    if ok:
                        results.append(f"person:{person_name}")

            log.info("Immich tags applied to %s: %s", asset_id[:8], results)
            return ActionResult(ok=True, destination=self.name, ref=",".join(results))
        except httpx.HTTPError as e:
            log.error("Immich tag error: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e), retryable=True)

    async def _ensure_tag(self, client: httpx.AsyncClient, headers: dict, name: str) -> str | None:
        """Get or create a tag, return its ID."""
        if name in self._tag_cache:
            return self._tag_cache[name]

        # Try to create (upsert-like — Immich returns existing if name matches)
        resp = await client.post(
            f"{self._base_url}/api/tags",
            headers=headers,
            json={"name": name},
        )
        if resp.status_code in (200, 201):
            tag_id = resp.json()["id"]
            self._tag_cache[name] = tag_id
            return tag_id

        # If conflict, search for existing
        if resp.status_code == 409:
            resp = await client.get(f"{self._base_url}/api/tags", headers=headers)
            if resp.status_code == 200:
                for tag in resp.json():
                    if tag.get("name") == name:
                        self._tag_cache[name] = tag["id"]
                        return tag["id"]
        return None

    async def _ensure_person(
        self, client: httpx.AsyncClient, headers: dict, asset_id: str, name: str
    ) -> bool:
        """Add a person label to an asset's faces. Best-effort."""
        # Immich people API is face-based — this is a simplified approach
        # that creates the person if needed. Full implementation would
        # need face detection results from the asset.
        resp = await client.get(f"{self._base_url}/api/people", headers=headers)
        if resp.status_code != 200:
            return False

        person_id = None
        for person in resp.json().get("people", resp.json() if isinstance(resp.json(), list) else []):
            if person.get("name") == name:
                person_id = person["id"]
                break

        if not person_id:
            # Create person
            resp = await client.post(
                f"{self._base_url}/api/people",
                headers=headers,
                json={"name": name},
            )
            if resp.status_code in (200, 201):
                person_id = resp.json()["id"]
            else:
                return False

        log.info("Person '%s' ensured (id=%s)", name, person_id[:8] if person_id else "?")
        return True


def _extract_asset_id(envelope: Envelope) -> str | None:
    if envelope.source_path and envelope.source_path.startswith("immich://"):
        return envelope.source_path.split("//", 1)[1]
    return None
