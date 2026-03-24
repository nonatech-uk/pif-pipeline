"""Paperless action handler — uploads documents to Paperless-ngx."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class PaperlessHandler(ActionHandler):
    """POST documents to the Paperless-ngx REST API."""

    name = "paperless"

    def __init__(self, base_url: str, api_token: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = api_token
        # Caches: name → PK
        self._doc_type_cache: dict[str, int] = {}
        self._correspondent_cache: dict[str, int] = {}
        self._tag_cache: dict[str, int] = {}

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Token {self._token}"}

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        if not envelope.raw_bytes:
            return ActionResult(ok=False, destination=self.name, reason="No file bytes")

        rendered = self._render_params(params.get("params", params), envelope)
        doc_type_name = rendered.get("document_type", "")
        title = rendered.get("title", envelope.file_name or "Untitled")
        correspondent_name = rendered.get("correspondent", "") or envelope.extracted.get("_correspondent", "")
        tag_names = rendered.get("tags", [])
        if isinstance(tag_names, str):
            tag_names = [tag_names]
        # Merge tags from Claude extraction
        extracted_tags = envelope.extracted.get("_tags", [])
        if extracted_tags:
            existing = {t.lower() for t in tag_names}
            for t in extracted_tags:
                if t.lower() not in existing:
                    tag_names.append(t)

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                # Resolve names → PKs
                doc_type_id = await self._resolve_doc_type(client, doc_type_name) if doc_type_name else None
                correspondent_id = await self._resolve_correspondent(client, correspondent_name) if correspondent_name else None
                tag_ids = []
                for tn in tag_names:
                    tid = await self._resolve_tag(client, tn)
                    if tid:
                        tag_ids.append(tid)

                # Build multipart form
                data: dict[str, Any] = {}
                if title:
                    data["title"] = title
                if doc_type_id is not None:
                    data["document_type"] = doc_type_id
                if correspondent_id is not None:
                    data["correspondent"] = correspondent_id
                for tid in tag_ids:
                    data.setdefault("tags", []).append(tid)
                if envelope.exif and envelope.exif.taken_at:
                    data["created_date"] = envelope.exif.taken_at.strftime("%Y-%m-%d")

                resp = await client.post(
                    f"{self._base_url}/api/documents/post_document/",
                    headers=self._headers(),
                    files={"document": (
                        envelope.file_name or "document",
                        envelope.raw_bytes,
                        envelope.media_type or "application/octet-stream",
                    )},
                    data=data,
                )

            if resp.status_code in (200, 201, 202):
                ref = resp.text.strip().strip('"')
                log.info("Paperless accepted: task=%s title=%s", ref, title)
                return ActionResult(ok=True, destination=self.name, ref=ref)

            log.error("Paperless error: HTTP %d — %s", resp.status_code, resp.text[:200])
            return ActionResult(
                ok=False, destination=self.name,
                reason=f"HTTP {resp.status_code}", retryable=resp.status_code >= 500,
            )
        except httpx.HTTPError as e:
            log.error("Paperless connection error: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e), retryable=True)

    async def _resolve_doc_type(self, client: httpx.AsyncClient, name: str) -> int | None:
        """Resolve a document type name to its PK, creating if needed."""
        if name in self._doc_type_cache:
            return self._doc_type_cache[name]
        pk = await self._find_or_create(client, "document_types", name)
        if pk is not None:
            self._doc_type_cache[name] = pk
        return pk

    async def _resolve_correspondent(self, client: httpx.AsyncClient, name: str) -> int | None:
        """Resolve a correspondent name to its PK, creating if needed."""
        if not name:
            return None
        if name in self._correspondent_cache:
            return self._correspondent_cache[name]
        pk = await self._find_or_create(client, "correspondents", name)
        if pk is not None:
            self._correspondent_cache[name] = pk
        return pk

    async def _resolve_tag(self, client: httpx.AsyncClient, name: str) -> int | None:
        """Resolve a tag name to its PK, creating if needed."""
        if name in self._tag_cache:
            return self._tag_cache[name]
        pk = await self._find_or_create(client, "tags", name)
        if pk is not None:
            self._tag_cache[name] = pk
        return pk

    async def _find_or_create(self, client: httpx.AsyncClient, endpoint: str, name: str) -> int | None:
        """Find an object by name or create it. Returns the PK."""
        headers = self._headers()

        # Search by name
        resp = await client.get(
            f"{self._base_url}/api/{endpoint}/",
            headers=headers,
            params={"name__iexact": name},
        )
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", data if isinstance(data, list) else [])
            for item in results:
                if item.get("name", "").lower() == name.lower():
                    log.debug("Paperless %s found: %s → %d", endpoint, name, item["id"])
                    return item["id"]

        # Not found — create
        resp = await client.post(
            f"{self._base_url}/api/{endpoint}/",
            headers=headers,
            json={"name": name},
        )
        if resp.status_code in (200, 201):
            pk = resp.json()["id"]
            log.info("Paperless %s created: %s → %d", endpoint, name, pk)
            return pk

        log.warning("Paperless failed to resolve %s '%s': %s", endpoint, name, resp.text[:100])
        return None
