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

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        if not envelope.raw_bytes:
            return ActionResult(ok=False, destination=self.name, reason="No file bytes")

        rendered = self._render_params(params.get("params", params), envelope)
        document_type = rendered.get("document_type", "")
        title = rendered.get("title", envelope.file_name or "Untitled")
        correspondent = rendered.get("correspondent")
        tags = rendered.get("tags", [])
        if isinstance(tags, str):
            tags = [tags]

        headers = {"Authorization": f"Token {self._token}"}

        # Build multipart form
        files = {
            "document": (
                envelope.file_name or "document",
                envelope.raw_bytes,
                envelope.media_type or "application/octet-stream",
            ),
        }
        data: dict[str, Any] = {}
        if title:
            data["title"] = title
        if document_type:
            data["document_type"] = document_type
        if correspondent:
            data["correspondent"] = correspondent
        for tag in tags:
            data.setdefault("tags", []).append(tag)
        if envelope.exif and envelope.exif.taken_at:
            data["created_date"] = envelope.exif.taken_at.strftime("%Y-%m-%d")

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self._base_url}/api/documents/post_document/",
                    headers=headers,
                    files=files,
                    data=data,
                )

            if resp.status_code in (200, 201, 202):
                # Paperless returns a task ID, not the doc ID immediately
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
