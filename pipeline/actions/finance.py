"""Finance action handler — posts structured metadata to the finance service."""

from __future__ import annotations

import base64
import logging
from typing import Any

import httpx

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class FinanceHandler(ActionHandler):
    """POST structured extracted metadata to the finance import-metadata endpoint."""

    name = "finance"

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url.rstrip("/")

    @property
    def idempotency_key(self) -> str | None:
        return "file_sha256"

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        extracted = envelope.extracted
        if not extracted:
            return ActionResult(ok=False, destination=self.name, reason="No extracted fields")

        # Build the metadata payload
        payload: dict[str, Any] = {
            "original_filename": envelope.file_name or "unknown",
            "source": params.get("params", params).get("source", "pipeline"),
            "extracted_date": extracted.get("date"),
            "extracted_amount": extracted.get("amount"),
            "extracted_currency": extracted.get("currency", "GBP"),
            "extracted_merchant": extracted.get("vendor") or extracted.get("merchant"),
        }

        # Skip if we don't have the minimum required fields
        if not payload["extracted_amount"] or not payload["extracted_merchant"]:
            log.info("Finance: insufficient extracted fields, skipping")
            return ActionResult(ok=False, destination=self.name, reason="Missing amount or merchant")

        # Include full extraction data for reference
        payload["ocr_data"] = extracted

        # Include file bytes so the receipt is viewable in the finance UI
        if envelope.raw_bytes:
            payload["file_bytes"] = base64.b64encode(envelope.raw_bytes).decode()
            payload["mime_type"] = envelope.media_type

        # Add Paperless ref if available
        paperless_result = envelope.action_results.get("paperless")
        if paperless_result and paperless_result.ok:
            payload["note"] = f"Paperless task: {paperless_result.ref}"

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{self._base_url}/api/v1/receipts/import-metadata",
                    json=payload,
                )

            if resp.status_code in (200, 201):
                data = resp.json()
                ref = str(data.get("id", ""))
                log.info("Finance accepted: receipt=%s merchant=%s", ref, payload["extracted_merchant"])
                return ActionResult(ok=True, destination=self.name, ref=ref)

            log.error("Finance error: HTTP %d — %s", resp.status_code, resp.text[:200])
            return ActionResult(
                ok=False, destination=self.name,
                reason=f"HTTP {resp.status_code}", retryable=resp.status_code >= 500,
            )
        except httpx.HTTPError as e:
            log.error("Finance connection error: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e), retryable=True)
