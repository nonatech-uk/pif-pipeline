"""Expense action handler — posts receipt data to the trip expenses service."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from pipeline.actions.base import ActionHandler
from pipeline.actions.finance import _normalise_date
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class ExpenseHandler(ActionHandler):
    """POST extracted receipt data to the trip expenses ingest endpoint."""

    name = "expense"

    def __init__(self, base_url: str, pipeline_secret: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._secret = pipeline_secret

    @property
    def idempotency_key(self) -> str | None:
        return "file_sha256"

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        extracted = envelope.extracted
        if not extracted:
            return ActionResult(ok=False, destination=self.name, reason="No extracted fields")

        merchant = (
            extracted.get("merchant")
            or extracted.get("vendor")
            or extracted.get("_correspondent")
        )
        amount = extracted.get("amount") or extracted.get("total")

        if not amount or not merchant:
            log.info("Expense: insufficient extracted fields (need merchant + amount)")
            return ActionResult(ok=False, destination=self.name, reason="Missing amount or merchant")

        payload = {
            "merchant": merchant,
            "amount": str(amount),
            "currency": extracted.get("currency", "GBP"),
            "date": _normalise_date(extracted.get("date") or extracted.get("start_date")),
            "pipeline_envelope_id": envelope.item_id,
            "source": "pipeline",
        }

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{self._base_url}/api/v1/expenses/ingest",
                    json=payload,
                    headers={"Authorization": f"Bearer {self._secret}"},
                )

            if resp.status_code in (200, 201):
                data = resp.json()
                if data.get("status") == "duplicate":
                    log.info("Expense: duplicate receipt (already ingested)")
                    return ActionResult(ok=True, destination=self.name, ref="duplicate")
                ref = str(data.get("expense_id", ""))
                log.info("Expense accepted: id=%s merchant=%s amount=%s", ref, merchant, amount)
                return ActionResult(ok=True, destination=self.name, ref=ref)

            if resp.status_code == 422:
                # No active trip — not an error, just nothing to do
                log.info("Expense: no active trip, skipping")
                return ActionResult(ok=True, destination=self.name, reason="No active trip")

            log.error("Expense error: HTTP %d — %s", resp.status_code, resp.text[:200])
            return ActionResult(
                ok=False, destination=self.name,
                reason=f"HTTP {resp.status_code}", retryable=resp.status_code >= 500,
            )
        except httpx.HTTPError as e:
            log.error("Expense connection error: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e), retryable=True)
