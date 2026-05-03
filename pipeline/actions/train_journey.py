"""Train journey action handler — posts rail journeys to the journal app."""

from __future__ import annotations

import logging
from typing import Any

import httpx

import pipeline.notify as notify_mod
from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class TrainJourneyHandler(ActionHandler):
    """Post rail journeys to the journal /rail-journeys/ingest endpoint."""

    name = "train_journey"

    def __init__(self, base_url: str, secret: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._secret = secret

    @property
    def idempotency_key(self) -> str | None:
        return "file_sha256"

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        extracted = envelope.extracted
        if not extracted:
            return ActionResult(ok=False, destination=self.name, reason="No extracted fields")

        journeys = extracted.get("journeys")
        if not journeys or not isinstance(journeys, list):
            return ActionResult(ok=False, destination=self.name, reason="No journeys in extracted fields")

        legs = []
        for j in journeys:
            if not j.get("date") or not j.get("from_station") or not j.get("to_station"):
                log.warning("Skipping journey with missing date/from/to: %s", j)
                continue
            legs.append({
                "date": j.get("date"),
                "time": j.get("time"),
                "from_station": j.get("from_station"),
                "from_code": j.get("from_code"),
                "to_station": j.get("to_station"),
                "to_code": j.get("to_code"),
                "ticket_type": j.get("ticket_type"),
                "ticket_class": j.get("ticket_class"),
                "direction": j.get("direction"),
                "train": j.get("train"),
                "via": j.get("via"),
                "price": j.get("price"),
            })

        if not legs:
            return ActionResult(ok=False, destination=self.name, reason="No valid journey legs")

        payload = {
            "operator": extracted.get("operator") or extracted.get("merchant"),
            "reference": extracted.get("reference"),
            "currency": extracted.get("currency", "GBP"),
            "journeys": legs,
            "source": "pipeline",
        }

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{self._base_url}/api/v1/rail-journeys/ingest",
                    json=payload,
                    headers={"Authorization": f"Bearer {self._secret}"},
                )

            if resp.status_code in (200, 201):
                data = resp.json()
                ids = data.get("ids", [])
                duplicates = data.get("duplicates", 0)
                ref = ",".join(str(i) for i in ids) if ids else "duplicate"
                log.info("Train journeys: %d created, %d duplicate (%s)", len(ids), duplicates, payload.get("reference"))

                notifier = notify_mod.get()
                if notifier and ids:
                    summary = ", ".join(
                        f"{l['from_station']}→{l['to_station']} {l['date']} {l.get('time') or ''}".strip()
                        for l in legs[:3]
                    )
                    await notifier.send(
                        f"Train booking added ({len(ids)} {'leg' if len(ids)==1 else 'legs'})",
                        f"{payload.get('operator') or 'Rail'}: {summary}",
                    )
                return ActionResult(ok=True, destination=self.name, ref=ref)

            log.error("Journal rail ingest error: HTTP %d — %s", resp.status_code, resp.text[:200])
            return ActionResult(
                ok=False, destination=self.name,
                reason=f"HTTP {resp.status_code}", retryable=resp.status_code >= 500,
            )
        except httpx.HTTPError as e:
            log.error("Journal rail ingest connection error: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e), retryable=True)
