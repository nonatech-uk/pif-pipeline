"""Location action handler — posts flight data to the my-locations API."""

from __future__ import annotations

import logging
from typing import Any

import httpx

import pipeline.notify as notify_mod
from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class LocationHandler(ActionHandler):
    """Post location events to the my-locations service."""

    name = "location"

    def __init__(self, base_url: str, secret: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._secret = secret

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        rendered = self._render_params(params.get("params", params), envelope)
        event_type = rendered.get("event_type", "unknown")

        if event_type != "flight":
            log.info("Location event type '%s' not yet supported, skipping", event_type)
            return ActionResult(ok=True, destination=self.name, ref=event_type)

        extracted = envelope.extracted
        if not extracted:
            return ActionResult(ok=False, destination=self.name, reason="No extracted fields")

        # Build list of legs — support both new multi-leg and legacy single-flight format
        legs = extracted.get("legs")
        if legs and isinstance(legs, list):
            payloads = []
            for leg in legs:
                payloads.append({
                    "date": leg.get("date") or extracted.get("date"),
                    "dep_airport": leg.get("origin"),
                    "arr_airport": leg.get("destination"),
                    "flight_number": leg.get("flight_number"),
                    "airline": leg.get("airline") or extracted.get("airline"),
                    "seat_number": leg.get("seat"),
                    "cabin_class": leg.get("cabin_class"),
                    "source": "pipeline",
                })
        else:
            payloads = [{
                "date": extracted.get("date"),
                "dep_airport": extracted.get("origin"),
                "arr_airport": extracted.get("destination"),
                "flight_number": extracted.get("flight_number"),
                "airline": extracted.get("airline"),
                "seat_number": extracted.get("seat"),
                "cabin_class": extracted.get("cabin_class"),
                "source": "pipeline",
            }]

        refs = []
        for payload in payloads:
            if not payload["date"] or not payload["dep_airport"] or not payload["arr_airport"]:
                log.warning("Skipping flight leg with missing date/origin/destination: %s", payload)
                continue
            ref = await self._ingest_flight(payload)
            if ref:
                refs.append(ref)

        if not refs:
            return ActionResult(ok=False, destination=self.name, reason="No valid flight legs")

        return ActionResult(ok=True, destination=self.name, ref=", ".join(refs))

    async def _ingest_flight(self, payload: dict) -> str | None:
        """Post a single flight leg to the location API."""
        route = f"{payload['dep_airport']}→{payload['arr_airport']}"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{self._base_url}/api/v1/flights/ingest",
                    json=payload,
                    headers={"Authorization": f"Bearer {self._secret}"},
                )

            if resp.status_code in (200, 201):
                data = resp.json()
                status = data.get("status", "ok")
                ref = str(data.get("id", status))
                log.info("Location flight ingested: %s (%s)", ref, route)

                notifier = notify_mod.get()
                if notifier:
                    if status == "duplicate":
                        await notifier.send(
                            "Flight: duplicate",
                            f"Duplicate flight ignored: {payload.get('airline', '')} {payload.get('flight_number', '')} {route} on {payload['date']}",
                        )
                    else:
                        await notifier.send(
                            "Flight added",
                            f"New flight recorded: {payload.get('airline', '')} {payload.get('flight_number', '')} {route} on {payload['date']}",
                        )

                return ref

            log.error("Location API error: HTTP %d — %s", resp.status_code, resp.text[:200])
            return None
        except httpx.HTTPError as e:
            log.error("Location API connection error: %s", e)
            return None
