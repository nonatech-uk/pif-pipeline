"""Location action handler — writes structured events to the location DB."""

from __future__ import annotations

import logging
from typing import Any

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class LocationHandler(ActionHandler):
    """Write a location event based on extracted fields (flights, travel, etc.)."""

    name = "location"

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        rendered = self._render_params(params.get("params", params), envelope)
        event_type = rendered.get("event_type", "unknown")

        extracted = envelope.extracted
        if not extracted:
            return ActionResult(ok=False, destination=self.name, reason="No extracted fields")

        # Build location event record
        event = {
            "event_type": event_type,
            "source_item_id": envelope.id,
            "source_type": envelope.source_type,
        }

        # Add GPS if available
        if envelope.exif:
            if envelope.exif.gps_lat is not None:
                event["lat"] = envelope.exif.gps_lat
                event["lng"] = envelope.exif.gps_lng
            if envelope.exif.taken_at:
                event["event_date"] = envelope.exif.taken_at.isoformat()

        # Add extracted fields
        for key in ("origin", "destination", "date", "date_from", "date_to",
                     "flight_number", "airline"):
            if key in extracted:
                event[key] = extracted[key]

        # For now, log the event — direct DB writes or API calls
        # will be wired up when the location service has an ingest endpoint
        log.info("Location event: %s — %s", event_type, event)

        return ActionResult(ok=True, destination=self.name, ref=event_type)
