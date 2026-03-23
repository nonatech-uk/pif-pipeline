"""Exception queue action handler — pushes unprocessable items for review."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pipeline.actions.base import ActionHandler
from pipeline.exceptions.queue import ExceptionItem, ExceptionQueue
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class ExceptionQueueHandler(ActionHandler):
    """Push an item to the exception queue for manual review."""

    name = "exception_queue"

    def __init__(self, db_path: Path) -> None:
        self._queue = ExceptionQueue(db_path)

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        reason = params.get("reason", "No rules matched")
        priority = params.get("review_priority", 50)

        classification_output = {}
        if envelope.classification:
            classification_output = envelope.classification.model_dump()

        # Serialize envelope without raw_bytes (too large for SQLite)
        envelope_dict = envelope.model_dump(exclude={"raw_bytes"})

        item = ExceptionItem(
            item_id=envelope.id,
            reason=reason,
            review_priority=priority,
            classification_output=classification_output,
            envelope_json=envelope_dict,
        )

        await self._queue.add(item)
        log.info("Exception queued: %s — %s (priority %d)", envelope.id[:8], reason, priority)

        return ActionResult(ok=True, destination=self.name, ref=envelope.id)
