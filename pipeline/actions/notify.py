"""Notify action handler — sends notifications via HA event or webhook."""

from __future__ import annotations

import logging
from typing import Any

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class NotifyHandler(ActionHandler):
    """Send a notification — currently logs, can be extended to HA events or webhooks."""

    name = "notify"

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        rendered = self._render_params(params.get("params", params), envelope)
        message = rendered.get("message", f"Pipeline notification for {envelope.id[:8]}")

        # For now, log the notification
        # TODO: POST to Home Assistant events API or webhook URL
        log.info("NOTIFY: %s", message)

        return ActionResult(ok=True, destination=self.name, ref="logged")
