"""Notify action handler — sends notifications via Pushover and email."""

from __future__ import annotations

import logging
from typing import Any

import pipeline.notify as notify_mod
from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class NotifyHandler(ActionHandler):
    """Send a notification via configured channels (Pushover, email)."""

    name = "notify"

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        rendered = self._render_params(params.get("params", params), envelope)
        title = rendered.get("title", f"Pipeline: {envelope.file_name or envelope.id[:8]}")
        message = rendered.get("message", f"Pipeline notification for {envelope.id[:8]}")

        notifier = notify_mod.get()
        if notifier:
            await notifier.send(title, message)
            return ActionResult(ok=True, destination=self.name, ref="sent")

        log.warning("NOTIFY: no notifier configured — %s", message)
        return ActionResult(ok=True, destination=self.name, ref="logged")
