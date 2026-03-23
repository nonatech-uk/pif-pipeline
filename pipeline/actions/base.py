"""Abstract base for action handlers and the handler registry."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pipeline.models import ActionResult, Envelope
from pipeline.rules.templates import render


class ActionHandler(ABC):
    """Base class for all action handlers."""

    name: str = "unknown"

    @abstractmethod
    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        """Execute the action. Returns an ActionResult."""
        ...

    def _render(self, template: str, envelope: Envelope, action_results: dict | None = None) -> str:
        """Render a Jinja2 template string against the envelope."""
        return render(template, envelope, action_results=action_results)

    def _render_params(self, params: dict[str, Any], envelope: Envelope, action_results: dict | None = None) -> dict[str, Any]:
        """Render all string values in a params dict."""
        rendered = {}
        for k, v in params.items():
            if isinstance(v, str) and "{{" in v:
                rendered[k] = self._render(v, envelope, action_results)
            else:
                rendered[k] = v
        return rendered

    @property
    def idempotency_key(self) -> str | None:
        return None
