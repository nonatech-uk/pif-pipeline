"""Rules loader — parse rules.yaml into Rule objects with SIGHUP reload."""

from __future__ import annotations

import logging
import signal
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


class Condition(BaseModel):
    """A single rule condition."""

    type: str
    # Type-specific fields stored as extras
    params: dict[str, Any] = Field(default_factory=dict)


class ActionSpec(BaseModel):
    """An action to fire when a rule matches."""

    handler: str
    params: dict[str, Any] = Field(default_factory=dict)


class Rule(BaseModel):
    """A single pipeline rule."""

    id: str
    name: str
    priority: int = 50
    conditions: list[Condition] = Field(default_factory=list)
    actions: list[ActionSpec] = Field(default_factory=list)
    on_match: str = "stop"  # stop | continue
    enabled: bool = True


class RulesLoader:
    """Loads rules from YAML, validates them, and supports SIGHUP reload."""

    def __init__(self, rules_path: Path) -> None:
        self._path = rules_path
        self._rules: list[Rule] = []
        self._load()
        self._register_sighup()

    @property
    def rules(self) -> list[Rule]:
        return self._rules

    def _load(self) -> None:
        """Load and validate rules from YAML."""
        if not self._path.exists():
            log.warning("Rules file not found: %s", self._path)
            self._rules = []
            return

        try:
            raw = yaml.safe_load(self._path.read_text()) or {}
        except yaml.YAMLError:
            log.exception("Invalid YAML in %s — keeping existing rules", self._path)
            return

        raw_rules = raw.get("rules", [])
        if not isinstance(raw_rules, list):
            log.error("'rules' must be a list in %s", self._path)
            return

        parsed: list[Rule] = []
        for i, entry in enumerate(raw_rules):
            try:
                rule = _parse_rule(entry)
                parsed.append(rule)
            except Exception:
                log.exception("Failed to parse rule #%d in %s", i, self._path)

        # Sort by priority ascending
        parsed.sort(key=lambda r: r.priority)

        old_count = len(self._rules)
        self._rules = parsed
        log.info("Loaded %d rules from %s (was %d)", len(parsed), self._path, old_count)

    def _register_sighup(self) -> None:
        """Register SIGHUP handler for hot reload."""
        try:
            signal.signal(signal.SIGHUP, self._on_sighup)
        except (OSError, ValueError):
            # Not available on all platforms / not main thread
            pass

    def _on_sighup(self, signum: int, frame: Any) -> None:
        log.info("SIGHUP received — reloading rules")
        self._load()

    def reload(self) -> None:
        """Manually trigger a reload."""
        self._load()


def _parse_rule(entry: dict) -> Rule:
    """Parse a single rule dict from YAML into a Rule object."""
    conditions = []
    for c in entry.get("conditions", []):
        ctype = c.pop("type", "")
        conditions.append(Condition(type=ctype, params=c))
        c["type"] = ctype  # restore for re-reads

    actions = []
    for a in entry.get("actions", []):
        handler = a.get("handler", "")
        params = a.get("params", {k: v for k, v in a.items() if k != "handler"})
        actions.append(ActionSpec(handler=handler, params=params))

    return Rule(
        id=entry.get("id", ""),
        name=entry.get("name", ""),
        priority=entry.get("priority", 50),
        conditions=conditions,
        actions=actions,
        on_match=entry.get("on_match", "stop"),
        enabled=entry.get("enabled", True),
    )
