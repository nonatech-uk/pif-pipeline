"""Audit log data models — every pipeline decision is recorded here."""

from __future__ import annotations

from datetime import datetime, UTC
from typing import Any

from pydantic import BaseModel, Field


class TierTrace(BaseModel):
    """Record of a single classifier tier's result."""

    tier: str  # deterministic | clip | llm | claude
    label: str | None = None
    confidence: float | None = None
    all_labels: dict[str, float] = Field(default_factory=dict)
    skipped: bool = False
    skip_reason: str | None = None
    duration_ms: int | None = None


class RuleTrace(BaseModel):
    """Record of a rule evaluation."""

    rule_id: str
    rule_name: str
    matched: bool
    conditions_met: list[str] = Field(default_factory=list)
    conditions_failed: list[str] = Field(default_factory=list)
    on_match: str | None = None  # stop | continue


class ActionTrace(BaseModel):
    """Record of an action execution."""

    handler: str
    destination: str
    ok: bool
    ref: str | None = None
    reason: str | None = None
    duration_ms: int | None = None


class DecisionTrace(BaseModel):
    """Full trace of how an item was processed."""

    tiers: list[TierTrace] = Field(default_factory=list)
    rules: list[RuleTrace] = Field(default_factory=list)
    actions: list[ActionTrace] = Field(default_factory=list)


class AuditEntry(BaseModel):
    """A single audit log entry — one per processed item."""

    item_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    source_type: str
    source_path: str | None = None
    file_sha256: str | None = None
    media_type: str | None = None

    # Final classification
    label: str | None = None
    confidence: float | None = None
    tier_used: str | None = None

    # Actions taken
    destinations: list[str] = Field(default_factory=list)
    exception_queued: bool = False

    # Full decision trace
    trace: DecisionTrace = Field(default_factory=DecisionTrace)

    # Extracted fields for cross-referencing
    extracted: dict[str, Any] = Field(default_factory=dict)
