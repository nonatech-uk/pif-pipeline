"""Core data models — the Envelope is the contract that flows through every stage."""

from __future__ import annotations

import uuid
from datetime import datetime, UTC
from typing import Any

from pydantic import BaseModel, Field


class ExifData(BaseModel):
    """EXIF metadata extracted from an image."""

    gps_lat: float | None = None
    gps_lng: float | None = None
    taken_at: datetime | None = None
    year: int | None = None
    camera_make: str | None = None
    camera_model: str | None = None


class ClassifyResult(BaseModel):
    """Output from a single classifier tier."""

    label: str
    confidence: float
    model: str  # deterministic | clip | llm | claude
    all_labels: dict[str, float] = Field(default_factory=dict)
    extracted: dict[str, Any] = Field(default_factory=dict)
    needs_escalation: bool = False


class ActionResult(BaseModel):
    """Outcome of executing a single action handler."""

    ok: bool
    destination: str
    ref: str | None = None  # e.g. paperless doc ID, immich album ID
    retryable: bool = False
    reason: str | None = None


class Envelope(BaseModel):
    """The normalised object that flows through the entire pipeline.

    Every stage reads from and writes to this single object.
    """

    id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    source_type: str  # scanner | camera | email
    source_path: str | None = None
    received_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    # File metadata
    media_type: str | None = None  # MIME type
    file_sha256: str | None = None
    file_size: int | None = None
    file_name: str | None = None
    raw_bytes: bytes | None = None

    # Email-specific
    source_email_from: str | None = None
    source_email_to: str | None = None
    source_email_subject: str | None = None

    # Extracted metadata
    exif: ExifData | None = None
    extracted: dict[str, Any] = Field(default_factory=dict)

    # Classification (set by tier_runner)
    classification: ClassifyResult | None = None
    tier_used: str | None = None
    all_tier_scores: dict[str, dict[str, float]] = Field(default_factory=dict)

    # Action results (set by action dispatcher)
    action_results: dict[str, ActionResult] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}
