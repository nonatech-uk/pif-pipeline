"""Diff original pipeline classification against Paperless edits."""

from __future__ import annotations

import logging
from datetime import datetime, UTC
from typing import Any

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


class Correction(BaseModel):
    """A single detected correction from a Paperless edit."""

    correction_type: str  # document_type | correspondent | tag_added | tag_removed | title
    field: str
    original_value: str | None = None
    corrected_value: str | None = None
    item_id: str | None = None
    label: str | None = None
    tier_used: str | None = None
    confidence: float | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


def diff_metadata(
    original: dict[str, Any],
    updated: dict[str, Any],
    item_id: str | None = None,
    label: str | None = None,
    tier_used: str | None = None,
    confidence: float | None = None,
) -> list[Correction]:
    """Compare original pipeline output against Paperless edits.

    Returns a list of corrections found.
    """
    corrections: list[Correction] = []
    common = dict(item_id=item_id, label=label, tier_used=tier_used, confidence=confidence)

    # Document type change
    orig_dt = original.get("document_type")
    new_dt = updated.get("document_type")
    if orig_dt and new_dt and str(orig_dt) != str(new_dt):
        corrections.append(Correction(
            correction_type="document_type",
            field="document_type",
            original_value=str(orig_dt),
            corrected_value=str(new_dt),
            **common,
        ))

    # Correspondent change
    orig_co = original.get("correspondent")
    new_co = updated.get("correspondent")
    if str(orig_co or "") != str(new_co or "") and (orig_co or new_co):
        corrections.append(Correction(
            correction_type="correspondent",
            field="correspondent",
            original_value=str(orig_co) if orig_co else None,
            corrected_value=str(new_co) if new_co else None,
            **common,
        ))

    # Title change
    orig_title = original.get("title", "")
    new_title = updated.get("title", "")
    if orig_title and new_title and orig_title != new_title:
        corrections.append(Correction(
            correction_type="title",
            field="title",
            original_value=orig_title,
            corrected_value=new_title,
            **common,
        ))

    # Tag changes
    orig_tags = set(original.get("tags", []))
    new_tags = set(updated.get("tags", []))

    for tag in new_tags - orig_tags:
        corrections.append(Correction(
            correction_type="tag_added",
            field="tags",
            original_value=None,
            corrected_value=str(tag),
            **common,
        ))

    for tag in orig_tags - new_tags:
        corrections.append(Correction(
            correction_type="tag_removed",
            field="tags",
            original_value=str(tag),
            corrected_value=None,
            **common,
        ))

    if corrections:
        log.info("Found %d corrections for item %s", len(corrections), item_id)

    return corrections
