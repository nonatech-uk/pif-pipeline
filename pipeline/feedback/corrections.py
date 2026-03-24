"""Corrections table — stores proposed changes from Paperless feedback."""

from __future__ import annotations

import json
import logging
from datetime import datetime, UTC

from pydantic import BaseModel, Field

from pipeline.db import get_pool
from pipeline.feedback.differ import Correction

log = logging.getLogger(__name__)


class ProposedAction(BaseModel):
    """A proposed action based on a correction."""

    description: str
    action_type: str  # lower_threshold | add_corpus_example | add_rule_condition | rename_tag


class CorrectionRecord(BaseModel):
    """A correction with its proposed action and status."""

    id: int
    correction: Correction
    proposed_action: ProposedAction | None = None
    status: str = "pending"  # pending | accepted | rejected
    accepted_at: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CorrectionsTable:
    """PostgreSQL-backed corrections table."""

    async def add(self, correction: Correction) -> int:
        """Add a correction and generate a proposed action."""
        proposed = _generate_proposal(correction)
        pool = get_pool()

        row_id = await pool.fetchval(
            """INSERT INTO corrections
               (correction_type, field, original_value, corrected_value,
                item_id, label, tier_used, confidence,
                proposed_action, status, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10)
               RETURNING id""",
            correction.correction_type,
            correction.field,
            correction.original_value,
            correction.corrected_value,
            correction.item_id,
            correction.label,
            correction.tier_used,
            correction.confidence,
            json.dumps(proposed.model_dump()) if proposed else None,
            correction.timestamp,
        )
        log.info("Correction added: #%d %s %s → %s",
                 row_id, correction.correction_type,
                 correction.original_value, correction.corrected_value)
        return row_id or 0

    async def list(self, status: str = "pending", limit: int = 50) -> list[CorrectionRecord]:
        """List corrections by status."""
        pool = get_pool()
        rows = await pool.fetch(
            """SELECT id, correction_type, field, original_value, corrected_value,
                      item_id, label, tier_used, confidence,
                      proposed_action, status, accepted_at, created_at
               FROM corrections
               WHERE status = $1
               ORDER BY created_at DESC
               LIMIT $2""",
            status, limit,
        )
        return [_row_to_record(r) for r in rows]

    async def accept(self, correction_id: int) -> bool:
        """Accept a correction."""
        pool = get_pool()
        result = await pool.execute(
            """UPDATE corrections
               SET status = 'accepted', accepted_at = $1
               WHERE id = $2 AND status = 'pending'""",
            datetime.now(UTC), correction_id,
        )
        return result.split()[-1] != "0"

    async def reject(self, correction_id: int) -> bool:
        """Reject a correction."""
        pool = get_pool()
        result = await pool.execute(
            """UPDATE corrections
               SET status = 'rejected'
               WHERE id = $1 AND status = 'pending'""",
            correction_id,
        )
        return result.split()[-1] != "0"

    async def count(self, status: str = "pending") -> int:
        pool = get_pool()
        row = await pool.fetchval(
            "SELECT COUNT(*) FROM corrections WHERE status = $1", status
        )
        return row or 0


def _row_to_record(row) -> CorrectionRecord:
    proposed = None
    pa = row["proposed_action"]
    if pa:
        try:
            p = json.loads(pa) if isinstance(pa, str) else pa
            proposed = ProposedAction(**p)
        except (json.JSONDecodeError, TypeError):
            pass

    return CorrectionRecord(
        id=row["id"],
        correction=Correction(
            correction_type=row["correction_type"],
            field=row["field"],
            original_value=row["original_value"],
            corrected_value=row["corrected_value"],
            item_id=row["item_id"],
            label=row["label"],
            tier_used=row["tier_used"],
            confidence=row["confidence"],
            timestamp=row["created_at"],
        ),
        proposed_action=proposed,
        status=row["status"],
        accepted_at=row["accepted_at"],
        created_at=row["created_at"],
    )


def _generate_proposal(correction: Correction) -> ProposedAction | None:
    """Generate a proposed action for a correction."""
    ct = correction.correction_type

    if ct == "document_type":
        return ProposedAction(
            description=f"Lower CLIP/Claude threshold for '{correction.corrected_value}' — was misclassified as '{correction.original_value}'",
            action_type="lower_threshold",
        )

    if ct == "correspondent":
        if correction.original_value and correction.corrected_value:
            return ProposedAction(
                description=f"Map correspondent '{correction.original_value}' → '{correction.corrected_value}' for future documents from this vendor",
                action_type="add_corpus_example",
            )

    if ct == "tag_added":
        return ProposedAction(
            description=f"Add tag '{correction.corrected_value}' to extraction hints for label '{correction.label}'",
            action_type="add_rule_condition",
        )

    if ct == "tag_removed":
        return ProposedAction(
            description=f"Stop suggesting tag '{correction.original_value}' for label '{correction.label}'",
            action_type="rename_tag",
        )

    if ct == "title":
        return ProposedAction(
            description=f"Adjust title template — user prefers '{correction.corrected_value}' over '{correction.original_value}'",
            action_type="add_corpus_example",
        )

    return None
