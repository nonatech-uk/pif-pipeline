"""Corrections table — stores proposed changes from Paperless feedback."""

from __future__ import annotations

import json
import logging
from datetime import datetime, UTC
from pathlib import Path

import aiosqlite
from pydantic import BaseModel, Field

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


_SCHEMA = """
CREATE TABLE IF NOT EXISTS corrections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    correction_type TEXT NOT NULL,
    field TEXT NOT NULL,
    original_value TEXT,
    corrected_value TEXT,
    item_id TEXT,
    label TEXT,
    tier_used TEXT,
    confidence REAL,
    proposed_action TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    accepted_at TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_corrections_status ON corrections(status);
"""


class CorrectionsTable:
    """SQLite-backed corrections table."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    async def _get_db(self) -> aiosqlite.Connection:
        db = await aiosqlite.connect(self._db_path)
        await db.executescript(_SCHEMA)
        return db

    async def add(self, correction: Correction) -> int:
        """Add a correction and generate a proposed action."""
        proposed = _generate_proposal(correction)

        db = await self._get_db()
        try:
            cursor = await db.execute(
                """INSERT INTO corrections
                   (correction_type, field, original_value, corrected_value,
                    item_id, label, tier_used, confidence,
                    proposed_action, status, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
                (
                    correction.correction_type,
                    correction.field,
                    correction.original_value,
                    correction.corrected_value,
                    correction.item_id,
                    correction.label,
                    correction.tier_used,
                    correction.confidence,
                    json.dumps(proposed.model_dump()) if proposed else None,
                    correction.timestamp.isoformat(),
                ),
            )
            await db.commit()
            row_id = cursor.lastrowid
            log.info("Correction added: #%d %s %s → %s",
                     row_id, correction.correction_type,
                     correction.original_value, correction.corrected_value)
            return row_id or 0
        finally:
            await db.close()

    async def list(self, status: str = "pending", limit: int = 50) -> list[CorrectionRecord]:
        """List corrections by status."""
        db = await self._get_db()
        try:
            cursor = await db.execute(
                """SELECT id, correction_type, field, original_value, corrected_value,
                          item_id, label, tier_used, confidence,
                          proposed_action, status, accepted_at, created_at
                   FROM corrections
                   WHERE status = ?
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (status, limit),
            )
            rows = await cursor.fetchall()
            return [_row_to_record(r) for r in rows]
        finally:
            await db.close()

    async def accept(self, correction_id: int) -> bool:
        """Accept a correction."""
        db = await self._get_db()
        try:
            cursor = await db.execute(
                """UPDATE corrections
                   SET status = 'accepted', accepted_at = ?
                   WHERE id = ? AND status = 'pending'""",
                (datetime.now(UTC).isoformat(), correction_id),
            )
            await db.commit()
            return (cursor.rowcount or 0) > 0
        finally:
            await db.close()

    async def reject(self, correction_id: int) -> bool:
        """Reject a correction."""
        db = await self._get_db()
        try:
            cursor = await db.execute(
                """UPDATE corrections
                   SET status = 'rejected'
                   WHERE id = ? AND status = 'pending'""",
                (correction_id,),
            )
            await db.commit()
            return (cursor.rowcount or 0) > 0
        finally:
            await db.close()

    async def count(self, status: str = "pending") -> int:
        db = await self._get_db()
        try:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM corrections WHERE status = ?", (status,)
            )
            row = await cursor.fetchone()
            return row[0] if row else 0
        finally:
            await db.close()


def _row_to_record(row: tuple) -> CorrectionRecord:
    proposed = None
    if row[9]:
        try:
            p = json.loads(row[9])
            proposed = ProposedAction(**p)
        except (json.JSONDecodeError, TypeError):
            pass

    return CorrectionRecord(
        id=row[0],
        correction=Correction(
            correction_type=row[1],
            field=row[2],
            original_value=row[3],
            corrected_value=row[4],
            item_id=row[5],
            label=row[6],
            tier_used=row[7],
            confidence=row[8],
            timestamp=datetime.fromisoformat(row[12]),
        ),
        proposed_action=proposed,
        status=row[10],
        accepted_at=datetime.fromisoformat(row[11]) if row[11] else None,
        created_at=datetime.fromisoformat(row[12]),
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
