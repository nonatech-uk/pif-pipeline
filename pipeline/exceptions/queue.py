"""SQLite-backed exception queue for items that couldn't be automatically processed."""

from __future__ import annotations

import json
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import aiosqlite
from pydantic import BaseModel, Field


class ExceptionItem(BaseModel):
    """An item in the exception queue."""

    item_id: str
    reason: str
    review_priority: int = 50  # 1 = urgent, 100 = low
    classification_output: dict[str, Any] = Field(default_factory=dict)
    envelope_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    status: str = "pending"  # pending | triaged | discarded
    triage_action: str | None = None
    triage_destination: str | None = None
    triage_reason: str | None = None
    triaged_at: datetime | None = None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS exceptions (
    item_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    review_priority INTEGER NOT NULL DEFAULT 50,
    classification_output TEXT NOT NULL DEFAULT '{}',
    envelope_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    triage_action TEXT,
    triage_destination TEXT,
    triage_reason TEXT,
    triaged_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_exceptions_status ON exceptions(status);
CREATE INDEX IF NOT EXISTS idx_exceptions_priority ON exceptions(review_priority);
"""


class ExceptionQueue:
    """SQLite-backed queue for pipeline exceptions."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    async def _get_db(self) -> aiosqlite.Connection:
        db = await aiosqlite.connect(self._db_path)
        await db.executescript(_SCHEMA)
        return db

    async def add(self, item: ExceptionItem) -> None:
        """Add an item to the exception queue."""
        db = await self._get_db()
        try:
            await db.execute(
                """INSERT OR REPLACE INTO exceptions
                   (item_id, reason, review_priority, classification_output,
                    envelope_json, created_at, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    item.item_id,
                    item.reason,
                    item.review_priority,
                    json.dumps(item.classification_output),
                    json.dumps(item.envelope_json),
                    item.created_at.isoformat(),
                    item.status,
                ),
            )
            await db.commit()
        finally:
            await db.close()

    async def list(
        self, status: str = "pending", limit: int = 50
    ) -> list[ExceptionItem]:
        """List exception items, ordered by priority (urgent first)."""
        db = await self._get_db()
        try:
            cursor = await db.execute(
                """SELECT item_id, reason, review_priority, classification_output,
                          envelope_json, created_at, status,
                          triage_action, triage_destination, triage_reason, triaged_at
                   FROM exceptions
                   WHERE status = ?
                   ORDER BY review_priority ASC, created_at DESC
                   LIMIT ?""",
                (status, limit),
            )
            rows = await cursor.fetchall()
            return [
                ExceptionItem(
                    item_id=r[0],
                    reason=r[1],
                    review_priority=r[2],
                    classification_output=json.loads(r[3]),
                    envelope_json=json.loads(r[4]),
                    created_at=datetime.fromisoformat(r[5]),
                    status=r[6],
                    triage_action=r[7],
                    triage_destination=r[8],
                    triage_reason=r[9],
                    triaged_at=datetime.fromisoformat(r[10]) if r[10] else None,
                )
                for r in rows
            ]
        finally:
            await db.close()

    async def triage(
        self,
        item_id: str,
        action: str,
        destination: str | None = None,
        reason: str | None = None,
    ) -> bool:
        """Triage an exception: file_as, retrigger, discard, or snooze."""
        db = await self._get_db()
        try:
            cursor = await db.execute(
                """UPDATE exceptions
                   SET status = 'triaged',
                       triage_action = ?,
                       triage_destination = ?,
                       triage_reason = ?,
                       triaged_at = ?
                   WHERE item_id = ? AND status = 'pending'""",
                (
                    action,
                    destination,
                    reason,
                    datetime.now(UTC).isoformat(),
                    item_id,
                ),
            )
            await db.commit()
            return cursor.rowcount > 0
        finally:
            await db.close()

    async def count(self, status: str = "pending") -> int:
        """Count items with a given status."""
        db = await self._get_db()
        try:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM exceptions WHERE status = ?", (status,)
            )
            row = await cursor.fetchone()
            return row[0] if row else 0
        finally:
            await db.close()
