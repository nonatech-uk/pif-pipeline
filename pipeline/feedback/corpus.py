"""Few-shot corpus management for improving extraction quality."""

from __future__ import annotations

import json
import logging
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import aiosqlite
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


class CorpusExample(BaseModel):
    """A few-shot example for extraction prompts."""

    id: int = 0
    item_id: str
    document_type: str
    extracted_fields: dict[str, Any] = Field(default_factory=dict)
    raw_text: str = ""
    confidence: float = 0.0
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


_SCHEMA = """
CREATE TABLE IF NOT EXISTS corpus (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id TEXT NOT NULL,
    document_type TEXT NOT NULL,
    extracted_fields TEXT NOT NULL DEFAULT '{}',
    raw_text TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL DEFAULT 0.0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_corpus_type ON corpus(document_type);
"""

MAX_CORPUS_SIZE = 200


class FewShotCorpus:
    """Manages few-shot examples for improving extraction."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    async def _get_db(self) -> aiosqlite.Connection:
        db = await aiosqlite.connect(self._db_path)
        await db.executescript(_SCHEMA)
        return db

    async def add_example(
        self,
        item_id: str,
        document_type: str,
        extracted_fields: dict[str, Any],
        raw_text: str = "",
        confidence: float = 0.0,
    ) -> int:
        """Add a few-shot example. Evicts oldest low-confidence if at capacity."""
        db = await self._get_db()
        try:
            # Check capacity
            cursor = await db.execute("SELECT COUNT(*) FROM corpus")
            row = await cursor.fetchone()
            count = row[0] if row else 0

            if count >= MAX_CORPUS_SIZE:
                # Evict lowest confidence example
                await db.execute(
                    "DELETE FROM corpus WHERE id = (SELECT id FROM corpus ORDER BY confidence ASC, created_at ASC LIMIT 1)"
                )

            cursor = await db.execute(
                """INSERT INTO corpus (item_id, document_type, extracted_fields, raw_text, confidence, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    item_id,
                    document_type,
                    json.dumps(extracted_fields),
                    raw_text,
                    confidence,
                    datetime.now(UTC).isoformat(),
                ),
            )
            await db.commit()
            return cursor.lastrowid or 0
        finally:
            await db.close()

    async def select_similar(self, document_type: str, n: int = 5) -> list[CorpusExample]:
        """Select the best examples for a document type.

        Simple approach: return highest-confidence examples of the same type.
        Could be upgraded to embedding-based similarity later.
        """
        db = await self._get_db()
        try:
            cursor = await db.execute(
                """SELECT id, item_id, document_type, extracted_fields, raw_text, confidence, created_at
                   FROM corpus
                   WHERE document_type = ?
                   ORDER BY confidence DESC
                   LIMIT ?""",
                (document_type, n),
            )
            rows = await cursor.fetchall()
            return [
                CorpusExample(
                    id=r[0],
                    item_id=r[1],
                    document_type=r[2],
                    extracted_fields=json.loads(r[3]),
                    raw_text=r[4],
                    confidence=r[5],
                    created_at=datetime.fromisoformat(r[6]),
                )
                for r in rows
            ]
        finally:
            await db.close()

    async def count(self, document_type: str | None = None) -> int:
        db = await self._get_db()
        try:
            if document_type:
                cursor = await db.execute(
                    "SELECT COUNT(*) FROM corpus WHERE document_type = ?", (document_type,)
                )
            else:
                cursor = await db.execute("SELECT COUNT(*) FROM corpus")
            row = await cursor.fetchone()
            return row[0] if row else 0
        finally:
            await db.close()
