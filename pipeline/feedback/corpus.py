"""Few-shot corpus management for improving extraction quality."""

from __future__ import annotations

import json
import logging
from datetime import datetime, UTC
from typing import Any

from pydantic import BaseModel, Field

from pipeline.db import get_pool

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


MAX_CORPUS_SIZE = 200


class FewShotCorpus:
    """Manages few-shot examples for improving extraction."""

    async def add_example(
        self,
        item_id: str,
        document_type: str,
        extracted_fields: dict[str, Any],
        raw_text: str = "",
        confidence: float = 0.0,
    ) -> int:
        """Add a few-shot example. Evicts oldest low-confidence if at capacity."""
        pool = get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM corpus")

            if count >= MAX_CORPUS_SIZE:
                await conn.execute(
                    "DELETE FROM corpus WHERE id = (SELECT id FROM corpus ORDER BY confidence ASC, created_at ASC LIMIT 1)"
                )

            row_id = await conn.fetchval(
                """INSERT INTO corpus (item_id, document_type, extracted_fields, raw_text, confidence, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   RETURNING id""",
                item_id,
                document_type,
                json.dumps(extracted_fields),
                raw_text,
                confidence,
                datetime.now(UTC),
            )
            return row_id or 0

    async def select_similar(self, document_type: str, n: int = 5) -> list[CorpusExample]:
        """Select the best examples for a document type."""
        pool = get_pool()
        rows = await pool.fetch(
            """SELECT id, item_id, document_type, extracted_fields, raw_text, confidence, created_at
               FROM corpus
               WHERE document_type = $1
               ORDER BY confidence DESC
               LIMIT $2""",
            document_type, n,
        )
        return [
            CorpusExample(
                id=r["id"],
                item_id=r["item_id"],
                document_type=r["document_type"],
                extracted_fields=json.loads(r["extracted_fields"]) if isinstance(r["extracted_fields"], str) else r["extracted_fields"],
                raw_text=r["raw_text"],
                confidence=r["confidence"],
                created_at=r["created_at"],
            )
            for r in rows
        ]

    async def count(self, document_type: str | None = None) -> int:
        pool = get_pool()
        if document_type:
            return await pool.fetchval(
                "SELECT COUNT(*) FROM corpus WHERE document_type = $1", document_type
            ) or 0
        return await pool.fetchval("SELECT COUNT(*) FROM corpus") or 0
