"""PostgreSQL-backed exception queue for items that couldn't be automatically processed."""

from __future__ import annotations

import json
from datetime import datetime, UTC
from typing import Any

from pydantic import BaseModel, Field

from pipeline.db import get_pool


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


class ExceptionQueue:
    """PostgreSQL-backed queue for pipeline exceptions."""

    async def add(self, item: ExceptionItem) -> None:
        """Add an item to the exception queue."""
        pool = get_pool()
        await pool.execute(
            """INSERT INTO exceptions
               (item_id, reason, review_priority, classification_output,
                envelope_json, created_at, status)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (item_id) DO UPDATE SET
                 reason = EXCLUDED.reason,
                 review_priority = EXCLUDED.review_priority,
                 classification_output = EXCLUDED.classification_output,
                 envelope_json = EXCLUDED.envelope_json,
                 created_at = EXCLUDED.created_at,
                 status = EXCLUDED.status""",
            item.item_id,
            item.reason,
            item.review_priority,
            json.dumps(item.classification_output),
            json.dumps(item.envelope_json),
            item.created_at,
            item.status,
        )

    async def list(
        self, status: str = "pending", limit: int = 50
    ) -> list[ExceptionItem]:
        """List exception items, ordered by priority (urgent first)."""
        pool = get_pool()
        rows = await pool.fetch(
            """SELECT item_id, reason, review_priority, classification_output,
                      envelope_json, created_at, status,
                      triage_action, triage_destination, triage_reason, triaged_at
               FROM exceptions
               WHERE status = $1
               ORDER BY review_priority ASC, created_at DESC
               LIMIT $2""",
            status, limit,
        )
        return [
            ExceptionItem(
                item_id=r["item_id"],
                reason=r["reason"],
                review_priority=r["review_priority"],
                classification_output=json.loads(r["classification_output"]) if isinstance(r["classification_output"], str) else r["classification_output"],
                envelope_json=json.loads(r["envelope_json"]) if isinstance(r["envelope_json"], str) else r["envelope_json"],
                created_at=r["created_at"],
                status=r["status"],
                triage_action=r["triage_action"],
                triage_destination=r["triage_destination"],
                triage_reason=r["triage_reason"],
                triaged_at=r["triaged_at"],
            )
            for r in rows
        ]

    async def triage(
        self,
        item_id: str,
        action: str,
        destination: str | None = None,
        reason: str | None = None,
    ) -> bool:
        """Triage an exception: file_as, retrigger, discard, or snooze."""
        pool = get_pool()
        result = await pool.execute(
            """UPDATE exceptions
               SET status = 'triaged',
                   triage_action = $1,
                   triage_destination = $2,
                   triage_reason = $3,
                   triaged_at = $4
               WHERE item_id = $5 AND status = 'pending'""",
            action, destination, reason, datetime.now(UTC), item_id,
        )
        return result.split()[-1] != "0"

    async def count(self, status: str = "pending") -> int:
        """Count items with a given status."""
        pool = get_pool()
        row = await pool.fetchval(
            "SELECT COUNT(*) FROM exceptions WHERE status = $1", status
        )
        return row or 0
