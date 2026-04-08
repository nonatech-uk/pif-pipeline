"""PostgreSQL-backed audit log with query methods."""

from __future__ import annotations

import json
from datetime import datetime, date

from pipeline.audit.models import AuditEntry, DecisionTrace
from pipeline.db import get_pool


class AuditLog:
    """Audit log backed by PostgreSQL."""

    async def write(self, entry: AuditEntry) -> None:
        """Write an entry to the audit log."""
        pool = get_pool()
        await pool.execute(
            """INSERT INTO audit_log
               (item_id, timestamp, source_type, source_path, file_sha256,
                media_type, label, confidence, tier_used,
                destinations, exception_queued, trace, extracted)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
            entry.item_id,
            entry.timestamp,
            entry.source_type,
            entry.source_path,
            entry.file_sha256,
            entry.media_type,
            entry.label,
            entry.confidence,
            entry.tier_used,
            entry.destinations,
            entry.exception_queued,
            entry.trace.model_dump_json(),
            json.dumps(entry.extracted),
        )

    async def count_today(self) -> int:
        """Count entries from today."""
        pool = get_pool()
        return await pool.fetchval(
            "SELECT COUNT(*) FROM audit_log WHERE timestamp::date = CURRENT_DATE"
        ) or 0

    async def count_by_date(self, target: date) -> int:
        """Count entries for a specific date."""
        pool = get_pool()
        return await pool.fetchval(
            "SELECT COUNT(*) FROM audit_log WHERE timestamp::date = $1", target
        ) or 0

    async def last_timestamp(self) -> datetime | None:
        """Return the timestamp of the most recent entry, or None."""
        pool = get_pool()
        return await pool.fetchval(
            "SELECT timestamp FROM audit_log ORDER BY timestamp DESC LIMIT 1"
        )

    async def get_decision_trace(self, item_id: str) -> AuditEntry | None:
        """Look up a specific item's audit entry by ID."""
        pool = get_pool()
        row = await pool.fetchrow(
            """SELECT item_id, timestamp, source_type, source_path, file_sha256,
                      media_type, label, confidence, tier_used,
                      destinations, exception_queued, trace, extracted
               FROM audit_log
               WHERE item_id = $1
               ORDER BY timestamp DESC LIMIT 1""",
            item_id,
        )
        return _row_to_entry(row) if row else None

    async def get_by_sha256(self, sha256: str) -> AuditEntry | None:
        """Look up an audit entry by file SHA256 hash."""
        pool = get_pool()
        row = await pool.fetchrow(
            """SELECT item_id, timestamp, source_type, source_path, file_sha256,
                      media_type, label, confidence, tier_used,
                      destinations, exception_queued, trace, extracted
               FROM audit_log
               WHERE file_sha256 = $1
               ORDER BY timestamp DESC LIMIT 1""",
            sha256,
        )
        return _row_to_entry(row) if row else None

    async def recent(self, limit: int = 50) -> list[AuditEntry]:
        """Return the most recent entries, newest first."""
        pool = get_pool()
        rows = await pool.fetch(
            """SELECT item_id, timestamp, source_type, source_path, file_sha256,
                      media_type, label, confidence, tier_used,
                      destinations, exception_queued, trace, extracted
               FROM audit_log
               WHERE exception_queued = FALSE
               ORDER BY timestamp DESC
               LIMIT $1""",
            limit,
        )
        return [_row_to_entry(r) for r in rows]

    async def search(
        self,
        source: str | None = None,
        label: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        hide_ignored: bool = False,
        archived: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[AuditEntry], int]:
        """Search audit log with filters. Returns (entries, total_count).

        archived: None=all, False=unarchived only, True=archived only.
        """
        pool = get_pool()

        conditions = []
        params = []
        idx = 1

        if source:
            conditions.append(f"source_type = ${idx}")
            params.append(source)
            idx += 1
        if label:
            conditions.append(f"label = ${idx}")
            params.append(label)
            idx += 1
        if date_from:
            conditions.append(f"timestamp::date >= ${idx}")
            params.append(date_from)
            idx += 1
        if date_to:
            conditions.append(f"timestamp::date <= ${idx}")
            params.append(date_to)
            idx += 1
        if hide_ignored:
            conditions.append("NOT ('ignored' = ANY(destinations))")
        if archived is False:
            conditions.append("archived_at IS NULL")
        elif archived is True:
            conditions.append("archived_at IS NOT NULL")

        conditions.append("exception_queued = FALSE")
        where = f"WHERE {' AND '.join(conditions)}"

        total = await pool.fetchval(
            f"SELECT COUNT(*) FROM audit_log {where}", *params
        ) or 0

        rows = await pool.fetch(
            f"""SELECT item_id, timestamp, source_type, source_path, file_sha256,
                       media_type, label, confidence, tier_used,
                       destinations, exception_queued, trace, extracted
                FROM audit_log {where}
                ORDER BY timestamp DESC
                LIMIT ${idx} OFFSET ${idx + 1}""",
            *params, limit, offset,
        )

        return [_row_to_entry(r) for r in rows], total

    async def archive_all(self) -> list[dict]:
        """Archive all non-archived entries. Returns list of archived items."""
        pool = get_pool()
        rows = await pool.fetch(
            """UPDATE audit_log
               SET archived_at = now()
               WHERE archived_at IS NULL
               RETURNING item_id, source_type, source_path, extracted""",
        )
        return [dict(r) for r in rows]


def _row_to_entry(row) -> AuditEntry:
    trace_data = row["trace"]
    if isinstance(trace_data, str):
        trace_data = json.loads(trace_data)
    extracted = row["extracted"]
    if isinstance(extracted, str):
        extracted = json.loads(extracted)

    return AuditEntry(
        item_id=row["item_id"],
        timestamp=row["timestamp"],
        source_type=row["source_type"],
        source_path=row["source_path"],
        file_sha256=row["file_sha256"],
        media_type=row["media_type"],
        label=row["label"],
        confidence=row["confidence"],
        tier_used=row["tier_used"],
        destinations=list(row["destinations"]),
        exception_queued=row["exception_queued"],
        trace=DecisionTrace.model_validate(trace_data),
        extracted=extracted,
    )
