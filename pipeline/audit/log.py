"""Append-only JSONL audit log with basic query methods."""

from __future__ import annotations

import json
from datetime import datetime, UTC, date
from pathlib import Path

import aiofiles

from pipeline.audit.models import AuditEntry


class AuditLog:
    """Append-only audit log backed by a JSONL file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    async def write(self, entry: AuditEntry) -> None:
        """Append an entry to the audit log."""
        line = entry.model_dump_json() + "\n"
        async with aiofiles.open(self._path, "a") as f:
            await f.write(line)

    def _iter_entries(self) -> list[AuditEntry]:
        """Read all entries (sync, for queries). Returns newest last."""
        if not self._path.exists():
            return []
        entries = []
        for line in self._path.read_text().splitlines():
            line = line.strip()
            if line:
                entries.append(AuditEntry.model_validate_json(line))
        return entries

    def count_today(self) -> int:
        """Count entries from today."""
        today = date.today()
        return sum(1 for e in self._iter_entries() if e.timestamp.date() == today)

    def count_by_date(self, target: date) -> int:
        """Count entries for a specific date."""
        return sum(1 for e in self._iter_entries() if e.timestamp.date() == target)

    def last_timestamp(self) -> datetime | None:
        """Return the timestamp of the most recent entry, or None."""
        entries = self._iter_entries()
        return entries[-1].timestamp if entries else None

    def get_decision_trace(self, item_id: str) -> AuditEntry | None:
        """Look up a specific item's audit entry by ID."""
        for entry in reversed(self._iter_entries()):
            if entry.item_id == item_id:
                return entry
        return None

    def get_by_sha256(self, sha256: str) -> AuditEntry | None:
        """Look up an audit entry by file SHA256 hash."""
        for entry in reversed(self._iter_entries()):
            if entry.file_sha256 == sha256:
                return entry
        return None

    def recent(self, limit: int = 50) -> list[AuditEntry]:
        """Return the most recent entries, newest first."""
        entries = self._iter_entries()
        return list(reversed(entries[-limit:]))
