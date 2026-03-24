"""GET /api/status — pipeline health snapshot."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter

from pipeline.api.deps import get_audit_log, get_corrections, get_exception_queue

router = APIRouter()


@router.get("/status")
async def pipeline_status():
    """Pipeline status: counts, last item, exception queue size."""
    audit = get_audit_log()
    queue = get_exception_queue()

    today = audit.count_today()
    last = audit.last_timestamp()
    exceptions_pending = await queue.count("pending")

    # Count auto-filed (not exception-queued) today
    entries = audit.recent(200)
    auto_filed_today = sum(
        1 for e in entries
        if e.timestamp.date() == date.today() and not e.exception_queued
    )
    corrections_pending = await get_corrections().count("pending")

    return {
        "processed_today": today,
        "auto_filed_today": auto_filed_today,
        "exceptions_pending": exceptions_pending,
        "corrections_pending": corrections_pending,
        "last_processed": last.isoformat() if last else None,
    }
