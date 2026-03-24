"""Exception queue API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pipeline.api.deps import get_exception_queue

router = APIRouter()


class TriageRequest(BaseModel):
    action: str  # file_as | retrigger | discard | snooze
    destination: str | None = None
    reason: str | None = None


@router.get("/exceptions")
async def list_exceptions(status: str = "pending", limit: int = 50):
    """List exception queue items."""
    queue = get_exception_queue()
    items = await queue.list(status=status, limit=limit)
    return {
        "items": [
            {
                "item_id": item.item_id,
                "reason": item.reason,
                "review_priority": item.review_priority,
                "classification": item.classification_output,
                "envelope": item.envelope_json,
                "created_at": item.created_at.isoformat(),
                "status": item.status,
            }
            for item in items
        ],
        "total": len(items),
    }


@router.get("/exceptions/{item_id}")
async def get_exception(item_id: str):
    """Get a single exception item."""
    queue = get_exception_queue()
    items = await queue.list(status="pending", limit=1000)
    for item in items:
        if item.item_id == item_id:
            return {
                "item_id": item.item_id,
                "reason": item.reason,
                "review_priority": item.review_priority,
                "classification": item.classification_output,
                "envelope": item.envelope_json,
                "created_at": item.created_at.isoformat(),
                "status": item.status,
            }
    raise HTTPException(404, "Exception not found")


@router.post("/exceptions/{item_id}/triage")
async def triage_exception(item_id: str, body: TriageRequest):
    """Triage an exception: file_as, retrigger, discard, or snooze."""
    queue = get_exception_queue()
    ok = await queue.triage(item_id, body.action, body.destination, body.reason)
    if not ok:
        raise HTTPException(404, "Exception not found or already triaged")
    return {"ok": True, "item_id": item_id, "action": body.action}
