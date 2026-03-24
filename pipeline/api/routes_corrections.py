"""Corrections API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from pipeline.api.deps import get_corrections

router = APIRouter()


@router.get("/corrections")
async def list_corrections(status: str = "pending", limit: int = 50):
    """List corrections from Paperless feedback."""
    corrections = get_corrections()
    items = await corrections.list(status=status, limit=limit)
    return {
        "items": [
            {
                "id": item.id,
                "correction_type": item.correction.correction_type,
                "field": item.correction.field,
                "original_value": item.correction.original_value,
                "corrected_value": item.correction.corrected_value,
                "item_id": item.correction.item_id,
                "label": item.correction.label,
                "tier_used": item.correction.tier_used,
                "confidence": item.correction.confidence,
                "proposed_action": item.proposed_action.model_dump() if item.proposed_action else None,
                "status": item.status,
                "created_at": item.created_at.isoformat(),
            }
            for item in items
        ],
        "total": len(items),
    }


@router.post("/corrections/{correction_id}/accept")
async def accept_correction(correction_id: int):
    """Accept a proposed correction."""
    corrections = get_corrections()
    ok = await corrections.accept(correction_id)
    if not ok:
        raise HTTPException(404, "Correction not found or already processed")
    return {"ok": True, "id": correction_id}


@router.post("/corrections/{correction_id}/reject")
async def reject_correction(correction_id: int):
    """Reject a proposed correction."""
    corrections = get_corrections()
    ok = await corrections.reject(correction_id)
    if not ok:
        raise HTTPException(404, "Correction not found or already processed")
    return {"ok": True, "id": correction_id}
