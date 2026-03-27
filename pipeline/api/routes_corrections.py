"""Corrections API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pipeline.api.deps import get_audit_log, get_corrections
from pipeline.feedback.differ import Correction

router = APIRouter()


class ManualCorrectionRequest(BaseModel):
    item_id: str
    corrections: list[dict]  # [{field, original, corrected}]


@router.post("/corrections")
async def create_corrections(req: ManualCorrectionRequest):
    """Create manual corrections from the UI."""
    audit = get_audit_log()
    entry = await audit.get_decision_trace(req.item_id)
    label = entry.label if entry else None
    tier_used = entry.tier_used if entry else None
    confidence = entry.confidence if entry else None

    corrections_table = get_corrections()
    ids = []
    for c in req.corrections:
        correction = Correction(
            correction_type=c.get("field", "unknown"),
            field=c.get("field", ""),
            original_value=c.get("original"),
            corrected_value=c.get("corrected"),
            item_id=req.item_id,
            label=label,
            tier_used=tier_used,
            confidence=confidence,
        )
        row_id = await corrections_table.add(correction)
        await corrections_table.accept(row_id)
        ids.append(row_id)

    return {"ok": True, "ids": ids}


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
