"""Corrections API routes — placeholder for Phase 7."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/corrections")
async def list_corrections(status: str = "pending"):
    """List pending corrections. Placeholder — implemented in Phase 7."""
    return {"items": [], "total": 0}
