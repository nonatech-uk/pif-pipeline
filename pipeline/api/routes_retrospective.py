"""Retrospective runner API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pipeline.api.deps import get_retrospective_runner

router = APIRouter()


class StartRequest(BaseModel):
    mode: str = "classify"  # classify | suggest | commit
    tier_ceiling: str = "clip"
    sample_pct: float = 10.0
    date_from: str | None = None
    date_to: str | None = None
    sources: list[str] | None = None


@router.post("/retrospective/start")
async def start_retrospective(body: StartRequest):
    """Start a retrospective run."""
    runner = get_retrospective_runner()
    if not runner:
        raise HTTPException(503, "Retrospective runner not configured")

    run_id = await runner.start(
        mode=body.mode,
        tier_ceiling=body.tier_ceiling,
        sample_pct=body.sample_pct,
        date_from=body.date_from,
        date_to=body.date_to,
        sources=body.sources,
    )
    return {"ok": True, "run_id": run_id}


@router.get("/retrospective/{run_id}/status")
async def run_status(run_id: str):
    """Get status of a retrospective run."""
    runner = get_retrospective_runner()
    if not runner:
        raise HTTPException(503, "Retrospective runner not configured")

    state = runner.get_state(run_id)
    if not state:
        raise HTTPException(404, "Run not found")

    return {
        "run_id": state.run_id,
        "status": state.status,
        "mode": state.mode,
        "tier_ceiling": state.tier_ceiling,
        "sample_pct": state.sample_pct,
        "processed": state.processed,
        "total": state.total,
        "filed": state.filed,
        "exceptions": state.exceptions,
        "api_calls": state.api_calls,
        "started_at": state.started_at.isoformat() if state.started_at else None,
        "finished_at": state.finished_at.isoformat() if state.finished_at else None,
        "findings": state.findings.summary(),
        "error": state.error_message,
    }


@router.post("/retrospective/{run_id}/pause")
async def pause_run(run_id: str):
    """Pause a running retrospective."""
    runner = get_retrospective_runner()
    if not runner:
        raise HTTPException(503, "Retrospective runner not configured")
    if not runner.pause(run_id):
        raise HTTPException(404, "Run not found")
    return {"ok": True}


@router.get("/retrospective/history")
async def run_history():
    """List all retrospective runs."""
    runner = get_retrospective_runner()
    if not runner:
        return {"items": []}

    runs = runner.list_runs()
    return {
        "items": [
            {
                "run_id": r.run_id,
                "status": r.status,
                "mode": r.mode,
                "processed": r.processed,
                "misclassified": r.findings.misclassified_count,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            }
            for r in runs
        ],
    }
