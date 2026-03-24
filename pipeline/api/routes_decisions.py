"""Decisions (audit log) API routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from pipeline.api.deps import get_audit_log

router = APIRouter()


@router.get("/decisions")
async def list_decisions(source: str = "all", limit: int = 50):
    """List recent decisions from the audit log."""
    audit = get_audit_log()
    entries = await audit.recent(limit=limit)

    if source != "all":
        entries = [e for e in entries if e.source_type == source]

    return {
        "items": [
            {
                "item_id": e.item_id,
                "timestamp": e.timestamp.isoformat(),
                "source_type": e.source_type,
                "source_path": e.source_path,
                "file_sha256": e.file_sha256,
                "media_type": e.media_type,
                "label": e.label,
                "confidence": e.confidence,
                "tier_used": e.tier_used,
                "destinations": e.destinations,
                "exception_queued": e.exception_queued,
                "extracted": e.extracted,
            }
            for e in entries
        ],
        "total": len(entries),
    }


@router.get("/decisions/{item_id}")
async def get_decision(item_id: str):
    """Get full decision trace for an item."""
    audit = get_audit_log()
    entry = await audit.get_decision_trace(item_id)
    if not entry:
        raise HTTPException(404, "Decision not found")

    return {
        "item_id": entry.item_id,
        "timestamp": entry.timestamp.isoformat(),
        "source_type": entry.source_type,
        "source_path": entry.source_path,
        "file_sha256": entry.file_sha256,
        "media_type": entry.media_type,
        "label": entry.label,
        "confidence": entry.confidence,
        "tier_used": entry.tier_used,
        "destinations": entry.destinations,
        "exception_queued": entry.exception_queued,
        "extracted": entry.extracted,
        "trace": {
            "tiers": [
                {
                    "tier": t.tier,
                    "label": t.label,
                    "confidence": t.confidence,
                    "all_labels": t.all_labels,
                    "skipped": t.skipped,
                    "skip_reason": t.skip_reason,
                    "duration_ms": t.duration_ms,
                }
                for t in entry.trace.tiers
            ],
            "rules": [
                {
                    "rule_id": r.rule_id,
                    "rule_name": r.rule_name,
                    "matched": r.matched,
                    "conditions_met": r.conditions_met,
                    "conditions_failed": r.conditions_failed,
                    "on_match": r.on_match,
                }
                for r in entry.trace.rules
            ],
            "actions": [
                {
                    "handler": a.handler,
                    "destination": a.destination,
                    "ok": a.ok,
                    "ref": a.ref,
                    "reason": a.reason,
                    "duration_ms": a.duration_ms,
                }
                for a in entry.trace.actions
            ],
        },
    }
