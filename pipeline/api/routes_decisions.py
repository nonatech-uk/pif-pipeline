"""Decisions (audit log) API routes."""

from __future__ import annotations

import asyncio
import imaplib
import json
import logging
import re
from collections import Counter
from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from pipeline.api.deps import get_audit_log, get_settings
from pipeline.db import get_pool

log = logging.getLogger(__name__)
router = APIRouter()


class FeedbackRequest(BaseModel):
    feedback: int  # 1 or -1
    note: str | None = None


@router.get("/decisions")
async def list_decisions(
    source: str = "all",
    label: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    hide_ignored: bool = False,
    archived: bool | None = None,
    feedback: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """List decisions from the audit log with optional filters.

    feedback: 'positive', 'negative', 'unreviewed', or None for all.
    """
    audit = get_audit_log()
    entries, total = await audit.search(
        source=source if source != "all" else None,
        label=label,
        date_from=date_from,
        date_to=date_to,
        hide_ignored=hide_ignored,
        archived=archived,
        feedback=feedback,
        limit=limit,
        offset=offset,
    )

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
                "feedback": e.feedback,
                "feedback_note": e.feedback_note,
            }
            for e in entries
        ],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/decisions/labels")
async def list_labels():
    """Get distinct labels from the audit log."""
    audit = get_audit_log()
    pool = __import__("pipeline.db", fromlist=["get_pool"]).get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT label FROM audit_log WHERE label IS NOT NULL ORDER BY label"
    )
    return {"labels": [r["label"] for r in rows]}


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
        "feedback": entry.feedback,
        "feedback_note": entry.feedback_note,
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


@router.post("/decisions/{item_id}/feedback")
async def submit_feedback(item_id: str, body: FeedbackRequest):
    """Set user feedback on a decision (1=good, -1=bad)."""
    if body.feedback not in (1, -1):
        raise HTTPException(400, "feedback must be 1 or -1")
    audit = get_audit_log()
    ok = await audit.set_feedback(item_id, body.feedback, body.note)
    if not ok:
        raise HTTPException(404, "Decision not found")
    return {"ok": True}


@router.post("/decisions/archive")
async def archive_decisions() -> dict[str, Any]:
    """Archive all unarchived decisions, move Pipelined emails to Archive, learn from dispositions."""
    audit = get_audit_log()
    items = await audit.archive_all()

    if not items:
        return {"archived_count": 0, "emails": {}, "suggestions": []}

    # Separate email items
    email_items = []
    for item in items:
        if item["source_type"] != "email":
            continue
        sp = item.get("source_path") or ""
        if not sp.startswith("email://"):
            continue
        message_id = sp.removeprefix("email://").rsplit("/", 1)[0]
        if not message_id:
            continue
        extracted = item.get("extracted")
        if isinstance(extracted, str):
            extracted = json.loads(extracted)
        sender = (extracted or {}).get("_email_from", "")
        feedback = item.get("feedback")
        email_items.append({"item_id": item["item_id"], "message_id": message_id, "sender": sender, "feedback": feedback})

    # Check IMAP dispositions and move remaining to Archive/Trash based on feedback
    email_summary = {"moved_to_archive": 0, "already_moved": [], "deleted": 0, "trashed": 0, "gone": 0}
    suggestions: list[dict] = []

    if email_items:
        dispositions = await _check_and_archive_emails(email_items)
        deleted_senders: list[str] = []

        for d in dispositions:
            if d["disposition"] == "archived":
                email_summary["moved_to_archive"] += 1
            elif d["disposition"] == "deleted":
                email_summary["trashed"] += 1
                if d.get("sender"):
                    deleted_senders.append(d["sender"])
            elif d["disposition"] == "gone":
                email_summary["gone"] += 1
            elif d["disposition"].startswith("moved:"):
                folder = d["disposition"].removeprefix("moved:")
                email_summary["already_moved"].append({"item_id": d["item_id"], "folder": folder})

        # Suggest ignore rules for senders with 2+ deleted emails
        sender_counts = Counter(s for s in deleted_senders if s)
        pool = get_pool()
        for sender, count in sender_counts.items():
            if count >= 2:
                # Extract just the email address
                match = re.search(r"<([^>]+)>", sender)
                address = match.group(1).lower() if match else sender.strip().lower()
                # Check not already ignored
                existing = await pool.fetchval(
                    "SELECT 1 FROM email_ignore_senders WHERE address = $1", address
                )
                if not existing:
                    await pool.execute(
                        """INSERT INTO corrections (correction_type, field, original_value, corrected_value, proposed_action)
                           VALUES ($1, $2, $3, $4, $5)""",
                        "sender_ignored",
                        "source_email_from",
                        sender,
                        "ignore",
                        json.dumps({
                            "action_type": "add_ignore_sender",
                            "description": f"Add {address} to ignore list ({count} emails deleted from Pipelined)",
                        }),
                    )
                    suggestions.append({"sender": address, "count": count})

    return {
        "archived_count": len(items),
        "emails": email_summary,
        "suggestions": suggestions,
    }


async def _check_and_archive_emails(
    email_items: list[dict],
) -> list[dict]:
    """Check where emails are now and move remaining Pipelined ones to Archive."""
    settings = get_settings()
    if not settings.services.imap_user or not settings.services.imap_password:
        return []

    def _do_check() -> list[dict]:
        conn = imaplib.IMAP4_SSL(settings.services.imap_host, settings.services.imap_port)
        results = []
        try:
            conn.login(settings.services.imap_user, settings.services.imap_password)

            # List all folders for searching
            _, folder_data = conn.list()
            all_folders = []
            for line in (folder_data or []):
                if isinstance(line, bytes):
                    # Parse IMAP LIST response: (flags) "delimiter" "name"
                    match = re.search(rb'"[^"]*"\s+"?([^"]+)"?$', line)
                    if match:
                        all_folders.append(match.group(1).decode("utf-8"))
            # Filter out system folders we don't need to search
            skip_folders = {"Sent", "Drafts", "Junk", "Outbox"}
            search_folders = [f for f in all_folders if f not in skip_folders]

            # Ensure Archive and Trash folders exist
            for folder in ("Archive", "Trash"):
                try:
                    conn.select(folder)
                    conn.close()
                except imaplib.IMAP4.error:
                    conn.create(folder)
                    conn.subscribe(folder)
                    log.info("Created IMAP folder: %s", folder)

            for item in email_items:
                mid = item["message_id"]
                item_id = item["item_id"]
                sender = item.get("sender", "")
                fb = item.get("feedback")
                # Negative feedback → Trash, otherwise → Archive
                target_folder = "Trash" if fb == -1 else "Archive"
                disposition = "gone"  # default if not found anywhere

                # Check Pipelined first
                try:
                    conn.select("Pipelined")
                    _, data = conn.uid("SEARCH", None, "HEADER", "Message-ID", mid)
                    if data and data[0]:
                        uids = data[0].split()
                        for uid in uids:
                            typ, _ = conn.uid("MOVE", uid, target_folder)
                            if typ != "OK":
                                conn.uid("COPY", uid, target_folder)
                                conn.uid("STORE", uid, "+FLAGS", "\\Deleted")
                        conn.expunge()
                        disp = "deleted" if fb == -1 else "archived"
                        results.append({"item_id": item_id, "disposition": disp, "sender": sender})
                        continue
                except imaplib.IMAP4.error:
                    pass

                # Not in Pipelined — search other folders
                for folder in search_folders:
                    if folder == "Pipelined":
                        continue
                    try:
                        conn.select(folder)
                        _, data = conn.uid("SEARCH", None, "HEADER", "Message-ID", mid)
                        if data and data[0]:
                            if folder in ("Trash", "Deleted Items", "Deleted Messages"):
                                disposition = "deleted"
                            else:
                                disposition = f"moved:{folder}"
                            break
                    except imaplib.IMAP4.error:
                        continue

                results.append({"item_id": item_id, "disposition": disposition, "sender": sender})

        except Exception:
            log.exception("IMAP archive check failed")
        finally:
            try:
                conn.logout()
            except Exception:
                pass

        return results

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _do_check)
