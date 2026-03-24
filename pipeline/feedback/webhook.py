"""Paperless webhook receiver — catches document edits and generates corrections."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request

from pipeline.audit.log import AuditLog
from pipeline.feedback.corrections import CorrectionsTable
from pipeline.feedback.differ import diff_metadata

log = logging.getLogger(__name__)

router = APIRouter()

# Set during app startup
_audit_log: AuditLog | None = None
_corrections: CorrectionsTable | None = None
_paperless_url: str = ""
_paperless_token: str = ""
_webhook_secret: str = ""


def configure(
    audit_log: AuditLog,
    corrections: CorrectionsTable,
    paperless_url: str,
    paperless_token: str,
    webhook_secret: str,
) -> None:
    """Configure the webhook handler with shared instances."""
    global _audit_log, _corrections, _paperless_url, _paperless_token, _webhook_secret
    _audit_log = audit_log
    _corrections = corrections
    _paperless_url = paperless_url
    _paperless_token = paperless_token
    _webhook_secret = webhook_secret


@router.post("/webhook/paperless")
async def paperless_webhook(
    request: Request,
    x_paperless_secret: str | None = Header(None, alias="X-Paperless-Secret"),
):
    """Receive Paperless document_updated webhook."""
    # Validate secret if configured
    if _webhook_secret and x_paperless_secret != _webhook_secret:
        raise HTTPException(403, "Invalid webhook secret")

    payload = await request.json()
    event_type = payload.get("type", "")
    doc_id = payload.get("document_id") or payload.get("id")

    if event_type not in ("document_updated", "document.updated") and "document_id" not in payload:
        log.debug("Ignoring Paperless webhook event: %s", event_type)
        return {"ok": True, "ignored": True}

    if not doc_id:
        log.warning("Paperless webhook missing document_id")
        return {"ok": True, "ignored": True}

    log.info("Paperless webhook: %s doc_id=%s", event_type, doc_id)

    if not _audit_log or not _corrections:
        log.warning("Feedback not configured — ignoring webhook")
        return {"ok": True, "ignored": True}

    # Fetch current document state from Paperless
    updated = await _fetch_document(doc_id)
    if not updated:
        return {"ok": True, "error": "Could not fetch document"}

    # Find original classification in audit log by Paperless ref
    original_entry = _find_audit_entry(str(doc_id), updated.get("title", ""))
    if not original_entry:
        log.info("No audit entry found for Paperless doc %s — may not be pipeline-originated", doc_id)
        return {"ok": True, "ignored": True, "reason": "No matching audit entry"}

    # Build original metadata from what the pipeline sent
    original_meta = _build_original_meta(original_entry)

    # Diff
    corrections = diff_metadata(
        original=original_meta,
        updated=updated,
        item_id=original_entry.item_id,
        label=original_entry.label,
        tier_used=original_entry.tier_used,
        confidence=original_entry.confidence,
    )

    # Store corrections
    for correction in corrections:
        await _corrections.add(correction)

    return {
        "ok": True,
        "corrections_found": len(corrections),
        "item_id": original_entry.item_id,
    }


async def _fetch_document(doc_id: int | str) -> dict[str, Any] | None:
    """Fetch document metadata from Paperless API."""
    if not _paperless_url or not _paperless_token:
        return None

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{_paperless_url}/api/documents/{doc_id}/",
                headers={"Authorization": f"Token {_paperless_token}"},
            )
            if resp.status_code != 200:
                log.error("Failed to fetch Paperless doc %s: HTTP %d", doc_id, resp.status_code)
                return None

            data = resp.json()

            # Resolve IDs to names for diffing
            result: dict[str, Any] = {
                "title": data.get("title", ""),
                "document_type": await _resolve_name(client, "document_types", data.get("document_type")),
                "correspondent": await _resolve_name(client, "correspondents", data.get("correspondent")),
                "tags": [],
            }
            for tag_id in data.get("tags", []):
                tag_name = await _resolve_name(client, "tags", tag_id)
                if tag_name:
                    result["tags"].append(tag_name)

            return result
    except httpx.HTTPError:
        log.exception("Error fetching Paperless document")
        return None


async def _resolve_name(client: httpx.AsyncClient, endpoint: str, pk: int | None) -> str | None:
    """Resolve a Paperless PK to its name."""
    if pk is None:
        return None
    try:
        resp = await client.get(
            f"{_paperless_url}/api/{endpoint}/{pk}/",
            headers={"Authorization": f"Token {_paperless_token}"},
        )
        if resp.status_code == 200:
            return resp.json().get("name")
    except httpx.HTTPError:
        pass
    return str(pk)


def _find_audit_entry(paperless_ref: str, title: str) -> Any:
    """Find the audit entry that produced this Paperless document."""
    if not _audit_log:
        return None

    entries = _audit_log.recent(500)
    for entry in entries:
        # Match by Paperless task ref in action traces
        for action in entry.trace.actions:
            if action.handler == "paperless" and action.ok and action.ref:
                # Store doc_id mapping if we ever get it
                pass

        # Match by vendor/merchant name in title (either direction)
        if entry.extracted:
            title_lower = title.lower()
            for field in ("vendor", "merchant", "_correspondent"):
                name = (entry.extracted.get(field) or "").lower()
                if name and (name in title_lower or title_lower in name
                             or _words_overlap(name, title_lower) >= 0.6):
                    return entry

        # Match by SHA256 if Paperless stores original_filename matching ours
        if entry.file_sha256:
            # Check all recent entries — the most recent with matching vendor is most likely
            pass

    # Fallback: match by date in title and label type
    for entry in entries:
        if entry.label and entry.label in ("invoice", "receipt") and entry.extracted:
            extracted_date = entry.extracted.get("date", "")
            if extracted_date:
                compact = extracted_date.replace("-", "")
                if compact in title.replace("-", ""):
                    return entry

    return None


def _build_original_meta(entry: Any) -> dict[str, Any]:
    """Build the original metadata dict from an audit entry."""
    extracted = entry.extracted or {}
    meta: dict[str, Any] = {
        "document_type": entry.label,  # What the pipeline classified it as
        "correspondent": extracted.get("_correspondent"),
        "tags": extracted.get("_tags", []),
    }

    # Build the title the pipeline would have generated
    if entry.label == "invoice":
        date_str = (extracted.get("date") or "").replace("-", "")
        vendor = extracted.get("vendor", "")
        meta["title"] = f"{date_str}-{vendor}" if date_str else vendor
    elif entry.label == "receipt":
        date_str = (extracted.get("date") or "").replace("-", "")
        merchant = extracted.get("merchant", "")
        meta["title"] = f"{date_str}-{merchant}" if date_str else merchant

    return meta


def _words_overlap(a: str, b: str) -> float:
    """Fraction of words in a that also appear in b."""
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a:
        return 0.0
    return len(words_a & words_b) / len(words_a)
