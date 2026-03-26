"""Document preview API — proxies original content from Paperless, Immich, or extracted text."""

from __future__ import annotations

import json
import logging

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from pipeline.api.deps import get_audit_log, get_exception_queue, get_settings

log = logging.getLogger(__name__)

router = APIRouter()


async def _resolve_item(item_id: str, context: str) -> dict:
    """Look up item metadata from audit_log or exceptions table."""
    if context == "exception":
        eq = get_exception_queue()
        item = await eq.get(item_id)
        if item:
            env = item.get("envelope_json", {})
            return {
                "source_type": env.get("source_type"),
                "source_path": env.get("source_path"),
                "media_type": env.get("media_type"),
                "extracted": env.get("extracted", {}),
                "trace_actions": [],
            }

    audit = get_audit_log()
    entry = await audit.get_decision_trace(item_id)
    if not entry:
        return {}

    actions = []
    if entry.trace and entry.trace.actions:
        actions = [
            {"handler": a.handler, "ok": a.ok, "ref": a.ref}
            for a in entry.trace.actions
        ]

    return {
        "source_type": entry.source_type,
        "source_path": entry.source_path,
        "media_type": entry.media_type,
        "extracted": entry.extracted or {},
        "trace_actions": actions,
    }


def _find_action_ref(actions: list[dict], handler: str) -> str | None:
    """Find a successful action ref by handler name."""
    for a in actions:
        if a.get("handler") == handler and a.get("ok") and a.get("ref"):
            return a["ref"]
    return None


@router.get("/preview/{item_id}")
async def preview(
    item_id: str,
    size: str = Query("thumbnail", pattern="^(thumbnail|full)$"),
    context: str = Query("audit", pattern="^(audit|exception)$"),
):
    """Proxy document preview from the appropriate source."""
    meta = await _resolve_item(item_id, context)
    if not meta:
        raise HTTPException(404, "Item not found")

    source_type = meta.get("source_type", "")
    source_path = meta.get("source_path", "")
    extracted = meta.get("extracted", {})
    actions = meta.get("trace_actions", [])
    settings = get_settings()

    # Strategy 1: Immich asset — proxy thumbnail/original
    if source_path.startswith("immich://"):
        asset_id = source_path.removeprefix("immich://")
        return await _proxy_immich(settings, asset_id, size)

    # Strategy 2: Paperless — if the item was filed there, proxy the preview
    paperless_ref = _find_action_ref(actions, "paperless")
    if paperless_ref:
        return await _proxy_paperless(settings, paperless_ref, size)

    # Strategy 3: Email body text — return as HTML
    body_text = extracted.get("body_text", "")
    if body_text and source_type == "email":
        html = f"<html><body><pre style='white-space:pre-wrap;font-family:sans-serif;padding:1em'>{body_text}</pre></body></html>"
        return Response(content=html, media_type="text/html")

    raise HTTPException(404, "No preview available for this item")


async def _proxy_immich(settings, asset_id: str, size: str) -> StreamingResponse:
    """Proxy an Immich asset thumbnail or original."""
    api_key = settings.immich_api_key
    base_url = settings.services.immich_url

    if size == "full":
        url = f"{base_url}/api/assets/{asset_id}/original"
    else:
        url = f"{base_url}/api/assets/{asset_id}/thumbnail?size=preview"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, headers={"x-api-key": api_key})
        if resp.status_code != 200:
            raise HTTPException(502, f"Immich returned {resp.status_code}")
        return Response(
            content=resp.content,
            media_type=resp.headers.get("content-type", "image/jpeg"),
        )


async def _proxy_paperless(settings, task_ref: str, size: str) -> StreamingResponse:
    """Resolve a Paperless task ref to a document, then proxy its preview."""
    base_url = settings.services.paperless_url
    token = settings.paperless_api_key
    headers = {"Authorization": f"Token {token}"}

    async with httpx.AsyncClient(timeout=30) as client:
        # Resolve task → document PK
        resp = await client.get(
            f"{base_url}/api/tasks/?task_id={task_ref}",
            headers=headers,
        )
        if resp.status_code != 200:
            raise HTTPException(502, f"Paperless tasks API returned {resp.status_code}")

        tasks = resp.json()
        # Response is a list of task objects
        doc_pk = None
        if isinstance(tasks, list):
            for t in tasks:
                if t.get("task_id") == task_ref and t.get("related_document"):
                    doc_pk = t["related_document"]
                    break
        elif isinstance(tasks, dict) and tasks.get("related_document"):
            doc_pk = tasks["related_document"]

        if not doc_pk:
            raise HTTPException(404, "Paperless document not yet available (task may still be processing)")

        # Fetch the preview or original
        if size == "full":
            url = f"{base_url}/api/documents/{doc_pk}/download/"
        else:
            url = f"{base_url}/api/documents/{doc_pk}/preview/"

        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(502, f"Paperless document API returned {resp.status_code}")

        return Response(
            content=resp.content,
            media_type=resp.headers.get("content-type", "application/pdf"),
        )
