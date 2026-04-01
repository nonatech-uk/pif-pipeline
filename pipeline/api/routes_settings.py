"""Settings API — manage email ignore senders list."""

from __future__ import annotations

import imaplib
import logging
import re
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from pipeline.api.deps import get_settings
from pipeline.db import get_pool

log = logging.getLogger(__name__)
router = APIRouter()


class IgnoreSenderRequest(BaseModel):
    address: str
    note: str = ""


class IgnoreFromItemRequest(BaseModel):
    item_id: str


@router.get("/settings/ignore-senders")
async def list_ignore_senders() -> dict[str, Any]:
    pool = get_pool()
    rows = await pool.fetch(
        "SELECT id, address, note, created_at FROM email_ignore_senders ORDER BY created_at DESC"
    )
    return {"items": [dict(r) for r in rows]}


@router.post("/settings/ignore-senders")
async def add_ignore_sender(body: IgnoreSenderRequest) -> dict[str, Any]:
    pool = get_pool()
    row = await pool.fetchrow(
        """INSERT INTO email_ignore_senders (address, note)
           VALUES ($1, $2)
           ON CONFLICT (address) DO UPDATE SET note = EXCLUDED.note
           RETURNING id, address, note, created_at""",
        body.address.strip().lower(),
        body.note,
    )
    return dict(row)


@router.delete("/settings/ignore-senders/{sender_id}")
async def delete_ignore_sender(sender_id: int) -> dict[str, bool]:
    pool = get_pool()
    result = await pool.execute(
        "DELETE FROM email_ignore_senders WHERE id = $1", sender_id
    )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Sender not found")
    return {"ok": True}


@router.post("/settings/ignore-senders/from-item")
async def ignore_sender_from_item(body: IgnoreFromItemRequest) -> dict[str, Any]:
    """Add sender to ignore list from a pipeline item and move email back to INBOX."""
    pool = get_pool()

    # Try exceptions table first, then extracted fields, then IMAP lookup
    sender = await pool.fetchval(
        "SELECT envelope_json->>'source_email_from' FROM exceptions WHERE item_id = $1",
        body.item_id,
    )
    source_path = None
    if not sender:
        row = await pool.fetchrow(
            "SELECT source_path, extracted FROM audit_log WHERE item_id = $1 AND source_type = 'email' LIMIT 1",
            body.item_id,
        )
        if row:
            source_path = row["source_path"]
            # Check extracted._email_from (stored for newer items)
            import json
            extracted = row["extracted"]
            if isinstance(extracted, str):
                extracted = json.loads(extracted)
            if isinstance(extracted, dict):
                sender = extracted.get("_email_from")

    # Last resort: fetch From header from IMAP by Message-ID
    if not sender and source_path and source_path.startswith("email://"):
        message_id = source_path.removeprefix("email://").rsplit("/", 1)[0]
        if message_id:
            sender = await _lookup_sender_via_imap(message_id)

    if not sender:
        raise HTTPException(status_code=404, detail="Could not determine sender for this item")

    # Extract just the email address from "Name <email>" format
    match = re.search(r"<([^>]+)>", sender)
    address = match.group(1).lower() if match else sender.strip().lower()

    # Add to ignore list
    await pool.execute(
        """INSERT INTO email_ignore_senders (address, note)
           VALUES ($1, $2)
           ON CONFLICT (address) DO NOTHING""",
        address,
        f"Added from item {body.item_id[:8]}",
    )

    # Move email back to INBOX
    moved_back = False
    if not source_path:
        source_path = await pool.fetchval(
            "SELECT source_path FROM audit_log WHERE item_id = $1 LIMIT 1",
            body.item_id,
        )
    if not source_path:
        source_path = await pool.fetchval(
            "SELECT envelope_json->>'source_path' FROM exceptions WHERE item_id = $1",
            body.item_id,
        )

    if source_path and source_path.startswith("email://"):
        message_id = source_path.removeprefix("email://").rsplit("/", 1)[0]
        if message_id:
            moved_back = await _move_email_back(message_id)

    return {"address": address, "sender": sender, "moved_back": moved_back}


async def _lookup_sender_via_imap(message_id: str) -> str | None:
    """Fetch the From header of an email by Message-ID from IMAP."""
    import asyncio

    settings = get_settings()
    if not settings.services.imap_user or not settings.services.imap_password:
        return None

    def _fetch() -> str | None:
        conn = imaplib.IMAP4_SSL(settings.services.imap_host, settings.services.imap_port)
        try:
            conn.login(settings.services.imap_user, settings.services.imap_password)
            for folder in ("Pipelined", "INBOX"):
                conn.select(folder)
                _, data = conn.uid("SEARCH", None, "HEADER", "Message-ID", message_id)
                if data and data[0]:
                    uid = data[0].split()[0]
                    _, parts = conn.uid("FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (FROM)])")
                    if parts and parts[0]:
                        header = parts[0][1]
                        if isinstance(header, bytes):
                            header = header.decode("utf-8", errors="replace")
                        # Parse "From: Name <email>\r\n"
                        return header.replace("From:", "").strip().strip("\r\n")
            return None
        except Exception:
            log.exception("IMAP sender lookup failed for %s", message_id)
            return None
        finally:
            try:
                conn.logout()
            except Exception:
                pass

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch)


async def _move_email_back(message_id: str) -> bool:
    """Move an email from Pipelined back to INBOX, stripping any (Duplicate) prefix."""
    import asyncio
    import email as emaillib
    import email.policy
    import time as _time

    settings = get_settings()
    if not settings.services.imap_user or not settings.services.imap_password:
        return False

    def _do_move() -> bool:
        conn = imaplib.IMAP4_SSL(settings.services.imap_host, settings.services.imap_port)
        try:
            conn.login(settings.services.imap_user, settings.services.imap_password)
            conn.select("Pipelined")
            _, data = conn.uid("SEARCH", None, "HEADER", "Message-ID", message_id)
            if not data or not data[0]:
                log.info("Email %s not found in Pipelined", message_id)
                return False

            uids = data[0].split()
            for uid in uids:
                _, raw_data = conn.uid("FETCH", uid, "(RFC822 FLAGS INTERNALDATE)")
                if not raw_data or not raw_data[0]:
                    continue
                raw_msg = raw_data[0][1]
                msg = emaillib.message_from_bytes(raw_msg, policy=emaillib.policy.compat32)

                # Strip (Duplicate) prefix from subject
                old_subject = msg.get("Subject", "")
                cleaned = re.sub(r"^\(Duplicate\)\s*", "", old_subject)
                if cleaned != old_subject:
                    del msg["Subject"]
                    msg["Subject"] = cleaned

                # Parse flags and date
                meta = raw_data[0][0].decode() if isinstance(raw_data[0][0], bytes) else str(raw_data[0][0])
                flags_match = re.search(r"FLAGS \(([^)]*)\)", meta)
                flags = flags_match.group(1) if flags_match else ""
                flags = " ".join(f for f in flags.split() if f != "\\Recent")
                date_match = re.search(r'INTERNALDATE "([^"]+)"', meta)
                idate = f'"{date_match.group(1)}"' if date_match else imaplib.Time2Internaldate(_time.time())

                conn.append("INBOX", f"({flags})", idate, msg.as_bytes())
                conn.uid("STORE", uid, "+FLAGS", "\\Deleted")

            conn.expunge()
            log.info("Moved email %s back to INBOX", message_id)
            return True
        except Exception:
            log.exception("Failed to move email %s back to INBOX", message_id)
            return False
        finally:
            try:
                conn.logout()
            except Exception:
                pass

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _do_move)
