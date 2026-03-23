"""Email watcher — IMAP IDLE loop for new messages with attachment extraction."""

from __future__ import annotations

import asyncio
import email as emaillib
import email.header
import email.policy
import logging
from collections.abc import AsyncGenerator
from datetime import datetime, UTC

import aioimaplib

from pipeline.ingest.base import SourceWatcher
from pipeline.models import Envelope

log = logging.getLogger(__name__)

# MIME types we'll ingest as attachments
_ATTACHMENT_TYPES = {
    "image/jpeg", "image/png", "image/webp", "image/gif", "image/heic", "image/heif",
    "application/pdf",
    "text/plain",
}


def _decode_header(raw: str | None) -> str:
    if not raw:
        return ""
    parts = email.header.decode_header(raw)
    decoded = []
    for data, charset in parts:
        if isinstance(data, bytes):
            decoded.append(data.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(data)
    return " ".join(decoded)


class EmailWatcher(SourceWatcher):
    """Watches an IMAP mailbox via IDLE for new messages."""

    source_type = "email"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        folder: str = "INBOX",
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._folder = folder

    async def watch(self) -> AsyncGenerator[Envelope, None]:
        while True:
            try:
                async for envelope in self._watch_loop():
                    yield envelope
            except Exception:
                log.exception("IMAP connection error, reconnecting in 30s")
                await asyncio.sleep(30)

    async def _watch_loop(self) -> AsyncGenerator[Envelope, None]:
        client = aioimaplib.IMAP4_SSL(host=self._host, port=self._port)
        await client.wait_hello_from_server()
        await client.login(self._user, self._password)
        await client.select(self._folder)
        log.info("Email watcher connected: %s@%s/%s", self._user, self._host, self._folder)

        try:
            while True:
                # Check for unseen messages
                status, data = await client.search("UNSEEN")
                if status == "OK" and data[0]:
                    uids = data[0].split()
                    for uid in uids:
                        async for envelope in self._process_message(client, uid):
                            yield envelope

                # IDLE until new mail arrives
                idle = await client.idle_start(timeout=300)
                await asyncio.wait_for(idle, timeout=310)
        finally:
            try:
                await client.logout()
            except Exception:
                pass

    async def _process_message(
        self, client: aioimaplib.IMAP4_SSL, uid: bytes
    ) -> AsyncGenerator[Envelope, None]:
        """Fetch and parse a single message, yielding an Envelope per attachment."""
        uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
        status, data = await client.fetch(uid_str, "(RFC822)")
        if status != "OK" or not data:
            return

        # data is a list of response lines; find the one with message bytes
        raw_email = None
        for item in data:
            if isinstance(item, bytes) and len(item) > 100:
                raw_email = item
                break

        if raw_email is None:
            return

        msg = emaillib.message_from_bytes(raw_email, policy=emaillib.policy.default)
        from_addr = _decode_header(msg.get("From", ""))
        subject = _decode_header(msg.get("Subject", ""))
        log.info("Email from %s: %s", from_addr, subject)

        attachment_count = 0
        for part in msg.walk():
            content_type = part.get_content_type()
            filename = part.get_filename()

            if content_type in _ATTACHMENT_TYPES and filename:
                payload = part.get_payload(decode=True)
                if payload:
                    envelope = self._build_envelope(
                        payload,
                        source_type=self.source_type,
                        source_path=f"email://{uid_str}/{filename}",
                        file_name=filename,
                        source_email_from=from_addr,
                        source_email_subject=subject,
                    )
                    attachment_count += 1
                    yield envelope

        # If no attachments, extract body text as a text envelope
        if attachment_count == 0:
            body = msg.get_body(preferencelist=("plain",))
            if body:
                text = body.get_content()
                if isinstance(text, str) and text.strip():
                    raw_bytes = text.encode("utf-8")
                    envelope = self._build_envelope(
                        raw_bytes,
                        source_type=self.source_type,
                        source_path=f"email://{uid_str}/body.txt",
                        file_name="body.txt",
                        source_email_from=from_addr,
                        source_email_subject=subject,
                    )
                    yield envelope
