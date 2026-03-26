"""Email watcher — polls IMAP for new messages, deduplicates via Message-ID."""

from __future__ import annotations

import asyncio
import email as emaillib
import email.header
import email.policy
import imaplib
import logging
from collections.abc import AsyncGenerator
from functools import partial

from bs4 import BeautifulSoup

from pipeline.db import get_pool
from pipeline.ingest.base import SourceWatcher
from pipeline.models import Envelope

log = logging.getLogger(__name__)

# MIME types we'll ingest as attachments
_ATTACHMENT_TYPES = {
    "image/jpeg", "image/png", "image/webp", "image/gif", "image/heic", "image/heif",
    "application/pdf",
    "text/plain",
}

_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS email_processed (
    message_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


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


def _html_to_text(html: str) -> str:
    """Extract readable text from HTML for classification."""
    soup = BeautifulSoup(html, "html.parser")
    # Remove script/style elements
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)


def _html_to_pdf(html: str) -> bytes:
    """Render HTML email body to PDF bytes."""
    from weasyprint import HTML as WeasyprintHTML
    return WeasyprintHTML(string=html).write_pdf()


class _ImapSession:
    """Wraps a single IMAP connection for a poll cycle."""

    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self._conn = imaplib.IMAP4_SSL(host, port)
        self._conn.login(user, password)

    def close(self) -> None:
        try:
            self._conn.logout()
        except Exception:
            pass

    def list_message_ids(self, folder: str) -> list[tuple[str, bytes]]:
        """Return (Message-ID, sequence number) pairs for all messages in folder."""
        self._conn.select(folder, readonly=True)
        _, data = self._conn.search(None, "ALL")
        if not data or not data[0]:
            return []

        results: list[tuple[str, bytes]] = []
        for num in data[0].split():
            _, header_data = self._conn.fetch(
                num, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])",
            )
            if not header_data or not header_data[0]:
                continue
            raw_header = (
                header_data[0][1]
                if isinstance(header_data[0], tuple)
                else header_data[0]
            )
            if not isinstance(raw_header, bytes):
                continue

            msg_id = (
                emaillib.message_from_bytes(raw_header, policy=emaillib.policy.default)
                .get("Message-ID", "")
                .strip()
            )
            if msg_id:
                results.append((msg_id, num))

        return results

    def fetch_message(self, folder: str, num: bytes) -> bytes | None:
        """Fetch full RFC822 message by sequence number."""
        self._conn.select(folder, readonly=True)
        _, data = self._conn.fetch(num, "(BODY.PEEK[])")
        if not data or not data[0]:
            return None
        raw = data[0][1] if isinstance(data[0], tuple) else data[0]
        return raw if isinstance(raw, bytes) else None

class EmailWatcher(SourceWatcher):
    """Polls an IMAP mailbox for new messages, deduplicating via Message-ID in the DB."""

    source_type = "email"

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        folder: str = "INBOX",
        poll_interval: int = 60,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._folder = folder
        self._poll_interval = poll_interval

    async def _ensure_table(self) -> None:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.execute(_TABLE_SCHEMA)

    async def _is_processed(self, message_id: str) -> bool:
        pool = get_pool()
        row = await pool.fetchval(
            "SELECT 1 FROM email_processed WHERE message_id = $1", message_id,
        )
        return row is not None

    async def _mark_processed(self, message_id: str) -> None:
        pool = get_pool()
        await pool.execute(
            "INSERT INTO email_processed (message_id) VALUES ($1) ON CONFLICT DO NOTHING",
            message_id,
        )

    async def watch(self) -> AsyncGenerator[Envelope, None]:
        await self._ensure_table()
        log.info(
            "Email watcher polling %s@%s/%s every %ds",
            self._user, self._host, self._folder, self._poll_interval,
        )

        import httpx
        hc_url = "https://hc.mees.st/ping/5c7b6230-b715-4694-ba61-b22b539e2612"

        while True:
            try:
                for envelope in await self._poll():
                    yield envelope
                async with httpx.AsyncClient(timeout=10) as hc:
                    await hc.get(hc_url)
            except Exception:
                log.exception("Email poll error, retrying in %ds", self._poll_interval)
                try:
                    async with httpx.AsyncClient(timeout=10) as hc:
                        await hc.get(f"{hc_url}/fail")
                except Exception:
                    pass

            await asyncio.sleep(self._poll_interval)

    async def _poll(self) -> list[Envelope]:
        """Fetch and parse new emails. Returns list of envelopes."""
        loop = asyncio.get_running_loop()
        envelopes: list[Envelope] = []

        # Step 1: Connect, list, and fetch all new messages
        session = await loop.run_in_executor(
            None,
            partial(_ImapSession, self._host, self._port, self._user, self._password),
        )
        try:
            listing = await loop.run_in_executor(
                None, session.list_message_ids, self._folder,
            )

            # Step 2: Filter out already-processed messages
            new_messages = []
            for msg_id, num in (listing or []):
                if not await self._is_processed(msg_id):
                    new_messages.append((msg_id, num))

            if not new_messages:
                return envelopes

            log.info("Found %d new email(s) to process", len(new_messages))

            # Step 3: Fetch all new messages upfront (before any actions modify the folder)
            fetched: list[tuple[str, bytes]] = []
            for msg_id, num in new_messages:
                try:
                    raw_email = await loop.run_in_executor(
                        None, session.fetch_message, self._folder, num,
                    )
                    if raw_email:
                        fetched.append((msg_id, raw_email))
                    else:
                        log.warning("Failed to fetch message %s", msg_id)
                except Exception:
                    log.exception("Failed to fetch email %s", msg_id)
        finally:
            await loop.run_in_executor(None, session.close)

        # Step 4: Parse fetched messages (IMAP session closed — safe for email_move)
        for msg_id, raw_email in fetched:
            try:
                async for envelope in self._parse_message(raw_email, msg_id):
                    envelopes.append(envelope)
                await self._mark_processed(msg_id)
                log.info("Processed email: %s", msg_id)
            except Exception:
                log.exception("Failed to process email %s", msg_id)

        return envelopes

    async def _parse_message(
        self, raw_email: bytes, message_id: str,
    ) -> AsyncGenerator[Envelope, None]:
        """Parse a raw email, yielding an Envelope per attachment or the body."""
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
                        source_path=f"email://{message_id}/{filename}",
                        file_name=filename,
                        source_email_from=from_addr,
                        source_email_subject=subject,
                    )
                    attachment_count += 1
                    yield envelope

        # If no attachments, extract body (prefer plain text, fall back to HTML→PDF)
        if attachment_count == 0:
            body_part = msg.get_body(preferencelist=("plain", "html"))
            if not body_part:
                return

            text = body_part.get_content()
            if not isinstance(text, str) or not text.strip():
                return

            if body_part.get_content_type() == "text/html":
                # Convert HTML to PDF for downstream (paperless/finance)
                loop = asyncio.get_running_loop()
                pdf_bytes = await loop.run_in_executor(None, _html_to_pdf, text)
                plain_text = _html_to_text(text)
                file_name = f"{subject or 'email'}.pdf"

                envelope = self._build_envelope(
                    pdf_bytes,
                    source_type=self.source_type,
                    source_path=f"email://{message_id}/{file_name}",
                    file_name=file_name,
                    source_email_from=from_addr,
                    source_email_subject=subject,
                )
                # Store extracted plain text for classification
                envelope.extracted["body_text"] = plain_text
                yield envelope
            else:
                raw_bytes = text.encode("utf-8")
                envelope = self._build_envelope(
                    raw_bytes,
                    source_type=self.source_type,
                    source_path=f"email://{message_id}/body.txt",
                    file_name="body.txt",
                    source_email_from=from_addr,
                    source_email_subject=subject,
                )
                yield envelope
