"""Unsubscribe processor — polls IMAP folder for emails and attempts to unsubscribe."""

from __future__ import annotations

import asyncio
import email as emaillib
import email.header
import email.policy
import email.utils
import imaplib
import logging
from functools import partial

from pipeline.db import get_pool
from pipeline.notify import Priority
import pipeline.notify as notify_mod
from pipeline.unsubscribe.extract import extract_unsubscribe_link
from pipeline.unsubscribe.execute import attempt_unsubscribe

log = logging.getLogger(__name__)

_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS unsubscribe_processed (
    message_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    detail TEXT,
    sender TEXT,
    subject TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS unsubscribed_senders (
    address TEXT NOT NULL,
    recipient TEXT NOT NULL,
    sender_name TEXT,
    unsubscribed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (address, recipient)
);
"""

_FOLDER = "Auto/Unsubscribe"


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


class UnsubscribeProcessor:
    """Polls an IMAP folder for emails to unsubscribe from."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        anthropic_api_key: str,
        poll_interval: int = 300,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._anthropic_api_key = anthropic_api_key
        self._poll_interval = poll_interval

    async def run(self) -> None:
        """Main loop — poll and process forever."""
        await self._ensure_table()
        log.info(
            "Unsubscribe processor polling %s@%s/%s every %ds",
            self._user, self._host, _FOLDER, self._poll_interval,
        )

        while True:
            try:
                await self._poll_and_process()
            except asyncio.CancelledError:
                log.info("Unsubscribe processor cancelled")
                return
            except Exception:
                log.exception("Unsubscribe poll error, retrying in %ds", self._poll_interval)
                notifier = notify_mod.get()
                if notifier:
                    await notifier.send(
                        "Unsubscribe processor error",
                        "Poll cycle failed — see logs for details.",
                        Priority.HIGH,
                    )

            await asyncio.sleep(self._poll_interval)

    async def _ensure_table(self) -> None:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.execute(_TABLE_SCHEMA)

    async def _is_processed(self, message_id: str) -> bool:
        pool = get_pool()
        row = await pool.fetchval(
            "SELECT 1 FROM unsubscribe_processed WHERE message_id = $1", message_id,
        )
        return row is not None

    async def _record(self, message_id: str, status: str, detail: str, sender: str, subject: str) -> None:
        pool = get_pool()
        await pool.execute(
            """INSERT INTO unsubscribe_processed (message_id, status, detail, sender, subject)
               VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING""",
            message_id, status, detail, sender, subject,
        )

    async def _record_sender(self, from_header: str, to_header: str) -> None:
        """Record the sender's email address in unsubscribed_senders."""
        _, sender_addr = email.utils.parseaddr(from_header)
        _, recipient_addr = email.utils.parseaddr(to_header)
        if not sender_addr or not recipient_addr:
            return
        pool = get_pool()
        await pool.execute(
            """INSERT INTO unsubscribed_senders (address, recipient, sender_name)
               VALUES ($1, $2, $3) ON CONFLICT DO NOTHING""",
            sender_addr.lower(), recipient_addr.lower(), from_header,
        )

    async def _poll_and_process(self) -> None:
        loop = asyncio.get_running_loop()

        # Connect and list messages
        try:
            messages = await loop.run_in_executor(None, self._imap_list_messages)
        except imaplib.IMAP4.error as e:
            if b"does not exist" in getattr(e, "args", (b"",))[0] if isinstance(getattr(e, "args", (b"",))[0], bytes) else "does not exist" in str(e):
                log.debug("Folder %s does not exist yet — skipping", _FOLDER)
                return
            raise

        if not messages:
            return

        # Filter to unprocessed, and clean up any that succeeded but weren't deleted
        new_messages = []
        stale_ids = []
        for msg_id, raw_email in messages:
            if not await self._is_processed(msg_id):
                new_messages.append((msg_id, raw_email))
            else:
                # Check if this was a successful unsubscribe whose deletion failed
                pool = get_pool()
                status = await pool.fetchval(
                    "SELECT status FROM unsubscribe_processed WHERE message_id = $1", msg_id,
                )
                if status == "success":
                    stale_ids.append(msg_id)

        if stale_ids:
            log.info("Cleaning up %d stale email(s) from %s", len(stale_ids), _FOLDER)
            loop = asyncio.get_running_loop()
            for msg_id in stale_ids:
                try:
                    await loop.run_in_executor(
                        None, partial(self._imap_delete_message, msg_id),
                    )
                except Exception:
                    log.exception("Failed to clean up stale email %s", msg_id)

        if not new_messages:
            return

        log.info("Found %d new email(s) in %s", len(new_messages), _FOLDER)

        for msg_id, raw_email in new_messages:
            await self._process_one(msg_id, raw_email)

    def _imap_list_messages(self) -> list[tuple[str, bytes]]:
        """Connect to IMAP, fetch all messages from the unsubscribe folder."""
        conn = imaplib.IMAP4_SSL(self._host, self._port)
        try:
            conn.login(self._user, self._password)
            typ, _ = conn.select(_FOLDER, readonly=True)
            if typ != "OK":
                return []

            _, data = conn.search(None, "ALL")
            if not data or not data[0]:
                return []

            results: list[tuple[str, bytes]] = []
            for num in data[0].split():
                # Get Message-ID header
                _, header_data = conn.fetch(
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
                if not msg_id:
                    continue

                # Fetch full message
                _, msg_data = conn.fetch(num, "(BODY.PEEK[])")
                if not msg_data or not msg_data[0]:
                    continue
                raw = msg_data[0][1] if isinstance(msg_data[0], tuple) else msg_data[0]
                if isinstance(raw, bytes):
                    results.append((msg_id, raw))

            return results
        finally:
            try:
                conn.logout()
            except Exception:
                pass

    async def _process_one(self, message_id: str, raw_email: bytes) -> None:
        """Process a single email: extract link, attempt unsubscribe, notify."""
        msg = emaillib.message_from_bytes(raw_email, policy=emaillib.policy.default)
        sender = _decode_header(msg.get("From", ""))
        recipient = _decode_header(msg.get("To", ""))
        subject = _decode_header(msg.get("Subject", ""))
        log.info("Processing unsubscribe: %s → %s — %s", sender, recipient, subject)

        # Extract unsubscribe target
        # Use compat32 policy for header parsing (extract needs raw headers)
        msg_compat = emaillib.message_from_bytes(raw_email, policy=emaillib.policy.compat32)
        target = extract_unsubscribe_link(msg_compat)

        notifier = notify_mod.get()

        if not target:
            log.info("No unsubscribe link found for: %s", sender)
            await self._record(message_id, "no_link", "No unsubscribe link found", sender, subject)
            if notifier:
                await notifier.send(
                    "Unsubscribe: no link found",
                    f"From: {sender}\nSubject: {subject}",
                    Priority.LOW,
                )
            return

        # Attempt unsubscribe
        result = await attempt_unsubscribe(target, self._anthropic_api_key)
        log.info(
            "Unsubscribe %s: %s — %s",
            "succeeded" if result.success else "failed",
            sender,
            result.detail,
        )

        status = "success" if result.success else "failed"
        await self._record(message_id, status, result.detail, sender, subject)

        if result.success:
            # Record sender so we can detect future emails from them
            try:
                await self._record_sender(sender, recipient)
            except Exception:
                log.exception("Failed to record sender %s", sender)

            # Delete the email from the folder
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(
                    None,
                    partial(self._imap_delete_message, message_id),
                )
                log.info("Deleted email %s from %s", message_id, _FOLDER)
            except Exception:
                log.exception("Failed to delete email %s after successful unsubscribe", message_id)

            if notifier:
                await notifier.send(
                    f"Unsubscribed: {sender}",
                    f"Subject: {subject}\nMethod: {result.method_used}\n{result.detail}",
                )
        else:
            if notifier:
                await notifier.send(
                    f"Unsubscribe failed: {sender}",
                    f"Subject: {subject}\nReason: {result.detail}",
                    Priority.LOW,
                )

    def _imap_delete_message(self, message_id: str) -> None:
        """Delete an email by Message-ID from the unsubscribe folder."""
        conn = imaplib.IMAP4_SSL(self._host, self._port)
        try:
            conn.login(self._user, self._password)
            conn.select(_FOLDER)

            _, data = conn.uid("SEARCH", None, "HEADER", "Message-ID", message_id)
            if not data or not data[0]:
                log.warning("Email %s not found in %s for deletion", message_id, _FOLDER)
                return

            for uid in data[0].split():
                conn.uid("STORE", uid, "+FLAGS", "\\Deleted")

            conn.expunge()
            log.info("Deleted email %s from %s", message_id, _FOLDER)
        finally:
            try:
                conn.logout()
            except Exception:
                pass


async def check_unsubscribed_sender(from_header: str, to_addr: str) -> str | None:
    """Check if an email sender was previously unsubscribed from for this recipient.

    Returns the recorded sender address if found, None otherwise.
    """
    _, sender_addr = email.utils.parseaddr(from_header)
    if not sender_addr or not to_addr:
        return None
    pool = get_pool()
    row = await pool.fetchval(
        "SELECT address FROM unsubscribed_senders WHERE address = $1 AND recipient = $2",
        sender_addr.lower(), to_addr.lower(),
    )
    return row
