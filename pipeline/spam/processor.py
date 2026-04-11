"""Spam blacklist processor — polls IMAP folder, blacklists senders via Mailcow, moves to Junk."""

from __future__ import annotations

import asyncio
import email as emaillib
import email.header
import email.policy
import email.utils
import imaplib
import logging
import re
import time as _time
from functools import partial

import httpx

from pipeline.db import get_pool
from pipeline.notify import Priority
import pipeline.notify as notify_mod

log = logging.getLogger(__name__)

_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS spam_processed (
    message_id TEXT PRIMARY KEY,
    sender TEXT,
    subject TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_FOLDER = "Auto/Spam"
_DEST_FOLDER = "Junk"
_SUBJECT_PREFIX = "[Processed]"


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


class SpamProcessor:
    """Polls an IMAP folder for spam emails, blacklists senders via Mailcow, and moves to Junk."""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        mailcow_url: str,
        mailcow_api_key: str,
        poll_interval: int = 300,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._mailcow_url = mailcow_url.rstrip("/")
        self._mailcow_api_key = mailcow_api_key
        self._poll_interval = poll_interval
        # Extract domain from IMAP user for Mailcow domain-policy
        self._domain = self._user.split("@", 1)[1] if "@" in self._user else ""

    async def run(self) -> None:
        """Main loop — poll and process forever."""
        await self._ensure_table()
        log.info(
            "Spam processor polling %s@%s/%s every %ds",
            self._user, self._host, _FOLDER, self._poll_interval,
        )

        while True:
            try:
                await self._poll_and_process()
            except asyncio.CancelledError:
                log.info("Spam processor cancelled")
                return
            except Exception:
                log.exception("Spam poll error, retrying in %ds", self._poll_interval)
                notifier = notify_mod.get()
                if notifier:
                    await notifier.send(
                        "Spam processor error",
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
            "SELECT 1 FROM spam_processed WHERE message_id = $1", message_id,
        )
        return row is not None

    async def _record(self, message_id: str, sender: str, subject: str) -> None:
        pool = get_pool()
        await pool.execute(
            """INSERT INTO spam_processed (message_id, sender, subject)
               VALUES ($1, $2, $3) ON CONFLICT DO NOTHING""",
            message_id, sender, subject,
        )

    async def _poll_and_process(self) -> None:
        loop = asyncio.get_running_loop()

        try:
            messages = await loop.run_in_executor(None, self._imap_list_messages)
        except imaplib.IMAP4.error as e:
            err_arg = getattr(e, "args", (b"",))[0]
            err_str = err_arg if isinstance(err_arg, str) else str(err_arg)
            if "does not exist" in err_str:
                log.debug("Folder %s does not exist yet — skipping", _FOLDER)
                return
            raise

        if not messages:
            return

        new_messages = []
        stale_ids = []
        for msg_id, raw_email in messages:
            if not await self._is_processed(msg_id):
                new_messages.append((msg_id, raw_email))
            else:
                stale_ids.append(msg_id)

        if stale_ids:
            log.info("Cleaning up %d stale email(s) from %s", len(stale_ids), _FOLDER)
            for msg_id in stale_ids:
                try:
                    await loop.run_in_executor(
                        None, partial(self._imap_move_to_junk, msg_id),
                    )
                except Exception:
                    log.exception("Failed to clean up stale email %s", msg_id)

        if not new_messages:
            return

        log.info("Found %d new email(s) in %s", len(new_messages), _FOLDER)

        for msg_id, raw_email in new_messages:
            await self._process_one(msg_id, raw_email)

    def _imap_list_messages(self) -> list[tuple[str, bytes]]:
        """Connect to IMAP, fetch all messages from the spam folder."""
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
        """Process a single spam email: blacklist sender, prefix subject, move to Junk."""
        msg = emaillib.message_from_bytes(raw_email, policy=emaillib.policy.default)
        sender = _decode_header(msg.get("From", ""))
        subject = _decode_header(msg.get("Subject", ""))
        _, sender_addr = email.utils.parseaddr(sender)
        log.info("Processing spam: %s — %s", sender, subject)

        notifier = notify_mod.get()

        if not sender_addr:
            log.warning("No sender address found for spam email: %s", message_id)
            await self._record(message_id, sender, subject)
            return

        # Step 1: Blacklist sender via Mailcow API
        try:
            await self._blacklist_sender(sender_addr)
            log.info("Blacklisted sender: %s", sender_addr)
        except Exception:
            log.exception("Failed to blacklist sender %s", sender_addr)
            if notifier:
                await notifier.send(
                    f"Spam blacklist failed: {sender_addr}",
                    f"Subject: {subject}\nCould not add to Mailcow blacklist — see logs.",
                    Priority.HIGH,
                )
            return

        # Step 2: Prefix subject and move to Junk
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, partial(self._imap_move_to_junk, message_id),
            )
            log.info("Moved spam email to %s: %s", _DEST_FOLDER, message_id)
        except Exception:
            log.exception("Failed to move spam email %s to %s", message_id, _DEST_FOLDER)

        # Step 3: Record and notify
        await self._record(message_id, sender_addr, subject)

        if notifier:
            await notifier.send(
                f"Spam blacklisted: {sender_addr}",
                f"Subject: {subject}\nSender added to domain blacklist and email moved to {_DEST_FOLDER}.",
            )

    async def _blacklist_sender(self, sender_addr: str) -> None:
        """Add sender to Mailcow domain-wide blacklist."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self._mailcow_url}/api/v1/add/domain-policy",
                headers={"X-API-Key": self._mailcow_api_key},
                json={
                    "domain": self._domain,
                    "object_list": "bl",
                    "object_from": sender_addr,
                    "object_to": "",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and data and data[0].get("type") != "success":
                raise RuntimeError(f"Mailcow API error: {data[0].get('msg')}")

    def _imap_move_to_junk(self, message_id: str) -> None:
        """Prefix subject with [Processed] and move email from Auto/Spam to Junk."""
        conn = imaplib.IMAP4_SSL(self._host, self._port)
        try:
            conn.login(self._user, self._password)

            # Ensure Junk folder exists
            try:
                conn.select(_DEST_FOLDER)
                conn.close()
            except imaplib.IMAP4.error:
                conn.create(_DEST_FOLDER)
                conn.subscribe(_DEST_FOLDER)
                log.info("Created IMAP folder: %s", _DEST_FOLDER)

            conn.select(_FOLDER)
            _, data = conn.uid("SEARCH", None, "HEADER", "Message-ID", message_id)
            if not data or not data[0]:
                log.info("Email %s already moved from %s", message_id, _FOLDER)
                return

            for uid in data[0].split():
                _, raw_data = conn.uid("FETCH", uid, "(RFC822 FLAGS INTERNALDATE)")
                if not raw_data or not raw_data[0]:
                    continue
                raw_msg = raw_data[0][1]
                msg = emaillib.message_from_bytes(raw_msg, policy=emaillib.policy.compat32)

                # Prepend prefix to subject
                old_subject = msg.get("Subject", "")
                if _SUBJECT_PREFIX not in old_subject:
                    del msg["Subject"]
                    msg["Subject"] = f"{_SUBJECT_PREFIX} {old_subject}"

                # Parse flags and date from FETCH response
                meta = raw_data[0][0].decode() if isinstance(raw_data[0][0], bytes) else str(raw_data[0][0])
                flags_match = re.search(r"FLAGS \(([^)]*)\)", meta)
                flags = flags_match.group(1) if flags_match else ""
                flags = " ".join(f for f in flags.split() if f != "\\Recent")
                date_match = re.search(r'INTERNALDATE "([^"]+)"', meta)
                idate = f'"{date_match.group(1)}"' if date_match else imaplib.Time2Internaldate(_time.time())

                conn.append(_DEST_FOLDER, f"({flags})", idate, msg.as_bytes())
                conn.uid("STORE", uid, "+FLAGS", "\\Deleted")

            conn.expunge()
            log.info("Moved email %s to %s with prefix %s", message_id, _DEST_FOLDER, _SUBJECT_PREFIX)
        finally:
            try:
                conn.logout()
            except Exception:
                pass
