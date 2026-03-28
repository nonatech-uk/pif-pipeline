"""Email move action — moves the source email to an IMAP folder."""

from __future__ import annotations

import imaplib
import logging
from typing import Any

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)


class EmailMoveHandler(ActionHandler):
    """Move the originating email to a specified IMAP folder."""

    name = "email_move"

    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        if envelope.source_type != "email":
            return ActionResult(ok=False, destination=self.name, reason="Not an email envelope")

        folder = params.get("folder", "Pipelined")
        source_folder = params.get("source_folder", "INBOX")
        subject_prefix = params.get("subject_prefix")

        # Extract Message-ID from source_path: email://<message-id>/filename
        source_path = envelope.source_path or ""
        if not source_path.startswith("email://"):
            return ActionResult(ok=False, destination=self.name, reason="No email source path")

        message_id = source_path.removeprefix("email://").rsplit("/", 1)[0]
        if not message_id:
            return ActionResult(ok=False, destination=self.name, reason="No Message-ID in source path")

        try:
            import asyncio
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self._move, source_folder, message_id, folder, subject_prefix,
            )
            # Send pushover notification on successful move
            from pipeline import notify as notify_mod
            notifier = notify_mod.get()
            if notifier:
                subject = envelope.source_email_subject or envelope.file_name or "unknown"
                prefix = f"{subject_prefix} " if subject_prefix else ""
                await notifier.send(
                    f"Email → {folder}",
                    f"{prefix}{subject}",
                )
            return ActionResult(ok=True, destination=self.name, ref=folder)
        except Exception as e:
            log.exception("Failed to move email %s to %s", message_id, folder)
            return ActionResult(ok=False, destination=self.name, reason=str(e))

    def _move(
        self, source_folder: str, message_id: str, dest_folder: str,
        subject_prefix: str | None = None,
    ) -> None:
        """Move email by Message-ID. Runs in a thread.

        If *subject_prefix* is set, the subject is modified by fetching the
        full message, prepending the prefix, appending the altered copy to
        *dest_folder*, and deleting the original (IMAP has no in-place edit).
        """
        conn = imaplib.IMAP4_SSL(self._host, self._port)
        try:
            conn.login(self._user, self._password)

            # Ensure destination folder exists
            try:
                conn.select(dest_folder)
                conn.close()
            except imaplib.IMAP4.error:
                conn.create(dest_folder)
                conn.subscribe(dest_folder)
                log.info("Created IMAP folder: %s", dest_folder)

            # Find the message by Message-ID header
            conn.select(source_folder)
            _, data = conn.uid("SEARCH", None, "HEADER", "Message-ID", message_id)
            if not data or not data[0]:
                log.info("Email %s already moved from %s", message_id, source_folder)
                return

            uids = data[0].split()

            if not subject_prefix:
                # Simple move — no subject rewrite needed
                for uid in uids:
                    typ, _ = conn.uid("MOVE", uid, dest_folder)
                    if typ != "OK":
                        conn.uid("COPY", uid, dest_folder)
                        conn.uid("STORE", uid, "+FLAGS", "\\Deleted")
            else:
                # Rewrite subject: fetch → modify → append → delete original
                import email as emaillib
                import email.policy
                import time as _time

                for uid in uids:
                    _, raw_data = conn.uid("FETCH", uid, "(RFC822 FLAGS INTERNALDATE)")
                    if not raw_data or not raw_data[0]:
                        continue
                    raw_msg = raw_data[0][1]
                    msg = emaillib.message_from_bytes(raw_msg, policy=emaillib.policy.compat32)

                    # Prepend prefix to subject
                    old_subject = msg.get("Subject", "")
                    if subject_prefix not in old_subject:
                        del msg["Subject"]
                        msg["Subject"] = f"{subject_prefix} {old_subject}"

                    # Parse flags and date from the FETCH response
                    import re
                    meta = raw_data[0][0].decode() if isinstance(raw_data[0][0], bytes) else str(raw_data[0][0])
                    flags_match = re.search(r"FLAGS \(([^)]*)\)", meta)
                    flags = flags_match.group(1) if flags_match else ""
                    # Remove \Recent — cannot be set by client
                    flags = " ".join(f for f in flags.split() if f != "\\Recent")
                    date_match = re.search(r'INTERNALDATE "([^"]+)"', meta)
                    idate = f'"{date_match.group(1)}"' if date_match else imaplib.Time2Internaldate(_time.time())

                    conn.append(dest_folder, f"({flags})", idate, msg.as_bytes())
                    conn.uid("STORE", uid, "+FLAGS", "\\Deleted")

            conn.expunge()
            log.info("Moved email %s to %s", message_id, dest_folder)
        finally:
            try:
                conn.logout()
            except Exception:
                pass
