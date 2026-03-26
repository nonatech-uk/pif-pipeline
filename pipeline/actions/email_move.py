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
                None, self._move, source_folder, message_id, folder,
            )
            return ActionResult(ok=True, destination=self.name, ref=folder)
        except Exception as e:
            log.exception("Failed to move email %s to %s", message_id, folder)
            return ActionResult(ok=False, destination=self.name, reason=str(e))

    def _move(self, source_folder: str, message_id: str, dest_folder: str) -> None:
        """Move email by Message-ID. Runs in a thread."""
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
            _, data = conn.search(None, "HEADER", "Message-ID", message_id)
            if not data or not data[0]:
                raise ValueError(f"Message-ID {message_id} not found in {source_folder}")

            nums = data[0].split()
            for num in nums:
                conn.copy(num, dest_folder)
                conn.store(num, "+FLAGS", "\\Deleted")
            conn.expunge()
            log.info("Moved email %s to %s", message_id, dest_folder)
        finally:
            try:
                conn.logout()
            except Exception:
                pass
