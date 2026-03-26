"""Notification module — Pushover and email alerts for pipeline events."""

from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from enum import Enum

import httpx

log = logging.getLogger(__name__)


class Priority(Enum):
    LOW = -1
    NORMAL = 0
    HIGH = 1


class Notifier:
    """Send notifications via Pushover and/or email."""

    def __init__(
        self,
        pushover_app_token: str = "",
        pushover_user_key: str = "",
        smtp_host: str = "",
        smtp_port: int = 587,
        smtp_user: str = "",
        smtp_password: str = "",
        email_to: str = "",
        email_from: str = "",
    ) -> None:
        self._pushover_token = pushover_app_token
        self._pushover_user = pushover_user_key
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port
        self._smtp_user = smtp_user
        self._smtp_password = smtp_password
        self._email_to = email_to
        self._email_from = email_from or smtp_user

    @property
    def pushover_enabled(self) -> bool:
        return bool(self._pushover_token and self._pushover_user)

    @property
    def email_enabled(self) -> bool:
        return bool(self._smtp_host and self._smtp_user and self._email_to)

    async def send(
        self,
        title: str,
        message: str,
        priority: Priority = Priority.NORMAL,
    ) -> None:
        """Send notification to all configured channels."""
        if self.pushover_enabled:
            await self._send_pushover(title, message, priority)
        if self.email_enabled:
            self._send_email(title, message)
        if not self.pushover_enabled and not self.email_enabled:
            log.warning("No notification channels configured")

    async def _send_pushover(
        self, title: str, message: str, priority: Priority
    ) -> None:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    "https://api.pushover.net/1/messages.json",
                    data={
                        "token": self._pushover_token,
                        "user": self._pushover_user,
                        "title": title,
                        "message": message,
                        "priority": priority.value,
                    },
                )
                resp.raise_for_status()
                log.info("Pushover notification sent: %s", title)
        except Exception:
            log.exception("Failed to send Pushover notification")

    def _send_email(self, title: str, message: str) -> None:
        try:
            msg = EmailMessage()
            msg["Subject"] = f"[Pipeline] {title}"
            msg["From"] = self._email_from
            msg["To"] = self._email_to
            msg.set_content(message)

            with smtplib.SMTP_SSL(self._smtp_host, self._smtp_port) as smtp:
                smtp.login(self._smtp_user, self._smtp_password)
                smtp.send_message(msg)
            log.info("Email notification sent: %s", title)
        except Exception:
            log.exception("Failed to send email notification")


# Module-level singleton, configured at startup
_notifier: Notifier | None = None


def configure(notifier: Notifier) -> None:
    global _notifier
    _notifier = notifier


def get() -> Notifier | None:
    return _notifier
