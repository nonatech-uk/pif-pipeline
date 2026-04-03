"""Extract unsubscribe links from email headers and HTML body."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from email.message import Message

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


@dataclass
class UnsubscribeTarget:
    url: str
    method: str          # "POST" or "GET"
    post_body: str | None
    source: str          # "header-oneclick", "header-url", "body-link"


def extract_unsubscribe_link(msg: Message) -> UnsubscribeTarget | None:
    """Extract the best unsubscribe target from an email message.

    Priority:
    1. RFC 8058 one-click (List-Unsubscribe-Post + List-Unsubscribe https URL)
    2. List-Unsubscribe header https URL (GET)
    3. HTML body link containing "unsubscribe"
    """
    header_url = _parse_list_unsubscribe_header(msg)
    has_oneclick = msg.get("List-Unsubscribe-Post", "").strip().lower() == "list-unsubscribe=one-click"

    # 1. RFC 8058 one-click
    if header_url and has_oneclick:
        log.info("Found RFC 8058 one-click unsubscribe: %s", header_url)
        return UnsubscribeTarget(
            url=header_url,
            method="POST",
            post_body="List-Unsubscribe=One-Click",
            source="header-oneclick",
        )

    # 2. List-Unsubscribe header URL (GET)
    if header_url:
        log.info("Found List-Unsubscribe header URL: %s", header_url)
        return UnsubscribeTarget(
            url=header_url,
            method="GET",
            post_body=None,
            source="header-url",
        )

    # 3. HTML body link
    body_url = _find_body_unsubscribe_link(msg)
    if body_url:
        log.info("Found unsubscribe link in email body: %s", body_url)
        return UnsubscribeTarget(
            url=body_url,
            method="GET",
            post_body=None,
            source="body-link",
        )

    return None


def _parse_list_unsubscribe_header(msg: Message) -> str | None:
    """Extract the first https URL from the List-Unsubscribe header (RFC 2369)."""
    raw = msg.get("List-Unsubscribe", "")
    if not raw:
        return None

    # Header format: <url1>, <url2>, ...
    urls = re.findall(r"<(https?://[^>]+)>", raw)
    # Prefer https, skip mailto
    for url in urls:
        if url.startswith("https://"):
            return url
    # Fall back to http if no https
    for url in urls:
        if url.startswith("http://"):
            return url
    return None


def _find_body_unsubscribe_link(msg: Message) -> str | None:
    """Scan the HTML body for an <a> tag containing 'unsubscribe'."""
    html = _get_html_body(msg)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        text = tag.get_text(strip=True).lower()

        # Match on link text or href containing "unsubscribe"
        if "unsubscribe" in text or "unsubscribe" in href.lower():
            if href.startswith(("https://", "http://")):
                return href

    return None


def _get_html_body(msg: Message) -> str | None:
    """Extract the HTML body from a parsed email message."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
    elif msg.get_content_type() == "text/html":
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return None
