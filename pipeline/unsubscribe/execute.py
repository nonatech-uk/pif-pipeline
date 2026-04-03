"""Execute unsubscribe requests — one-click POST, GET, or Claude-assisted confirmation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import anthropic
import httpx
from bs4 import BeautifulSoup

from pipeline.unsubscribe.extract import UnsubscribeTarget

log = logging.getLogger(__name__)

_MAX_REQUESTS = 3
_TIMEOUT = 15

# Keywords that indicate a successful unsubscribe on the response page
_SUCCESS_KEYWORDS = [
    "unsubscribed",
    "successfully",
    "removed",
    "confirmed",
    "you have been",
    "no longer",
    "opt out",
    "opted out",
    "preferences updated",
    "subscription cancelled",
    "subscription canceled",
]

_CLAUDE_PROMPT = """\
You are analyzing an unsubscribe confirmation page. The user wants to complete \
the unsubscribe process automatically.

Analyze the HTML below and determine what action is needed.

Return ONLY a JSON object with one of these structures:

1. If a form needs to be submitted:
{"action": "submit_form", "url": "https://...", "method": "POST", "fields": {"key": "value"}}

2. If the page confirms unsubscribe is already done:
{"action": "already_done", "message": "reason"}

3. If you cannot complete the unsubscribe (CAPTCHA, login required, JS-only, etc.):
{"action": "cannot_complete", "reason": "explanation"}

Important:
- For form URLs, use the absolute URL (combine with the page URL if relative).
- Include all hidden form fields and any pre-selected/default values.
- If there are multiple forms, pick the one related to unsubscribing.
- If the page shows a success message, return already_done."""


@dataclass
class UnsubscribeResult:
    success: bool
    method_used: str     # "oneclick-post", "get", "confirmation-claude", "failed"
    detail: str          # Human-readable outcome for notification


async def attempt_unsubscribe(
    target: UnsubscribeTarget,
    anthropic_api_key: str,
) -> UnsubscribeResult:
    """Attempt to unsubscribe using the given target URL."""
    try:
        async with httpx.AsyncClient(
            timeout=_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; UnsubscribeBot/1.0)"},
        ) as client:
            if target.method == "POST" and target.post_body:
                return await _oneclick_post(client, target)
            else:
                return await _get_and_process(client, target, anthropic_api_key)
    except httpx.TimeoutException:
        return UnsubscribeResult(False, "failed", f"Request timed out: {target.url}")
    except httpx.HTTPError as e:
        return UnsubscribeResult(False, "failed", f"HTTP error: {e}")
    except Exception as e:
        log.exception("Unexpected error during unsubscribe attempt")
        return UnsubscribeResult(False, "failed", f"Unexpected error: {e}")


async def _oneclick_post(
    client: httpx.AsyncClient,
    target: UnsubscribeTarget,
) -> UnsubscribeResult:
    """RFC 8058 one-click unsubscribe via POST."""
    log.info("Attempting one-click POST to %s", target.url)
    resp = await client.post(
        target.url,
        content=target.post_body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if 200 <= resp.status_code < 300:
        return UnsubscribeResult(True, "oneclick-post", f"One-click unsubscribe successful (HTTP {resp.status_code})")

    return UnsubscribeResult(
        False, "failed",
        f"One-click POST returned HTTP {resp.status_code}",
    )


async def _get_and_process(
    client: httpx.AsyncClient,
    target: UnsubscribeTarget,
    anthropic_api_key: str,
) -> UnsubscribeResult:
    """GET the unsubscribe URL and process the response page."""
    log.info("GET %s", target.url)
    resp = await client.get(target.url)

    if resp.status_code >= 400:
        return UnsubscribeResult(False, "failed", f"GET returned HTTP {resp.status_code}")

    body = resp.text
    if _page_indicates_success(body):
        return UnsubscribeResult(True, "get", "Unsubscribe confirmed by page content")

    # Check if the page has a form or button that needs interaction
    if _page_has_form(body):
        return await _claude_confirmation(client, target.url, body, anthropic_api_key)

    # Page loaded OK but no clear success or form — assume success for simple redirects
    if resp.status_code in (200, 204):
        return UnsubscribeResult(
            True, "get",
            "Page loaded without error (no explicit confirmation found)",
        )

    return UnsubscribeResult(False, "failed", f"Unclear response from {target.url}")


def _page_indicates_success(html: str) -> bool:
    """Check if the page text contains unsubscribe success keywords."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True).lower()

    return any(kw in text for kw in _SUCCESS_KEYWORDS)


def _page_has_form(html: str) -> bool:
    """Check if the page contains a form or button likely related to unsubscribing."""
    soup = BeautifulSoup(html, "html.parser")
    # Look for forms
    forms = soup.find_all("form")
    if forms:
        return True
    # Look for buttons with unsubscribe-related text
    for btn in soup.find_all(["button", "input"]):
        btn_text = btn.get_text(strip=True).lower()
        btn_value = (btn.get("value") or "").lower()
        if "unsubscribe" in btn_text or "unsubscribe" in btn_value or "confirm" in btn_text:
            return True
    return False


async def _claude_confirmation(
    client: httpx.AsyncClient,
    page_url: str,
    html: str,
    anthropic_api_key: str,
) -> UnsubscribeResult:
    """Use Claude to understand and complete a confirmation page."""
    log.info("Confirmation page detected — asking Claude to analyze")

    # Trim HTML to avoid sending huge pages
    trimmed = _trim_html(html)

    try:
        claude = anthropic.Anthropic(api_key=anthropic_api_key)
        response = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            system=_CLAUDE_PROMPT,
            messages=[{
                "role": "user",
                "content": f"Page URL: {page_url}\n\nHTML:\n{trimmed}",
            }],
        )

        text = response.content[0].text.strip()
        parsed = _parse_json(text)
        if not parsed:
            return UnsubscribeResult(False, "failed", f"Claude returned unparseable response: {text[:200]}")

        action = parsed.get("action")

        if action == "already_done":
            return UnsubscribeResult(True, "confirmation-claude", parsed.get("message", "Already unsubscribed"))

        if action == "cannot_complete":
            return UnsubscribeResult(False, "failed", parsed.get("reason", "Claude could not complete unsubscribe"))

        if action == "submit_form":
            return await _submit_claude_form(client, page_url, parsed)

        return UnsubscribeResult(False, "failed", f"Unknown Claude action: {action}")

    except anthropic.APIError as e:
        log.exception("Claude API error during confirmation analysis")
        return UnsubscribeResult(False, "failed", f"Claude API error: {e}")


async def _submit_claude_form(
    client: httpx.AsyncClient,
    page_url: str,
    form_spec: dict,
) -> UnsubscribeResult:
    """Submit a form as directed by Claude."""
    url = form_spec.get("url", "")
    method = form_spec.get("method", "POST").upper()
    fields = form_spec.get("fields", {})

    if not url:
        return UnsubscribeResult(False, "failed", "Claude returned empty form URL")

    # Resolve relative URLs
    if url.startswith("/"):
        from urllib.parse import urlparse
        parsed = urlparse(page_url)
        url = f"{parsed.scheme}://{parsed.netloc}{url}"

    log.info("Submitting form: %s %s", method, url)

    if method == "POST":
        resp = await client.post(url, data=fields)
    else:
        resp = await client.get(url, params=fields)

    if 200 <= resp.status_code < 400:
        # Check if the result page confirms success
        if _page_indicates_success(resp.text):
            return UnsubscribeResult(True, "confirmation-claude", "Form submitted and unsubscribe confirmed")
        return UnsubscribeResult(True, "confirmation-claude", f"Form submitted (HTTP {resp.status_code})")

    return UnsubscribeResult(
        False, "failed",
        f"Form submission returned HTTP {resp.status_code}",
    )


def _trim_html(html: str, max_chars: int = 8000) -> str:
    """Trim HTML to a reasonable size for Claude, keeping meaningful content."""
    soup = BeautifulSoup(html, "html.parser")
    # Remove scripts and styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    result = str(soup)
    if len(result) > max_chars:
        result = result[:max_chars]
    return result


def _parse_json(text: str) -> dict | None:
    """Parse JSON from Claude response, stripping markdown fences if present."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [line for line in lines if not line.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None
