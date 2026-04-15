"""Claude API classifier — vision classification + structured extraction."""

from __future__ import annotations

import base64
import io
import json
import logging
from pathlib import Path

import anthropic
from PIL import Image

from pipeline.classify.base import Classifier
from pipeline.models import ClassifyResult, Envelope

log = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).parent / "prompts"

# Image MIME types Claude vision supports
_VISION_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}

# Claude vision API limit is 5 MB for base64-encoded images
_MAX_IMAGE_BYTES = 5 * 1024 * 1024

# Labels that trigger structured extraction
_EXTRACTABLE = {
    "receipt", "invoice", "boarding_pass", "bank_statement",
    "insurance_policy", "tax_document", "wine_label",
}


class ClaudeClassifier(Classifier):
    """Tier 4: Claude API — vision classify + structured extraction."""

    name = "claude"
    EXTRACTABLE_LABELS = _EXTRACTABLE

    def __init__(self, api_key: str, threshold: float = 0.80) -> None:
        self._client = anthropic.Anthropic(api_key=api_key)
        self._threshold = threshold
        self._classify_prompt = (_PROMPTS_DIR / "classify.txt").read_text()
        self._extract_prompt = (_PROMPTS_DIR / "extract.txt").read_text()

    async def classify(self, envelope: Envelope) -> ClassifyResult | None:
        if not envelope.raw_bytes:
            return None

        try:
            # Step 1: Classify
            result = await self._classify_step(envelope)
            if not result:
                return None

            # Step 2: Extract structured fields if high-value label
            if result.label in _EXTRACTABLE and result.confidence >= self._threshold:
                extracted = await self._extract_step(envelope, result.label)
                if extracted:
                    result.extracted = extracted

            return result
        except anthropic.APIStatusError as e:
            log.exception("Claude API error (status %s)", e.status_code)
            if e.status_code in (402, 429):
                import pipeline.notify as notify_mod
                notifier = notify_mod.get()
                if notifier:
                    reason = "Insufficient credits" if e.status_code == 402 else "Rate limited"
                    import asyncio
                    asyncio.ensure_future(notifier.send(
                        f"Claude API: {reason}",
                        f"{reason} — classification falling back to manual review.\n\n{e.message}",
                        notify_mod.Priority.HIGH,
                    ))
            return None
        except anthropic.APIError:
            log.exception("Claude API error")
            return None

    async def _classify_step(self, envelope: Envelope) -> ClassifyResult | None:
        """Classify the document/image using Claude vision or text."""
        content = _build_content(envelope)
        if not content:
            return None

        response = self._client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=256,
            system=self._classify_prompt,
            messages=[{"role": "user", "content": content}],
        )

        text = response.content[0].text.strip()
        parsed = _parse_json(text)
        if not parsed or "label" not in parsed:
            log.warning("Claude classify returned unparseable response: %s", text[:200])
            return None

        label = parsed["label"]
        confidence = float(parsed.get("confidence", 0.8))

        reasoning = parsed.get("reasoning", "")
        log.info("Claude classify: %s @ %.2f — %s", label, confidence, reasoning)

        result = ClassifyResult(
            label=label,
            confidence=confidence,
            model=self.name,
        )
        if reasoning:
            result.extracted = {"_summary": reasoning}
        return result

    async def _extract_step(self, envelope: Envelope, label: str) -> dict | None:
        """Extract structured fields for a classified document."""
        content = _build_content(envelope)
        if not content:
            return None

        prompt = self._extract_prompt.replace("{{label}}", label)

        response = self._client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=prompt,
            messages=[{"role": "user", "content": content}],
        )

        text = response.content[0].text.strip()
        parsed = _parse_json(text)
        if not parsed:
            return parsed

        # Merge fields, tags, and correspondent into a flat dict
        result = parsed.get("fields", {}) if "fields" in parsed else parsed
        if "tags" in parsed:
            result["_tags"] = parsed["tags"]
        if "correspondent" in parsed:
            result["_correspondent"] = parsed["correspondent"]
        return result


def _downsize_image(raw: bytes, media_type: str) -> tuple[bytes, str]:
    """Shrink an image until its base64 encoding fits under the API limit.

    Progressively reduces JPEG quality, then scales down if still too large.
    Returns (image_bytes, media_type) — output is always JPEG if resizing was needed.
    """
    # Already small enough?
    if len(raw) * 4 // 3 <= _MAX_IMAGE_BYTES:
        return raw, media_type

    img = Image.open(io.BytesIO(raw))
    img = img.convert("RGB")  # drop alpha for JPEG

    # Try quality reduction first (95 → 60 in steps of 5)
    for quality in range(95, 55, -5):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality)
        if len(buf.getvalue()) * 4 // 3 <= _MAX_IMAGE_BYTES:
            log.info("Downsized image: quality=%d, %d→%d bytes", quality, len(raw), len(buf.getvalue()))
            return buf.getvalue(), "image/jpeg"

    # Quality alone wasn't enough — scale down in 20% steps
    w, h = img.size
    for scale in (0.8, 0.6, 0.4, 0.25):
        resized = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        buf = io.BytesIO()
        resized.save(buf, format="JPEG", quality=70)
        if len(buf.getvalue()) * 4 // 3 <= _MAX_IMAGE_BYTES:
            log.info("Downsized image: scale=%.0f%% quality=70, %d→%d bytes", scale * 100, len(raw), len(buf.getvalue()))
            return buf.getvalue(), "image/jpeg"

    # Last resort — return smallest attempt
    log.warning("Image still large after max downsize (%d bytes)", len(buf.getvalue()))
    return buf.getvalue(), "image/jpeg"


def _build_content(envelope: Envelope) -> list[dict] | None:
    """Build Claude message content — image or text depending on type."""
    if not envelope.raw_bytes:
        return None

    media_type = envelope.media_type or ""

    # Vision for supported image types
    if media_type in _VISION_TYPES:
        img_bytes, img_type = _downsize_image(envelope.raw_bytes, media_type)
        b64 = base64.standard_b64encode(img_bytes).decode()
        return [
            {
                "type": "image",
                "source": {"type": "base64", "media_type": img_type, "data": b64},
            },
            {"type": "text", "text": "Classify this image."},
        ]

    # PDF — send as document
    if media_type == "application/pdf":
        b64 = base64.standard_b64encode(envelope.raw_bytes).decode()
        return [
            {
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf", "data": b64},
            },
            {"type": "text", "text": "Classify this document."},
        ]

    # Plain text
    if media_type.startswith("text/"):
        text = envelope.raw_bytes.decode("utf-8", errors="replace")[:4000]
        return [{"type": "text", "text": f"Classify this document:\n\n{text}"}]

    log.warning("Unsupported media type for Claude: %s", media_type)
    return None


def _parse_json(text: str) -> dict | None:
    """Parse JSON from Claude response, stripping markdown fences if present."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first and last fence lines
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None
