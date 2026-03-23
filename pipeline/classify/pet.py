"""Pet classifier — few-shot pet identification using Claude vision."""

from __future__ import annotations

import base64
import json
import logging
import mimetypes
from pathlib import Path

import anthropic

from pipeline.classify.base import Classifier
from pipeline.models import ClassifyResult, Envelope

log = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent / "prompts" / "pet_recognition.txt"

# Max reference images per pet
_MAX_REF_IMAGES = 4


class PetClassifier(Classifier):
    """Second-pass classifier: identifies which known pet appears in a photo.

    Called only when main classification label is pet_photo.
    """

    name = "pet"

    def __init__(self, api_key: str, pets_dir: Path) -> None:
        self._client = anthropic.Anthropic(api_key=api_key)
        self._pets_dir = pets_dir
        self._prompt_template = _PROMPT_PATH.read_text()
        self._pet_names = self._discover_pets()

    def _discover_pets(self) -> list[str]:
        """Find pet directories with reference images."""
        if not self._pets_dir.exists():
            return []
        names = []
        for d in sorted(self._pets_dir.iterdir()):
            if d.is_dir() and any(d.iterdir()):
                names.append(d.name)
        log.info("Pet classifier: %d pets found: %s", len(names), names)
        return names

    async def classify(self, envelope: Envelope) -> ClassifyResult | None:
        if not envelope.raw_bytes or not self._pet_names:
            return None

        media_type = envelope.media_type or ""
        if not media_type.startswith("image/"):
            return None

        try:
            return await self._identify(envelope)
        except anthropic.APIError:
            log.exception("Pet recognition API error")
            return None

    async def _identify(self, envelope: Envelope) -> ClassifyResult | None:
        """Run few-shot pet identification."""
        content: list[dict] = []

        # Add reference images for each pet
        for pet_name in self._pet_names:
            pet_dir = self._pets_dir / pet_name
            images = sorted(pet_dir.glob("*"))[:_MAX_REF_IMAGES]
            for img_path in images:
                mime = mimetypes.guess_type(str(img_path))[0]
                if not mime or not mime.startswith("image/"):
                    continue
                b64 = base64.standard_b64encode(img_path.read_bytes()).decode()
                content.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": mime, "data": b64},
                })
                content.append({
                    "type": "text",
                    "text": f"Reference photo of {pet_name.replace('_', ' ')}",
                })

        # Add query image
        b64 = base64.standard_b64encode(envelope.raw_bytes).decode()
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": envelope.media_type, "data": b64},
        })
        content.append({"type": "text", "text": "Query image — identify which pets (if any) appear."})

        pet_list = ", ".join(n.replace("_", " ") for n in self._pet_names)
        system_prompt = self._prompt_template.replace("{{pet_list}}", pet_list)

        response = self._client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=256,
            system=system_prompt,
            messages=[{"role": "user", "content": content}],
        )

        text = response.content[0].text.strip()
        parsed = _parse_json(text)
        if not parsed or "pets" not in parsed:
            log.warning("Pet recognition unparseable: %s", text[:200])
            return None

        pets = parsed["pets"]
        if not pets:
            log.info("No known pets recognised")
            return None

        # Return top pet match
        top = max(pets, key=lambda p: p.get("confidence", 0))
        log.info("Pet recognised: %s @ %.2f", top["name"], top["confidence"])

        return ClassifyResult(
            label="pet_photo",
            confidence=top["confidence"],
            model=self.name,
            extracted={"pets": pets},
        )


def _parse_json(text: str) -> dict | None:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None
