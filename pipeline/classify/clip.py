"""CLIP zero-shot classifier — free, local image classification."""

from __future__ import annotations

import io
import logging
from typing import Any

from pipeline.classify.base import Classifier
from pipeline.config import ClipLabel
from pipeline.models import ClassifyResult, Envelope

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:
    pass

log = logging.getLogger(__name__)

# Lazy-loaded model components
_model: Any = None
_processor: Any = None


def _load_model() -> tuple[Any, Any]:
    """Lazy-load CLIP model and processor on first use."""
    global _model, _processor
    if _model is not None:
        return _model, _processor

    log.info("Loading CLIP model (first use)...")
    import torch
    from transformers import CLIPModel, CLIPProcessor

    model_name = "openai/clip-vit-base-patch32"
    _processor = CLIPProcessor.from_pretrained(model_name)
    _model = CLIPModel.from_pretrained(model_name)
    _model.eval()
    log.info("CLIP model loaded")
    return _model, _processor


class CLIPClassifier(Classifier):
    """Tier 2: CLIP zero-shot image classification against configured labels."""

    name = "clip"

    def __init__(self, labels: dict[str, ClipLabel]) -> None:
        self._labels = labels

    async def classify(self, envelope: Envelope) -> ClassifyResult | None:
        # CLIP only works on images
        if not envelope.media_type or not envelope.media_type.startswith("image/"):
            log.debug("CLIP skipping non-image: %s", envelope.media_type)
            return None

        if not envelope.raw_bytes:
            return None

        if not self._labels:
            log.warning("No CLIP labels configured")
            return None

        try:
            return await self._run_clip(envelope)
        except Exception:
            log.exception("CLIP classification failed")
            return None

    async def _run_clip(self, envelope: Envelope) -> ClassifyResult | None:
        import torch
        from PIL import Image

        model, processor = _load_model()

        image = Image.open(io.BytesIO(envelope.raw_bytes)).convert("RGB")

        label_names = list(self._labels.keys())
        # Build natural language prompts for zero-shot
        text_prompts = [f"a photo of a {name.replace('_', ' ')}" for name in label_names]

        inputs = processor(text=text_prompts, images=image, return_tensors="pt", padding=True)

        with torch.no_grad():
            outputs = model(**inputs)
            logits = outputs.logits_per_image[0]
            probs = logits.softmax(dim=0)

        scores = {name: round(float(prob), 4) for name, prob in zip(label_names, probs)}

        # Find top label
        top_name = max(scores, key=scores.get)
        top_score = scores[top_name]
        threshold = self._labels[top_name].threshold

        log.info("CLIP top: %s @ %.3f (threshold %.2f)", top_name, top_score, threshold)

        if top_score >= threshold:
            return ClassifyResult(
                label=top_name,
                confidence=top_score,
                model=self.name,
                all_labels=scores,
            )

        # Below threshold — escalate
        log.info("CLIP below threshold, escalating")
        return None
