"""Tier runner — escalation ladder: deterministic → CLIP → (LLM stub) → Claude.

Stops at the first tier that returns a result with confidence >= threshold.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from pipeline.audit.log import AuditLog
from pipeline.audit.models import TierTrace
from pipeline.classify.base import Classifier
from pipeline.classify.clip import CLIPClassifier
from pipeline.classify.claude import ClaudeClassifier
from pipeline.classify.deterministic import DeterministicClassifier
from pipeline.classify.pet import PetClassifier
from pipeline.config import Settings
from pipeline.models import ClassifyResult, Envelope

log = logging.getLogger(__name__)

# Tier order and which ceiling value enables each
_TIER_ORDER = ["deterministic", "clip", "llm", "claude"]


class TierRunner:
    """Runs classifiers in escalation order, stopping on confident results."""

    def __init__(self, settings: Settings, audit_log: AuditLog) -> None:
        self._settings = settings
        self._audit_log = audit_log
        self._ceiling = settings.tiers.ceiling
        self._tiers: list[Classifier] = self._build_tiers()
        self._pet_classifier = self._build_pet_classifier()

    def _build_tiers(self) -> list[Classifier]:
        """Build the classifier chain up to the configured ceiling."""
        ceiling_idx = _TIER_ORDER.index(self._ceiling) if self._ceiling in _TIER_ORDER else len(_TIER_ORDER) - 1
        tiers: list[Classifier] = []

        # Tier 1: Deterministic (always)
        if ceiling_idx >= 0:
            tiers.append(DeterministicClassifier(audit_log=self._audit_log))

        # Tier 2: CLIP (if ceiling >= clip)
        if ceiling_idx >= 1 and self._settings.clip_labels:
            tiers.append(CLIPClassifier(labels=self._settings.clip_labels))

        # Tier 3: Local LLM — stub for now (phase 10)
        # if ceiling_idx >= 2: tiers.append(LocalLLMClassifier(...))

        # Tier 4: Claude (if ceiling >= claude)
        if ceiling_idx >= 3 and self._settings.anthropic_api_key:
            tiers.append(ClaudeClassifier(
                api_key=self._settings.anthropic_api_key,
                threshold=self._settings.classifier.claude_threshold_default,
            ))

        log.info("Tier runner: ceiling=%s, active tiers=%s",
                 self._ceiling, [t.name for t in tiers])
        return tiers

    def _build_pet_classifier(self) -> PetClassifier | None:
        """Build pet classifier if API key and pet photos exist."""
        if not self._settings.anthropic_api_key:
            return None
        pets_dir = self._settings.resolve_path(self._settings.paths.pets_dir)
        if not pets_dir.exists():
            return None
        classifier = PetClassifier(
            api_key=self._settings.anthropic_api_key,
            pets_dir=pets_dir,
        )
        if classifier._pet_names:
            return classifier
        return None

    async def run(self, envelope: Envelope) -> tuple[Envelope, list[TierTrace]]:
        """Run the classification tiers. Updates the envelope in-place and returns traces."""
        traces: list[TierTrace] = []

        for tier in self._tiers:
            t0 = time.monotonic()
            try:
                result = await tier.classify(envelope)
            except Exception:
                log.exception("Tier %s failed", tier.name)
                traces.append(TierTrace(
                    tier=tier.name,
                    skipped=True,
                    skip_reason="exception",
                    duration_ms=int((time.monotonic() - t0) * 1000),
                ))
                continue

            duration_ms = int((time.monotonic() - t0) * 1000)

            if result is None:
                traces.append(TierTrace(
                    tier=tier.name,
                    skipped=True,
                    skip_reason="no_match",
                    duration_ms=duration_ms,
                ))
                continue

            traces.append(TierTrace(
                tier=tier.name,
                label=result.label,
                confidence=result.confidence,
                all_labels=result.all_labels,
                duration_ms=duration_ms,
            ))

            # Record all scores from this tier
            if result.all_labels:
                envelope.all_tier_scores[tier.name] = result.all_labels

            # Set classification on the envelope
            envelope.classification = result
            envelope.tier_used = tier.name
            if result.extracted:
                envelope.extracted.update(result.extracted)

            log.info("Classified by %s: %s @ %.2f (%dms)",
                     tier.name, result.label, result.confidence, duration_ms)

            if result.needs_escalation:
                log.info("Marginal confidence — escalating to next tier for verification")
                continue

            break

        # If classified by a non-Claude tier but label needs extraction, run Claude extraction
        if (
            envelope.classification
            and envelope.tier_used != "claude"
            and envelope.classification.label in ClaudeClassifier.EXTRACTABLE_LABELS
            and not any(k in envelope.extracted for k in ("date", "vendor", "merchant", "amount"))
        ):
            claude_tier = next((t for t in self._tiers if isinstance(t, ClaudeClassifier)), None)
            if claude_tier:
                t0 = time.monotonic()
                try:
                    extracted = await claude_tier._extract_step(envelope, envelope.classification.label)
                    duration_ms = int((time.monotonic() - t0) * 1000)
                    if extracted:
                        envelope.extracted.update(extracted)
                        log.info("Extraction by claude for %s (%dms)",
                                 envelope.classification.label, duration_ms)
                except Exception:
                    log.exception("Claude extraction failed")

        # Second pass: pet recognition if classified as pet_photo
        if (
            envelope.classification
            and envelope.classification.label == "pet_photo"
            and self._pet_classifier
        ):
            t0 = time.monotonic()
            try:
                pet_result = await self._pet_classifier.classify(envelope)
                duration_ms = int((time.monotonic() - t0) * 1000)
                if pet_result and pet_result.extracted:
                    envelope.extracted.update(pet_result.extracted)
                    traces.append(TierTrace(
                        tier="pet",
                        label="pet_photo",
                        confidence=pet_result.confidence,
                        duration_ms=duration_ms,
                    ))
            except Exception:
                log.exception("Pet classifier failed")

        return envelope, traces
