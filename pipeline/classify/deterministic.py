"""Deterministic classifier — free, instant checks based on metadata."""

from __future__ import annotations

import logging
import re

from pipeline.audit.log import AuditLog
from pipeline.classify.base import Classifier
from pipeline.models import ClassifyResult, Envelope

log = logging.getLogger(__name__)

# Filename patterns → suggested labels
_FILENAME_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"boarding", re.I), "boarding_pass"),
    (re.compile(r"invoice", re.I), "invoice"),
    (re.compile(r"receipt", re.I), "receipt"),
    (re.compile(r"statement", re.I), "bank_statement"),
    (re.compile(r"insurance|policy", re.I), "insurance_policy"),
    (re.compile(r"tax|hmrc|p60|p45", re.I), "tax_document"),
    (re.compile(r"wine|label|vin|chateau|domaine", re.I), "wine_label"),
]


class DeterministicClassifier(Classifier):
    """Tier 1: metadata-based classification — GPS, MIME, filename, duplicates."""

    name = "deterministic"

    def __init__(self, audit_log: AuditLog | None = None) -> None:
        self._audit_log = audit_log

    async def classify(self, envelope: Envelope) -> ClassifyResult | None:
        # 1. Duplicate check via SHA256
        if self._audit_log and envelope.file_sha256:
            existing = self._audit_log.get_by_sha256(envelope.file_sha256)
            if existing:
                log.info("Duplicate detected: sha256=%s", envelope.file_sha256[:12])
                return ClassifyResult(
                    label="duplicate",
                    confidence=1.0,
                    model=self.name,
                )

        # 2. GPS present on an image → geo_tagged
        if (
            envelope.exif
            and envelope.exif.gps_lat is not None
            and envelope.exif.gps_lng is not None
            and envelope.media_type
            and envelope.media_type.startswith("image/")
        ):
            log.info("GPS-tagged image: (%s, %s)", envelope.exif.gps_lat, envelope.exif.gps_lng)
            return ClassifyResult(
                label="geo_tagged",
                confidence=1.0,
                model=self.name,
            )

        # 3. Filename pattern matching
        if envelope.file_name:
            for pattern, label in _FILENAME_PATTERNS:
                if pattern.search(envelope.file_name):
                    log.info("Filename match: '%s' → %s", envelope.file_name, label)
                    return ClassifyResult(
                        label=label,
                        confidence=0.85,
                        model=self.name,
                    )

        # No confident match — escalate
        return None
