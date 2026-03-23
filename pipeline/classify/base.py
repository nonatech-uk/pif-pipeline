"""Abstract base for all classifier tiers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from pipeline.models import ClassifyResult, Envelope


class Classifier(ABC):
    """Base class for a single classifier tier."""

    name: str = "unknown"

    @abstractmethod
    async def classify(self, envelope: Envelope) -> ClassifyResult | None:
        """Classify an envelope. Return None to escalate to the next tier."""
        ...
