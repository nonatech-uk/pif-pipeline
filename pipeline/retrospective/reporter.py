"""Findings aggregation for retrospective runs."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RunFindings(BaseModel):
    """Aggregated findings from a retrospective run."""

    misclassified_count: int = 0
    new_pet_examples: int = 0
    clip_calibration_points: int = 0
    geo_albums_enriched: int = 0
    exception_count: int = 0
    tier_distribution: dict[str, int] = Field(default_factory=dict)
    mismatches: list[dict] = Field(default_factory=list)

    def record_classification(self, tier: str) -> None:
        self.tier_distribution[tier] = self.tier_distribution.get(tier, 0) + 1

    def record_mismatch(self, doc_id: str, current: str | None, predicted: str | None, confidence: float) -> None:
        self.misclassified_count += 1
        self.mismatches.append({
            "doc_id": str(doc_id),
            "current": current,
            "predicted": predicted,
            "confidence": confidence,
        })

    def summary(self) -> dict:
        return {
            "misclassified": self.misclassified_count,
            "new_pet_examples": self.new_pet_examples,
            "clip_calibration": self.clip_calibration_points,
            "geo_enriched": self.geo_albums_enriched,
            "exceptions": self.exception_count,
            "tier_distribution": self.tier_distribution,
            "mismatches_sample": self.mismatches[:20],
        }
