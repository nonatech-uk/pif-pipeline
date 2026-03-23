"""Condition evaluators — each takes an Envelope and condition params, returns bool."""

from __future__ import annotations

import logging
import math
from typing import Any

from pipeline.models import Envelope

log = logging.getLogger(__name__)


def evaluate_condition(condition_type: str, params: dict[str, Any], envelope: Envelope) -> bool:
    """Dispatch to the appropriate condition evaluator."""
    evaluator = _EVALUATORS.get(condition_type)
    if not evaluator:
        log.warning("Unknown condition type: %s", condition_type)
        return False
    return evaluator(params, envelope)


def _classification(params: dict, envelope: Envelope) -> bool:
    """Match on classification label and optional minimum confidence."""
    if not envelope.classification:
        return False

    label_spec = params.get("label")
    if label_spec is None:
        return False

    # Support single label or list of labels
    if isinstance(label_spec, list):
        labels = label_spec
    else:
        labels = [label_spec]

    if envelope.classification.label not in labels:
        return False

    min_conf = params.get("min_confidence", 0.0)
    return envelope.classification.confidence >= min_conf


def _gps_proximity(params: dict, envelope: Envelope) -> bool:
    """Match if envelope GPS is within radius_km of a point."""
    if not envelope.exif or envelope.exif.gps_lat is None or envelope.exif.gps_lng is None:
        return False

    lat = params.get("lat")
    lng = params.get("lng")
    radius_km = params.get("radius_km", 1.0)

    if lat is None or lng is None:
        return False

    dist = _haversine(envelope.exif.gps_lat, envelope.exif.gps_lng, lat, lng)
    return dist <= radius_km


def _media_type(params: dict, envelope: Envelope) -> bool:
    """Match on MIME type, supports wildcards like image/*."""
    if not envelope.media_type:
        return False

    pattern = params.get("value", "")
    if pattern.endswith("/*"):
        prefix = pattern[:-1]  # "image/"
        return envelope.media_type.startswith(prefix)
    return envelope.media_type == pattern


def _date_range(params: dict, envelope: Envelope) -> bool:
    """Match if EXIF taken_at falls within a date range."""
    if not envelope.exif or not envelope.exif.taken_at:
        return False

    from datetime import date, datetime

    taken = envelope.exif.taken_at.date()

    date_from = params.get("from")
    date_to = params.get("to")

    if date_from:
        if isinstance(date_from, str):
            date_from = date.fromisoformat(date_from)
        if taken < date_from:
            return False

    if date_to:
        if isinstance(date_to, str):
            date_to = date.fromisoformat(date_to)
        if taken > date_to:
            return False

    return True


def _source_type(params: dict, envelope: Envelope) -> bool:
    """Match on source type (scanner, camera, email)."""
    value = params.get("value", "")
    if isinstance(value, list):
        return envelope.source_type in value
    return envelope.source_type == value


def _pet_recognition(params: dict, envelope: Envelope) -> bool:
    """Match if a specific pet was recognised with minimum confidence."""
    pets = envelope.extracted.get("pets", [])
    if not pets:
        return False

    pet_name = params.get("pet")
    min_conf = params.get("min_confidence", 0.5)

    for p in pets:
        if p.get("name") == pet_name and p.get("confidence", 0) >= min_conf:
            return True
    return False


def _empty(params: dict, envelope: Envelope) -> bool:
    """Always matches — catch-all condition."""
    return True


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in kilometres."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


_EVALUATORS = {
    "classification": _classification,
    "gps_proximity": _gps_proximity,
    "media_type": _media_type,
    "date_range": _date_range,
    "source_type": _source_type,
    "pet_recognition": _pet_recognition,
    "empty": _empty,
}
