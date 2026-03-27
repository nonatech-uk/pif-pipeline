"""Wine action handler — posts wine labels to the wine service."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)

WINE_SERVICE_URL = os.environ.get("WINE_SERVICE_URL", "http://wine:8200")
WINE_PIPELINE_SECRET = os.environ.get("WINE_PIPELINE_SECRET", "")


class WineHandler(ActionHandler):
    """Post a wine label to the wine service API."""

    name = "wine"

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        extracted = envelope.extracted
        if not extracted:
            return ActionResult(ok=False, destination=self.name, reason="No extracted fields")

        producer = extracted.get("producer", "").strip()
        wine_name = extracted.get("wine_name", "").strip()
        if not producer or not wine_name:
            return ActionResult(ok=False, destination=self.name, reason="Missing producer or wine_name")

        vintage = None
        raw_vintage = extracted.get("vintage")
        if raw_vintage:
            try:
                vintage = int(str(raw_vintage).strip())
            except ValueError:
                pass

        abv = None
        raw_abv = extracted.get("abv")
        if raw_abv:
            try:
                abv = float(str(raw_abv).strip().rstrip("%"))
            except ValueError:
                pass

        metadata = {
            "producer": producer,
            "wine_name": wine_name,
            "vintage": vintage,
            "region": extracted.get("region"),
            "country": extracted.get("country"),
            "grape_variety": extracted.get("grape_variety"),
            "abv": abv,
            "gps_lat": envelope.exif.gps_lat if envelope.exif else None,
            "gps_lng": envelope.exif.gps_lng if envelope.exif else None,
            "pipeline_item_id": envelope.id,
            "logged_at": envelope.received_at.isoformat(),
        }

        files = {}
        if envelope.raw_bytes and envelope.media_type and envelope.media_type.startswith("image/"):
            ext = _ext_for_mime(envelope.media_type)
            filename = f"{envelope.id}{ext}"
            files["label"] = (filename, envelope.raw_bytes, envelope.media_type)

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{WINE_SERVICE_URL}/api/v1/ingest",
                    data={"metadata": json.dumps(metadata)},
                    files=files or None,
                    headers={"X-Pipeline-Secret": WINE_PIPELINE_SECRET},
                )
                resp.raise_for_status()
                result = resp.json()
        except Exception as e:
            log.error("Wine ingest failed: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e))

        wine_id = result.get("wine_id", "?")
        log.info("Wine ingested: %s — %s %s (wine_id=%s)", envelope.id[:8], producer, wine_name, wine_id)
        return ActionResult(ok=True, destination=self.name, ref=str(wine_id))


def _ext_for_mime(mime: str) -> str:
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/heic": ".heic",
        "image/heif": ".heif",
    }.get(mime, ".jpg")
