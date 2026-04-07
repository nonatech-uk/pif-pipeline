"""Stuff action handler — decodes barcodes and posts items to the home inventory."""

from __future__ import annotations

import io
import logging
import re
from typing import Any

import httpx
from PIL import Image
from pyzbar.pyzbar import decode as pyzbar_decode

from pipeline.actions.base import ActionHandler
from pipeline.models import ActionResult, Envelope

log = logging.getLogger(__name__)

_ISBN_RE = re.compile(r"^(978|979)\d{10}$")


class StuffHandler(ActionHandler):
    """Decode barcode from image, look up product, create stuff item."""

    name = "stuff_barcode"

    def __init__(self, base_url: str, pipeline_secret: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._secret = pipeline_secret

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        if not envelope.raw_bytes:
            return ActionResult(ok=False, destination=self.name, reason="No image bytes")

        # Decode barcode from image
        barcode = self._decode_barcode(envelope.raw_bytes)
        if not barcode:
            return ActionResult(ok=False, destination=self.name, reason="No barcode detected in image")

        log.info("Decoded barcode: %s from %s", barcode, envelope.id[:8])

        # Determine type and look up
        is_isbn = bool(_ISBN_RE.match(barcode))
        lookup_result = await self._lookup(barcode, is_isbn)

        # Build item payload
        item: dict[str, Any] = {
            "barcode": barcode,
        }

        if is_isbn:
            item.update({
                "name": lookup_result.get("title", f"Book ({barcode})"),
                "description": lookup_result.get("description"),
                "category": "Media",
                "media_type": "book",
                "media_title": lookup_result.get("title"),
                "media_subtitle": lookup_result.get("subtitle"),
                "media_creator": lookup_result.get("creator"),
                "media_isbn": barcode,
                "media_cover_url": lookup_result.get("cover_url"),
                "media_publisher": lookup_result.get("publisher"),
                "media_pages": lookup_result.get("pages"),
                "media_format": lookup_result.get("physical_format"),
                "media_language": lookup_result.get("language"),
                "media_publish_date": lookup_result.get("publish_date"),
                "media_genre": ", ".join((lookup_result.get("subjects") or [])[:3]) or None,
            })
        else:
            item.update({
                "name": lookup_result.get("name", f"Product ({barcode})"),
                "brand": lookup_result.get("brand"),
                "category": lookup_result.get("category"),
            })

        # Attach Immich asset ID
        asset_id = _extract_asset_id(envelope)
        if asset_id:
            item["immich_asset_id"] = asset_id

        # POST to stuff ingest
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    f"{self._base_url}/api/v1/ingest",
                    json=item,
                    headers={"X-Pipeline-Secret": self._secret},
                )
                resp.raise_for_status()
                result = resp.json()
        except Exception as e:
            log.error("Stuff ingest failed: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e))

        item_id = result.get("item_id", "?")
        status = result.get("status", "created")
        log.info("Stuff item %s (%s): %s", item_id, status, item.get("name"))
        return ActionResult(ok=True, destination=self.name, ref=str(item_id))

    def _decode_barcode(self, raw_bytes: bytes) -> str | None:
        """Decode a barcode from image bytes using pyzbar."""
        try:
            img = Image.open(io.BytesIO(raw_bytes))
            results = pyzbar_decode(img)
            if results:
                return results[0].data.decode("utf-8")
        except Exception:
            log.exception("Barcode decode error")
        return None

    async def _lookup(self, barcode: str, is_isbn: bool) -> dict:
        """Look up barcode via OpenLibrary (ISBN) or Open Food Facts."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                if is_isbn:
                    resp = await client.get(
                        f"https://openlibrary.org/isbn/{barcode}.json",
                        follow_redirects=True,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        # Resolve authors
                        authors = []
                        for ref in data.get("authors", []):
                            key = ref.get("key", "")
                            if key:
                                a_resp = await client.get(
                                    f"https://openlibrary.org{key}.json",
                                    follow_redirects=True,
                                )
                                if a_resp.status_code == 200:
                                    authors.append(a_resp.json().get("name", ""))
                        cover_id = (data.get("covers") or [None])[0]

                        # Parse publish date
                        publish_date = None
                        raw_date = data.get("publish_date", "")
                        if raw_date:
                            from datetime import datetime as _dt
                            for fmt in ("%B %d, %Y", "%Y-%m-%d", "%Y"):
                                try:
                                    publish_date = _dt.strptime(raw_date, fmt).date().isoformat()
                                    break
                                except ValueError:
                                    continue

                        # Language codes
                        lang_map = {"eng": "English", "fre": "French", "ger": "German", "spa": "Spanish"}
                        languages = []
                        for lang_ref in data.get("languages", []):
                            code = lang_ref.get("key", "").rsplit("/", 1)[-1]
                            languages.append(lang_map.get(code, code))

                        # Fetch work-level description
                        description = None
                        works_list = data.get("works", [])
                        if works_list:
                            work_key = works_list[0].get("key", "")
                            if work_key:
                                w_resp = await client.get(
                                    f"https://openlibrary.org{work_key}.json",
                                    follow_redirects=True,
                                )
                                if w_resp.status_code == 200:
                                    w_data = w_resp.json()
                                    desc = w_data.get("description", "")
                                    if isinstance(desc, dict):
                                        desc = desc.get("value", "")
                                    description = desc or None

                        return {
                            "title": data.get("title"),
                            "subtitle": data.get("subtitle"),
                            "description": description,
                            "creator": ", ".join(authors) if authors else None,
                            "cover_url": f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else None,
                            "publisher": ", ".join(data.get("publishers", [])) or None,
                            "pages": data.get("number_of_pages"),
                            "physical_format": data.get("physical_format"),
                            "language": ", ".join(languages) if languages else None,
                            "publish_date": publish_date,
                            "subjects": data.get("subjects", []),
                        }
                else:
                    resp = await client.get(
                        f"https://world.openfoodfacts.org/api/v2/product/{barcode}.json",
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if data.get("status") == 1:
                            product = data.get("product", {})
                            return {
                                "name": product.get("product_name"),
                                "brand": product.get("brands"),
                                "category": product.get("categories"),
                            }
        except Exception:
            log.exception("Barcode lookup failed for %s", barcode)

        return {}


def _extract_asset_id(envelope: Envelope) -> str | None:
    if envelope.source_path and envelope.source_path.startswith("immich://"):
        return envelope.source_path.split("//", 1)[1]
    return None
