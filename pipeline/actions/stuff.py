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

    def __init__(self, base_url: str, pipeline_secret: str, anthropic_api_key: str = "") -> None:
        self._base_url = base_url.rstrip("/")
        self._secret = pipeline_secret
        self._api_key = anthropic_api_key

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
            # If OpenLibrary has no description, extract blurb from the back cover
            description = lookup_result.get("description")
            if not description and self._api_key and envelope.raw_bytes:
                description = await self._extract_blurb(envelope.raw_bytes, envelope.media_type or "image/jpeg")

            item.update({
                "name": lookup_result.get("title", f"Book ({barcode})"),
                "description": description,
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

    async def _extract_blurb(self, raw_bytes: bytes, media_type: str) -> str | None:
        """Use Claude vision to read the blurb/description from a book's back cover."""
        import anthropic
        import base64

        try:
            client = anthropic.AsyncAnthropic(api_key=self._api_key)
            message = await client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=512,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": base64.b64encode(raw_bytes).decode(),
                            },
                        },
                        {
                            "type": "text",
                            "text": "This is the back cover of a book. Extract the book's description or blurb text — the paragraph(s) that describe what the book is about. Return ONLY the blurb text, nothing else. If there is no blurb visible, respond with just: NONE",
                        },
                    ],
                }],
            )
            text = message.content[0].text.strip()
            if text.upper() == "NONE":
                return None
            log.info("Extracted blurb (%d chars) from back cover", len(text))
            return text
        except Exception:
            log.exception("Back cover blurb extraction failed")
            return None

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


class StuffBookHandler(ActionHandler):
    """Identify a book from its cover photo using Claude vision, look up metadata, create stuff item."""

    name = "stuff_book"

    def __init__(self, base_url: str, pipeline_secret: str, anthropic_api_key: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._secret = pipeline_secret
        self._api_key = anthropic_api_key

    async def execute(self, envelope: Envelope, params: dict[str, Any]) -> ActionResult:
        if not envelope.raw_bytes:
            return ActionResult(ok=False, destination=self.name, reason="No image bytes")

        # Extract title/author from cover using Claude vision
        book_info = await self._extract_book_info(envelope.raw_bytes, envelope.media_type or "image/jpeg")
        if not book_info or not book_info.get("title"):
            return ActionResult(ok=False, destination=self.name, reason="Could not identify book from cover")

        title = book_info["title"]
        author = book_info.get("author")
        log.info("Extracted book info: %s by %s from %s", title, author, envelope.id[:8])

        # Search OpenLibrary by title/author
        lookup_result = await self._search_openlibrary(title, author)

        # Build item payload
        item: dict[str, Any] = {
            "name": lookup_result.get("title", title),
            "category": "Media",
            "media_type": "book",
            "media_title": lookup_result.get("title", title),
            "media_subtitle": lookup_result.get("subtitle"),
            "media_creator": lookup_result.get("creator") or author,
            "media_isbn": lookup_result.get("isbn"),
            "media_cover_url": lookup_result.get("cover_url"),
            "media_publisher": lookup_result.get("publisher"),
            "media_pages": lookup_result.get("pages"),
            "media_format": lookup_result.get("physical_format"),
            "media_language": lookup_result.get("language"),
            "media_publish_date": lookup_result.get("publish_date"),
            "media_genre": ", ".join((lookup_result.get("subjects") or [])[:3]) or None,
            "description": lookup_result.get("description"),
        }

        asset_id = _extract_asset_id(envelope)
        if asset_id:
            item["immich_asset_id"] = asset_id

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
            log.error("Stuff book ingest failed: %s", e)
            return ActionResult(ok=False, destination=self.name, reason=str(e))

        item_id = result.get("item_id", "?")
        status = result.get("status", "created")
        log.info("Stuff book %s (%s): %s by %s", item_id, status, title, author)
        return ActionResult(ok=True, destination=self.name, ref=str(item_id))

    async def _extract_book_info(self, raw_bytes: bytes, media_type: str) -> dict | None:
        """Use Claude vision to extract title and author from a book cover."""
        import anthropic
        import base64
        import json as json_mod

        try:
            client = anthropic.AsyncAnthropic(api_key=self._api_key)
            message = await client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=256,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": base64.b64encode(raw_bytes).decode(),
                            },
                        },
                        {
                            "type": "text",
                            "text": 'Extract the book title and author from this cover photo. Respond with JSON only, no markdown fences: {"title": "...", "author": "..."}. If you cannot determine a field, use null.',
                        },
                    ],
                }],
            )
            text = message.content[0].text.strip()
            # Strip markdown fences if present
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                text = text.rsplit("```", 1)[0].strip()
            return json_mod.loads(text)
        except Exception:
            log.exception("Book cover extraction failed")
            return None

    async def _search_openlibrary(self, title: str, author: str | None) -> dict:
        """Search OpenLibrary by title/author and return metadata."""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                params = {"title": title, "limit": "3"}
                if author:
                    params["author"] = author
                resp = await client.get(
                    "https://openlibrary.org/search.json",
                    params=params,
                )
                if resp.status_code != 200:
                    return {}

                data = resp.json()
                docs = data.get("docs", [])
                if not docs:
                    log.info("OpenLibrary: no results for '%s' by '%s'", title, author)
                    return {}

                doc = docs[0]
                log.info("OpenLibrary match: '%s' by %s", doc.get("title"), doc.get("author_name"))

                # Get ISBN if available, then do full edition lookup
                isbns = doc.get("isbn", [])
                isbn_13 = next((i for i in isbns if len(i) == 13), None)
                if isbn_13:
                    edition = await self._lookup_isbn(client, isbn_13)
                    if edition:
                        return edition

                # Fall back to search result metadata
                cover_id = doc.get("cover_i")
                subjects = doc.get("subject", [])
                return {
                    "title": doc.get("title"),
                    "creator": ", ".join(doc.get("author_name", [])) or None,
                    "isbn": isbn_13,
                    "cover_url": f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else None,
                    "publisher": ", ".join(doc.get("publisher", [])[:2]) or None,
                    "pages": doc.get("number_of_pages_median"),
                    "publish_date": f"{doc['first_publish_year']}-01-01" if doc.get("first_publish_year") else None,
                    "subjects": subjects[:5],
                }
        except Exception:
            log.exception("OpenLibrary search failed for '%s'", title)
        return {}

    async def _lookup_isbn(self, client: httpx.AsyncClient, isbn: str) -> dict | None:
        """Full edition lookup by ISBN — same logic as StuffHandler._lookup for ISBN."""
        try:
            resp = await client.get(
                f"https://openlibrary.org/isbn/{isbn}.json",
                follow_redirects=True,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()

            # Resolve authors
            authors = []
            for ref in data.get("authors", []):
                key = ref.get("key", "")
                if key:
                    a_resp = await client.get(f"https://openlibrary.org{key}.json", follow_redirects=True)
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

            # Work-level description
            description = None
            works_list = data.get("works", [])
            if works_list:
                work_key = works_list[0].get("key", "")
                if work_key:
                    w_resp = await client.get(f"https://openlibrary.org{work_key}.json", follow_redirects=True)
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
                "isbn": isbn,
                "cover_url": f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else None,
                "publisher": ", ".join(data.get("publishers", [])) or None,
                "pages": data.get("number_of_pages"),
                "physical_format": data.get("physical_format"),
                "language": ", ".join(languages) if languages else None,
                "publish_date": publish_date,
                "subjects": data.get("subjects", []),
            }
        except Exception:
            log.exception("ISBN lookup failed for %s", isbn)
        return None


def _extract_asset_id(envelope: Envelope) -> str | None:
    if envelope.source_path and envelope.source_path.startswith("immich://"):
        return envelope.source_path.split("//", 1)[1]
    return None
