"""Corpus iterators for Immich and Paperless — used by the retrospective runner."""

from __future__ import annotations

import logging
from typing import Any, AsyncGenerator

import httpx

log = logging.getLogger(__name__)


class ImmichCorpus:
    """Async iterator over Immich assets with rate limiting."""

    def __init__(self, base_url: str, api_key: str, concurrency: int = 6) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._concurrency = concurrency

    async def iter_assets(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        sample_pct: float = 100.0,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Yield (asset_id, metadata, bytes) for each asset."""
        headers = {"x-api-key": self._api_key}
        page = 1
        page_size = 100
        count = 0

        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                params: dict[str, Any] = {"page": page, "size": page_size}
                if date_from:
                    params["takenAfter"] = date_from
                if date_to:
                    params["takenBefore"] = date_to

                resp = await client.get(
                    f"{self._base_url}/api/assets",
                    headers=headers,
                    params=params,
                )
                if resp.status_code != 200:
                    log.error("Immich assets fetch failed: HTTP %d", resp.status_code)
                    break

                assets = resp.json()
                if not assets:
                    break

                for asset in assets:
                    count += 1
                    # Sample
                    if sample_pct < 100 and (count % int(100 / sample_pct)) != 0:
                        continue

                    yield {
                        "asset_id": asset["id"],
                        "metadata": asset,
                        "source": "immich",
                    }

                if len(assets) < page_size:
                    break
                page += 1

        log.info("Immich corpus: yielded from %d assets", count)


class PaperlessCorpus:
    """Async iterator over Paperless documents."""

    def __init__(self, base_url: str, api_token: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = api_token

    async def iter_documents(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        sample_pct: float = 100.0,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Yield document metadata for each Paperless document."""
        headers = {"Authorization": f"Token {self._token}"}
        page = 1
        count = 0

        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                params: dict[str, Any] = {"page": page, "page_size": 100}
                if date_from:
                    params["created__date__gte"] = date_from
                if date_to:
                    params["created__date__lte"] = date_to

                resp = await client.get(
                    f"{self._base_url}/api/documents/",
                    headers=headers,
                    params=params,
                )
                if resp.status_code != 200:
                    log.error("Paperless fetch failed: HTTP %d", resp.status_code)
                    break

                data = resp.json()
                results = data.get("results", [])
                if not results:
                    break

                for doc in results:
                    count += 1
                    if sample_pct < 100 and (count % int(100 / sample_pct)) != 0:
                        continue

                    yield {
                        "doc_id": doc["id"],
                        "metadata": doc,
                        "current_type": doc.get("document_type"),
                        "current_correspondent": doc.get("correspondent"),
                        "current_tags": doc.get("tags", []),
                        "title": doc.get("title", ""),
                        "source": "paperless",
                    }

                if not data.get("next"):
                    break
                page += 1

        log.info("Paperless corpus: yielded from %d documents", count)
