#!/usr/bin/env python3
"""Download all Vivino label images and update wine_log URLs.

Usage:
    python3 scripts/download_vivino_labels.py [DATABASE_URL]

Downloads label images from Vivino URLs in wine_log, saves them to
data/wine-labels/, and updates the database URLs to wine.mees.st/labels/...
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from pathlib import Path
from urllib.parse import urlparse

import asyncpg
import httpx

LABELS_DIR = Path("/zfs/Apps/AppData/pipeline/data/wine-labels")
BASE_URL = "https://wine.mees.st"
DEFAULT_DSN = "postgresql://pipeline:xiAsvijVRs3WME6UbpDbfspt@localhost:5432/pipeline"
CONCURRENCY = 10


async def main() -> None:
    dsn = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DSN
    conn = await asyncpg.connect(dsn)
    LABELS_DIR.mkdir(parents=True, exist_ok=True)

    # Get all vivino-sourced log entries with Vivino image URLs
    rows = await conn.fetch("""
        SELECT id, wine_id, label_image_url
        FROM wine_log
        WHERE source = 'vivino'
          AND label_image_url IS NOT NULL
          AND label_image_url LIKE 'https://images.vivino.com/%'
    """)
    print(f"Found {len(rows)} Vivino label images to download")

    sem = asyncio.Semaphore(CONCURRENCY)
    downloaded = 0
    skipped = 0
    failed = 0

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        async def download_one(row: asyncpg.Record) -> tuple[str, int, str | None]:
            nonlocal downloaded, skipped, failed
            log_id = row["id"]
            wine_id = row["wine_id"]
            url = row["label_image_url"]

            # Generate a stable filename from the URL
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            ext = Path(urlparse(url).path).suffix or ".jpg"
            filename = f"vivino-{wine_id}-{url_hash}{ext}"
            filepath = LABELS_DIR / filename
            new_url = f"{BASE_URL}/labels/{filename}"

            if filepath.exists():
                skipped += 1
                return "skip", log_id, new_url

            async with sem:
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200 and len(resp.content) > 100:
                        filepath.write_bytes(resp.content)
                        downloaded += 1
                        return "ok", log_id, new_url
                    else:
                        failed += 1
                        return "fail", log_id, None
                except Exception as e:
                    failed += 1
                    print(f"  Error downloading {url}: {e}")
                    return "fail", log_id, None

        tasks = [download_one(row) for row in rows]
        results = await asyncio.gather(*tasks)

    # Update database URLs for successful downloads
    update_count = 0
    for status, log_id, new_url in results:
        if new_url:
            await conn.execute(
                "UPDATE wine_log SET label_image_url = $1 WHERE id = $2",
                new_url, log_id,
            )
            update_count += 1

    # Fix ownership
    print(f"\nDownloaded: {downloaded}")
    print(f"Already had: {skipped}")
    print(f"Failed: {failed}")
    print(f"DB URLs updated: {update_count}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
