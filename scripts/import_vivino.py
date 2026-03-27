#!/usr/bin/env python3
"""One-off import of Vivino export data into the pipeline wine tables.

Usage:
    python3 scripts/import_vivino.py [DATABASE_URL]

Default DSN: postgresql://pipeline:xiAsvijVRs3WME6UbpDbfspt@localhost:5432/pipeline
"""

from __future__ import annotations

import asyncio
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

VIVINO_DIR = Path("/zfs/tank/home/stu/doc-central/Code/vivino_data")
DEFAULT_DSN = "postgresql://pipeline:xiAsvijVRs3WME6UbpDbfspt@localhost:5432/pipeline"


def parse_vintage(v: str) -> int | None:
    if not v or not v.strip():
        return None
    try:
        return int(v.strip())
    except ValueError:
        return None


def parse_float(v: str) -> float | None:
    if not v or not v.strip():
        return None
    try:
        return float(v.strip())
    except ValueError:
        return None


def parse_int(v: str) -> int | None:
    if not v or not v.strip():
        return None
    try:
        return int(v.strip())
    except ValueError:
        return None


def parse_dt(v: str) -> datetime | None:
    if not v or not v.strip():
        return None
    v = v.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(v, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def read_csv(name: str) -> list[dict[str, str]]:
    path = VIVINO_DIR / name
    if not path.exists():
        print(f"  Skipping {name} — not found")
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


async def upsert_wine(conn: asyncpg.Connection, row: dict[str, str]) -> int:
    """Upsert a wine record and return its id."""
    return await conn.fetchval("""
        INSERT INTO wine (producer, name, vintage, region, country, wine_type,
                          style, vivino_url, vivino_avg_rating, vivino_ratings_count,
                          drinking_window)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (producer, name, COALESCE(vintage, 0)) DO UPDATE
            SET region = COALESCE(wine.region, EXCLUDED.region),
                country = COALESCE(wine.country, EXCLUDED.country),
                wine_type = COALESCE(wine.wine_type, EXCLUDED.wine_type),
                style = COALESCE(wine.style, EXCLUDED.style),
                vivino_url = COALESCE(wine.vivino_url, EXCLUDED.vivino_url),
                vivino_avg_rating = COALESCE(EXCLUDED.vivino_avg_rating, wine.vivino_avg_rating),
                vivino_ratings_count = COALESCE(EXCLUDED.vivino_ratings_count, wine.vivino_ratings_count),
                drinking_window = COALESCE(wine.drinking_window, EXCLUDED.drinking_window),
                updated_at = now()
        RETURNING id
    """,
        row.get("Winery", "").strip(),
        row.get("Wine name", "").strip(),
        parse_vintage(row.get("Vintage", "")),
        row.get("Region", "").strip() or None,
        row.get("Country", "").strip() or None,
        row.get("Wine type", "").strip() or None,
        row.get("Regional wine style", "").strip() or None,
        row.get("Link to wine", "").strip() or None,
        parse_float(row.get("Average rating", "")),
        parse_int(row.get("Wine ratings count", "")),
        row.get("Drinking Window", "").strip() or None,
    )


async def main() -> None:
    dsn = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DSN
    conn = await asyncpg.connect(dsn)
    print(f"Connected to {dsn.split('@')[-1]}")

    try:
        # --- Step 1: Import full_wine_list.csv ---
        print("\n--- Importing full_wine_list.csv ---")
        rows = read_csv("full_wine_list.csv")
        wine_count = 0
        log_count = 0
        for row in rows:
            producer = row.get("Winery", "").strip()
            wine_name = row.get("Wine name", "").strip()
            if not producer or not wine_name:
                continue

            wine_id = await upsert_wine(conn, row)
            wine_count += 1

            # Create log entry for each scan
            logged_at = parse_dt(row.get("Scan date", ""))
            if not logged_at:
                continue

            rating = parse_float(row.get("Your rating", ""))
            review = row.get("Your review", "").strip() or None
            personal_note = row.get("Personal Note", "").strip() or None
            location = row.get("Scan/Review Location", "").strip() or None
            label_image = row.get("Label image", "").strip() or None

            # Avoid duplicate log entries
            existing = await conn.fetchval("""
                SELECT id FROM wine_log
                WHERE wine_id = $1 AND logged_at = $2 AND source = 'vivino'
            """, wine_id, logged_at)
            if existing:
                continue

            await conn.execute("""
                INSERT INTO wine_log (wine_id, logged_at, source, rating, review,
                                      personal_note, location, label_image_url)
                VALUES ($1, $2, 'vivino', $3, $4, $5, $6, $7)
            """, wine_id, logged_at, rating, review, personal_note, location, label_image)
            log_count += 1

        print(f"  Wines upserted: {wine_count}, log entries created: {log_count}")

        # --- Step 2: Correlate label_scans.csv GPS ---
        print("\n--- Correlating label_scans.csv GPS data ---")
        scans = read_csv("label_scans.csv")
        # Build a lookup: label URL -> (lat, lng)
        gps_lookup: dict[str, tuple[float, float]] = {}
        for scan in scans:
            url = scan.get("Label Photo", "").strip()
            loc = scan.get("Client Location", "").strip()
            if url and loc and "," in loc:
                parts = loc.split(",", 1)
                lat = parse_float(parts[0])
                lng = parse_float(parts[1])
                if lat is not None and lng is not None:
                    gps_lookup[url] = (lat, lng)

        # Update wine_log entries that have matching label URLs
        updated = 0
        logs_with_images = await conn.fetch("""
            SELECT id, label_image_url FROM wine_log
            WHERE label_image_url IS NOT NULL AND gps_lat IS NULL AND source = 'vivino'
        """)
        for log_row in logs_with_images:
            coords = gps_lookup.get(log_row["label_image_url"])
            if coords:
                await conn.execute("""
                    UPDATE wine_log SET gps_lat = $1, gps_lng = $2 WHERE id = $3
                """, coords[0], coords[1], log_row["id"])
                updated += 1
        print(f"  Log entries updated with GPS: {updated} (of {len(gps_lookup)} scans with coords)")

        # --- Step 3: Import cellar.csv ---
        print("\n--- Importing cellar.csv ---")
        rows = read_csv("cellar.csv")
        cellar_count = 0
        for row in rows:
            producer = row.get("Winery", "").strip()
            wine_name = row.get("Wine name", "").strip()
            if not producer or not wine_name:
                continue

            wine_id = await upsert_wine(conn, row)
            quantity = parse_int(row.get("User cellar count", "")) or 0

            await conn.execute("""
                INSERT INTO wine_cellar (wine_id, quantity, storage_location)
                VALUES ($1, $2, NULL)
                ON CONFLICT (wine_id, storage_location) DO UPDATE
                    SET quantity = EXCLUDED.quantity, updated_at = now()
            """, wine_id, quantity)
            cellar_count += 1
        print(f"  Cellar entries: {cellar_count}")

        # --- Step 4: Import wishlisted.csv ---
        print("\n--- Importing wishlisted.csv ---")
        rows = read_csv("wishlisted.csv")
        wish_count = 0
        for row in rows:
            producer = row.get("Winery", "").strip()
            wine_name = row.get("Wine name", "").strip()
            if not producer or not wine_name:
                continue

            wine_id = await upsert_wine(conn, row)
            wishlisted_at = parse_dt(row.get("Wishlisted date", ""))
            personal_note = row.get("Personal Note", "").strip() or None

            await conn.execute("""
                INSERT INTO wine_wishlist (wine_id, wishlisted_at, notes)
                VALUES ($1, $2, $3)
                ON CONFLICT (wine_id) DO NOTHING
            """, wine_id, wishlisted_at or datetime.now(timezone.utc), personal_note)
            wish_count += 1
        print(f"  Wishlist entries: {wish_count}")

        # --- Summary ---
        total_wines = await conn.fetchval("SELECT count(*) FROM wine")
        total_logs = await conn.fetchval("SELECT count(*) FROM wine_log")
        total_cellar = await conn.fetchval("SELECT count(*) FROM wine_cellar")
        total_wish = await conn.fetchval("SELECT count(*) FROM wine_wishlist")
        print(f"\n--- Done ---")
        print(f"  Total wines: {total_wines}")
        print(f"  Total log entries: {total_logs}")
        print(f"  Total cellar entries: {total_cellar}")
        print(f"  Total wishlist entries: {total_wish}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
