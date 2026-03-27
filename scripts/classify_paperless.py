#!/usr/bin/env python3
"""Bulk classify Paperless documents — assign document types and correspondents.

Learns from already-classified docs, uses Claude Haiku for classification,
updates Paperless via API, and feeds results into the pipeline corpus.

Usage:
  python scripts/classify_paperless.py --dry-run --recent 2y
  python scripts/classify_paperless.py --recent 2y
  python scripts/classify_paperless.py --doc-id 1234
  python scripts/classify_paperless.py --all
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, UTC

import anthropic
import httpx

PAPERLESS_URL = os.environ.get("PAPERLESS_URL", "http://paperless:8000")
PAPERLESS_TOKEN = os.environ.get("PAPERLESS_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

_headers = {"Authorization": f"Token {PAPERLESS_TOKEN}"}


# ── Paperless API helpers ──────────────────────────────────────────────

def paperless_get(path, params=None):
    r = httpx.get(f"{PAPERLESS_URL}/api{path}", headers=_headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def paperless_patch(doc_id, data):
    r = httpx.patch(f"{PAPERLESS_URL}/api/documents/{doc_id}/", headers=_headers, json=data, timeout=30)
    r.raise_for_status()
    return r.json()


def paperless_post(endpoint, data):
    r = httpx.post(f"{PAPERLESS_URL}/api/{endpoint}/", headers=_headers, json=data, timeout=30)
    r.raise_for_status()
    return r.json()


def load_taxonomy():
    """Load all correspondents, document types, and tags from Paperless."""
    correspondents = {}
    page = 1
    while True:
        data = paperless_get("/correspondents/", {"page": page, "page_size": 100})
        for c in data["results"]:
            correspondents[c["name"]] = c["id"]
        if not data["next"]:
            break
        page += 1

    doc_types = {}
    page = 1
    while True:
        data = paperless_get("/document_types/", {"page": page, "page_size": 100})
        for dt in data["results"]:
            doc_types[dt["name"]] = dt["id"]
        if not data["next"]:
            break
        page += 1

    tags = {}
    page = 1
    while True:
        data = paperless_get("/tags/", {"page": page, "page_size": 100})
        for t in data["results"]:
            tags[t["name"]] = t["id"]
        if not data["next"]:
            break
        page += 1

    return correspondents, doc_types, tags


def load_training_examples():
    """Fetch well-classified docs (have both correspondent + type) as training data."""
    examples = []
    page = 1
    while True:
        data = paperless_get("/documents/", {
            "page": page,
            "page_size": 100,
            "correspondent__isnull": False,
            "document_type__isnull": False,
            "ordering": "-created",
        })
        for doc in data["results"]:
            examples.append({
                "title": doc["title"],
                "correspondent": doc["correspondent"],
                "document_type": doc["document_type"],
                "tags": doc["tags"],
            })
        if not data["next"]:
            break
        page += 1
    return examples


def resolve_name(endpoint, name, cache):
    """Find or create a Paperless entity by name. Returns PK."""
    # Case-insensitive lookup
    for existing_name, pk in cache.items():
        if existing_name.lower() == name.lower():
            return pk
    # Create new
    data = paperless_post(endpoint, {"name": name})
    pk = data["id"]
    cache[name] = pk
    print(f"    Created {endpoint}: {name} → {pk}")
    return pk


# ── Classification ─────────────────────────────────────────────────────

def build_system_prompt(correspondents, doc_types, tags, training_examples, corr_id_to_name, dt_id_to_name, tag_id_to_name):
    """Build Claude system prompt with taxonomy and training examples."""
    corr_list = "\n".join(f"  - {name}" for name in sorted(correspondents.keys()))
    type_list = "\n".join(f"  - {name}" for name in sorted(doc_types.keys()))

    # Build training examples (sample up to 100)
    examples = training_examples[:100]
    example_lines = []
    for ex in examples:
        corr_name = corr_id_to_name.get(ex["correspondent"], "?")
        dt_name = dt_id_to_name.get(ex["document_type"], "?")
        tag_names = [tag_id_to_name.get(t, "?") for t in (ex.get("tags") or [])]
        example_lines.append(f"  Title: {ex['title']}\n  → Type: {dt_name}, Correspondent: {corr_name}, Tags: {', '.join(tag_names)}")

    examples_text = "\n\n".join(example_lines)

    return f"""You are a document classifier for a personal document management system. Given a document's title and OCR content, assign:
1. **document_type** — one of the existing types, or suggest a new one if none fit
2. **correspondent** — the organisation/person the document is from. Use an existing correspondent if possible. Use "No Correspondent" for personal documents, photos, or items with no clear sender.
3. **tags** — 2-5 relevant tags from existing tags, or suggest new ones

## Existing Document Types
{type_list}

## Existing Correspondents
{corr_list}

## Training Examples (correctly classified documents)
{examples_text}

## Rules
- Prefer existing types and correspondents over creating new ones
- Use "No Correspondent" where no organisation makes sense
- Bank statements, utility bills, and financial docs should always have a correspondent
- Return ONLY valid JSON, no markdown fences:
{{"document_type": "Type Name", "correspondent": "Correspondent Name", "tags": ["tag1", "tag2"], "confidence": 0.9, "reasoning": "brief explanation"}}"""


def classify_document(client, system_prompt, title, content):
    """Classify a single document using Claude Haiku."""
    user_msg = f"Title: {title}\n\nContent (first 4000 chars):\n{content[:4000]}"

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=256,
        system=system_prompt,
        messages=[{"role": "user", "content": user_msg}],
    )

    text = response.content[0].text.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    return json.loads(text)


# ── Corpus integration ─────────────────────────────────────────────────

def record_corpus(doc_id, doc_type, correspondent, tags, content, confidence):
    """Record classification in pipeline corpus table."""
    if not DATABASE_URL:
        return
    try:
        import asyncio
        import asyncpg

        async def _insert():
            conn = await asyncpg.connect(DATABASE_URL)
            await conn.execute(
                """INSERT INTO corpus (item_id, document_type, extracted_fields, raw_text, confidence)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT DO NOTHING""",
                f"paperless-{doc_id}",
                doc_type,
                json.dumps({"correspondent": correspondent, "tags": tags}),
                content[:500],
                confidence,
            )
            await conn.close()

        asyncio.run(_insert())
    except Exception as e:
        print(f"    Corpus write failed: {e}")


# ── Main ───────────────────────────────────────────────────────────────

def parse_recent(value):
    """Parse '2y', '6m', '30d' into a date."""
    if value.endswith("y"):
        return datetime.now(UTC) - timedelta(days=int(value[:-1]) * 365)
    elif value.endswith("m"):
        return datetime.now(UTC) - timedelta(days=int(value[:-1]) * 30)
    elif value.endswith("d"):
        return datetime.now(UTC) - timedelta(days=int(value[:-1]))
    raise ValueError(f"Invalid period: {value}")


def main():
    parser = argparse.ArgumentParser(description="Bulk classify Paperless documents")
    parser.add_argument("--dry-run", action="store_true", help="Preview without updating")
    parser.add_argument("--recent", type=str, help="Period: 2y, 6m, 30d")
    parser.add_argument("--all", action="store_true", help="Process all unclassified")
    parser.add_argument("--doc-id", type=int, help="Process a single document")
    parser.add_argument("--batch-size", type=int, default=25)
    args = parser.parse_args()

    if not PAPERLESS_TOKEN:
        print("Error: PAPERLESS_API_KEY not set")
        return 1
    if not ANTHROPIC_KEY:
        print("Error: ANTHROPIC_API_KEY not set")
        return 1

    print("Loading taxonomy...")
    correspondents, doc_types, tags = load_taxonomy()
    print(f"  {len(correspondents)} correspondents, {len(doc_types)} doc types, {len(tags)} tags")

    # Ensure "No Correspondent" exists
    if "No Correspondent" not in correspondents:
        resolve_name("correspondents", "No Correspondent", correspondents)

    # Reverse maps for training examples
    corr_id_to_name = {v: k for k, v in correspondents.items()}
    dt_id_to_name = {v: k for k, v in doc_types.items()}
    tag_id_to_name = {v: k for k, v in tags.items()}

    print("Loading training examples...")
    training = load_training_examples()
    print(f"  {len(training)} well-classified documents as training data")

    system_prompt = build_system_prompt(correspondents, doc_types, tags, training, corr_id_to_name, dt_id_to_name, tag_id_to_name)

    # Build query params for target documents
    params = {"ordering": "-created", "page_size": args.batch_size}

    if args.doc_id:
        # Single document mode
        target_docs = [paperless_get(f"/documents/{args.doc_id}/")]
    else:
        if not args.all and not args.recent:
            print("Error: specify --recent, --all, or --doc-id")
            return 1

        if args.recent:
            since = parse_recent(args.recent)
            params["created__date__gte"] = since.strftime("%Y-%m-%d")

        # Fetch target docs (missing correspondent OR document type)
        target_docs = []
        page = 1
        while True:
            data = paperless_get("/documents/", {**params, "page": page})
            for doc in data["results"]:
                if doc["correspondent"] is None or doc["document_type"] is None:
                    target_docs.append(doc)
            if not data["next"]:
                break
            page += 1

    print(f"\n{len(target_docs)} documents to classify {'(DRY RUN)' if args.dry_run else ''}\n")

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    classified = 0
    skipped = 0
    errors = 0

    for i, doc in enumerate(target_docs):
        doc_id = doc["id"]
        title = doc.get("title", "Untitled")
        content = doc.get("content", "")

        # If we only have summary data (from list), fetch full doc for content
        if not content and not args.doc_id:
            try:
                full = paperless_get(f"/documents/{doc_id}/")
                content = full.get("content", "")
            except Exception as e:
                print(f"  [{i+1}/{len(target_docs)}] #{doc_id} — fetch error: {e}")
                errors += 1
                continue

        if not content or len(content.strip()) < 20:
            print(f"  [{i+1}/{len(target_docs)}] #{doc_id} {title[:50]} — skipped (no content)")
            skipped += 1
            continue

        try:
            result = classify_document(client, system_prompt, title, content)
        except Exception as e:
            print(f"  [{i+1}/{len(target_docs)}] #{doc_id} {title[:50]} — classification error: {e}")
            errors += 1
            time.sleep(1)
            continue

        assigned_type = result.get("document_type", "")
        assigned_corr = result.get("correspondent", "")
        assigned_tags = result.get("tags", [])
        confidence = result.get("confidence", 0)
        reasoning = result.get("reasoning", "")

        current_corr = corr_id_to_name.get(doc.get("correspondent"), "None")
        current_type = dt_id_to_name.get(doc.get("document_type"), "None")

        print(f"  [{i+1}/{len(target_docs)}] #{doc_id} {title[:60]}")
        print(f"    Type: {current_type} → {assigned_type}")
        print(f"    Corr: {current_corr} → {assigned_corr}")
        print(f"    Tags: {', '.join(assigned_tags)}")
        print(f"    Confidence: {confidence}  Reason: {reasoning[:80]}")

        if not args.dry_run:
            try:
                patch = {}

                # Only update fields that are currently unset
                if doc.get("document_type") is None and assigned_type:
                    type_id = resolve_name("document_types", assigned_type, doc_types)
                    patch["document_type"] = type_id

                if doc.get("correspondent") is None and assigned_corr:
                    corr_id = resolve_name("correspondents", assigned_corr, correspondents)
                    patch["correspondent"] = corr_id

                # Add new tags (don't remove existing)
                if assigned_tags:
                    existing_tag_ids = set(doc.get("tags") or [])
                    for tag_name in assigned_tags:
                        tag_id = resolve_name("tags", tag_name, tags)
                        existing_tag_ids.add(tag_id)
                    patch["tags"] = list(existing_tag_ids)

                if patch:
                    paperless_patch(doc_id, patch)
                    print(f"    ✓ Updated")
                    # Refresh reverse maps
                    corr_id_to_name = {v: k for k, v in correspondents.items()}
                    dt_id_to_name = {v: k for k, v in doc_types.items()}
                    tag_id_to_name = {v: k for k, v in tags.items()}

                record_corpus(doc_id, assigned_type, assigned_corr, assigned_tags, content, confidence)
            except Exception as e:
                print(f"    ✗ Update failed: {e}")
                errors += 1

        classified += 1
        time.sleep(1)  # Rate limit

    print(f"\nDone: {classified} classified, {skipped} skipped, {errors} errors")
    return 0


if __name__ == "__main__":
    sys.exit(main())
