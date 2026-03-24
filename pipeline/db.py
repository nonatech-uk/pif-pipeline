"""PostgreSQL connection pool for the pipeline."""

from __future__ import annotations

import logging

import asyncpg

log = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_pool(dsn: str) -> asyncpg.Pool:
    """Create the shared connection pool and run schema migrations."""
    global _pool
    _pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)

    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA)
    log.info("Database pool ready (%s)", dsn.split("@")[-1] if "@" in dsn else dsn)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool not initialised — call init_pool() first")
    return _pool


_SCHEMA = """
CREATE TABLE IF NOT EXISTS exceptions (
    item_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    review_priority INTEGER NOT NULL DEFAULT 50,
    classification_output JSONB NOT NULL DEFAULT '{}',
    envelope_json JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'pending',
    triage_action TEXT,
    triage_destination TEXT,
    triage_reason TEXT,
    triaged_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_exceptions_status ON exceptions(status);
CREATE INDEX IF NOT EXISTS idx_exceptions_priority ON exceptions(review_priority);

CREATE TABLE IF NOT EXISTS corrections (
    id SERIAL PRIMARY KEY,
    correction_type TEXT NOT NULL,
    field TEXT NOT NULL,
    original_value TEXT,
    corrected_value TEXT,
    item_id TEXT,
    label TEXT,
    tier_used TEXT,
    confidence REAL,
    proposed_action JSONB,
    status TEXT NOT NULL DEFAULT 'pending',
    accepted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_corrections_status ON corrections(status);

CREATE TABLE IF NOT EXISTS corpus (
    id SERIAL PRIMARY KEY,
    item_id TEXT NOT NULL,
    document_type TEXT NOT NULL,
    extracted_fields JSONB NOT NULL DEFAULT '{}',
    raw_text TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_corpus_type ON corpus(document_type);

CREATE TABLE IF NOT EXISTS audit_log (
    item_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_type TEXT NOT NULL,
    source_path TEXT,
    file_sha256 TEXT,
    media_type TEXT,
    label TEXT,
    confidence REAL,
    tier_used TEXT,
    destinations TEXT[] NOT NULL DEFAULT '{}',
    exception_queued BOOLEAN NOT NULL DEFAULT false,
    trace JSONB NOT NULL DEFAULT '{}',
    extracted JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_log_item_id ON audit_log(item_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_log_sha256 ON audit_log(file_sha256);
"""
