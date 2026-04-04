"""Lightweight feature-usage tracker — async variant (asyncpg).

Buffers API hits and page-views in memory, flushing to a dedicated
``usage`` PostgreSQL database every FLUSH_INTERVAL seconds via an
asyncio background task.  Completely opt-in: if *dsn* is empty the
tracker is a silent no-op.
"""

import asyncio
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone

import asyncpg
from fastapi import APIRouter, Request

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FLUSH_INTERVAL = 30  # seconds
_SKIP_PREFIXES = ("/health", "/assets")
_SKIP_EXTENSIONS = (".js", ".css", ".ico", ".png", ".jpg", ".svg", ".woff", ".woff2", ".map")
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I,
)
_INT_SEGMENT_RE = re.compile(r"(?<=/)\d+(?=/|$)")

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------
_app_name: str = ""
_pool: asyncpg.Pool | None = None
_lock = asyncio.Lock()
_api_buf: dict[tuple, int] = defaultdict(int)
_pv_buf: dict[tuple, int] = defaultdict(int)
_flush_task: asyncio.Task | None = None


# ---------------------------------------------------------------------------
# Path normalisation
# ---------------------------------------------------------------------------
def _normalise(path: str) -> str:
    path = _UUID_RE.sub("{id}", path)
    path = _INT_SEGMENT_RE.sub("{id}", path)
    return path


def _current_hour() -> datetime:
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Background flush
# ---------------------------------------------------------------------------
async def _flush() -> None:
    async with _lock:
        api_snap = dict(_api_buf)
        pv_snap = dict(_pv_buf)
        _api_buf.clear()
        _pv_buf.clear()

    if not api_snap and not pv_snap:
        return

    try:
        if _pool is None:
            return
        async with _pool.acquire() as conn:
            for (app, method, path, email, status, hour), count in api_snap.items():
                await conn.execute(
                    """INSERT INTO api_usage (app, method, path, user_email, status_code, hour, count)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)
                       ON CONFLICT (app, method, path, user_email, status_code, hour)
                       DO UPDATE SET count = api_usage.count + EXCLUDED.count""",
                    app, method, path, email, status, hour, count,
                )
            for (app, path, email, hour), count in pv_snap.items():
                await conn.execute(
                    """INSERT INTO page_views (app, path, user_email, hour, count)
                       VALUES ($1, $2, $3, $4, $5)
                       ON CONFLICT (app, path, user_email, hour)
                       DO UPDATE SET count = page_views.count + EXCLUDED.count""",
                    app, path, email, hour, count,
                )
    except Exception:
        log.exception("usage_tracker: flush failed")


async def _flush_loop() -> None:
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        await _flush()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
async def init_usage_tracker(app_name: str, dsn: str) -> None:
    """Initialise the tracker.  *dsn* may be empty to disable tracking."""
    global _app_name, _pool, _flush_task
    _app_name = app_name
    if not dsn:
        log.info("usage_tracker: disabled (no USAGE_DSN)")
        return
    try:
        _pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        _flush_task = asyncio.create_task(_flush_loop())
        log.info("usage_tracker: enabled for %s", app_name)
    except Exception:
        log.exception("usage_tracker: failed to connect — tracking disabled")
        _pool = None


async def shutdown_usage_tracker() -> None:
    global _pool, _flush_task
    if _flush_task is not None:
        _flush_task.cancel()
        try:
            await _flush_task
        except asyncio.CancelledError:
            pass
    await _flush()  # final drain
    if _pool is not None:
        await _pool.close()
        _pool = None


async def track_usage_middleware(request: Request, call_next):
    """FastAPI HTTP middleware that records endpoint usage."""
    path = request.url.path
    if not _pool or any(path.startswith(p) for p in _SKIP_PREFIXES):
        return await call_next(request)
    if any(path.endswith(ext) for ext in _SKIP_EXTENSIONS):
        return await call_next(request)

    response = await call_next(request)

    norm = _normalise(path)
    email = request.headers.get("Remote-Email", "")
    key = (_app_name, request.method, norm, email, response.status_code, _current_hour())
    async with _lock:
        _api_buf[key] += 1
    return response


# ---------------------------------------------------------------------------
# Pageview endpoint
# ---------------------------------------------------------------------------
usage_pageview_router = APIRouter()


@usage_pageview_router.post("/usage/pageview", status_code=204)
async def record_pageview(request: Request):
    if not _pool:
        return
    body = await request.json()
    pv_path = body.get("path", "")
    if not pv_path:
        return
    email = request.headers.get("Remote-Email", "")
    key = (_app_name, pv_path, email, _current_hour())
    async with _lock:
        _pv_buf[key] += 1
