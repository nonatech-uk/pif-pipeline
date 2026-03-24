"""Pipeline entrypoint — gathers all source watchers and processes Envelopes."""

from __future__ import annotations

import asyncio
import logging
import sys
import time

import uvicorn
from fastapi import FastAPI

from pipeline.actions import registry
from pipeline.audit.log import AuditLog
from pipeline.audit.models import ActionTrace, AuditEntry, DecisionTrace
from pipeline.classify.tier_runner import TierRunner
from pipeline.config import load_settings, Settings
from pipeline.exceptions.queue import ExceptionItem, ExceptionQueue
from pipeline.ingest.email import EmailWatcher
from pipeline.ingest.immich import ImmichWatcher, router as immich_router
from pipeline.ingest.scanner import ScannerWatcher
from pipeline.models import Envelope
from pipeline.rules.engine import RulesEngine
from pipeline.rules.loader import RulesLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("pipeline")

# Module-level references set during startup
_tier_runner: TierRunner | None = None
_rules_engine: RulesEngine | None = None
_audit_log: AuditLog | None = None
_exception_queue: ExceptionQueue | None = None


async def process_envelope(envelope: Envelope) -> None:
    """Process a single envelope through the full pipeline."""
    log.info(
        "INGEST  id=%s  source=%s  type=%s  size=%s  file=%s",
        envelope.id[:8], envelope.source_type, envelope.media_type,
        envelope.file_size, envelope.file_name,
    )

    # --- Step 1: Classify ---
    tier_traces = []
    if _tier_runner:
        envelope, tier_traces = await _tier_runner.run(envelope)

    if envelope.classification:
        log.info(
            "CLASSIFY  %s @ %.2f via %s",
            envelope.classification.label, envelope.classification.confidence, envelope.tier_used,
        )
    else:
        log.warning("CLASSIFY  no classification for %s", envelope.id[:8])

    # --- Step 2: Evaluate rules ---
    rule_traces = []
    fired_rules = []
    if _rules_engine:
        fired_rules, rule_traces = _rules_engine.evaluate(envelope)

    # --- Step 3: Execute actions ---
    action_traces = []
    destinations = []

    if fired_rules:
        for fired in fired_rules:
            for action_spec in fired.action_specs:
                handler = registry.get(action_spec.handler)
                if not handler:
                    log.warning("Unknown action handler: %s", action_spec.handler)
                    action_traces.append(ActionTrace(
                        handler=action_spec.handler, destination=action_spec.handler,
                        ok=False, reason="Handler not registered",
                    ))
                    continue

                t0 = time.monotonic()
                result = await handler.execute(envelope, action_spec.params)
                duration_ms = int((time.monotonic() - t0) * 1000)

                # Store result on envelope for cross-referencing between actions
                envelope.action_results[handler.name] = result
                destinations.append(handler.name)

                action_traces.append(ActionTrace(
                    handler=handler.name, destination=handler.name,
                    ok=result.ok, ref=result.ref, reason=result.reason,
                    duration_ms=duration_ms,
                ))

                log.info(
                    "ACTION  %s → %s (ref=%s, %dms)",
                    handler.name, "OK" if result.ok else "FAIL", result.ref, duration_ms,
                )
    else:
        # No rules matched — push to exception queue
        if _exception_queue:
            classification_output = envelope.classification.model_dump() if envelope.classification else {}
            envelope_dict = envelope.model_dump(exclude={"raw_bytes"})
            await _exception_queue.add(ExceptionItem(
                item_id=envelope.id,
                reason="No rules matched",
                review_priority=50,
                classification_output=classification_output,
                envelope_json=envelope_dict,
            ))
            destinations.append("exception_queue")
            log.info("EXCEPTION  no rules matched — queued for review")

    # --- Step 4: Write audit log ---
    if _audit_log:
        entry = AuditEntry(
            item_id=envelope.id,
            source_type=envelope.source_type,
            source_path=envelope.source_path,
            file_sha256=envelope.file_sha256,
            media_type=envelope.media_type,
            label=envelope.classification.label if envelope.classification else None,
            confidence=envelope.classification.confidence if envelope.classification else None,
            tier_used=envelope.tier_used,
            destinations=destinations,
            exception_queued=not fired_rules,
            trace=DecisionTrace(
                tiers=tier_traces,
                rules=rule_traces,
                actions=action_traces,
            ),
            extracted=envelope.extracted,
        )
        await _audit_log.write(entry)
        log.info("AUDIT  written for %s → %s", envelope.id[:8], destinations)


async def run_watcher(watcher, name: str) -> None:
    """Run a single watcher, processing each envelope it yields."""
    log.info("Starting watcher: %s", name)
    try:
        async for envelope in watcher.watch():
            try:
                await process_envelope(envelope)
            except Exception:
                log.exception("Failed to process envelope %s", envelope.id[:8])
    except asyncio.CancelledError:
        log.info("Watcher %s cancelled", name)
    except Exception:
        log.exception("Watcher %s crashed", name)


def create_app() -> FastAPI:
    """Create the FastAPI app with webhook routes and dashboard API."""
    from pipeline.api.app import create_dashboard_app

    app = create_dashboard_app()
    # Add ingest webhook routes to the same app
    app.include_router(immich_router)
    return app


async def main() -> None:
    global _tier_runner, _rules_engine, _audit_log, _exception_queue

    settings = load_settings()
    log.info("Pipeline starting — project root: %s", settings.project_root)
    log.info("Tier ceiling: %s", settings.tiers.ceiling)

    # Initialise core components
    _audit_log = AuditLog(settings.resolve_path(settings.paths.audit_log))
    _exception_queue = ExceptionQueue(settings.resolve_path(settings.paths.exceptions_db))
    _tier_runner = TierRunner(settings, _audit_log)

    rules_loader = RulesLoader(settings.project_root / "shared" / "rules.yaml")
    _rules_engine = RulesEngine(rules_loader)

    # Share instances with dashboard API
    from pipeline.api import deps as api_deps
    api_deps._settings = settings
    api_deps._audit_log = _audit_log
    api_deps._exception_queue = _exception_queue

    # Register action handlers
    registry.register_all(settings)
    log.info("Action handlers: %s", list(registry.all_handlers().keys()))

    # Build watchers
    watchers: list[tuple[str, object]] = []

    drop_folder = settings.resolve_path(settings.paths.drop_folder)
    watchers.append(("scanner", ScannerWatcher(drop_folder)))

    if settings.immich_api_key:
        watchers.append(("immich", ImmichWatcher(
            immich_url=settings.services.immich_url,
            api_key=settings.immich_api_key,
        )))
    else:
        log.warning("IMMICH_API_KEY not set — Immich watcher disabled")

    if settings.services.imap_user and settings.services.imap_password:
        watchers.append(("email", EmailWatcher(
            host=settings.services.imap_host,
            port=settings.services.imap_port,
            user=settings.services.imap_user,
            password=settings.services.imap_password,
        )))
    else:
        log.warning("IMAP credentials not set — Email watcher disabled")

    # Start FastAPI server for webhooks
    app = create_app()
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning")
    server = uvicorn.Server(config)

    tasks = [asyncio.create_task(server.serve(), name="uvicorn")]
    for name, watcher in watchers:
        tasks.append(asyncio.create_task(run_watcher(watcher, name), name=name))

    log.info("Pipeline running — %d watchers, tier ceiling=%s", len(watchers), settings.tiers.ceiling)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
