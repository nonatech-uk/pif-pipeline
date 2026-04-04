"""Pipeline entrypoint — gathers all source watchers and processes Envelopes."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from pipeline.actions import registry
from pipeline.audit.log import AuditLog
from pipeline.audit.models import ActionTrace, AuditEntry, DecisionTrace
from pipeline.classify.tier_runner import TierRunner
from pipeline.config import load_settings, Settings
from pipeline.api.usage_tracker import init_usage_tracker, shutdown_usage_tracker
from pipeline.db import init_pool, close_pool
from pipeline.exceptions.queue import ExceptionItem, ExceptionQueue
from pipeline.ingest.email import EmailWatcher
from pipeline.ingest.immich import ImmichWatcher
from pipeline.ingest.scanner import ScannerWatcher
from pipeline.models import Envelope
from pipeline.notify import Notifier, Priority
import pipeline.notify as notify_mod
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

    # --- Step 0: Check for previously unsubscribed senders ---
    if envelope.source_type == "email" and envelope.source_email_from and envelope.source_email_to:
        from pipeline.unsubscribe.processor import check_unsubscribed_sender
        unsub_addr = await check_unsubscribed_sender(envelope.source_email_from, envelope.source_email_to)
        if unsub_addr:
            log.warning("Email from unsubscribed sender: %s → %s", unsub_addr, envelope.source_email_to)
            notifier = notify_mod.get()
            if notifier:
                await notifier.send(
                    f"Unsubscribed sender reappeared: {unsub_addr}",
                    f"From: {envelope.source_email_from}\nTo: {envelope.source_email_to}\nSubject: {envelope.source_email_subject}\n\nThis sender was previously unsubscribed from.",
                    Priority.HIGH,
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
            classification_output = envelope.classification.model_dump(mode="json") if envelope.classification else {}
            envelope_dict = envelope.model_dump(mode="json", exclude={"raw_bytes"})
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
        audit_extracted = dict(envelope.extracted)
        if envelope.source_email_from:
            audit_extracted["_email_from"] = envelope.source_email_from
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
            extracted=audit_extracted,
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
                notifier = notify_mod.get()
                if notifier:
                    await notifier.send(
                        f"Processing failed: {envelope.file_name or envelope.id[:8]}",
                        f"Source: {envelope.source_type}\nFile: {envelope.file_name}\nError during pipeline processing — item queued for manual review.",
                        Priority.HIGH,
                    )
    except asyncio.CancelledError:
        log.info("Watcher %s cancelled", name)
    except Exception:
        log.exception("Watcher %s crashed", name)
        notifier = notify_mod.get()
        if notifier:
            await notifier.send(
                f"Watcher crashed: {name}",
                f"The {name} watcher has crashed and is no longer polling. Pipeline restart required.",
                Priority.HIGH,
            )


def create_app() -> FastAPI:
    """Create the FastAPI app with webhook routes and dashboard API."""
    from pipeline.api.app import create_dashboard_app
    from pipeline.feedback.webhook import router as feedback_router

    from pipeline.api.app import mount_static

    app = create_dashboard_app()
    # Add ingest + feedback webhook routes to the same app
    app.include_router(feedback_router)
    # Static files must be mounted last — catch-all route
    mount_static(app)
    return app


async def main() -> None:
    global _tier_runner, _rules_engine, _audit_log, _exception_queue

    settings = load_settings()
    log.info("Pipeline starting — project root: %s", settings.project_root)
    log.info("Tier ceiling: %s", settings.tiers.ceiling)

    # Initialise notifications
    notifier = Notifier(
        pushover_app_token=settings.pushover_app_token,
        pushover_user_key=settings.pushover_user_key,
        smtp_host=settings.services.imap_host,
        smtp_port=465,
        smtp_user=settings.services.imap_user,
        smtp_password=settings.services.imap_password,
        email_to=settings.services.imap_user,
    )
    notify_mod.configure(notifier)
    log.info("Notifications: pushover=%s email=%s", notifier.pushover_enabled, notifier.email_enabled)

    # Initialise database pool
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    await init_pool(database_url)

    # Usage tracking
    usage_dsn = os.environ.get("USAGE_DSN", "")
    await init_usage_tracker("pipeline", usage_dsn)

    # Initialise core components
    _audit_log = AuditLog()
    _exception_queue = ExceptionQueue()
    _tier_runner = TierRunner(settings, _audit_log)

    rules_loader = RulesLoader(settings.project_root / "shared" / "rules.yaml")
    _rules_engine = RulesEngine(rules_loader)

    # Corrections table
    from pipeline.feedback.corrections import CorrectionsTable
    corrections = CorrectionsTable()

    # Share instances with dashboard API
    from pipeline.api import deps as api_deps
    api_deps._settings = settings
    api_deps._audit_log = _audit_log
    api_deps._exception_queue = _exception_queue
    api_deps._corrections = corrections
    api_deps._rules_loader = rules_loader

    # Retrospective runner
    from pipeline.retrospective.runner import RetrospectiveRunner
    from pipeline.retrospective.sources import ImmichCorpus, PaperlessCorpus
    immich_corpus = ImmichCorpus(settings.services.immich_url, settings.immich_api_key) if settings.immich_api_key else None
    paperless_corpus = PaperlessCorpus(settings.services.paperless_url, settings.paperless_api_key) if settings.paperless_api_key else None
    api_deps._retrospective_runner = RetrospectiveRunner(
        tier_runner=_tier_runner,
        immich_corpus=immich_corpus,
        paperless_corpus=paperless_corpus,
        corrections=corrections,
    )

    # Configure feedback webhook
    from pipeline.feedback import webhook as feedback_webhook
    feedback_webhook.configure(
        audit_log=_audit_log,
        corrections=corrections,
        paperless_url=settings.services.paperless_url,
        paperless_token=settings.paperless_api_key,
        webhook_secret=os.environ.get("PAPERLESS_WEBHOOK_SECRET", ""),
    )

    # Register action handlers
    registry.register_all(settings)
    log.info("Action handlers: %s", list(registry.all_handlers().keys()))

    # Build watchers
    watchers: list[tuple[str, object]] = []

    drop_folder = settings.resolve_path(settings.paths.drop_folder)
    processed_folder = Path(os.environ.get("SCANNER_PROCESSED_DIR", ""))
    watchers.append(("scanner", ScannerWatcher(
        drop_folder,
        processed_folder=processed_folder if processed_folder.parts else None,
    )))

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

    # Unsubscribe processor (standalone task, not a watcher)
    unsub_processor = None
    if settings.services.imap_user and settings.services.imap_password and settings.anthropic_api_key:
        from pipeline.unsubscribe.processor import UnsubscribeProcessor
        unsub_processor = UnsubscribeProcessor(
            host=settings.services.imap_host,
            port=settings.services.imap_port,
            user=settings.services.imap_user,
            password=settings.services.imap_password,
            anthropic_api_key=settings.anthropic_api_key,
        )
    else:
        log.warning("IMAP or Anthropic credentials not set — Unsubscribe processor disabled")

    # Start FastAPI server for webhooks
    app = create_app()
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning")
    server = uvicorn.Server(config)

    tasks = [asyncio.create_task(server.serve(), name="uvicorn")]
    for name, watcher in watchers:
        tasks.append(asyncio.create_task(run_watcher(watcher, name), name=name))
    if unsub_processor:
        tasks.append(asyncio.create_task(unsub_processor.run(), name="unsubscribe"))

    log.info("Pipeline running — %d watchers, tier ceiling=%s", len(watchers), settings.tiers.ceiling)

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_usage_tracker()
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
