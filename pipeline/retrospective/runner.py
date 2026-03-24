"""Retrospective runner — bulk classify over Immich + Paperless corpus."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime, UTC
from typing import Any

from pydantic import BaseModel, Field

from pipeline.classify.tier_runner import TierRunner
from pipeline.feedback.corrections import CorrectionsTable
from pipeline.ingest.base import SourceWatcher
from pipeline.retrospective.reporter import RunFindings
from pipeline.retrospective.sources import ImmichCorpus, PaperlessCorpus

log = logging.getLogger(__name__)


class RunState(BaseModel):
    """State of a retrospective run."""

    run_id: str
    status: str = "pending"  # pending | running | paused | complete | error
    mode: str = "classify"  # classify | suggest | commit
    tier_ceiling: str = "clip"
    sample_pct: float = 10.0
    date_from: str | None = None
    date_to: str | None = None
    sources: list[str] = Field(default_factory=lambda: ["immich", "paperless"])

    processed: int = 0
    total: int = 0
    filed: int = 0
    exceptions: int = 0
    api_calls: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    findings: RunFindings = Field(default_factory=RunFindings)
    error_message: str | None = None


# In-memory run storage
_runs: dict[str, RunState] = {}
_pause_events: dict[str, asyncio.Event] = {}


class RetrospectiveRunner:
    """Orchestrates bulk classification runs."""

    def __init__(
        self,
        tier_runner: TierRunner,
        immich_corpus: ImmichCorpus | None,
        paperless_corpus: PaperlessCorpus | None,
        corrections: CorrectionsTable | None = None,
    ) -> None:
        self._tier_runner = tier_runner
        self._immich = immich_corpus
        self._paperless = paperless_corpus
        self._corrections = corrections

    async def start(
        self,
        mode: str = "classify",
        tier_ceiling: str = "clip",
        sample_pct: float = 10.0,
        date_from: str | None = None,
        date_to: str | None = None,
        sources: list[str] | None = None,
    ) -> str:
        """Start a retrospective run. Returns run_id."""
        run_id = uuid.uuid4().hex[:12]
        state = RunState(
            run_id=run_id,
            mode=mode,
            tier_ceiling=tier_ceiling,
            sample_pct=sample_pct,
            date_from=date_from,
            date_to=date_to,
            sources=sources or ["immich", "paperless"],
        )
        _runs[run_id] = state
        _pause_events[run_id] = asyncio.Event()
        _pause_events[run_id].set()  # not paused

        asyncio.create_task(self._run(state))
        return run_id

    def pause(self, run_id: str) -> bool:
        ev = _pause_events.get(run_id)
        if ev:
            ev.clear()
            state = _runs.get(run_id)
            if state:
                state.status = "paused"
            return True
        return False

    def resume(self, run_id: str) -> bool:
        ev = _pause_events.get(run_id)
        if ev:
            ev.set()
            state = _runs.get(run_id)
            if state:
                state.status = "running"
            return True
        return False

    @staticmethod
    def get_state(run_id: str) -> RunState | None:
        return _runs.get(run_id)

    @staticmethod
    def list_runs() -> list[RunState]:
        return sorted(_runs.values(), key=lambda r: r.started_at or datetime.min.replace(tzinfo=UTC), reverse=True)

    async def _run(self, state: RunState) -> None:
        state.status = "running"
        state.started_at = datetime.now(UTC)
        log.info("Retrospective %s starting: mode=%s ceiling=%s sample=%.0f%%",
                 state.run_id, state.mode, state.tier_ceiling, state.sample_pct)

        try:
            if "paperless" in state.sources and self._paperless:
                async for item in self._paperless.iter_documents(
                    date_from=state.date_from, date_to=state.date_to,
                    sample_pct=state.sample_pct,
                ):
                    await _pause_events[state.run_id].wait()
                    await self._process_paperless_item(state, item)

            if "immich" in state.sources and self._immich:
                async for item in self._immich.iter_assets(
                    date_from=state.date_from, date_to=state.date_to,
                    sample_pct=state.sample_pct,
                ):
                    await _pause_events[state.run_id].wait()
                    await self._process_immich_item(state, item)

            state.status = "complete"
        except Exception as e:
            log.exception("Retrospective %s failed", state.run_id)
            state.status = "error"
            state.error_message = str(e)
        finally:
            state.finished_at = datetime.now(UTC)
            log.info("Retrospective %s finished: %d processed, %d mismatches",
                     state.run_id, state.processed, state.findings.misclassified_count)

    async def _process_paperless_item(self, state: RunState, item: dict) -> None:
        """Classify a Paperless document and compare with current metadata."""
        state.processed += 1
        # For Paperless, we'd need to fetch the PDF bytes to classify
        # For now, record the existing classification for comparison
        # Full implementation would download and re-classify
        state.findings.record_classification("existing")

    async def _process_immich_item(self, state: RunState, item: dict) -> None:
        """Classify an Immich asset."""
        state.processed += 1
        # Full implementation would download bytes and run through tier_runner
        state.findings.record_classification("existing")
