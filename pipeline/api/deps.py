"""Shared dependencies for dashboard API routes."""

from __future__ import annotations

from pipeline.audit.log import AuditLog
from pipeline.config import load_settings, Settings
from pipeline.exceptions.queue import ExceptionQueue
from pipeline.feedback.corrections import CorrectionsTable
from pipeline.retrospective.runner import RetrospectiveRunner

_settings: Settings | None = None
_audit_log: AuditLog | None = None
_exception_queue: ExceptionQueue | None = None
_corrections: CorrectionsTable | None = None
_retrospective_runner: RetrospectiveRunner | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


def get_audit_log() -> AuditLog:
    global _audit_log
    if _audit_log is None:
        s = get_settings()
        _audit_log = AuditLog(s.resolve_path(s.paths.audit_log))
    return _audit_log


def get_exception_queue() -> ExceptionQueue:
    global _exception_queue
    if _exception_queue is None:
        s = get_settings()
        _exception_queue = ExceptionQueue(s.resolve_path(s.paths.exceptions_db))
    return _exception_queue


def get_retrospective_runner() -> RetrospectiveRunner | None:
    return _retrospective_runner


def get_corrections() -> CorrectionsTable:
    global _corrections
    if _corrections is None:
        s = get_settings()
        _corrections = CorrectionsTable(s.resolve_path(s.paths.corrections_db))
    return _corrections
