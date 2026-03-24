"""Shared dependencies for dashboard API routes."""

from __future__ import annotations

from pipeline.audit.log import AuditLog
from pipeline.config import load_settings, Settings
from pipeline.exceptions.queue import ExceptionQueue
from pipeline.feedback.corrections import CorrectionsTable
from pipeline.retrospective.runner import RetrospectiveRunner
from pipeline.rules.loader import RulesLoader

_settings: Settings | None = None
_audit_log: AuditLog | None = None
_exception_queue: ExceptionQueue | None = None
_corrections: CorrectionsTable | None = None
_retrospective_runner: RetrospectiveRunner | None = None
_rules_loader: RulesLoader | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


def get_audit_log() -> AuditLog:
    global _audit_log
    if _audit_log is None:
        _audit_log = AuditLog()
    return _audit_log


def get_exception_queue() -> ExceptionQueue:
    global _exception_queue
    if _exception_queue is None:
        _exception_queue = ExceptionQueue()
    return _exception_queue


def get_retrospective_runner() -> RetrospectiveRunner | None:
    return _retrospective_runner


def get_corrections() -> CorrectionsTable:
    global _corrections
    if _corrections is None:
        _corrections = CorrectionsTable()
    return _corrections


def get_rules_loader() -> RulesLoader:
    if _rules_loader is None:
        raise RuntimeError("Rules loader not initialised")
    return _rules_loader
