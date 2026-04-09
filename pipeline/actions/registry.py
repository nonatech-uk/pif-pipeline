"""Action handler registry — maps handler names to instances."""

from __future__ import annotations

import logging
from typing import Any

from pipeline.actions.base import ActionHandler
from pipeline.config import Settings

log = logging.getLogger(__name__)

_registry: dict[str, ActionHandler] = {}


def register(handler: ActionHandler) -> None:
    """Register a handler by name."""
    _registry[handler.name] = handler
    log.debug("Registered action handler: %s", handler.name)


def get(name: str) -> ActionHandler | None:
    """Look up a handler by name."""
    return _registry.get(name)


def all_handlers() -> dict[str, ActionHandler]:
    """Return all registered handlers."""
    return dict(_registry)


def register_all(settings: Settings) -> None:
    """Register all built-in handlers with the given settings."""
    from pipeline.actions.paperless import PaperlessHandler
    from pipeline.actions.finance import FinanceHandler
    from pipeline.actions.immich_album import ImmichAlbumHandler
    from pipeline.actions.immich_tag import ImmichTagHandler
    from pipeline.actions.location import LocationHandler
    from pipeline.actions.exception_queue import ExceptionQueueHandler
    from pipeline.actions.notify import NotifyHandler

    if settings.paperless_api_key:
        register(PaperlessHandler(
            base_url=settings.services.paperless_url,
            api_token=settings.paperless_api_key,
        ))
    else:
        log.warning("PAPERLESS_API_KEY not set — paperless handler disabled")

    register(FinanceHandler(base_url=settings.services.finance_url))

    if settings.immich_api_key:
        register(ImmichAlbumHandler(
            base_url=settings.services.immich_url,
            api_key=settings.immich_api_key,
        ))
        register(ImmichTagHandler(
            base_url=settings.services.immich_url,
            api_key=settings.immich_api_key,
        ))
    else:
        log.warning("IMMICH_API_KEY not set — immich handlers disabled")

    if settings.services.location_secret:
        register(LocationHandler(
            base_url=settings.services.location_url,
            secret=settings.services.location_secret,
        ))
    else:
        log.warning("LOCATION_SECRET not set — location handler disabled")
    from pipeline.actions.wine import WineHandler
    from pipeline.actions.stuff import StuffHandler, StuffBookHandler

    register(ExceptionQueueHandler())
    register(NotifyHandler())
    register(WineHandler())

    if settings.services.stuff_pipeline_secret:
        register(StuffHandler(
            base_url=settings.services.stuff_url,
            pipeline_secret=settings.services.stuff_pipeline_secret,
            anthropic_api_key=settings.anthropic_api_key,
        ))
        if settings.anthropic_api_key:
            register(StuffBookHandler(
                base_url=settings.services.stuff_url,
                pipeline_secret=settings.services.stuff_pipeline_secret,
                anthropic_api_key=settings.anthropic_api_key,
            ))
    else:
        log.warning("STUFF_PIPELINE_SECRET not set — stuff handler disabled")

    if settings.services.trips_pipeline_secret:
        from pipeline.actions.expense import ExpenseHandler
        register(ExpenseHandler(
            base_url=settings.services.trips_url,
            pipeline_secret=settings.services.trips_pipeline_secret,
        ))
    else:
        log.warning("TRIPS_PIPELINE_SECRET not set — expense handler disabled")

    if settings.services.imap_user and settings.services.imap_password:
        from pipeline.actions.email_move import EmailMoveHandler
        register(EmailMoveHandler(
            host=settings.services.imap_host,
            port=settings.services.imap_port,
            user=settings.services.imap_user,
            password=settings.services.imap_password,
        ))
