"""Jinja2 template environment for rendering action parameters."""

from __future__ import annotations

from typing import Any

from jinja2 import Environment, BaseLoader, Undefined

from pipeline.models import Envelope


def _get_env() -> Environment:
    """Lazy-create the Jinja2 environment."""
    global _env
    if _env is None:
        _env = Environment(
            loader=BaseLoader(),
            undefined=Undefined,
            autoescape=False,
        )
        _env.filters["date_format"] = _date_format
        _env.filters["compact_date"] = _compact_date
        _env.filters["round"] = _round
    return _env


_env: Environment | None = None


def render(
    template_str: str,
    envelope: Envelope,
    params: dict[str, Any] | None = None,
    action_results: dict[str, Any] | None = None,
) -> str:
    """Render a Jinja2 template string with the envelope namespace."""
    namespace = _build_namespace(envelope, params, action_results)
    template = _get_env().from_string(template_str)
    return template.render(**namespace)


def _build_namespace(
    envelope: Envelope,
    params: dict[str, Any] | None = None,
    action_results: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the full template namespace from an envelope."""
    ns: dict[str, Any] = {}

    # source.*
    ns["source"] = {
        "type": envelope.source_type,
        "path": envelope.source_path,
        "received_at": envelope.received_at,
        "email_from": envelope.source_email_from,
        "email_subject": envelope.source_email_subject,
    }

    # exif.*
    if envelope.exif:
        ns["exif"] = envelope.exif.model_dump()
    else:
        ns["exif"] = {}

    # extracted.*
    ns["extracted"] = envelope.extracted

    # classification.*
    if envelope.classification:
        ns["classification"] = envelope.classification.model_dump()
    else:
        ns["classification"] = {}

    # file.*
    ns["file"] = {
        "sha256": envelope.file_sha256,
        "media_type": envelope.media_type,
        "size_bytes": envelope.file_size,
        "name": envelope.file_name,
    }

    # param.*
    ns["param"] = params or {}

    # action_results.*
    ns["action_results"] = action_results or {}

    return ns


def _date_format(value: Any, fmt: str = "%Y-%m-%d") -> str:
    """Format a datetime value."""
    if hasattr(value, "strftime"):
        return value.strftime(fmt)
    return str(value)


def _compact_date(value: Any) -> str:
    """Convert a date string or datetime to YYYYMMDD format."""
    if not value:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    # Parse ISO date string
    s = str(value).strip()
    try:
        from datetime import datetime
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y%m%d")
            except ValueError:
                continue
    except Exception:
        pass
    # Last resort: strip non-digits
    digits = "".join(c for c in s if c.isdigit())
    return digits[:8] if len(digits) >= 8 else s


def _round(value: Any, precision: int = 2) -> Any:
    """Round a numeric value."""
    try:
        return round(float(value), precision)
    except (TypeError, ValueError):
        return value
