"""Configuration — loads shared/config.yaml and merges with environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings


def _project_root() -> Path:
    """Return the project root (parent of the pipeline package)."""
    return Path(__file__).resolve().parent.parent


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load a YAML file and expand ${ENV_VAR} references."""
    import re

    text = path.read_text()

    def _replace(match: re.Match) -> str:
        return os.environ.get(match.group(1), "")

    text = re.sub(r"\$\{(\w+)\}", _replace, text)
    return yaml.safe_load(text) or {}


class PathsConfig(BaseSettings):
    drop_folder: str = "data/drop"
    data_dir: str = "data"
    pets_dir: str = "shared/pets"


class ServicesConfig(BaseSettings):
    immich_url: str = "http://127.0.0.1:2283"
    paperless_url: str = "http://paperless:8000"
    finance_url: str = "http://127.0.0.1:8000"
    stuff_url: str = "http://stuff:8300"
    stuff_pipeline_secret: str = ""
    imap_host: str = "mail.mees.st"
    imap_port: int = 993
    imap_user: str = ""
    imap_password: str = ""
    location_url: str = "http://host.containers.internal:8100"
    location_secret: str = ""
    trips_url: str = "http://trips:8400"
    trips_pipeline_secret: str = ""
    mailcow_url: str = "https://mail.mees.st"


class ClassifierConfig(BaseSettings):
    clip_threshold_default: float = 0.75
    llm_threshold_default: float = 0.70
    claude_threshold_default: float = 0.80


class TiersConfig(BaseSettings):
    ceiling: str = "claude"  # deterministic | clip | llm | claude


class ClipLabel(BaseSettings):
    threshold: float = 0.75
    escalate_above: float = 0.85  # below this, escalate to Claude for verification


class Settings(BaseSettings):
    """Top-level settings, merging YAML config with env vars."""

    project_root: Path = Field(default_factory=_project_root)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    services: ServicesConfig = Field(default_factory=ServicesConfig)
    classifier: ClassifierConfig = Field(default_factory=ClassifierConfig)
    tiers: TiersConfig = Field(default_factory=TiersConfig)
    clip_labels: dict[str, ClipLabel] = Field(default_factory=dict)

    # API keys from env
    anthropic_api_key: str = ""
    immich_api_key: str = ""
    paperless_api_key: str = ""

    # Mailcow
    mailcow_api_key: str = ""

    # Notifications
    pushover_app_token: str = ""
    pushover_user_key: str = ""

    def resolve_path(self, relative: str) -> Path:
        """Resolve a config-relative path to an absolute path."""
        return self.project_root / relative

    model_config = {"env_prefix": "PIPELINE_"}


def load_settings(config_path: Path | None = None) -> Settings:
    """Load settings from YAML config merged with environment variables."""
    root = _project_root()
    if config_path is None:
        config_path = root / "shared" / "config.yaml"

    raw = _load_yaml(config_path) if config_path.exists() else {}

    paths = PathsConfig(**(raw.get("paths", {})))
    services = ServicesConfig(**(raw.get("services", {})))
    classifier_cfg = ClassifierConfig(**(raw.get("classifier", {})))
    tiers = TiersConfig(**(raw.get("tiers", {})))

    # Load CLIP labels
    clip_path = root / "shared" / "clip_labels.yaml"
    clip_labels: dict[str, ClipLabel] = {}
    if clip_path.exists():
        clip_raw = yaml.safe_load(clip_path.read_text()) or {}
        for name, cfg in clip_raw.get("labels", {}).items():
            clip_labels[name] = ClipLabel(**(cfg if isinstance(cfg, dict) else {}))

    return Settings(
        project_root=root,
        paths=paths,
        services=services,
        classifier=classifier_cfg,
        tiers=tiers,
        clip_labels=clip_labels,
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        immich_api_key=os.environ.get("IMMICH_API_KEY", ""),
        paperless_api_key=os.environ.get("PAPERLESS_API_KEY", ""),
        mailcow_api_key=os.environ.get("MAILCOW_API_KEY", ""),
        pushover_app_token=os.environ.get("PUSHOVER_APP_TOKEN", ""),
        pushover_user_key=os.environ.get("PUSHOVER_USER_KEY", ""),
    )
