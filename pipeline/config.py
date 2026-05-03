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


class ImapAccount(BaseSettings):
    host: str = "mail.mees.st"
    port: int = 993
    user: str = ""
    password: str = ""
    hc_uuid: str = ""
    enable_spam: bool = True
    enable_unsubscribe: bool = True
    primary: bool = False


class ServicesConfig(BaseSettings):
    immich_url: str = "http://127.0.0.1:2283"
    paperless_url: str = "http://paperless:8000"
    finance_url: str = "http://127.0.0.1:8000"
    stuff_url: str = "http://stuff:8300"
    stuff_pipeline_secret: str = ""
    imap_accounts: list[ImapAccount] = Field(default_factory=list)
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

    # PIF (photo index seeding)
    pif_db_url: str = ""

    # Mailcow
    mailcow_api_key: str = ""

    # Notifications
    pushover_app_token: str = ""
    pushover_user_key: str = ""

    def resolve_path(self, relative: str) -> Path:
        """Resolve a config-relative path to an absolute path."""
        return self.project_root / relative

    def imap_account_for(self, address: str | None) -> "ImapAccount | None":
        """Find the configured IMAP account whose user matches *address*.

        *address* may be a bare address ("stu@x.y") or a full From/To header
        ("Stu <stu@x.y>"). Falls back to the primary account if no match,
        or None if no accounts are configured.
        """
        accounts = self.services.imap_accounts
        if not accounts:
            return None
        if address:
            addr = address.lower()
            for acct in accounts:
                if acct.user and acct.user.lower() in addr:
                    return acct
        return next((a for a in accounts if a.primary), accounts[0])

    model_config = {"env_prefix": "PIPELINE_"}


def load_settings(config_path: Path | None = None) -> Settings:
    """Load settings from YAML config merged with environment variables."""
    root = _project_root()
    if config_path is None:
        config_path = root / "shared" / "config.yaml"

    raw = _load_yaml(config_path) if config_path.exists() else {}

    paths = PathsConfig(**(raw.get("paths", {})))

    raw_services = dict(raw.get("services", {}))
    raw_accounts = raw_services.pop("imap_accounts", [])
    services = ServicesConfig(**raw_services)
    accounts = [ImapAccount(**a) for a in raw_accounts]
    accounts = [a for a in accounts if a.user and a.password]
    if accounts and not any(a.primary for a in accounts):
        accounts[0].primary = True
    services.imap_accounts = accounts

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
        pif_db_url=os.environ.get("PIF_DATABASE_URL", ""),
        mailcow_api_key=os.environ.get("MAILCOW_API_KEY", ""),
        pushover_app_token=os.environ.get("PUSHOVER_APP_TOKEN", ""),
        pushover_user_key=os.environ.get("PUSHOVER_USER_KEY", ""),
    )
