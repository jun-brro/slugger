"""Configuration loading from ~/.slugger/config.toml with env var overrides."""

from __future__ import annotations

import os
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

from slugger.models import SluggerConfig

SLUGGER_DIR = Path.home() / ".slugger"
CONFIG_PATH = SLUGGER_DIR / "config.toml"
CREDENTIALS_PATH = SLUGGER_DIR / "credentials.json"


def ensure_slugger_dir() -> Path:
    SLUGGER_DIR.mkdir(parents=True, exist_ok=True)
    # Restrict to owner-only on shared HPC clusters
    os.chmod(SLUGGER_DIR, 0o700)
    return SLUGGER_DIR


def load_config() -> SluggerConfig:
    """Load config from TOML file, then override with env vars."""
    data: dict = {}

    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            data = tomllib.load(f)

    google_credentials = os.environ.get(
        "SLUGGER_GOOGLE_CREDENTIALS",
        data.get("google_credentials", str(CREDENTIALS_PATH)),
    )
    spreadsheet_id = os.environ.get(
        "SLUGGER_SPREADSHEET_ID",
        data.get("spreadsheet_id", ""),
    )
    try:
        poll_interval = int(
            os.environ.get("SLUGGER_POLL_INTERVAL", data.get("poll_interval_sec", 60))
        )
    except (ValueError, TypeError):
        poll_interval = 60
    poll_interval = max(poll_interval, 5)  # Prevent tight-loop polling

    return SluggerConfig(
        google_credentials=google_credentials,
        spreadsheet_id=spreadsheet_id,
        poll_interval_sec=poll_interval,
    )
