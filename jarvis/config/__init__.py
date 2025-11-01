"""Application configuration utilities for Jarvis."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AppConfig:
    database_path: Path
    log_level: str = "INFO"


def load_settings(database: str | None = None) -> AppConfig:
    """Resolve application configuration from overrides and environment variables."""
    database_env = database or os.getenv("JARVIS_DATABASE", "data/messages.db")
    log_level = os.getenv("JARVIS_LOG_LEVEL", "INFO").upper()
    return AppConfig(database_path=Path(database_env), log_level=log_level)
