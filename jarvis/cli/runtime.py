"""Runtime helpers shared by CLI entrypoints."""
from __future__ import annotations

from jarvis.config import AppConfig, load_settings
from jarvis.logging import configure_logging


def configure_runtime(database: str | None = None, log_level: str | None = None) -> AppConfig:
    config = load_settings(database)
    configure_logging(log_level or config.log_level)
    return config
