"""Runtime helpers shared by CLI entrypoints."""
from __future__ import annotations

from jarvis.config import AppConfig, load_settings
from jarvis.logging import configure_logging


def configure_runtime(database: str | None = None, log_level: str | None = None, *, structured: bool | None = None) -> AppConfig:
    config = load_settings(database)
    effective_structured = structured if structured is not None else config.structured_logging
    configure_logging(log_level or config.log_level, structured=effective_structured)
    return config
