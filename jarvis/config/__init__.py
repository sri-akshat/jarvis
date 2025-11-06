"""Application configuration utilities for Jarvis."""
from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


DEFAULT_CONFIG_LOCATIONS = (
    Path("jarvis.ini"),
    Path("config/jarvis.ini"),
)


@dataclass
class AppConfig:
    database_path: Path
    log_level: str = "INFO"
    task_queue_url: str | None = None
    structured_logging: bool = False
    config_source: Path | None = None


def _load_config_file(config_path: Path | None) -> Dict[str, Any]:
    if config_path is None:
        for candidate in DEFAULT_CONFIG_LOCATIONS:
            if candidate.exists():
                config_path = candidate
                break
    if config_path is None or not config_path.exists():
        return {}

    parser = configparser.ConfigParser()
    parser.read(config_path)
    data: Dict[str, Any] = {"__path__": config_path}
    if parser.has_section("database"):
        data["database_path"] = parser.get("database", "path", fallback=None)
    if parser.has_section("queue"):
        data["task_queue_url"] = parser.get("queue", "task_queue_url", fallback=None)
    if parser.has_section("logging"):
        data["log_level"] = parser.get("logging", "level", fallback=None)
        structured = parser.get("logging", "structured", fallback=None)
        if structured is not None:
            data["structured_logging"] = parser.getboolean("logging", "structured", fallback=False)
    return data


def _normalize_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return None


def load_settings(database: str | None = None) -> AppConfig:
    """Resolve application configuration from config files, env vars, and overrides."""
    config_file_env = os.getenv("JARVIS_CONFIG_FILE")
    config_data = _load_config_file(Path(config_file_env)) if config_file_env else _load_config_file(None)

    database_path = database or os.getenv("JARVIS_DATABASE") or config_data.get("database_path") or "data/messages.db"
    log_level = (
        os.getenv("JARVIS_LOG_LEVEL")
        or config_data.get("log_level")
        or "INFO"
    )
    task_queue_url = os.getenv("JARVIS_TASK_QUEUE") or config_data.get("task_queue_url")
    structured_logging_env = _normalize_bool(os.getenv("JARVIS_STRUCTURED_LOGGING"))
    if structured_logging_env is None:
        structured_logging = bool(config_data.get("structured_logging", False))
    else:
        structured_logging = structured_logging_env

    return AppConfig(
        database_path=Path(database_path),
        log_level=log_level.upper(),
        task_queue_url=task_queue_url,
        structured_logging=structured_logging,
        config_source=config_data.get("__path__") if config_data else None,
    )
