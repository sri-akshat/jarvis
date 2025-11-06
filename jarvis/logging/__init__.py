"""Logging helpers."""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """Very small JSON formatter to avoid extra dependencies."""

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - thin wrapper
        payload: Dict[str, Any] = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str | None = None, structured: bool = False) -> None:
    resolved = getattr(logging, (level or "INFO").upper(), logging.INFO)
    logging.basicConfig(level=resolved, force=True)
    root = logging.getLogger()
    for handler in root.handlers:
        if structured:
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
