"""Entry point wrapper for the ingestion processing worker."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

from jarvis.ingestion.workers.processing import main

if __name__ == "__main__":
    main()
