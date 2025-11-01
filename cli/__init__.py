"""CLI entry points for Jarvis utilities."""

from ._bootstrap import ensure_project_root

ensure_project_root()

# Re-export commonly used helpers for tests and tooling.
from . import enqueue_local_files as _enqueue_local_files

enqueue_local_files = _enqueue_local_files

__all__ = ["enqueue_local_files"]
