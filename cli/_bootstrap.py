"""Utilities to ensure CLI scripts can be executed directly."""
from __future__ import annotations

import sys
from pathlib import Path


def ensure_project_root() -> None:
    """Add the repository root to sys.path if missing.

    When CLI modules are executed as standalone scripts
    (``python cli/foo.py``), Python sets ``sys.path[0]`` to the
    CLI directory. Importing ``jarvis`` then fails because the package
    lives one level above. This helper adds the repository root so that
    absolute imports continue to work in both script and module contexts.
    """
    root = Path(__file__).resolve().parents[1]
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
