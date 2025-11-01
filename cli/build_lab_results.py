"""CLI helper to derive structured lab results from entity mentions."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.domains.lab import LabFactBuilder

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--extractor",
        default="llm:mistral",
        help="Extractor identifier to use (default: llm:mistral)",
    )
    parser.add_argument(
        "--content-id",
        help="Process only the specified content id",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (e.g. INFO, DEBUG)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = configure_runtime(args.database, args.log_level)
    builder = LabFactBuilder(
        database_path=str(config.database_path),
        extractor=args.extractor,
    )
    processed = builder.run(content_id=args.content_id)
    logger.info("Derived %s lab measurement(s) from %s.", processed, args.extractor)


if __name__ == "__main__":
    main()
