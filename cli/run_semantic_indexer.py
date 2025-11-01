"""CLI entry point for running the semantic indexing pipeline."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.semantic_indexer import SemanticIndexer

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of attachments to process in this run",
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
    indexer = SemanticIndexer(str(config.database_path))
    processed = indexer.run(limit=args.limit)
    logger.info("Semantic indexing completed for %s attachment(s).", processed)


if __name__ == "__main__":
    main()
