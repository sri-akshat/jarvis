"""Utility to remove spaCy-derived entities and relations from the datastore."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging
import sqlite3
from pathlib import Path

from jarvis.cli import configure_runtime

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (e.g. INFO, DEBUG)",
    )
    return parser.parse_args()


def remove_spacy_artifacts(path: Path) -> None:
    db_path = Path(path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("BEGIN;")
        conn.execute(
            """
            DELETE FROM entity_mentions
            WHERE extractor LIKE 'spacy:%'
            """
        )
        conn.execute(
            """
            DELETE FROM graph_relations
            WHERE properties LIKE '%spacy:%'
            """
        )
        conn.execute(
            """
            DELETE FROM graph_entities
            WHERE entity_id NOT IN (
                SELECT DISTINCT entity_id FROM entity_mentions
            )
            AND entity_id NOT IN (
                SELECT DISTINCT source_id FROM graph_relations
            )
            AND entity_id NOT IN (
                SELECT DISTINCT target_id FROM graph_relations
            )
            """
        )
        conn.commit()


def main() -> None:
    args = parse_args()
    config = configure_runtime(args.database, args.log_level)
    remove_spacy_artifacts(config.database_path)
    logger.info("Removed spaCy-derived mentions and pruned orphaned graph nodes.")


if __name__ == "__main__":
    main()
