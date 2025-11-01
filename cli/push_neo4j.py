"""Push the SQLite-backed knowledge graph into Neo4j."""
from __future__ import annotations

import argparse
import getpass
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.neo4j_exporter import (
    Neo4jConnectionConfig,
    Neo4jGraphExporter,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--uri",
        default="bolt://localhost:7687",
        help="Neo4j Bolt URI (default: bolt://localhost:7687)",
    )
    parser.add_argument(
        "--user",
        default="neo4j",
        help="Neo4j username (default: neo4j)",
    )
    parser.add_argument(
        "--password",
        help="Neo4j password (will prompt if omitted)",
    )
    parser.add_argument(
        "--neo4j-database",
        default=None,
        help="Optional Neo4j database name (default: server default)",
    )
    parser.add_argument(
        "--clear-existing",
        action="store_true",
        help="Remove all nodes/edges in Neo4j before import.",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (e.g. INFO, DEBUG)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.password is None:
        args.password = getpass.getpass("Neo4j password: ")
    config = configure_runtime(args.database, args.log_level)
    connection = Neo4jConnectionConfig(
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.neo4j_database,
    )
    exporter = Neo4jGraphExporter(
        str(config.database_path),
        connection,
        clear_existing=args.clear_existing,
    )
    nodes, edges = exporter.run()
    logger.info("Exported %s node(s) and %s relation(s) to Neo4j.", nodes, edges)


if __name__ == "__main__":
    main()
