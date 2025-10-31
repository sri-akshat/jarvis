"""CLI entry point for ingesting Gmail messages into a data store."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from jarvis.cli import configure_runtime
from jarvis.ingestion.common.datastore import SQLiteDataStore
from jarvis.ingestion.common.pipelines import ingest_messages
from jarvis.ingestion.gmail.service import GmailService

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Gmail search query string")
    parser.add_argument(
        "--credentials",
        required=True,
        help="Path to Google OAuth client credentials JSON",
    )
    parser.add_argument(
        "--token",
        default=str(Path.home() / ".gmail-token.json"),
        help="Path to store the OAuth token JSON",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="SQLite database path for persisting results (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--user-id",
        default="me",
        help="Gmail user id (default: me)",
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
    service = GmailService(
        credentials_path=args.credentials,
        token_path=args.token,
        user_id=args.user_id,
    )
    datastore = SQLiteDataStore(config.database_path)
    logger.info("Fetching Gmail messages for query '%s'", args.query)
    ingest_messages(service, datastore, args.query)
    logger.info("Ingestion complete for query '%s'", args.query)


if __name__ == "__main__":
    main()
