"""CLI entry point for ingesting Gmail messages into a data store."""
from __future__ import annotations

try:  # pragma: no cover - import fallback for script execution
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

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
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of messages to ingest (default: all)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Number of messages the Gmail API should fetch per request (1-500, default: 100)",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=50,
        help="Log progress every N messages (default: 50; set to 0 to disable)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Persist messages in batches of this size (default: 200; <=0 means process all at once)",
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
    datastore = SQLiteDataStore(
        config.database_path,
        queue_target=config.task_queue_url,
    )
    logger.info("Fetching Gmail messages for query '%s'", args.query)
    message_count, attachment_count, task_count = ingest_messages(
        service,
        datastore,
        args.query,
        limit=args.limit,
        page_size=args.page_size,
        progress_interval=(args.progress_interval or None),
        batch_size=args.batch_size,
    )
    logger.info(
        "Ingestion complete for query '%s': %s message(s), %s attachment(s), %s task(s) queued.",
        args.query,
        message_count,
        attachment_count,
        task_count,
    )


if __name__ == "__main__":
    main()
