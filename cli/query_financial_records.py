"""CLI tool to inspect derived financial records."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.domains.financial import fetch_financial_records

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--counterparty",
        help="Filter by counterparty substring",
    )
    parser.add_argument(
        "--record-type",
        help="Filter by record type (invoice/payment)",
    )
    parser.add_argument(
        "--reference",
        help="Filter by reference substring",
    )
    parser.add_argument(
        "--extractor",
        default="llm:mistral",
        help="Extractor identifier to query",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows to display (default: 10)",
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
    records = fetch_financial_records(
        database_path=str(config.database_path),
        extractor=args.extractor,
        counterparty_filter=args.counterparty,
        record_type=args.record_type,
        reference_filter=args.reference,
        limit=args.limit,
    )
    if not records:
        print("No financial records found.")
        return
    logger.info("Displaying %s financial record(s).", len(records))
    for idx, rec in enumerate(records, start=1):
        print(f"#{idx}")
        if rec.record_type:
            print(f"  Type: {rec.record_type}")
        if rec.amount_text or rec.amount_value is not None:
            value = rec.amount_text or rec.amount_value
            currency = rec.currency or ""
            print(f"  Amount: {value} {currency}".strip())
        if rec.counterparty:
            print(f"  Counterparty: {rec.counterparty}")
        if rec.reference:
            print(f"  Reference: {rec.reference}")
        if rec.date:
            print(f"  Date: {rec.date}")
        if rec.subject:
            print(f"  Subject: {rec.subject}")
        if rec.filename:
            print(f"  File: {rec.filename}")
        print(f"  Message ID: {rec.message_id}")
        if rec.attachment_id:
            print(f"  Attachment ID: {rec.attachment_id}")
        print()


if __name__ == "__main__":
    main()
