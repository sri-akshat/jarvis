"""CLI tool for inspecting lab results derived from the knowledge graph."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.queries import fetch_lab_results

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--test",
        help="Substring filter for lab test name",
    )
    parser.add_argument(
        "--patient",
        help="Substring filter for patient name",
    )
    parser.add_argument(
        "--subject",
        help="Substring filter for message subject",
    )
    parser.add_argument(
        "--extractor",
        default="llm:mistral",
        help="Extractor identifier to query (default: llm:mistral)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of rows to display (default: 10)",
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
    results = fetch_lab_results(
        database_path=str(config.database_path),
        extractor=args.extractor,
        test_filter=args.test,
        patient_filter=args.patient,
        subject_filter=args.subject,
        limit=args.limit,
    )
    if not results:
        print("No lab results found.")
        return
    logger.info("Displaying %s lab result(s).", len(results))
    for idx, result in enumerate(results, start=1):
        print(f"#{idx}")
        if result.test_name:
            print(f"  Test: {result.test_name}")
        print(f"  Value: {result.value} {result.units or ''}".strip())
        if result.reference_range:
            print(f"  Reference: {result.reference_range}")
        if result.patient:
            print(f"  Patient: {result.patient}")
        if result.date:
            print(f"  Date: {result.date}")
        if result.subject:
            print(f"  Subject: {result.subject}")
        if result.filename:
            print(f"  File: {result.filename}")
        print(f"  Message ID: {result.message_id}")
        if result.attachment_id:
            print(f"  Attachment ID: {result.attachment_id}")
        print()


if __name__ == "__main__":
    main()
