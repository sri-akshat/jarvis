"""CLI tool to inspect derived medical events."""
from __future__ import annotations

from cli._bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.domains.medical import fetch_medical_events

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--event-type",
        help="Filter by event type (e.g., medication, diagnosis)",
    )
    parser.add_argument(
        "--patient",
        help="Filter by patient substring",
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
    events = fetch_medical_events(
        database_path=str(config.database_path),
        extractor=args.extractor,
        event_type=args.event_type,
        patient_filter=args.patient,
        limit=args.limit,
    )
    if not events:
        print("No medical events found.")
        return
    logger.info("Displaying %s medical event(s).", len(events))
    for idx, event in enumerate(events, start=1):
        print(f"#{idx}")
        if event.event_type:
            print(f"  Type: {event.event_type}")
        if event.description:
            print(f"  Description: {event.description}")
        if event.attributes:
            print(f"  Attributes: {event.attributes}")
        if event.patient:
            print(f"  Patient: {event.patient}")
        if event.clinician:
            print(f"  Clinician: {event.clinician}")
        if event.facility:
            print(f"  Facility: {event.facility}")
        if event.date:
            print(f"  Date: {event.date}")
        if event.subject:
            print(f"  Subject: {event.subject}")
        if event.filename:
            print(f"  File: {event.filename}")
        print(f"  Message ID: {event.message_id}")
        if event.attachment_id:
            print(f"  Attachment ID: {event.attachment_id}")
        print()


if __name__ == "__main__":
    main()
