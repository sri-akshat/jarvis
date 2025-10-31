"""CLI helper to export attachments from the messaging SQLite datastore."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export attachment blobs from data/messages.db"
    )
    parser.add_argument(
        "--database",
        default="data/messages.db",
        help="Path to the SQLite database (default: data/messages.db)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--filename",
        help="Exact filename of the attachment to export",
    )
    group.add_argument(
        "--message-id",
        help="Message id for which to export all attachments",
    )
    parser.add_argument(
        "--output-dir",
        default="exported_attachments",
        help="Directory where exported files will be written",
    )
    return parser.parse_args()


def fetch_attachments(
    connection: sqlite3.Connection, *, filename: str | None, message_id: str | None
) -> Iterable[Tuple[str, str, bytes]]:
    cursor = connection.cursor()
    if filename:
        cursor.execute(
            """
            SELECT message_id, filename, data
            FROM attachments
            WHERE filename = ?
            """,
            (filename,),
        )
    else:
        cursor.execute(
            """
            SELECT message_id, filename, data
            FROM attachments
            WHERE message_id = ?
            """,
            (message_id,),
        )
    yield from cursor.fetchall()


def export_attachment(output_dir: Path, message_id: str, filename: str, data: bytes):
    output_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_dir / filename
    # Avoid overwriting by adding a suffix if the file is already present.
    if target_path.exists():
        stem = target_path.stem
        suffix = target_path.suffix
        counter = 1
        while True:
            candidate = output_dir / f"{stem}_{counter}{suffix}"
            if not candidate.exists():
                target_path = candidate
                break
            counter += 1
    target_path.write_bytes(data)
    print(f"Exported {filename} (message {message_id}) -> {target_path}")


def main() -> None:
    args = parse_args()
    connection = sqlite3.connect(args.database)
    try:
        attachments = list(
            fetch_attachments(
                connection,
                filename=args.filename,
                message_id=args.message_id,
            )
        )
    finally:
        connection.close()

    if not attachments:
        scope = (
            f"filename '{args.filename}'" if args.filename else f"message '{args.message_id}'"
        )
        raise SystemExit(f"No attachments found for {scope}")

    output_dir = Path(args.output_dir)
    for message_id, filename, data in attachments:
        export_attachment(output_dir, message_id, filename, data)


if __name__ == "__main__":
    main()
