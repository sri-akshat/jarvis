"""Scan a local directory and enqueue files for ingestion."""
from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import sqlite3

import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge import task_queue


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        help="Root directory to scan for documents",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--extensions",
        nargs="*",
        default=[".pdf", ".txt", ".md"],
        help="File extensions to include (default: .pdf .txt .md)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively walk subdirectories",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (e.g. INFO, DEBUG)",
    )
    return parser.parse_args()


def iter_files(root: Path, recursive: bool, extensions: Iterable[str]) -> Iterable[Path]:
    ext_set = {ext.lower() for ext in extensions}
    if recursive:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in ext_set:
                yield path
    else:
        for path in root.iterdir():
            if path.is_file() and path.suffix.lower() in ext_set:
                yield path


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def upsert_local_file(
    database: Path,
    path: Path,
    *,
    sha: str,
) -> bool:
    content_id = f"file:{sha}"
    mime_type, _ = mimetypes.guess_type(path.name)
    metadata = {
        "path": str(path),
        "source": "local",
        "filename": path.name,
    }
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(database) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        stat = path.stat()
        conn.execute(
            """
            INSERT INTO content_registry (
                content_id, message_id, attachment_id, content_type, mime_type,
                sha256, created_at, metadata
            ) VALUES (?, NULL, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT(content_id) DO NOTHING
            """,
            (
                content_id,
                "local_file",
                mime_type or "application/octet-stream",
                sha,
                now,
                json.dumps(metadata, sort_keys=True),
            ),
        )
        inserted = (
            conn.execute(
                "SELECT changes()"
            ).fetchone()[0]
            > 0
        )
        if inserted:
            conn.execute(
                """
                INSERT INTO local_files (
                    content_id, path, size, modified_at, metadata
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(content_id) DO UPDATE SET
                    path = excluded.path,
                    size = excluded.size,
                    modified_at = excluded.modified_at,
                    metadata = excluded.metadata
                """,
                (
                    content_id,
                    str(path),
                    stat.st_size,
                    datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    json.dumps(metadata, sort_keys=True),
                ),
            )
    if inserted:
        task_queue.enqueue_task(
            str(database),
            "semantic_index",
            {"content_id": f"file:{sha}"},
        )
    return inserted


def file_already_registered(database: Path, sha: str) -> bool:
    with sqlite3.connect(database) as conn:
        row = conn.execute(
            """
            SELECT 1 FROM content_registry
            WHERE sha256 = ? AND content_type = 'local_file'
            """,
            (sha,),
        ).fetchone()
        return row is not None


def main() -> None:
    args = parse_args()
    config = configure_runtime(args.database, args.log_level)
    root = Path(args.directory).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"Directory does not exist: {root}")
    database = config.database_path.expanduser()
    total = 0
    skipped = 0
    for file_path in iter_files(root, args.recursive, args.extensions):
        total += 1
        sha = compute_sha256(file_path)
        if file_already_registered(database, sha):
            skipped += 1
            continue
        inserted = upsert_local_file(database, file_path, sha=sha)
        if inserted:
            logger.info("Enqueued %s", file_path)
        else:
            skipped += 1
    logger.info(
        "Processed %s file(s); enqueued %s new item(s); skipped %s already ingested file(s).",
        total,
        total - skipped,
        skipped,
    )


if __name__ == "__main__":
    main()
