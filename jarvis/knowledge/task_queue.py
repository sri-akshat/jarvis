"""SQLite-backed task queue for ingestion workflows."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from typing import Optional, Sequence


STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"


QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS task_queue (
    task_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    locked_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""


@dataclass
class Task:
    task_id: str
    task_type: str
    payload: dict
    attempts: int
    available_at: datetime
    locked_at: Optional[datetime]
    last_error: Optional[str]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_queue_tables(conn: sqlite3.Connection) -> None:
    conn.execute(QUEUE_SCHEMA)


def _compute_task_id(task_type: str, payload: dict) -> str:
    raw = f"{task_type}:{json.dumps(payload, sort_keys=True)}"
    return sha1(raw.encode("utf-8")).hexdigest()


def enqueue_task(
    database_path: str,
    task_type: str,
    payload: dict,
    *,
    available_at: Optional[datetime] = None,
) -> None:
    task_id = _compute_task_id(task_type, payload)
    now = _now()
    available = (available_at or now).isoformat()
    with sqlite3.connect(database_path) as conn:
        ensure_queue_tables(conn)
        conn.execute(
            """
            INSERT INTO task_queue (
                task_id, task_type, status, payload, attempts,
                available_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                status = CASE
                    WHEN task_queue.status IN ('completed','failed') THEN excluded.status
                    ELSE task_queue.status
                END,
                available_at = excluded.available_at,
                updated_at = excluded.updated_at
            """,
            (
                task_id,
                task_type,
                STATUS_PENDING,
                json.dumps(payload, sort_keys=True),
                available,
                now.isoformat(),
                now.isoformat(),
            ),
        )


def fetch_and_lock_task(
    database_path: str,
    *,
    task_types: Optional[Sequence[str]] = None,
    lock_timeout_seconds: int = 300,
) -> Optional[Task]:
    now = _now()
    lock_deadline = (now - timedelta(seconds=lock_timeout_seconds)).isoformat()
    with sqlite3.connect(database_path) as conn:
        ensure_queue_tables(conn)
        conn.execute("BEGIN IMMEDIATE")
        if task_types:
            placeholder = ",".join("?" for _ in task_types)
            query = f"""
                SELECT task_id, task_type, payload, attempts, available_at, locked_at, last_error
                FROM task_queue
                WHERE task_type IN ({placeholder})
                  AND status IN ('pending', 'in_progress')
                  AND (
                        (status = 'pending' AND available_at <= ?)
                     OR (status = 'in_progress' AND locked_at <= ?)
                  )
                ORDER BY available_at ASC
                LIMIT 1
            """
            params = (*task_types, now.isoformat(), lock_deadline)
        else:
            query = """
                SELECT task_id, task_type, payload, attempts, available_at, locked_at, last_error
                FROM task_queue
                WHERE status IN ('pending', 'in_progress')
                  AND (
                        (status = 'pending' AND available_at <= ?)
                     OR (status = 'in_progress' AND locked_at <= ?)
                  )
                ORDER BY available_at ASC
                LIMIT 1
            """
            params = (now.isoformat(), lock_deadline)
        row = conn.execute(query, params).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        task_id, task_type, payload_json, attempts, available_at, locked_at, last_error = row
        conn.execute(
            """
            UPDATE task_queue
            SET status = ?, locked_at = ?, updated_at = ?
            WHERE task_id = ?
            """,
            (STATUS_IN_PROGRESS, now.isoformat(), now.isoformat(), task_id),
        )
        conn.execute("COMMIT")
        payload = json.loads(payload_json)
        available_dt = datetime.fromisoformat(available_at)
        locked_dt = datetime.fromisoformat(locked_at) if locked_at else None
        return Task(
            task_id=task_id,
            task_type=task_type,
            payload=payload,
            attempts=attempts,
            available_at=available_dt,
            locked_at=locked_dt,
            last_error=last_error,
        )


def complete_task(database_path: str, task_id: str) -> None:
    with sqlite3.connect(database_path) as conn:
        ensure_queue_tables(conn)
        conn.execute("DELETE FROM task_queue WHERE task_id = ?", (task_id,))


def fail_task(
    database_path: str,
    task_id: str,
    *,
    error: str,
    retry_delay_seconds: int = 300,
    max_attempts: int = 5,
) -> None:
    now = _now()
    with sqlite3.connect(database_path) as conn:
        ensure_queue_tables(conn)
        attempts = conn.execute(
            "SELECT attempts FROM task_queue WHERE task_id = ?", (task_id,)
        ).fetchone()
        attempts = (attempts[0] if attempts else 0) + 1
        status = STATUS_PENDING if attempts < max_attempts else STATUS_FAILED
        available_at = (now + timedelta(seconds=retry_delay_seconds)).isoformat()
        conn.execute(
            """
            UPDATE task_queue
            SET status = ?, attempts = ?, available_at = ?, locked_at = NULL,
                last_error = ?, updated_at = ?
            WHERE task_id = ?
            """,
            (status, attempts, available_at, error[:1024], now.isoformat(), task_id),
        )
