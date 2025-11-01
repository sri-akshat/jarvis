from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from jarvis.knowledge import task_queue


def _open(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def test_enqueue_and_fetch(tmp_path):
    db_path = tmp_path / "queue.db"
    payload = {"content_id": "cid-1"}

    task_queue.enqueue_task(str(db_path), "semantic_index", payload)
    # dedupe should not create extra row
    task_queue.enqueue_task(str(db_path), "semantic_index", payload)

    with _open(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM task_queue").fetchone()[0]
        assert count == 1

    task = task_queue.fetch_and_lock_task(str(db_path))
    assert task is not None
    assert task.task_type == "semantic_index"
    assert task.payload == payload

    # Completing removes the task.
    task_queue.complete_task(str(db_path), task.task_id)
    with _open(str(db_path)) as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM task_queue").fetchone()[0]
        assert remaining == 0


def test_fail_task_retries(tmp_path):
    db_path = tmp_path / "queue.db"
    payload = {"content_id": "cid-2"}
    task_queue.enqueue_task(str(db_path), "semantic_index", payload)

    task = task_queue.fetch_and_lock_task(str(db_path))
    assert task is not None

    task_queue.fail_task(
        str(db_path),
        task.task_id,
        error="transient",
        retry_delay_seconds=0,
        max_attempts=2,
    )

    task = task_queue.fetch_and_lock_task(str(db_path))
    assert task is not None
    assert task.attempts == 1

    # After exceeding max attempts the task transitions to failed.
    task_queue.fail_task(
        str(db_path),
        task.task_id,
        error="boom",
        retry_delay_seconds=0,
        max_attempts=2,
    )

    with _open(str(db_path)) as conn:
        status = conn.execute(
            "SELECT status FROM task_queue WHERE task_id = ?", (task.task_id,)
        ).fetchone()[0]
        assert status == task_queue.STATUS_FAILED
