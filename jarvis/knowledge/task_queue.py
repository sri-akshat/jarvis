"""Task queue utilities for ingestion workflows."""
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from typing import Optional, Sequence

try:  # pragma: no cover - optional import for redis-backed queue
    import redis
    from redis.exceptions import WatchError
except ImportError:  # pragma: no cover
    redis = None
    WatchError = Exception  # type: ignore[misc,assignment]


STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

_QUEUE_PREFIX = os.getenv("JARVIS_TASK_QUEUE_PREFIX", "jarvis:task_queue")
_READY_KEY_SUFFIX = ":ready"
_DELAYED_KEY_SUFFIX = ":delayed"
_IN_PROGRESS_KEY_SUFFIX = ":in_progress"
_TASK_KEY_SUFFIX = ":task:"
_PROMOTION_BATCH_SIZE = 128

_redis_clients: dict[str, "redis.Redis"] = {}

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


def _is_redis_target(target: str) -> bool:
    lowered = target.lower()
    return lowered.startswith(("redis://", "rediss://", "unix://"))


def _ensure_redis() -> None:
    if redis is None:  # pragma: no cover - runtime guard
        raise RuntimeError(
            "Redis support is not available. Install the 'redis' package to use the Redis task queue."
        )


def _get_redis_client(redis_url: str) -> "redis.Redis":
    _ensure_redis()
    client = _redis_clients.get(redis_url)
    if client is None:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        _redis_clients[redis_url] = client
    return client


def _ready_key(_: str) -> str:
    return f"{_QUEUE_PREFIX}{_READY_KEY_SUFFIX}"


def _delayed_key(_: str) -> str:
    return f"{_QUEUE_PREFIX}{_DELAYED_KEY_SUFFIX}"


def _in_progress_key(_: str) -> str:
    return f"{_QUEUE_PREFIX}{_IN_PROGRESS_KEY_SUFFIX}"


def _task_key(task_id: str) -> str:
    return f"{_QUEUE_PREFIX}{_TASK_KEY_SUFFIX}{task_id}"


def enqueue_task(
    queue_target: str,
    task_type: str,
    payload: dict,
    *,
    available_at: Optional[datetime] = None,
) -> None:
    target = str(queue_target)
    if _is_redis_target(target):
        _redis_enqueue_task(target, task_type, payload, available_at=available_at)
    else:
        _sqlite_enqueue_task(target, task_type, payload, available_at=available_at)


def fetch_and_lock_task(
    queue_target: str,
    *,
    task_types: Optional[Sequence[str]] = None,
    lock_timeout_seconds: int = 300,
) -> Optional[Task]:
    target = str(queue_target)
    if _is_redis_target(target):
        return _redis_fetch_and_lock_task(
            target,
            task_types=task_types,
            lock_timeout_seconds=lock_timeout_seconds,
        )
    return _sqlite_fetch_and_lock_task(
        target,
        task_types=task_types,
        lock_timeout_seconds=lock_timeout_seconds,
    )


def complete_task(queue_target: str, task_id: str) -> None:
    target = str(queue_target)
    if _is_redis_target(target):
        _redis_complete_task(target, task_id)
    else:
        _sqlite_complete_task(target, task_id)


def fail_task(
    queue_target: str,
    task_id: str,
    *,
    error: str,
    retry_delay_seconds: int = 300,
    max_attempts: int = 5,
) -> None:
    target = str(queue_target)
    if _is_redis_target(target):
        _redis_fail_task(
            target,
            task_id,
            error=error,
            retry_delay_seconds=retry_delay_seconds,
            max_attempts=max_attempts,
        )
    else:
        _sqlite_fail_task(
            target,
            task_id,
            error=error,
            retry_delay_seconds=retry_delay_seconds,
            max_attempts=max_attempts,
        )


def _sqlite_enqueue_task(
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


def _sqlite_fetch_and_lock_task(
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


def _sqlite_complete_task(database_path: str, task_id: str) -> None:
    with sqlite3.connect(database_path) as conn:
        ensure_queue_tables(conn)
        conn.execute("DELETE FROM task_queue WHERE task_id = ?", (task_id,))


def _sqlite_fail_task(
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


def _redis_enqueue_task(  # pragma: no cover - requires redis runtime
    redis_url: str,
    task_type: str,
    payload: dict,
    *,
    available_at: Optional[datetime] = None,
) -> None:
    client = _get_redis_client(redis_url)
    task_id = _compute_task_id(task_type, payload)
    now = _now()
    available_dt = available_at or now
    payload_json = json.dumps(payload, sort_keys=True)
    task_key = _task_key(task_id)
    ready_key = _ready_key(redis_url)
    delayed_key = _delayed_key(redis_url)
    in_progress_key = _in_progress_key(redis_url)

    while True:
        pipe = client.pipeline()
        try:
            pipe.watch(task_key)
            existing_raw = pipe.get(task_key)
            if existing_raw:
                task_data = json.loads(existing_raw)
                status = task_data.get("status", STATUS_PENDING)
                if status in (STATUS_COMPLETED, STATUS_FAILED):
                    task_data["attempts"] = 0
                    task_data["last_error"] = None
                task_data.update(
                    task_type=task_type,
                    payload=payload_json,
                    available_at=available_dt.isoformat(),
                    status=STATUS_PENDING,
                    locked_at=None,
                    last_error=task_data.get("last_error"),
                    updated_at=now.isoformat(),
                )
            else:
                task_data = {
                    "task_id": task_id,
                    "task_type": task_type,
                    "payload": payload_json,
                    "attempts": 0,
                    "available_at": available_dt.isoformat(),
                    "locked_at": None,
                    "status": STATUS_PENDING,
                    "last_error": None,
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }
            pipe.multi()
            pipe.set(task_key, json.dumps(task_data, sort_keys=True))
            pipe.lrem(ready_key, 0, task_id)
            pipe.zrem(delayed_key, task_id)
            pipe.zrem(in_progress_key, task_id)
            if available_dt <= now:
                pipe.rpush(ready_key, task_id)
            else:
                pipe.zadd(delayed_key, {task_id: available_dt.timestamp()})
            pipe.execute()
            break
        except WatchError:  # pragma: no cover - rare contention
            continue
        finally:
            pipe.reset()


def _redis_fetch_and_lock_task(  # pragma: no cover - requires redis runtime
    redis_url: str,
    *,
    task_types: Optional[Sequence[str]] = None,
    lock_timeout_seconds: int = 300,
) -> Optional[Task]:
    client = _get_redis_client(redis_url)
    now = _now()
    _requeue_stale_tasks(client, redis_url, now, lock_timeout_seconds)
    _promote_due_tasks(client, redis_url, now)

    ready_key = _ready_key(redis_url)
    queue_length = client.llen(ready_key)
    if queue_length == 0:
        return None

    for _ in range(queue_length):
        task_id = client.lpop(ready_key)
        if task_id is None:
            return None
        task_key = _task_key(task_id)
        raw = client.get(task_key)
        if not raw:
            continue
        data = json.loads(raw)
        if task_types and data.get("task_type") not in task_types:
            data["status"] = STATUS_PENDING
            data["locked_at"] = None
            data["updated_at"] = now.isoformat()
            client.set(task_key, json.dumps(data, sort_keys=True))
            _redis_reschedule_task(client, redis_url, task_id, data, now=now)
            continue
        data["status"] = STATUS_IN_PROGRESS
        data["locked_at"] = now.isoformat()
        data["updated_at"] = now.isoformat()
        payload = json.loads(data.get("payload", "{}"))
        attempts = data.get("attempts", 0)
        available_at = datetime.fromisoformat(data.get("available_at", now.isoformat()))
        locked_at = datetime.fromisoformat(data["locked_at"])
        last_error = data.get("last_error")
        pipe = client.pipeline()
        pipe.set(task_key, json.dumps(data, sort_keys=True))
        pipe.zadd(_in_progress_key(redis_url), {task_id: now.timestamp()})
        pipe.execute()
        return Task(
            task_id=task_id,
            task_type=data["task_type"],
            payload=payload,
            attempts=attempts,
            available_at=available_at,
            locked_at=locked_at,
            last_error=last_error,
        )
    return None


def _promote_due_tasks(client: "redis.Redis", redis_url: str, now: datetime) -> None:  # pragma: no cover - redis path
    delayed_key = _delayed_key(redis_url)
    ready_key = _ready_key(redis_url)
    deadline = now.timestamp()
    while True:
        due = client.zrangebyscore(
            delayed_key, "-inf", deadline, start=0, num=_PROMOTION_BATCH_SIZE
        )
        if not due:
            break
        pipe = client.pipeline()
        pipe.zrem(delayed_key, *due)
        pipe.rpush(ready_key, *due)
        pipe.execute()


def _requeue_stale_tasks(  # pragma: no cover - redis path
    client: "redis.Redis",
    redis_url: str,
    now: datetime,
    lock_timeout_seconds: int,
) -> None:
    in_progress_key = _in_progress_key(redis_url)
    cutoff = now.timestamp() - lock_timeout_seconds
    while True:
        stale = client.zrangebyscore(
            in_progress_key, "-inf", cutoff, start=0, num=_PROMOTION_BATCH_SIZE
        )
        if not stale:
            break
        for task_id in stale:
            task_key = _task_key(task_id)
            raw = client.get(task_key)
            if raw:
                data = json.loads(raw)
                data["status"] = STATUS_PENDING
                data["locked_at"] = None
                data["updated_at"] = now.isoformat()
                client.set(task_key, json.dumps(data, sort_keys=True))
                _redis_reschedule_task(client, redis_url, task_id, data, now=now)
            client.zrem(in_progress_key, task_id)


def _redis_reschedule_task(  # pragma: no cover - requires redis runtime
    client: "redis.Redis",
    redis_url: str,
    task_id: str,
    data: dict,
    *,
    now: Optional[datetime] = None,
) -> None:
    ready_key = _ready_key(redis_url)
    delayed_key = _delayed_key(redis_url)
    reference = now or _now()
    available_str = data.get("available_at", reference.isoformat())
    try:
        available_at = datetime.fromisoformat(available_str)
    except ValueError:
        available_at = reference
    pipe = client.pipeline()
    pipe.lrem(ready_key, 0, task_id)
    pipe.zrem(delayed_key, task_id)
    if available_at <= reference:
        pipe.rpush(ready_key, task_id)
    else:
        pipe.zadd(delayed_key, {task_id: available_at.timestamp()})
    pipe.execute()


def _redis_complete_task(redis_url: str, task_id: str) -> None:  # pragma: no cover - requires redis runtime
    client = _get_redis_client(redis_url)
    task_key = _task_key(task_id)
    pipe = client.pipeline()
    pipe.delete(task_key)
    pipe.lrem(_ready_key(redis_url), 0, task_id)
    pipe.zrem(_delayed_key(redis_url), task_id)
    pipe.zrem(_in_progress_key(redis_url), task_id)
    pipe.execute()


def _redis_fail_task(  # pragma: no cover - requires redis runtime
    redis_url: str,
    task_id: str,
    *,
    error: str,
    retry_delay_seconds: int = 300,
    max_attempts: int = 5,
) -> None:
    client = _get_redis_client(redis_url)
    task_key = _task_key(task_id)
    raw = client.get(task_key)
    if not raw:
        return
    now = _now()
    data = json.loads(raw)
    attempts = data.get("attempts", 0) + 1
    data["attempts"] = attempts
    data["last_error"] = error[:1024]
    data["locked_at"] = None
    data["updated_at"] = now.isoformat()
    client.zrem(_in_progress_key(redis_url), task_id)
    if attempts >= max_attempts:
        data["status"] = STATUS_FAILED
        client.set(task_key, json.dumps(data, sort_keys=True))
        return
    data["status"] = STATUS_PENDING
    available_at = now + timedelta(seconds=retry_delay_seconds)
    data["available_at"] = available_at.isoformat()
    client.set(task_key, json.dumps(data, sort_keys=True))
    _redis_reschedule_task(client, redis_url, task_id, data, now=now)
