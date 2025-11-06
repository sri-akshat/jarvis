"""Datastore implementations for message persistence."""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

from jarvis.ingestion.common.models import Attachment, Message
from jarvis.knowledge import task_queue

logger = logging.getLogger(__name__)


class DataStore(Protocol):
    def save_messages(
        self,
        messages: Iterable[Message],
        *,
        progress_interval: int | None = None,
    ) -> tuple[int, int, int]:
        ...


class SQLiteDataStore:
    """SQLite-backed message data store."""

    def __init__(
        self,
        database_path: Path | str,
        queue_target: str | None = None,
        connection_timeout: float = 30.0,
    ) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.queue_target = queue_target or str(self.database_path)
        self.connection_timeout = connection_timeout
        with self._connect() as conn:
            self._ensure_schema(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.database_path,
            timeout=self.connection_timeout,
        )
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                subject TEXT,
                sender TEXT,
                recipients TEXT,
                snippet TEXT,
                body TEXT,
                received_at TEXT,
                thread_id TEXT,
                metadata TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attachments (
                id TEXT,
                message_id TEXT,
                filename TEXT,
                mime_type TEXT,
                data BLOB,
                metadata TEXT,
                PRIMARY KEY (id, message_id),
                FOREIGN KEY (message_id) REFERENCES messages(id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS content_registry (
                content_id TEXT PRIMARY KEY,
                message_id TEXT,
                attachment_id TEXT,
                content_type TEXT,
                mime_type TEXT,
                sha256 TEXT,
                created_at TEXT,
                metadata TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attachment_texts (
                content_id TEXT,
                page INTEGER,
                chunk_index INTEGER,
                text TEXT,
                token_count INTEGER,
                sha256 TEXT,
                PRIMARY KEY (content_id, page, chunk_index)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS embeddings (
                embedding_id TEXT PRIMARY KEY,
                content_id TEXT,
                chunk_index INTEGER,
                model TEXT,
                dimensions INTEGER,
                vector BLOB,
                created_at TEXT,
                metadata TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_entities (
                entity_id TEXT PRIMARY KEY,
                label TEXT,
                properties TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_relations (
                relation_id TEXT PRIMARY KEY,
                source_id TEXT,
                target_id TEXT,
                relation_type TEXT,
                properties TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_mentions (
                mention_id TEXT PRIMARY KEY,
                extractor TEXT,
                content_id TEXT,
                chunk_index INTEGER,
                entity_id TEXT,
                label TEXT,
                text TEXT,
                start_char INTEGER,
                end_char INTEGER,
                confidence REAL,
                metadata TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS local_files (
                content_id TEXT PRIMARY KEY,
                path TEXT,
                size INTEGER,
                modified_at TEXT,
                metadata TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS message_search USING fts5(
                message_id UNINDEXED,
                subject,
                sender,
                recipients,
                body
            )
            """
        )

    def save_messages(
        self,
        messages: Iterable[Message],
        *,
        progress_interval: int | None = None,
    ) -> tuple[int, int, int]:
        pending_content_ids: list[str] = []
        message_count = 0
        attachment_count = 0
        with self._connect() as conn:
            self._ensure_schema(conn)
            for message in messages:
                message_count += 1
                conn.execute(
                    """
                    INSERT INTO messages (
                        id, subject, sender, recipients, snippet, body,
                        received_at, thread_id, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        subject=excluded.subject,
                        sender=excluded.sender,
                        recipients=excluded.recipients,
                        snippet=excluded.snippet,
                        body=excluded.body,
                        received_at=excluded.received_at,
                        thread_id=excluded.thread_id,
                        metadata=excluded.metadata
                    """,
                    (
                        message.id,
                        message.subject,
                        message.sender,
                        ",".join(message.recipients),
                        message.snippet,
                        message.body,
                        message.received_at.isoformat(),
                        message.thread_id,
                        json.dumps(message.metadata, sort_keys=True),
                    ),
                )
                message_content_id = self._register_message_content(conn, message)
                self._upsert_message_search(conn, message)
                pending_content_ids.append(message_content_id)
                for attachment in message.attachments:
                    attachment_count += 1
                    self._save_attachment(conn, message.id, attachment)
                    attachment_content_id = self._register_attachment_content(
                        conn, message, attachment
                    )
                    pending_content_ids.append(attachment_content_id)
                if progress_interval and message_count % progress_interval == 0:
                    logger.info(
                        "[datastore] Persisted %s message(s) so far (attachments %s)",
                        message_count,
                        attachment_count,
                    )
        for content_id in pending_content_ids:
            task_queue.enqueue_task(
                self.queue_target,
                "semantic_index",
                {"content_id": content_id},
            )
        return message_count, attachment_count, len(pending_content_ids)

    @staticmethod
    def _save_attachment(
        conn: sqlite3.Connection, message_id: str, attachment: Attachment
    ) -> None:
        conn.execute(
            """
            INSERT INTO attachments (
                id, message_id, filename, mime_type, data, metadata
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id, message_id) DO UPDATE SET
                filename=excluded.filename,
                mime_type=excluded.mime_type,
                data=excluded.data,
                metadata=excluded.metadata
            """,
            (
                attachment.id,
                message_id,
                attachment.filename,
                attachment.mime_type,
                attachment.data,
                json.dumps(attachment.metadata, sort_keys=True),
            ),
        )

    def _register_message_content(
        self, conn: sqlite3.Connection, message: Message
    ) -> str:
        content_id = f"message:{message.id}"
        conn.execute(
            """
            INSERT INTO content_registry (
                content_id, message_id, attachment_id, content_type, mime_type,
                sha256, created_at, metadata
            ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT(content_id) DO UPDATE SET
                mime_type=excluded.mime_type,
                sha256=excluded.sha256,
                metadata=excluded.metadata
            """,
            (
                content_id,
                message.id,
                "message",
                "text/plain",
                _sha256(message.body.encode("utf-8")) if message.body else None,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(
                    {
                        "subject": message.subject,
                        "sender": message.sender,
                        "recipients": list(message.recipients),
                        "thread_id": message.thread_id,
                    },
                    sort_keys=True,
                ),
            ),
        )
        return content_id

    def _register_attachment_content(
        self, conn: sqlite3.Connection, message: Message, attachment: Attachment
    ) -> str:
        content_id = f"attachment:{message.id}:{attachment.id}"
        attachment_meta = dict(attachment.metadata or {})
        attachment_meta.setdefault("filename", attachment.filename)
        attachment_meta.setdefault("message_subject", message.subject)
        attachment_meta.setdefault("message_sender", message.sender)
        attachment_meta.setdefault("message_recipients", list(message.recipients))
        attachment_meta.setdefault("thread_id", message.thread_id)
        conn.execute(
            """
            INSERT INTO content_registry (
                content_id, message_id, attachment_id, content_type, mime_type,
                sha256, created_at, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(content_id) DO UPDATE SET
                mime_type=excluded.mime_type,
                sha256=excluded.sha256,
                metadata=excluded.metadata
            """,
            (
                content_id,
                message.id,
                attachment.id,
                "attachment",
                attachment.mime_type,
                _sha256(attachment.data),
                datetime.now(timezone.utc).isoformat(),
                json.dumps(attachment_meta, sort_keys=True),
            ),
        )
        return content_id

    def _upsert_message_search(self, conn: sqlite3.Connection, message: Message) -> None:
        recipients = ",".join(recipient for recipient in message.recipients)
        conn.execute(
            "DELETE FROM message_search WHERE message_id = ?",
            (message.id,),
        )
        conn.execute(
            """
            INSERT INTO message_search (message_id, subject, sender, recipients, body)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message.id,
                message.subject or "",
                message.sender or "",
                recipients,
                message.body or "",
            ),
        )


def _sha256(data: bytes | None) -> str | None:
    if data is None:
        return None
    import hashlib

    return hashlib.sha256(data).hexdigest()
