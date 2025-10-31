from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, List, Tuple

import pytest

from jarvis.ingestion.common.datastore import SQLiteDataStore
from jarvis.ingestion.common.models import Attachment, Message


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "messages.db"


@pytest.fixture
def datastore_with_queue_spy(
    db_path: Path, monkeypatch
) -> Tuple[SQLiteDataStore, List[Tuple[str, dict]]]:
    """Return datastore and collect enqueued tasks."""
    from jarvis.knowledge import task_queue

    captured: List[Tuple[str, dict]] = []

    def _enqueue(db: str, task_type: str, payload: dict) -> None:
        captured.append((task_type, payload.copy()))

    monkeypatch.setattr(task_queue, "enqueue_task", _enqueue)
    datastore = SQLiteDataStore(db_path)
    return datastore, captured


@pytest.fixture
def make_attachment() -> Callable[..., Attachment]:
    def _factory(
        attachment_id: str = "att-1",
        filename: str = "file.txt",
        mime_type: str = "text/plain",
        data: bytes = b"hello world",
        metadata: dict | None = None,
    ) -> Attachment:
        return Attachment(
            id=attachment_id,
            filename=filename,
            mime_type=mime_type,
            data=data,
            metadata=metadata or {},
        )

    return _factory


@pytest.fixture
def make_message(
    make_attachment: Callable[..., Attachment]
) -> Callable[..., Message]:
    def _factory(
        message_id: str = "msg-1",
        subject: str = "Subject",
        body: str = "Body content",
        attachments: Iterable[Attachment] | None = None,
    ) -> Message:
        return Message(
            id=message_id,
            subject=subject,
            sender="sender@example.com",
            recipients=["recipient@example.com"],
            snippet=body[:50],
            body=body,
            received_at=datetime(2023, 8, 16, tzinfo=timezone.utc),
            attachments=list(attachments or []),
            metadata={"source": "pytest"},
            thread_id="thread-1",
        )

    return _factory
