"""Ingestion pipeline helpers."""
from __future__ import annotations

import logging
from typing import Iterable, List

from jarvis.ingestion.common.models import Message
from jarvis.ingestion.common.datastore import DataStore

logger = logging.getLogger(__name__)


def ingest_messages(
    service,
    datastore: DataStore,
    query: str,
    *,
    limit: int | None = None,
    page_size: int | None = None,
    progress_interval: int | None = None,
    batch_size: int | None = None,
) -> tuple[int, int, int]:
    """Fetch messages from service, persist them, and return ingestion counters."""
    messages: Iterable[Message] = service.search(
        query,
        limit=limit,
        page_size=page_size,
    )
    effective_batch = batch_size if batch_size and batch_size > 0 else None
    if effective_batch is None:
        return datastore.save_messages(messages, progress_interval=progress_interval)

    buffer: List[Message] = []
    total_messages = 0
    total_attachments = 0
    total_tasks = 0
    for message in messages:
        buffer.append(message)
        if len(buffer) >= effective_batch:
            msg_count, att_count, task_count = datastore.save_messages(
                buffer,
                progress_interval=progress_interval,
            )
            total_messages += msg_count
            total_attachments += att_count
            total_tasks += task_count
            buffer = []
            logger.info(
                "[ingest] Persisted %s message(s) so far (attachments=%s, tasks=%s)",
                total_messages,
                total_attachments,
                total_tasks,
            )
    if buffer:
        msg_count, att_count, task_count = datastore.save_messages(
            buffer,
            progress_interval=progress_interval,
        )
        total_messages += msg_count
        total_attachments += att_count
        total_tasks += task_count
    return total_messages, total_attachments, total_tasks
