"""Ingestion pipeline helpers."""
from __future__ import annotations

from typing import Iterable

from jarvis.ingestion.common.models import Message
from jarvis.ingestion.common.datastore import DataStore


def ingest_messages(service, datastore: DataStore, query: str) -> tuple[int, int, int]:
    """Fetch messages from service, persist them, and return ingestion counters."""
    messages: Iterable[Message] = service.search(query)
    return datastore.save_messages(messages)
