"""Ingestion pipeline helpers."""
from __future__ import annotations

from typing import Iterable

from jarvis.ingestion.common.models import Message
from jarvis.ingestion.common.datastore import DataStore


def ingest_messages(service, datastore: DataStore, query: str) -> None:
    """Fetch messages from service and persist them via datastore."""
    messages: Iterable[Message] = service.search(query)
    datastore.save_messages(messages)
