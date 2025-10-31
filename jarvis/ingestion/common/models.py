"""Dataclasses describing provider-agnostic messages and attachments."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class Attachment:
    id: str
    filename: str
    mime_type: str
    data: bytes
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class Message:
    id: str
    subject: str
    sender: str
    recipients: List[str]
    snippet: str
    body: str
    received_at: datetime
    attachments: List[Attachment] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)
    thread_id: Optional[str] = None
