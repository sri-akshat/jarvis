"""Medical event aggregation from entity mentions."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from typing import Dict, List, Optional, Tuple


MEDICAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS medical_events (
    event_id TEXT PRIMARY KEY,
    extractor TEXT,
    event_type TEXT,
    description TEXT,
    attributes TEXT,
    patient TEXT,
    clinician TEXT,
    facility TEXT,
    date_raw TEXT,
    date_parsed TEXT,
    message_id TEXT,
    attachment_id TEXT,
    content_id TEXT,
    chunk_index INTEGER,
    metadata TEXT,
    created_at TEXT
)
"""


@dataclass
class Mention:
    label: str
    text: str
    entity_id: str
    start: int
    end: int
    metadata: Dict

    @property
    def attributes(self) -> Dict:
        return self.metadata.get("attributes") or {}


class MedicalFactBuilder:
    EVENT_MAP = {
        "DIAGNOSIS": "DIAGNOSIS",
        "MEDICATION": "MEDICATION",
        "PRESCRIPTION": "PRESCRIPTION",
        "PROCEDURE": "PROCEDURE",
        "TREATMENT": "TREATMENT",
        "TEST": "TEST",
    }
    PATIENT_LABELS = {"PATIENT", "PERSON", "NAME"}
    CLINICIAN_LABELS = {"CLINICIAN", "DOCTOR", "PHYSICIAN"}
    FACILITY_LABELS = {"FACILITY", "ORGANIZATION", "HOSPITAL"}
    DATE_LABELS = {"DATE", "APPOINTMENT", "SCHEDULE"}
    DOSAGE_LABELS = {"DOSAGE", "FREQUENCY"}

    def __init__(self, database_path: str, extractor: str = "llm:mistral") -> None:
        self.database_path = database_path
        self.extractor = extractor
        with sqlite3.connect(self.database_path) as conn:
            conn.execute(MEDICAL_TABLE_SQL)

    def run(self, content_id: Optional[str] = None) -> int:
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            grouped = self._group_mentions(conn, content_id)
            processed = 0
            for key, mentions in grouped.items():
                processed += self._persist_events(conn, key, mentions)
            return processed

    def _group_mentions(
        self, conn: sqlite3.Connection, content_id: Optional[str]
    ) -> Dict[Tuple[str, int], List[Mention]]:
        sql = [
            "SELECT content_id, chunk_index, label, text, entity_id, start_char, end_char, metadata",
            "FROM entity_mentions",
            "WHERE extractor = ?",
        ]
        params: List = [self.extractor]
        if content_id:
            sql.append("  AND content_id = ?")
            params.append(content_id)
        sql.append("ORDER BY content_id, chunk_index, start_char")
        cursor = conn.execute("\n".join(sql), tuple(params))
        grouped: Dict[Tuple[str, int], List[Mention]] = {}
        for row in cursor.fetchall():
            metadata = json.loads(row[7]) if row[7] else {}
            key = (row[0], row[1])
            grouped.setdefault(key, []).append(
                Mention(
                    label=row[2].upper() if row[2] else "",
                    text=row[3] or "",
                    entity_id=row[4],
                    start=row[5],
                    end=row[6],
                    metadata=metadata,
                )
            )
        return grouped

    def _persist_events(
        self,
        conn: sqlite3.Connection,
        key: Tuple[str, int],
        mentions: List[Mention],
    ) -> int:
        content_id, chunk_index = key
        events = [m for m in mentions if m.label in self.EVENT_MAP]
        if not events:
            return 0
        conn.execute(
            "DELETE FROM medical_events WHERE content_id = ? AND chunk_index = ?",
            (content_id, chunk_index),
        )
        patients = [m for m in mentions if m.label in self.PATIENT_LABELS]
        clinicians = [m for m in mentions if m.label in self.CLINICIAN_LABELS]
        facilities = [m for m in mentions if m.label in self.FACILITY_LABELS]
        dates = [m for m in mentions if m.label in self.DATE_LABELS]
        dosages = [m for m in mentions if m.label in self.DOSAGE_LABELS]
        created_at = datetime.now(timezone.utc).isoformat()
        processed = 0
        for event in events:
            event_type = self.EVENT_MAP.get(event.label, event.label)
            attrs = dict(event.attributes)
            dosage = _nearest(event, dosages)
            if dosage:
                attrs.setdefault("dosage", dosage.text)
                attrs.setdefault("dosage_attributes", dosage.attributes)
            patient = _nearest_text(event, patients)
            clinician = _nearest_text(event, clinicians)
            facility = _nearest_text(event, facilities)
            date_raw = _nearest_text(event, dates)
            date_parsed = _normalize_date(date_raw) if date_raw else None
            metadata = {
                "subject": event.metadata.get("subject"),
                "filename": event.metadata.get("filename"),
                "page": event.metadata.get("page"),
                "message_id": event.metadata.get("message_id"),
                "attachment_id": event.metadata.get("attachment_id"),
            }
            event_id = _build_event_id(content_id, chunk_index, event.entity_id)
            conn.execute(
                """
                INSERT OR REPLACE INTO medical_events (
                    event_id, extractor, event_type, description, attributes, patient,
                    clinician, facility, date_raw, date_parsed, message_id, attachment_id,
                    content_id, chunk_index, metadata, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    self.extractor,
                    event_type,
                    event.text,
                    json.dumps(attrs, sort_keys=True),
                    patient,
                    clinician,
                    facility,
                    date_raw,
                    date_parsed,
                    metadata.get("message_id"),
                    metadata.get("attachment_id"),
                    content_id,
                    chunk_index,
                    json.dumps(metadata, sort_keys=True),
                    created_at,
                ),
            )
            processed += 1
        conn.commit()
        return processed


def _nearest(target: Mention, options: List[Mention]) -> Optional[Mention]:
    best = None
    best_distance = float("inf")
    for candidate in options:
        distance = min(
            abs(target.start - candidate.end),
            abs(candidate.start - target.end),
        )
        if distance < best_distance:
            best = candidate
            best_distance = distance
    return best


def _nearest_text(target: Mention, options: List[Mention]) -> Optional[str]:
    mention = _nearest(target, options)
    return mention.text if mention else None


def _normalize_date(value: str) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _build_event_id(content_id: str, chunk_index: int, entity_id: str) -> str:
    raw = f"{content_id}:{chunk_index}:{entity_id}"
    return sha1(raw.encode("utf-8")).hexdigest()
