from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class MedicalEvent:
    event_type: Optional[str]
    description: Optional[str]
    attributes: dict
    patient: Optional[str]
    clinician: Optional[str]
    facility: Optional[str]
    date: Optional[str]
    subject: Optional[str]
    filename: Optional[str]
    message_id: Optional[str]
    attachment_id: Optional[str]


def fetch_medical_events(
    database_path: str,
    *,
    extractor: str = "llm:mistral",
    event_type: Optional[str] = None,
    patient_filter: Optional[str] = None,
    limit: int = 20,
) -> List[MedicalEvent]:
    clauses = ["extractor = ?"]
    params: List = [extractor]
    if event_type:
        clauses.append("LOWER(event_type) = ?")
        params.append(event_type.lower())
    if patient_filter:
        clauses.append("LOWER(patient) LIKE ?")
        params.append(f"%{patient_filter.lower()}%")
    where_clause = " AND ".join(clauses)
    query = f"""
        SELECT
            event_type,
            description,
            attributes,
            patient,
            clinician,
            facility,
            COALESCE(date_parsed, date_raw),
            metadata,
            message_id,
            attachment_id
        FROM medical_events
        WHERE {where_clause}
        ORDER BY COALESCE(date_parsed, date_raw) DESC, created_at DESC
        LIMIT ?
    """
    params.append(limit)
    events: List[MedicalEvent] = []
    with sqlite3.connect(database_path) as conn:
        cursor = conn.execute(query, params)
        for row in cursor.fetchall():
            attributes = json.loads(row[2]) if row[2] else {}
            metadata = json.loads(row[7]) if row[7] else {}
            events.append(
                MedicalEvent(
                    event_type=row[0],
                    description=row[1],
                    attributes=attributes,
                    patient=row[3],
                    clinician=row[4],
                    facility=row[5],
                    date=row[6],
                    subject=metadata.get("subject"),
                    filename=metadata.get("filename"),
                    message_id=row[8],
                    attachment_id=row[9],
                )
            )
    return events
