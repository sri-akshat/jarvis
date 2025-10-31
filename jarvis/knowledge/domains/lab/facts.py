"""Lab result aggregation from entity mentions."""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from typing import Dict, List, Optional, Tuple


LAB_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS lab_results (
    result_id TEXT PRIMARY KEY,
    extractor TEXT,
    test_entity_id TEXT,
    measurement_entity_id TEXT,
    reference_entity_id TEXT,
    test_name TEXT,
    measurement_text TEXT,
    measurement_value REAL,
    measurement_units TEXT,
    reference_range TEXT,
    date_raw TEXT,
    date_parsed TEXT,
    patient TEXT,
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


class LabFactBuilder:
    def __init__(self, database_path: str, extractor: str = "llm:mistral") -> None:
        self.database_path = database_path
        self.extractor = extractor
        with sqlite3.connect(self.database_path) as conn:
            conn.execute(LAB_TABLE_SQL)

    def run(self, content_id: Optional[str] = None) -> int:
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            grouped = self._group_mentions(conn, content_id)
            processed = 0
            for key, mentions in grouped.items():
                processed += self._persist_lab_results(conn, key, mentions)
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

    def _persist_lab_results(
        self,
        conn: sqlite3.Connection,
        key: Tuple[str, int],
        mentions: List[Mention],
    ) -> int:
        content_id, chunk_index = key
        measurements = [m for m in mentions if m.label == "MEASUREMENT"]
        if not measurements:
            return 0
        conn.execute(
            "DELETE FROM lab_results WHERE content_id = ? AND chunk_index = ?",
            (content_id, chunk_index),
        )
        lab_tests = [m for m in mentions if m.label == "LAB_TEST"]
        references = [m for m in mentions if m.label == "REFERENCE_RANGE"]
        dates = [m for m in mentions if m.label == "DATE"]
        patients = [m for m in mentions if m.label in {"PATIENT", "PERSON", "NAME"}]
        created_at = datetime.now(timezone.utc).isoformat()
        processed = 0
        for measurement in measurements:
            test = _nearest(measurement, lab_tests)
            reference = _nearest(measurement, references)
            capture_date = _nearest(measurement, dates) or (dates[0] if dates else None)
            patient = patients[0].text if patients else None
            raw_value = measurement.attributes.get("value")
            value_text = raw_value if isinstance(raw_value, str) else measurement.text
            value_numeric = _parse_numeric(value_text)
            units = measurement.attributes.get("units") or _extract_units(measurement.text)
            reference_text = None
            if reference:
                reference_text = (
                    reference.attributes.get("range")
                    if isinstance(reference.attributes.get("range"), str)
                    else reference.text
                )
            date_raw = capture_date.text if capture_date else None
            date_parsed = _normalize_date(date_raw) if date_raw else None
            metadata = {
                "page": measurement.metadata.get("page"),
                "filename": measurement.metadata.get("filename"),
                "subject": measurement.metadata.get("subject"),
                "message_id": measurement.metadata.get("message_id"),
                "attachment_id": measurement.metadata.get("attachment_id"),
            }
            if measurement.attributes:
                metadata["measurement_attributes"] = measurement.attributes
            if reference and reference.attributes:
                metadata["reference_attributes"] = reference.attributes
            result_id = _build_result_id(
                content_id,
                chunk_index,
                measurement.entity_id,
                test.entity_id if test else "",
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO lab_results (
                    result_id, extractor, test_entity_id, measurement_entity_id,
                    reference_entity_id, test_name, measurement_text, measurement_value,
                    measurement_units, reference_range, date_raw, date_parsed, patient,
                    message_id, attachment_id, content_id, chunk_index, metadata, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_id,
                    self.extractor,
                    test.entity_id if test else None,
                    measurement.entity_id,
                    reference.entity_id if reference else None,
                    test.text if test else None,
                    value_text,
                    value_numeric,
                    units,
                    reference_text,
                    date_raw,
                    date_parsed,
                    patient,
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


def _parse_numeric(value: Optional[str]) -> Optional[float]:
    if not isinstance(value, str):
        return None
    match = re.search(r"-?\d+(?:[.,]\d+)?", value)
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _extract_units(text: str) -> Optional[str]:
    parts = text.split()
    if len(parts) >= 2:
        return parts[-1]
    return None


def _normalize_date(value: str) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _build_result_id(
    content_id: str,
    chunk_index: int,
    measurement_entity_id: str,
    test_entity_id: str,
) -> str:
    raw = f"{content_id}:{chunk_index}:{measurement_entity_id}:{test_entity_id}"
    return sha1(raw.encode("utf-8")).hexdigest()
