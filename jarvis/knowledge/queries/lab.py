from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class LabResult:
    test_name: Optional[str]
    value: Optional[str]
    value_numeric: Optional[float]
    units: Optional[str]
    reference_range: Optional[str]
    date: Optional[str]
    patient: Optional[str]
    subject: Optional[str]
    filename: Optional[str]
    message_id: Optional[str]
    attachment_id: Optional[str]


def fetch_lab_results(
    database_path: str,
    *,
    extractor: str = "llm:mistral",
    test_filter: Optional[str] = None,
    patient_filter: Optional[str] = None,
    subject_filter: Optional[str] = None,
    limit: int = 20,
) -> List[LabResult]:
    clauses = ["extractor = ?"]
    params: List = [extractor]
    if test_filter:
        clauses.append("LOWER(test_name) LIKE ?")
        params.append(f"%{test_filter.lower()}%")
    if patient_filter:
        clauses.append("LOWER(patient) LIKE ?")
        params.append(f"%{patient_filter.lower()}%")
    if subject_filter:
        clauses.append("LOWER(json_extract(metadata, '$.subject')) LIKE ?")
        params.append(f"%{subject_filter.lower()}%")
    where_clause = " AND ".join(clauses)
    query = f"""
        SELECT
            test_name,
            measurement_text,
            measurement_value,
            measurement_units,
            reference_range,
            COALESCE(date_parsed, date_raw),
            patient,
            metadata,
            message_id,
            attachment_id
        FROM lab_results
        WHERE {where_clause}
        ORDER BY COALESCE(date_parsed, date_raw) DESC, created_at DESC
        LIMIT ?
    """
    params.append(limit)
    results: List[LabResult] = []
    with sqlite3.connect(database_path) as conn:
        cursor = conn.execute(query, params)
        for row in cursor.fetchall():
            metadata = json.loads(row[7]) if row[7] else {}
            results.append(
                LabResult(
                    test_name=row[0],
                    value=row[1],
                    value_numeric=row[2],
                    units=row[3],
                    reference_range=row[4],
                    date=row[5],
                    patient=row[6],
                    subject=metadata.get("subject"),
                    filename=metadata.get("filename"),
                    message_id=row[8],
                    attachment_id=row[9],
                )
            )
    return results
