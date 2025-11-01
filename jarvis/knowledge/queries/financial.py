from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class FinancialRecord:
    record_type: Optional[str]
    amount_value: Optional[float]
    amount_text: Optional[str]
    currency: Optional[str]
    counterparty: Optional[str]
    reference: Optional[str]
    date: Optional[str]
    subject: Optional[str]
    filename: Optional[str]
    message_id: Optional[str]
    attachment_id: Optional[str]


def fetch_financial_records(
    database_path: str,
    *,
    extractor: str = "llm:mistral",
    counterparty_filter: Optional[str] = None,
    record_type: Optional[str] = None,
    reference_filter: Optional[str] = None,
    limit: int = 20,
) -> List[FinancialRecord]:
    clauses = ["extractor = ?"]
    params: List = [extractor]
    if counterparty_filter:
        clauses.append("LOWER(counterparty) LIKE ?")
        params.append(f"%{counterparty_filter.lower()}%")
    if record_type:
        clauses.append("LOWER(record_type) = ?")
        params.append(record_type.lower())
    if reference_filter:
        clauses.append("LOWER(reference) LIKE ?")
        params.append(f"%{reference_filter.lower()}%")
    where_clause = " AND ".join(clauses)
    query = f"""
        SELECT
            record_type,
            amount_value,
            amount_text,
            currency,
            counterparty,
            reference,
            COALESCE(date_parsed, date_raw),
            metadata,
            message_id,
            attachment_id
        FROM financial_records
        WHERE {where_clause}
        ORDER BY COALESCE(date_parsed, date_raw) DESC, created_at DESC
        LIMIT ?
    """
    params.append(limit)
    records: List[FinancialRecord] = []
    with sqlite3.connect(database_path) as conn:
        cursor = conn.execute(query, params)
        for row in cursor.fetchall():
            metadata = json.loads(row[7]) if row[7] else {}
            records.append(
                FinancialRecord(
                    record_type=row[0],
                    amount_value=row[1],
                    amount_text=row[2],
                    currency=row[3],
                    counterparty=row[4],
                    reference=row[5],
                    date=row[6],
                    subject=metadata.get("subject"),
                    filename=metadata.get("filename"),
                    message_id=row[8],
                    attachment_id=row[9],
                )
            )
    return records
