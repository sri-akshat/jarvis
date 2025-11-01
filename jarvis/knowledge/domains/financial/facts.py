"""Financial record aggregation from entity mentions."""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from typing import Dict, List, Optional, Tuple


FINANCIAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS financial_records (
    record_id TEXT PRIMARY KEY,
    extractor TEXT,
    record_type TEXT,
    amount_value REAL,
    amount_text TEXT,
    currency TEXT,
    counterparty TEXT,
    reference TEXT,
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


class FinancialFactBuilder:
    MONEY_LABELS = {"MONEY", "AMOUNT", "TOTAL"}
    RECORD_LABELS = {"INVOICE", "PAYMENT", "BILL", "RECEIPT"}
    COUNTERPARTY_LABELS = {"ORGANIZATION", "PERSON", "VENDOR", "CUSTOMER"}
    DATE_LABELS = {"DATE", "PAYMENT_DATE", "DUE_DATE"}
    REFERENCE_LABELS = {"REFERENCE", "INVOICE_REFERENCE", "INVOICE_NUMBER", "ORDER_NUMBER"}

    def __init__(self, database_path: str, extractor: str = "llm:mistral") -> None:
        self.database_path = database_path
        self.extractor = extractor
        with sqlite3.connect(self.database_path) as conn:
            conn.execute(FINANCIAL_TABLE_SQL)

    def run(self, content_id: Optional[str] = None) -> int:
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            grouped = self._group_mentions(conn, content_id)
            processed = 0
            for key, mentions in grouped.items():
                processed += self._persist_financial_records(conn, key, mentions)
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

    def _persist_financial_records(
        self,
        conn: sqlite3.Connection,
        key: Tuple[str, int],
        mentions: List[Mention],
    ) -> int:
        content_id, chunk_index = key
        money_mentions = [m for m in mentions if m.label in self.MONEY_LABELS]
        if not money_mentions:
            return 0
        conn.execute(
            "DELETE FROM financial_records WHERE content_id = ? AND chunk_index = ?",
            (content_id, chunk_index),
        )
        record_mentions = [m for m in mentions if m.label in self.RECORD_LABELS]
        counterparties = [m for m in mentions if m.label in self.COUNTERPARTY_LABELS]
        dates = [m for m in mentions if m.label in self.DATE_LABELS]
        references = [m for m in mentions if m.label in self.REFERENCE_LABELS]
        created_at = datetime.now(timezone.utc).isoformat()
        processed = 0
        for money in money_mentions:
            record = _nearest(money, record_mentions)
            record_type = record.label if record else "PAYMENT"
            counterparty = _nearest_text(money, counterparties)
            date = _nearest_text(money, dates)
            reference = _nearest_text(money, references)
            amount_text = money.attributes.get("amount")
            amount_text = amount_text if isinstance(amount_text, str) else money.text
            currency = (
                money.attributes.get("currency")
                if isinstance(money.attributes.get("currency"), str)
                else _extract_currency(amount_text)
            )
            amount_value = _parse_numeric(amount_text)
            metadata = {
                "subject": money.metadata.get("subject"),
                "filename": money.metadata.get("filename"),
                "page": money.metadata.get("page"),
                "message_id": money.metadata.get("message_id"),
                "attachment_id": money.metadata.get("attachment_id"),
            }
            if money.attributes:
                metadata["amount_attributes"] = money.attributes
            record_id = _build_record_id(
                content_id,
                chunk_index,
                money.entity_id,
                record.entity_id if record else "",
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO financial_records (
                    record_id, extractor, record_type, amount_value, amount_text,
                    currency, counterparty, reference, date_raw, date_parsed,
                    message_id, attachment_id, content_id, chunk_index, metadata, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record_id,
                    self.extractor,
                    record_type,
                    amount_value,
                    amount_text,
                    currency,
                    counterparty,
                    reference,
                    date,
                    _normalize_date(date) if date else None,
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


def _extract_currency(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str):
        return None
    upper = value.upper()
    for code in ("INR", "USD", "EUR", "GBP"):
        if code in upper:
            return code
    if upper.startswith("₹"):
        return "INR"
    if upper.startswith("$"):
        return "USD"
    if upper.startswith("€"):
        return "EUR"
    if upper.startswith("£"):
        return "GBP"
    return None


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


def _build_record_id(
    content_id: str,
    chunk_index: int,
    money_entity_id: str,
    record_entity_id: str,
) -> str:
    raw = f"{content_id}:{chunk_index}:{money_entity_id}:{record_entity_id}"
    return sha1(raw.encode("utf-8")).hexdigest()
