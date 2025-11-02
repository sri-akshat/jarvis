from __future__ import annotations

import pytest

from dataclasses import dataclass

import json
import sqlite3
from pathlib import Path

import pytest

from jarvis.agent.base import ToolContext
from jarvis.agent.tools.finance import finance_payments_tool
from jarvis.agent.tools.medical import medical_events_tool
from jarvis.knowledge.finance_graph import (
    PaymentMention,
    _normalise_currency,
    _parse_amount,
)
from jarvis.knowledge.neo4j_exporter import Neo4jConnectionConfig


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Rs. 200000.00", 200000.0),
        ("USD 3,955", 3955.0),
        ("Amount Paid: 12.50", 12.5),
        ("No digits here", None),
        ("Rs. 5 lakhs", 500000.0),
        ("Amount: 2 lakh", 200000.0),
        ("1 crore", 10000000.0),
    ],
)
def test_parse_amount(text, expected):
    assert _parse_amount(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Rs 1000", "INR"),
        ("Amount (INR)", "INR"),
        ("USD 45", "USD"),
        ("€25", "EUR"),
        ("£10", "GBP"),
        ("Unknown", None),
    ],
)
def test_normalise_currency(text, expected):
    assert _normalise_currency(text) == expected


def test_finance_tool_returns_totals(monkeypatch):
    mentions = [
        PaymentMention(
            amount_text="rs. 100000",
            amount_value=100000.0,
            currency="INR",
            subject="Payment",
            filename=None,
            content_type="message",
            message_id="m1",
            attachment_id=None,
        ),
        PaymentMention(
            amount_text="rs. 200000",
            amount_value=200000.0,
            currency="INR",
            subject="Payment",
            filename=None,
            content_type="message",
            message_id="m2",
            attachment_id=None,
        ),
    ]

    def fake_collect(config, counterparty, limit=None):
        assert counterparty == "dezignare"
        return mentions

    monkeypatch.setattr("jarvis.agent.tools.finance.collect_payments_from_graph", fake_collect)

    context = ToolContext(database_path="/tmp/dummy.db", neo4j_config=Neo4jConnectionConfig("bolt://localhost", "u", "p"))
    result = finance_payments_tool(context, {"counterparty": "dezignare"})
    assert result.success
    assert result.data["totals"]["INR"] == 300000.0
    assert len(result.data["mentions"]) == 2


def test_medical_tool_reads_events(tmp_path: Path):
    db_path = tmp_path / "events.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE medical_events (
                event_id TEXT,
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
        )
        metadata = json.dumps({"subject": "Prescription", "filename": "prescription.pdf"})
        attributes = json.dumps({"dosage": "500 mg"})
        conn.execute(
            """
            INSERT INTO medical_events (
                event_id, extractor, event_type, description, attributes, patient,
                clinician, facility, date_raw, date_parsed, message_id, attachment_id,
                content_id, chunk_index, metadata, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "evt1",
                "llm:mistral",
                "MEDICATION",
                "Metformin",
                attributes,
                "Akshat",
                "Dr. Singh",
                "City Hospital",
                "2024-01-01",
                "2024-01-01",
                "msg-1",
                "att-1",
                "content-1",
                0,
                metadata,
                "2025-01-01T00:00:00Z",
            ),
        )

    context = ToolContext(database_path=str(db_path))
    result = medical_events_tool(context, {"patient": "Akshat"})
    assert result.success
    assert result.data["patient_filter"] == "Akshat"
    events = result.data["events"]
    assert len(events) == 1
    assert events[0]["description"] == "Metformin"
