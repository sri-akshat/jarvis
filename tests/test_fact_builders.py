from __future__ import annotations

import copy
import json
import sqlite3

from jarvis.knowledge.entity_extractor import EntityMention, KnowledgeGraphBuilder
from jarvis.knowledge.domains.financial import (
    FinancialFactBuilder,
    fetch_financial_records,
)
from jarvis.knowledge.domains.lab import LabFactBuilder
from jarvis.knowledge.queries import fetch_lab_results
from jarvis.knowledge.domains.medical import (
    MedicalFactBuilder,
    fetch_medical_events,
)
from jarvis.knowledge.semantic_indexer import SemanticIndexer
from jarvis.ingestion.common.datastore import SQLiteDataStore


class StubExtractor:
    def __init__(self, mentions):
        self._mentions = mentions

    def extract(self, text: str):
        return [copy.deepcopy(m) for m in self._mentions]


def _prepare_content(
    db_path, make_message, body_text: str, mentions, extractor_name: str, monkeypatch
) -> str:
    from jarvis.knowledge import task_queue

    monkeypatch.setattr(task_queue, "enqueue_task", lambda *args, **kwargs: None)
    datastore = SQLiteDataStore(db_path)
    message = make_message(body=body_text)
    datastore.save_messages([message])
    indexer = SemanticIndexer(str(db_path))
    content_id = f"message:{message.id}"
    indexer.process_content_id(content_id)
    builder = KnowledgeGraphBuilder(
        database_path=str(db_path),
        extractor=StubExtractor(mentions),
        extractor_name=extractor_name,
    )
    builder.run(content_id=content_id)
    return content_id


def test_lab_fact_builder(db_path, make_message, monkeypatch):
    mentions = [
        EntityMention(text="HbA1c", label="LAB_TEST", start_char=0, end_char=5),
        EntityMention(
            text="5.6%",
            label="MEASUREMENT",
            start_char=20,
            end_char=24,
            metadata={"attributes": {"value": "5.6", "units": "%"}},
        ),
        EntityMention(
            text="4.0-5.7",
            label="REFERENCE_RANGE",
            start_char=30,
            end_char=37,
        ),
        EntityMention(
            text="2023-08-16",
            label="DATE",
            start_char=40,
            end_char=50,
        ),
        EntityMention(
            text="Akshat",
            label="PATIENT",
            start_char=55,
            end_char=61,
        ),
    ]
    content_id = _prepare_content(
        db_path,
        make_message,
        body_text="HbA1c result 5.6% reference 4.0-5.7 on 2023-08-16 for Akshat",
        mentions=mentions,
        extractor_name="test-lab",
        monkeypatch=monkeypatch,
    )

    builder = LabFactBuilder(str(db_path), extractor="test-lab")
    processed = builder.run(content_id=content_id)
    assert processed == 1

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT test_name, measurement_value, measurement_units, patient FROM lab_results"
    ).fetchone()
    conn.close()
    assert row == ("HbA1c", 5.6, "%", "Akshat")

    results = fetch_lab_results(
        str(db_path), extractor="test-lab", test_filter="HbA1c", patient_filter="Akshat"
    )
    assert results[0].value_numeric == 5.6


def test_financial_fact_builder(db_path, make_message, monkeypatch):
    mentions = [
        EntityMention(text="Invoice", label="INVOICE", start_char=0, end_char=7),
        EntityMention(
            text="INR 1500",
            label="MONEY",
            start_char=20,
            end_char=28,
            metadata={"attributes": {"currency": "INR", "amount": "1500"}},
        ),
        EntityMention(text="2023-08-01", label="DATE", start_char=40, end_char=50),
        EntityMention(text="Dezignare", label="ORGANIZATION", start_char=60, end_char=69),
        EntityMention(text="INV-42", label="INVOICE_REFERENCE", start_char=70, end_char=76),
    ]
    content_id = _prepare_content(
        db_path,
        make_message,
        body_text="Invoice INV-42 from Dezignare dated 2023-08-01 amount INR 1500",
        mentions=mentions,
        extractor_name="test-financial",
        monkeypatch=monkeypatch,
    )

    builder = FinancialFactBuilder(str(db_path), extractor="test-financial")
    processed = builder.run(content_id=content_id)
    assert processed == 1

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT record_type, amount_value, currency, counterparty, reference FROM financial_records"
    ).fetchone()
    conn.close()
    assert row == ("INVOICE", 1500.0, "INR", "Dezignare", "INV-42")

    records = fetch_financial_records(
        str(db_path), extractor="test-financial", counterparty_filter="Dezignare"
    )
    assert records[0].amount_value == 1500.0


def test_medical_fact_builder(db_path, make_message, monkeypatch):
    mentions = [
        EntityMention(text="Metformin", label="MEDICATION", start_char=0, end_char=9),
        EntityMention(
            text="500 mg daily",
            label="DOSAGE",
            start_char=15,
            end_char=27,
        ),
        EntityMention(text="Akshat", label="PATIENT", start_char=35, end_char=41),
        EntityMention(text="Dr. Singh", label="CLINICIAN", start_char=45, end_char=54),
        EntityMention(text="City Hospital", label="FACILITY", start_char=60, end_char=73),
        EntityMention(text="2023-08-20", label="DATE", start_char=80, end_char=90),
    ]
    content_id = _prepare_content(
        db_path,
        make_message,
        body_text="Metformin 500 mg daily prescribed by Dr. Singh at City Hospital on 2023-08-20 for Akshat",
        mentions=mentions,
        extractor_name="test-medical",
        monkeypatch=monkeypatch,
    )

    builder = MedicalFactBuilder(str(db_path), extractor="test-medical")
    processed = builder.run(content_id=content_id)
    assert processed == 1

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT event_type, description, patient, clinician, facility FROM medical_events"
    ).fetchone()
    attributes = json.loads(
        conn.execute("SELECT attributes FROM medical_events").fetchone()[0]
    )
    conn.close()

    assert row == ("MEDICATION", "Metformin", "Akshat", "Dr. Singh", "City Hospital")
    assert attributes["dosage"] == "500 mg daily"

    events = fetch_medical_events(
        str(db_path), extractor="test-medical", patient_filter="Akshat"
    )
    assert events[0].patient == "Akshat"
