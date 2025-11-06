from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest
import requests

from jarvis.agent import ToolContext, ToolExecutor, ToolOrchestrator, load_default_registry
from jarvis.knowledge.finance_graph import OllamaLLMClient
from jarvis.knowledge.semantic_indexer import SimpleEmbeddingGenerator


def _ensure_mistral_available(endpoint: str) -> None:
    try:
        model_name = os.getenv("OLLAMA_MODEL") or os.getenv("MISTRAL_MODEL") or "qwen2.5:7b"
        response = requests.post(
            endpoint,
            json={"model": model_name, "prompt": "hi", "stream": False},
            timeout=10,
        )
        if response.status_code >= 500:
            pytest.skip(f"Mistral endpoint unhealthy: {response.status_code}")
    except requests.RequestException:
        pytest.skip("Mistral endpoint not reachable")


@pytest.mark.skipif(
    not os.getenv("RUN_MISTRAL_TESTS"),
    reason="Set RUN_MISTRAL_TESTS=1 to run integration tests that call the local LLM.",
)
def test_agent_falls_back_to_semantic_search(tmp_path: Path, monkeypatch):
    endpoint = os.getenv("OLLAMA_ENDPOINT") or os.getenv("MISTRAL_ENDPOINT") or "http://localhost:11434/api/generate"
    model = os.getenv("OLLAMA_MODEL") or os.getenv("MISTRAL_MODEL") or "qwen2.5:7b"
    _ensure_mistral_available(endpoint)

    db_path = tmp_path / "messages.db"
    _populate_embeddings(str(db_path))

    from jarvis.knowledge.queries import lab as lab_module

    monkeypatch.setattr(lab_module, "fetch_lab_results", lambda *args, **kwargs: [])

    registry = load_default_registry()
    context = ToolContext(database_path=str(db_path))
    executor = ToolExecutor(context, registry)
    llm_client = OllamaLLMClient(model=model, endpoint=endpoint, timeout=90)
    orchestrator = ToolOrchestrator(registry, executor, llm_client)

    response = orchestrator.run("How is Meera's creatinine trending?")
    assert any(call.tool == "semantic_search" for call in response.tool_calls)


def _populate_embeddings(database_path: str) -> None:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    generator = SimpleEmbeddingGenerator()
    text = "Meera Dixit serum creatinine is 1.2 mg/dL on 2024-01-10."
    vector = generator.embed(text)
    created_at = datetime.utcnow().isoformat()
    metadata = json.dumps(
        {
            "page": 0,
            "filename": "meera_lab_report.pdf",
            "subject": "Kidney Function Test",
        }
    )

    schema = """
    CREATE TABLE IF NOT EXISTS attachment_texts (
        content_id TEXT,
        page INTEGER,
        chunk_index INTEGER,
        text TEXT,
        token_count INTEGER,
        sha256 TEXT,
        PRIMARY KEY (content_id, page, chunk_index)
    );
    CREATE TABLE IF NOT EXISTS embeddings (
        embedding_id TEXT PRIMARY KEY,
        content_id TEXT,
        chunk_index INTEGER,
        model TEXT,
        dimensions INTEGER,
        vector BLOB,
        created_at TEXT,
        metadata TEXT
    );
    CREATE TABLE IF NOT EXISTS lab_results (
        result_id TEXT,
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
    );
    """
    with sqlite3.connect(database_path) as conn:
        conn.executescript(schema)
        conn.execute(
            """
            INSERT OR REPLACE INTO attachment_texts
            (content_id, page, chunk_index, text, token_count, sha256)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("message:sample", 0, 0, text, len(text.split()), "hash"),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO embeddings
            (embedding_id, content_id, chunk_index, model, dimensions, vector, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "message:sample:0:hashed-bow-128",
                "message:sample",
                0,
                generator.model_name,
                generator.dimensions,
                vector.tobytes(),
                created_at,
                metadata,
            ),
        )
