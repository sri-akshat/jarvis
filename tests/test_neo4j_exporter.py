from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from jarvis.ingestion.common.datastore import SQLiteDataStore
from jarvis.knowledge.neo4j_exporter import (
    Neo4jConnectionConfig,
    Neo4jGraphExporter,
    _sanitize_label,
    _sanitize_rel_type,
)


class StubTx:
    def __init__(self) -> None:
        self.commands = []

    def run(self, query, **params):
        self.commands.append((query.strip(), params))


class StubSession:
    def __init__(self) -> None:
        self.write_calls: list[StubTx] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute_write(self, func, *args, **kwargs):
        tx = StubTx()
        func(tx, *args, **kwargs)
        self.write_calls.append(tx)


class StubDriver:
    def __init__(self) -> None:
        self.sessions: list[tuple[str | None, StubSession]] = []
        self.closed = False

    def session(self, database=None):
        session = StubSession()
        self.sessions.append((database, session))
        return session

    def close(self):
        self.closed = True


@pytest.fixture
def sample_graph(tmp_path: Path) -> Path:
    db_path = tmp_path / "graph.db"
    SQLiteDataStore(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO graph_entities (entity_id, label, properties)
            VALUES (?, ?, ?)
            """,
            ("PATIENT:1", "PATIENT", '{"name": "Akshat"}'),
        )
        conn.execute(
            """
            INSERT INTO graph_entities (entity_id, label, properties)
            VALUES (?, ?, ?)
            """,
            ("LAB_TEST:2", "LAB_TEST", '{"test_name": "HbA1c"}'),
        )
        conn.execute(
            """
            INSERT INTO graph_relations (
                relation_id, source_id, target_id, relation_type, properties
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "rel-1",
                "PATIENT:1",
                "LAB_TEST:2",
                "UNDERWENT",
                '{"created_at": "2024-01-01"}',
            ),
        )
    return db_path


def test_exporter_pushes_entities_and_relations(sample_graph: Path):
    driver = StubDriver()
    exporter = Neo4jGraphExporter(
        database_path=str(sample_graph),
        connection=Neo4jConnectionConfig(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="secret",
        ),
        clear_existing=True,
        driver_builder=lambda *args, **kwargs: driver,
    )

    nodes, edges = exporter.run()

    assert driver.closed is True
    assert nodes == 2
    assert edges == 1
    # Expect clear graph, 2 merge nodes, 1 merge edge
    session = driver.sessions[0][1]
    assert len(session.write_calls) == 4
    first_query, _ = session.write_calls[0].commands[0]
    assert "DETACH DELETE" in first_query
    node_queries = [
        call.commands[0]
        for call in session.write_calls[1:3]
    ]
    for query, params in node_queries:
        assert query.startswith("MERGE (n:")
        assert "SET n +=" in query
        assert "entity_id" in params["properties"]
    rel_query, rel_params = session.write_calls[3].commands[0]
    assert "MERGE (src)-[r:UNDERWENT" in rel_query
    assert rel_params["relation_id"] == "rel-1"


def test_label_sanitisation_helpers():
    assert _sanitize_label("123 Weird Label!") == "_123_Weird_Label"
    assert _sanitize_label("") == "Entity"
    assert _sanitize_rel_type("has space") == "has_space"
    assert _sanitize_rel_type("1bad-type") == "_1bad_type"
