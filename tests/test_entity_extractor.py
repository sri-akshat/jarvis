from __future__ import annotations

import copy
import json
import sqlite3

from jarvis.knowledge.entity_extractor import (
    EntityMention,
    KnowledgeGraphBuilder,
)
from jarvis.knowledge.semantic_indexer import SemanticIndexer
from jarvis.ingestion.common.datastore import SQLiteDataStore


class StubExtractor:
    def __init__(self, mentions):
        self._mentions = mentions

    def extract(self, text: str):
        # Return deep copies so each invocation is independent.
        return [copy.deepcopy(m) for m in self._mentions]


def test_knowledge_graph_builder_populates_entities(
    db_path, make_message, monkeypatch
):
    from jarvis.knowledge import task_queue

    monkeypatch.setattr(task_queue, "enqueue_task", lambda *args, **kwargs: None)
    datastore = SQLiteDataStore(db_path)
    message = make_message(
        message_id="msg-kgb", body="HbA1c result recorded as 5.6 percent."
    )
    datastore.save_messages([message])

    # Ensure text chunks exist.
    indexer = SemanticIndexer(str(db_path))
    indexer.process_content_id(f"message:{message.id}")

    mentions = [
        EntityMention(text="HbA1c", label="LAB_TEST", start_char=0, end_char=5),
        EntityMention(
            text="5.6 percent",
            label="MEASUREMENT",
            start_char=27,
            end_char=38,
            metadata={"attributes": {"value": "5.6", "units": "%"}},
        ),
    ]
    extractor = StubExtractor(mentions)
    builder = KnowledgeGraphBuilder(
        database_path=str(db_path),
        extractor=extractor,
        extractor_name="stub-extractor",
    )

    processed = builder.run(content_id=f"message:{message.id}")
    assert processed == 1

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    rows = conn.execute(
        """
        SELECT label, text, metadata
        FROM entity_mentions
        WHERE extractor = 'stub-extractor'
        ORDER BY text
        """
    ).fetchall()
    assert len(rows) == 2
    labels = {row[0] for row in rows}
    assert labels == {"LAB_TEST", "MEASUREMENT"}
    metadata_payloads = [json.loads(row[2]) for row in rows]
    assert any("attributes" in meta for meta in metadata_payloads)

    relations = conn.execute(
        "SELECT relation_type FROM graph_relations WHERE relation_type = 'MENTIONED_IN'"
    ).fetchall()
    assert relations

    conn.close()
