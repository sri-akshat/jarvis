from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from jarvis.knowledge.retriever import SemanticRetriever
from jarvis.knowledge.semantic_indexer import SimpleEmbeddingGenerator


def _setup_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "messages.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE attachment_texts (
            content_id TEXT,
            page INTEGER,
            chunk_index INTEGER,
            text TEXT,
            token_count INTEGER,
            sha256 TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE embeddings (
            embedding_id TEXT PRIMARY KEY,
            content_id TEXT,
            chunk_index INTEGER,
            model TEXT,
            dimensions INTEGER,
            vector BLOB,
            created_at TEXT,
            metadata TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE VIRTUAL TABLE message_search USING fts5(
            message_id UNINDEXED,
            subject,
            sender,
            recipients,
            body
        )
        """
    )
    generator = SimpleEmbeddingGenerator()
    created_at = datetime.utcnow().isoformat()

    def insert_message(content_id: str, message_id: str, text: str, metadata: dict) -> None:
        vec = generator.embed(text)
        conn.execute(
            """
            INSERT INTO attachment_texts (content_id, page, chunk_index, text, token_count, sha256)
            VALUES (?, 0, 0, ?, ?, NULL)
            """,
            (content_id, text, len(text.split())),
        )
        conn.execute(
            """
            INSERT INTO embeddings (
                embedding_id, content_id, chunk_index, model, dimensions,
                vector, created_at, metadata
            ) VALUES (?, ?, 0, ?, ?, ?, ?, ?)
            """,
            (
                f"{content_id}:0:{generator.model_name}",
                content_id,
                generator.model_name,
                generator.dimensions,
                vec.tobytes(),
                created_at,
                json.dumps(metadata, sort_keys=True),
            ),
        )
        conn.execute(
            """
            INSERT INTO message_search (message_id, subject, sender, recipients, body)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                message_id,
                metadata.get("subject", ""),
                metadata.get("sender", ""),
                ",".join(metadata.get("recipients", [])),
                text,
            ),
        )

    import json

    insert_message(
        content_id="message:adarsh:0",
        message_id="msg-adarsh",
        text="Hi Kiran, please schedule the final painting coat at Adarsh Sanctuary villa 156 on 9th June.",
        metadata={
            "message_id": "msg-adarsh",
            "subject": "Reg final painting coat",
            "sender": "Akshat Srivastava <sri.akshat@gmail.com>",
            "recipients": [
                "Kiran Kumar <kirankumar.de@adarshdevelopers.com>",
                "Ganesha HS <ganesha.hs@adarshdevelopers.com>",
            ],
            "source": "message",
        },
    )

    insert_message(
        content_id="message:bank:0",
        message_id="msg-bank",
        text="Please debit from my Bank of Baroda savings account, not the home loan account I referenced earlier. This payment is for the builder cheque.",
        metadata={
            "message_id": "msg-bank",
            "subject": "Re: Immediate fund transfer",
            "sender": "Akshat Srivastava <sri.akshat@gmail.com>",
            "recipients": ["Branch Manager <bank@bankofbaroda.com>"],
            "source": "message",
        },
    )

    insert_message(
        content_id="message:generic:0",
        message_id="msg-generic",
        text="Reminder: upload documents before the weekend.",
        metadata={
            "message_id": "msg-generic",
            "subject": "Reminder",
            "sender": "System <noreply@example.com>",
            "recipients": ["Akshat Srivastava <sri.akshat@gmail.com>"],
            "source": "message",
        },
    )

    conn.commit()
    conn.close()
    return db_path


def test_semantic_retriever_ranks_by_participant(tmp_path):
    db_path = _setup_db(tmp_path)
    retriever = SemanticRetriever(str(db_path))
    results = retriever.search("last conversation with Adarsh builder", top_k=3)
    assert results, "expected matches for Adarsh builder query"
    assert results[0].message_id == "msg-adarsh"
    assert results[0].subject == "Reg final painting coat"
    # bank email should not outrank the builder conversation despite similar wording
    bank_positions = [idx for idx, r in enumerate(results) if r.message_id == "msg-bank"]
    assert bank_positions, "bank message should be present for diagnostic purposes"
    assert bank_positions[0] > 0


def test_semantic_retriever_handles_home_loan_query(tmp_path):
    db_path = _setup_db(tmp_path)
    retriever = SemanticRetriever(str(db_path))
    results = retriever.search("home loan account number", top_k=3)
    assert results
    assert results[0].message_id == "msg-bank"
    assert "home loan" in results[0].text.lower()
