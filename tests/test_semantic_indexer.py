from __future__ import annotations

import sqlite3
from io import BytesIO
from pathlib import Path

import numpy as np

from jarvis.knowledge.semantic_indexer import ContentRecord, SemanticIndexer
from jarvis.ingestion.common.datastore import SQLiteDataStore


def test_semantic_indexer_processes_messages_and_attachments(
    db_path, make_message, make_attachment, monkeypatch
):
    from jarvis.knowledge import task_queue

    monkeypatch.setattr(task_queue, "enqueue_task", lambda *args, **kwargs: None)
    datastore = SQLiteDataStore(db_path)

    attachment = make_attachment(
        data=b"The attachment content is short and sweet.",
        filename="report.txt",
    )
    message = make_message(
        body="Hello Akshat,\nThis is a body with plain text.",
        attachments=[attachment],
    )
    datastore.save_messages([message])

    indexer = SemanticIndexer(str(db_path))
    processed = indexer.run()
    # One message body + one attachment.
    assert processed == 2

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    text_rows = conn.execute(
        "SELECT content_id, text FROM attachment_texts ORDER BY content_id"
    ).fetchall()
    assert len(text_rows) == 2
    content_ids = {row[0] for row in text_rows}
    assert content_ids == {
        f"message:{message.id}",
        f"attachment:{message.id}:{attachment.id}",
    }

    embedding_rows = conn.execute(
        "SELECT metadata FROM embeddings ORDER BY content_id"
    ).fetchall()
    assert len(embedding_rows) == 2
    metadata_entries = [row[0] for row in embedding_rows]
    assert all("source" in meta for meta in metadata_entries)

    conn.close()


def test_semantic_indexer_process_content_id(db_path, make_message, monkeypatch):
    from jarvis.knowledge import task_queue

    monkeypatch.setattr(task_queue, "enqueue_task", lambda *args, **kwargs: None)
    datastore = SQLiteDataStore(db_path)
    message = make_message(message_id="msg-process", body="Single message body.")
    datastore.save_messages([message])

    indexer = SemanticIndexer(str(db_path))
    processed = indexer.process_content_id(f"message:{message.id}")
    assert processed is True

    # A second call should still succeed but reprocess the text.
    processed_again = indexer.process_content_id(f"message:{message.id}")
    assert processed_again is True


def test_semantic_indexer_processes_local_files(
    db_path, tmp_path, monkeypatch
):
    from jarvis.knowledge import task_queue
    from cli import enqueue_local_files

    monkeypatch.setattr(task_queue, "enqueue_task", lambda *args, **kwargs: None)
    SQLiteDataStore(db_path)

    file_path = tmp_path / "notes.txt"
    file_path.write_text("This local file should be chunked and embedded.")

    sha = enqueue_local_files.compute_sha256(file_path)
    inserted = enqueue_local_files.upsert_local_file(Path(db_path), file_path, sha=sha)
    assert inserted is True

    indexer = SemanticIndexer(str(db_path))
    processed = indexer.run()
    assert processed == 1

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    content_id = f"file:{sha}"
    text_row = conn.execute(
        "SELECT text FROM attachment_texts WHERE content_id = ?", (content_id,)
    ).fetchone()
    assert text_row is not None
    embedding_row = conn.execute(
        "SELECT metadata, vector FROM embeddings WHERE content_id = ?", (content_id,)
    ).fetchone()
    assert embedding_row is not None
    metadata, vector = embedding_row
    assert '"source": "local_file"' in metadata
    # Vector should be normalised (unit length or zero for empty chunk)
    arr = np.frombuffer(vector, dtype=np.float32)
    assert np.isclose(np.linalg.norm(arr), 1.0)
    conn.close()


def test_semantic_indexer_extracts_pdf_binary(tmp_path, db_path):
    SQLiteDataStore(db_path)
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    buffer = BytesIO()
    writer.write(buffer)
    pdf_bytes = buffer.getvalue()

    record = ContentRecord(
        content_id="attachment:test",
        message_id="msg-1",
        attachment_id="att-1",
        filename="report.pdf",
        mime_type="application/pdf",
        data=pdf_bytes,
        source="attachment",
    )
    indexer = SemanticIndexer(str(db_path))
    texts = indexer._extract_text(record)
    assert texts == [""]
