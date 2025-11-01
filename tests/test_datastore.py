from __future__ import annotations

import sqlite3

from jarvis.ingestion.common.datastore import SQLiteDataStore


def test_save_messages_writes_rows_and_enqueues(
    datastore_with_queue_spy, make_message, make_attachment
):
    datastore, queued = datastore_with_queue_spy
    attachment = make_attachment()
    message = make_message(attachments=[attachment])

    datastore.save_messages([message])

    # Two tasks should be queued: message + attachment.
    assert len(queued) == 2
    task_types = {task_type for task_type, _ in queued}
    assert task_types == {"semantic_index"}

    conn = sqlite3.connect(datastore.database_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    assert (
        conn.execute("SELECT COUNT(*) FROM messages WHERE id = ?", (message.id,))
        .fetchone()[0]
        == 1
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM attachments WHERE id = ? AND message_id = ?",
            (attachment.id, message.id),
        ).fetchone()[0]
        == 1
    )
    # Content registry should contain both entries.
    content_ids = {
        row[0]
        for row in conn.execute(
            "SELECT content_id FROM content_registry WHERE message_id = ?", (message.id,)
        )
    }
    assert content_ids == {
        f"message:{message.id}",
        f"attachment:{message.id}:{attachment.id}",
    }
    conn.close()


def test_save_messages_allows_multiple_calls(db_path, make_message):
    datastore = SQLiteDataStore(db_path)
    message1 = make_message(message_id="msg-1", body="hello there")
    message2 = make_message(message_id="msg-2", body="second email")
    datastore.save_messages([message1])
    datastore.save_messages([message2])

    conn = sqlite3.connect(datastore.database_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    assert count == 2
    conn.close()
