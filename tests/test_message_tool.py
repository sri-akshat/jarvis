from __future__ import annotations

from jarvis.agent.base import ToolContext
from jarvis.agent.tools.messages import fetch_message_context_tool
from jarvis.ingestion.common.datastore import SQLiteDataStore


def test_fetch_message_context_returns_thread(tmp_path, make_message, make_attachment):
    db_path = tmp_path / "messages.db"
    datastore = SQLiteDataStore(db_path)

    base_message = make_message(
        message_id="msg-1",
        subject="Project Update",
        body="Initial plan sent",
        attachments=[
            make_attachment(
                attachment_id="att-1",
                filename="plan.pdf",
                mime_type="application/pdf",
            )
        ],
    )
    reply_message = make_message(
        message_id="msg-2",
        subject="Re: Project Update",
        body="Here is my response",
    )
    datastore.save_messages([base_message, reply_message])

    context = ToolContext(database_path=str(db_path))
    result = fetch_message_context_tool(
        context,
        {"message_id": "msg-2", "thread_window": 2, "include_body": True},
    )

    assert result.success
    data = result.data
    assert data["thread_id"] == reply_message.thread_id
    assert len(data["messages"]) == 2
    latest = data["messages"][-1]
    assert latest["message_id"] == "msg-2"
    assert latest["body"] == "Here is my response"
    first = data["messages"][0]
    assert first["attachments"][0]["filename"] == "plan.pdf"


def test_fetch_message_context_missing_message(tmp_path):
    db_path = tmp_path / "messages.db"
    SQLiteDataStore(db_path)
    context = ToolContext(database_path=str(db_path))
    result = fetch_message_context_tool(context, {"message_id": "does-not-exist"})
    assert not result.success
    assert "not found" in (result.error or "")

