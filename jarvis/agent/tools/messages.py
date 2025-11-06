"""Email message retrieval tools."""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List

from jarvis.agent.base import ToolContext, ToolParameter, ToolResult, ToolSpec


def _parse_recipients(raw: str | None) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _loads_metadata(raw: str | None) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def fetch_message_context_tool(context: ToolContext, params: Dict[str, Any]) -> ToolResult:
    message_id = params.get("message_id")
    if not message_id:
        return ToolResult.failure_result("Parameter 'message_id' is required.")
    thread_window = params.get("thread_window", 5)
    include_body = params.get("include_body", True)

    with sqlite3.connect(context.database_path) as conn:
        conn.row_factory = sqlite3.Row
        message_row = conn.execute(
            """
            SELECT id, subject, sender, recipients, snippet, body,
                   received_at, thread_id, metadata
            FROM messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()

        if not message_row:
            return ToolResult.failure_result(f"Message '{message_id}' not found.")

        thread_id = message_row["thread_id"]
        messages: List[sqlite3.Row]
        if thread_id and thread_window:
            messages = conn.execute(
                """
                SELECT id, subject, sender, recipients, snippet, body,
                       received_at, thread_id, metadata
                FROM messages
                WHERE thread_id = ?
                ORDER BY received_at DESC, id DESC
                LIMIT ?
                """,
                (thread_id, max(int(thread_window), 1)),
            ).fetchall()
            # ensure chronological order oldest -> newest for readability
            messages = list(reversed(messages))
        else:
            messages = [message_row]

        attachments_map: Dict[str, List[Dict[str, Any]]] = {}
        message_ids = [row["id"] for row in messages]
        placeholder = ",".join("?" for _ in message_ids)
        if message_ids:
            attachment_rows = conn.execute(
                f"""
                SELECT id, message_id, filename, mime_type, metadata
                FROM attachments
                WHERE message_id IN ({placeholder})
                """,
                message_ids,
            ).fetchall()
            for row in attachment_rows:
                attachments_map.setdefault(row["message_id"], []).append(
                    {
                        "attachment_id": row["id"],
                        "filename": row["filename"],
                        "mime_type": row["mime_type"],
                        "metadata": _loads_metadata(row["metadata"]),
                    }
                )

    payload_messages: List[Dict[str, Any]] = []
    for row in messages:
        payload_messages.append(
            {
                "message_id": row["id"],
                "subject": row["subject"],
                "sender": row["sender"],
                "recipients": _parse_recipients(row["recipients"]),
                "received_at": row["received_at"],
                "snippet": row["snippet"],
                "body": row["body"] if include_body else None,
                "metadata": _loads_metadata(row["metadata"]),
                "attachments": attachments_map.get(row["id"], []),
            }
        )

    return ToolResult.success_result(
        {
            "message_id": message_id,
            "thread_id": thread_id,
            "messages": payload_messages,
        }
    )


def register_message_tool() -> ToolSpec:
    return ToolSpec(
        name="fetch_message_context",
        description=(
            "Fetch an email message (and optionally recent messages in the same thread) "
            "for grounding answers."
        ),
        parameters=[
            ToolParameter(
                name="message_id",
                description="Gmail message ID to retrieve.",
                required=True,
            ),
            ToolParameter(
                name="thread_window",
                description=(
                    "Number of most recent messages from the same thread to include (default 5). "
                    "Set to 1 to fetch only the specified message."
                ),
                required=False,
            ),
            ToolParameter(
                name="include_body",
                description="Include full message body text (default true).",
                required=False,
            ),
        ],
        handler=fetch_message_context_tool,
    )
