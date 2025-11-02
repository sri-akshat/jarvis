"""Finance-related tool handlers."""
from __future__ import annotations

from typing import Any, Dict

from jarvis.agent.base import ToolContext, ToolParameter, ToolResult, ToolSpec
from jarvis.knowledge.finance_graph import (
    PaymentMention,
    aggregate_totals,
    collect_payments_from_graph,
)


def payment_mention_to_dict(mention: PaymentMention) -> Dict[str, Any]:
    return {
        "amount_text": mention.amount_text,
        "amount_value": mention.amount_value,
        "currency": mention.currency,
        "subject": mention.subject,
        "filename": mention.filename,
        "content_type": mention.content_type,
        "message_id": mention.message_id,
        "attachment_id": mention.attachment_id,
    }


def finance_payments_tool(context: ToolContext, params: Dict[str, Any]) -> ToolResult:
    counterparty = params.get("counterparty")
    if not counterparty:
        return ToolResult.failure_result("Parameter 'counterparty' is required.")
    if not context.neo4j_config:
        return ToolResult.failure_result("Neo4j configuration is required for finance tools.")
    limit = params.get("limit")
    mentions = collect_payments_from_graph(context.neo4j_config, counterparty, limit=limit)
    totals = aggregate_totals(mentions)
    data = {
        "counterparty": counterparty,
        "totals": totals,
        "mentions": [payment_mention_to_dict(m) for m in mentions],
    }
    return ToolResult.success_result(data)


def register_finance_tool() -> ToolSpec:
    return ToolSpec(
        name="finance_payments",
        description="Aggregate payment mentions for a counterparty using the Neo4j knowledge graph.",
        parameters=[
            ToolParameter(
                name="counterparty",
                description="Canonical name of the counterparty (e.g. 'dezignare india').",
                required=True,
            ),
            ToolParameter(
                name="limit",
                description="Optional maximum number of mentions to retrieve.",
                required=False,
            ),
        ],
        handler=finance_payments_tool,
    )
