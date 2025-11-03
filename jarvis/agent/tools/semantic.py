"""Semantic search tool handler."""
from __future__ import annotations

from typing import Any, Dict

from jarvis.agent.base import ToolContext, ToolParameter, ToolResult, ToolSpec
from jarvis.knowledge.retriever import SemanticRetriever


def semantic_search_tool(context: ToolContext, params: Dict[str, Any]) -> ToolResult:
    query = params.get("query")
    if not query:
        return ToolResult.failure_result("Parameter 'query' is required.")
    top_k = params.get("top_k", 5)
    retriever = SemanticRetriever(str(context.database_path))
    results = retriever.search(query, top_k=top_k)
    data = [
        {
            "score": result.score,
            "content_id": result.content_id,
            "text": result.text,
            "page": result.page,
            "filename": result.attachment_filename,
            "subject": result.subject,
        }
        for result in results
    ]
    return ToolResult.success_result({"query": query, "results": data})


def register_semantic_tool() -> ToolSpec:
    return ToolSpec(
        name="semantic_search",
        description="Retrieve top matching snippets from indexed documents using embeddings.",
        parameters=[
            ToolParameter(
                name="query",
                description="Natural language query to search for.",
                required=True,
            ),
            ToolParameter(
                name="top_k",
                description="Number of results to return (default 5).",
                required=False,
            ),
        ],
        handler=semantic_search_tool,
    )
