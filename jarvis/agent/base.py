"""Core data structures for the tool-based agent."""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Callable, Dict, List, Optional

from jarvis.knowledge.neo4j_exporter import Neo4jConnectionConfig


ToolHandler = Callable[[ "ToolContext", Dict[str, Any]], "ToolResult"]

logger = logging.getLogger(__name__)


@dataclass
class ToolParameter:
    name: str
    description: str
    required: bool = True


@dataclass
class ToolSpec:
    name: str
    description: str
    parameters: List[ToolParameter]
    handler: ToolHandler


@dataclass
class ToolContext:
    database_path: str
    neo4j_config: Optional[Neo4jConnectionConfig] = None
    defaults: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    success: bool
    data: Any = None
    error: Optional[str] = None

    @classmethod
    def success_result(cls, data: Any) -> "ToolResult":
        return cls(success=True, data=data)

    @classmethod
    def failure_result(cls, message: str) -> "ToolResult":
        return cls(success=False, error=message)

    def to_dict(self) -> Dict[str, Any]:
        return {"success": self.success, "data": self.data, "error": self.error}


@dataclass
class ToolCallRecord:
    tool: str
    params: Dict[str, Any]
    result: ToolResult


@dataclass
class AgentResponse:
    answer: str
    tool_calls: List[ToolCallRecord]


class ToolExecutor:
    def __init__(self, context: ToolContext, specs: Dict[str, ToolSpec]):
        self.context = context
        self.specs = specs

    def execute(self, tool_name: str, params: Dict[str, Any]) -> ToolResult:
        spec = self.specs.get(tool_name)
        if spec is None:
            return ToolResult.failure_result(f"Unknown tool '{tool_name}'")
        try:
            logger.debug("Executing tool %s with params %s", tool_name, params)
            result = spec.handler(self.context, params or {})
            logger.debug("Tool %s success=%s", tool_name, result.success)
            return result
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Tool %s raised error", tool_name)
            return ToolResult.failure_result(f"{type(exc).__name__}: {exc}")
