from __future__ import annotations

import json
from typing import Dict

from jarvis.agent.base import ToolContext, ToolExecutor, ToolResult, ToolSpec, ToolParameter
from jarvis.agent.orchestrator import OrchestratorConfig, ToolOrchestrator


class StubLLMClient:
    def __init__(self, responses: Dict[int, str]):
        self.responses = responses
        self.calls = 0

    def chat(self, prompt: str) -> str:
        response = self.responses.get(self.calls)
        if response is None:
            raise AssertionError(f"No stub response for call {self.calls}")
        self.calls += 1
        return response


def test_orchestrator_executes_tool_and_returns_answer():
    def handler(context: ToolContext, params: Dict[str, str]) -> ToolResult:
        return ToolResult.success_result({"echo": params.get("value")})

    spec = ToolSpec(
        name="echo",
        description="Return the provided value.",
        parameters=[ToolParameter(name="value", description="Value to echo")],
        handler=handler,
    )
    registry = {spec.name: spec}
    context = ToolContext(database_path="/tmp/test.db")
    executor = ToolExecutor(context, registry)
    llm = StubLLMClient(
        {
            0: json.dumps({"action": "call_tool", "tool": "echo", "params": {"value": "hello"}}),
            1: json.dumps({"action": "final", "answer": "Echoed hello"}),
        }
    )
    orchestrator = ToolOrchestrator(registry, executor, llm, OrchestratorConfig(max_loops=2))
    response = orchestrator.run("Say hello")
    assert response.answer == "Echoed hello"
    assert len(response.tool_calls) == 1
    call = response.tool_calls[0]
    assert call.tool == "echo"
    assert call.result.success
    assert call.result.data == {"echo": "hello"}


def test_orchestrator_parses_colon_format():
    def handler(context: ToolContext, params: Dict[str, str]) -> ToolResult:
        return ToolResult.success_result({"echo": params.get("value")})

    spec = ToolSpec(
        name="echo",
        description="Return the provided value.",
        parameters=[ToolParameter(name="value", description="Value to echo")],
        handler=handler,
    )
    registry = {spec.name: spec}
    context = ToolContext(database_path="/tmp/test.db")
    executor = ToolExecutor(context, registry)
    llm = StubLLMClient(
        {
            0: "Action: call_tool\nTool: echo\nParams: {\"value\": \"hi\"}",
            1: "Action: final\nAnswer: done",
        }
    )
    orchestrator = ToolOrchestrator(registry, executor, llm, OrchestratorConfig(max_loops=2))
    response = orchestrator.run("Say hi")
    assert response.answer == "done"
    assert len(response.tool_calls) == 1


def test_orchestrator_handles_unknown_tool():
    spec = ToolSpec(
        name="echo",
        description="Return the provided value.",
        parameters=[ToolParameter(name="value", description="Value to echo")],
        handler=lambda ctx, params: ToolResult.success_result({"echo": params.get("value")}),
    )
    registry = {spec.name: spec}
    context = ToolContext(database_path="/tmp/test.db")
    executor = ToolExecutor(context, registry)
    llm = StubLLMClient(
        {
            0: "Action: call_tool\nTool: non_existent\nParams: {}",
        }
    )
    orchestrator = ToolOrchestrator(registry, executor, llm, OrchestratorConfig(max_loops=1))
    response = orchestrator.run("Do something")
    assert "non_existent" in response.answer
    assert len(response.tool_calls) == 1
    assert not response.tool_calls[0].result.success


def test_parse_json_falls_back_to_semantic_search():
    text = (
        "Action: call_tool\n"
        "Note: other tools returned nothing so I will try semantic_search.\n"
        "Params: {\"query\": \"creatinine\"}"
    )
    parsed = ToolOrchestrator._parse_json(text)
    assert parsed["tool"] == "semantic_search"
