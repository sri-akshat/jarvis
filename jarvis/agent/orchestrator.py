"""LLM-driven tool orchestration."""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List

from jarvis.agent.base import AgentResponse, ToolCallRecord, ToolExecutor, ToolResult, ToolSpec
from jarvis.knowledge.finance_graph import MistralLLMClient

logger = logging.getLogger(__name__)

_JSON_PATTERN = re.compile(r"\{.*\}", re.DOTALL)


@dataclass
class OrchestratorConfig:
    max_loops: int = 3


class ToolOrchestrator:
    def __init__(
        self,
        specs: Dict[str, ToolSpec],
        executor: ToolExecutor,
        llm_client: MistralLLMClient,
        config: OrchestratorConfig | None = None,
    ) -> None:
        self.specs = specs
        self.executor = executor
        self.llm_client = llm_client
        self.config = config or OrchestratorConfig()

    def run(self, question: str) -> AgentResponse:
        transcript: List[ToolCallRecord] = []
        pending_action: Dict[str, Any] | None = None
        for iteration in range(self.config.max_loops):
            if pending_action is None:
                planner_payload = self._build_planner_prompt(question, transcript)
                raw_response = self.llm_client.chat(planner_payload)
                action = self._parse_json(raw_response)
            else:
                action = pending_action
                raw_response = json.dumps(pending_action)
                pending_action = None
            if not action:
                logger.warning("Planner returned unparseable response: %s", raw_response)
                break
            if action.get("action") == "final":
                answer = action.get("answer", "")
                return AgentResponse(answer=answer, tool_calls=transcript)
            if action.get("action") != "call_tool":
                logger.warning("Unknown planner action %s", action)
                break
            tool_name = action.get("tool")
            params = action.get("params", {})
            result = self.executor.execute(tool_name, params)
            record = ToolCallRecord(tool=tool_name, params=params, result=result)
            transcript.append(record)
            if not result.success:
                logger.info("Tool %s failed: %s", tool_name, result.error)
            # Ask LLM to produce final answer or decide next tool
            feedback_prompt = self._build_feedback_prompt(question, transcript)
            feedback_response = self.llm_client.chat(feedback_prompt)
            decision = self._parse_json(feedback_response)
            if not decision:
                logger.warning("Feedback response not parseable: %s", feedback_response)
                break
            if decision and decision.get("action") == "final":
                answer = decision.get("answer", "")
                return AgentResponse(answer=answer, tool_calls=transcript)
            if decision and decision.get("action") == "call_tool":
                if iteration == self.config.max_loops - 1:
                    logger.info("Reached max tool iterations. Returning best effort answer.")
                    break
                pending_action = decision
                continue
        return AgentResponse(answer="Unable to produce an answer.", tool_calls=transcript)

    def _build_tool_listing(self) -> str:
        lines: List[str] = []
        for spec in self.specs.values():
            params = []
            for param in spec.parameters:
                tag = "required" if param.required else "optional"
                params.append(f"    - {param.name} ({tag}): {param.description}")
            params_block = "\n".join(params) if params else "    - (no parameters)"
            lines.append(f"* {spec.name}: {spec.description}\n{params_block}")
        return "\n".join(lines)

    def _build_planner_prompt(self, question: str, transcript: List[ToolCallRecord]) -> str:
        tool_listing = self._build_tool_listing()
        history = self._format_history(transcript)
        return (
            "You are Jarvis, an assistant that can decide whether to call tools.\n"
            "Available tools:\n"
            f"{tool_listing}\n\n"
            "Respond strictly in JSON with keys: 'action'. If action is 'call_tool', include 'tool' and 'params'.\n"
            "If action is 'final', include 'answer'.\n\n"
            f"Conversation history:\n{history}\n"
            f"User question: {question}\n"
            "Decide the next step."
        )

    def _build_feedback_prompt(self, question: str, transcript: List[ToolCallRecord]) -> str:
        history = self._format_history(transcript)
        return (
            "You called a tool and received the output shown below.\n"
            "If the output is sufficient to answer the user question, respond with action 'final' and provide the answer.\n"
            "Otherwise respond with action 'call_tool' and specify the next tool and parameters.\n"
            "Remember to use valid JSON.\n\n"
            f"History:\n{history}\n"
            f"User question: {question}"
        )

    @staticmethod
    def _parse_json(text: str) -> Dict[str, Any] | None:
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = _JSON_PATTERN.search(text)
            if not match:
                return None
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return None

    @staticmethod
    def _format_history(transcript: List[ToolCallRecord]) -> str:
        if not transcript:
            return "(none)"
        lines: List[str] = []
        for idx, record in enumerate(transcript, start=1):
            lines.append(
                f"Tool call #{idx}: {record.tool}\nParams: {json.dumps(record.params, ensure_ascii=False)}"
            )
            lines.append(
                f"Result: {json.dumps(record.result.to_dict(), ensure_ascii=False)}"
            )
        return "\n".join(lines)
