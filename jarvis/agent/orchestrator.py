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

    def run(self, question: str, chat_history: List[tuple[str, str]] | None = None) -> AgentResponse:
        transcript: List[ToolCallRecord] = []
        pending_action: Dict[str, Any] | None = None
        for iteration in range(self.config.max_loops):
            if pending_action is None:
                planner_payload = self._build_planner_prompt(question, transcript, chat_history)
                raw_response = self.llm_client.chat(planner_payload)
                logger.debug("Planner prompt:\n%s", planner_payload)
                logger.debug("Planner response: %s", raw_response)
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
            if tool_name not in self.specs:
                warning = (
                    f"Tool '{tool_name}' is not available. Available tools: {', '.join(self.specs)}."
                )
                logger.warning(warning)
                result = ToolResult.failure_result(warning)
                transcript.append(ToolCallRecord(tool=tool_name, params=params, result=result))
                answer = (
                    f"I cannot use the tool '{tool_name}'. "
                    f"Please ask using one of: {', '.join(self.specs)}."
                )
                return AgentResponse(answer=answer, tool_calls=transcript)
            result = self.executor.execute(tool_name, params)
            record = ToolCallRecord(tool=tool_name, params=params, result=result)
            transcript.append(record)
            if not result.success:
                logger.info("Tool %s failed: %s", tool_name, result.error)
            # Ask LLM to produce final answer or decide next tool
            feedback_prompt = self._build_feedback_prompt(question, transcript, chat_history)
            feedback_response = self.llm_client.chat(feedback_prompt)
            logger.debug("Feedback prompt:\n%s", feedback_prompt)
            logger.debug("Feedback response: %s", feedback_response)
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
                next_tool = decision.get("tool")
                if next_tool and next_tool not in self.specs:
                    warning = (
                        f"Tool '{next_tool}' is not available. Available tools: {', '.join(self.specs)}."
                    )
                    logger.warning(warning)
                    result = ToolResult.failure_result(warning)
                    transcript.append(
                        ToolCallRecord(tool=next_tool, params=decision.get("params", {}), result=result)
                    )
                    answer = (
                        f"I cannot use the tool '{next_tool}'. "
                        f"Please ask using one of: {', '.join(self.specs)}."
                    )
                    return AgentResponse(answer=answer, tool_calls=transcript)
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

    def _build_planner_prompt(
        self,
        question: str,
        transcript: List[ToolCallRecord],
        chat_history: List[tuple[str, str]] | None,
    ) -> str:
        tool_listing = self._build_tool_listing()
        history = self._format_history(transcript)
        conversation = self._format_chat_history(chat_history)
        return (
            "You are Jarvis, an assistant that can decide whether to call tools.\n"
            "Available tools:\n"
            f"{tool_listing}\n\n"
            "Respond strictly in JSON with keys: 'action'. If action is 'call_tool', include 'tool' and 'params'.\n"
            "If action is 'final', include 'answer'.\n\n"
            "You must only use tools that appear in the list above. If none apply, respond with action 'final'.\n\n"
            f"Conversation (most recent last):\n{conversation}\n\n"
            f"Conversation history:\n{history}\n"
            f"User question: {question}\n"
            "Decide the next step."
        )

    def _build_feedback_prompt(
        self,
        question: str,
        transcript: List[ToolCallRecord],
        chat_history: List[tuple[str, str]] | None,
    ) -> str:
        history = self._format_history(transcript)
        conversation = self._format_chat_history(chat_history)
        return (
            "You called a tool and received the output shown below.\n"
            "If the output is sufficient to answer the user question, respond with action 'final' and provide the answer.\n"
            "Otherwise respond with action 'call_tool' and specify the next tool and parameters.\n"
            "Remember to use valid JSON.\n\n"
            "Only call tools that exist in the earlier tool list. If no listed tool applies, respond with action 'final' and explain the limitation.\n\n"
            f"Conversation (most recent last):\n{conversation}\n\n"
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
            if match:
                try:
                    candidate = json.loads(match.group(0))
                    if isinstance(candidate, dict) and candidate.get("action"):
                        return candidate
                except json.JSONDecodeError:
                    pass
            kv_pairs: Dict[str, str] = {}
            for line in text.splitlines():
                if ":" not in line:
                    continue
                key, _, value = line.partition(":")
                key = key.strip().lower()
                value = value.strip()
                if key:
                    if key.startswith("next tool"):
                        kv_pairs["tool"] = value
                    elif key.startswith("tool requested"):
                        kv_pairs["tool"] = value
                    else:
                        kv_pairs[key] = value
            if not kv_pairs:
                return None
            action = kv_pairs.get("action")
            if not action:
                return None
            result: Dict[str, Any] = {"action": action.strip().lower()}
            if result["action"] == "call_tool":
                tool = kv_pairs.get("tool")
                if not tool:
                    return None
                result["tool"] = tool.strip()
                params_text = kv_pairs.get("params", "{}")
                try:
                    result["params"] = json.loads(params_text)
                except json.JSONDecodeError:
                    result["params"] = {}
            elif result["action"] == "final":
                result["answer"] = kv_pairs.get("answer", "")
            return result

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

    @staticmethod
    def _format_chat_history(chat_history: List[tuple[str, str]] | None) -> str:
        if not chat_history:
            return "(no prior turns)"
        lines: List[str] = []
        for user, agent in chat_history[-5:]:  # limit context for brevity
            lines.append(f"User: {user}")
            lines.append(f"Agent: {agent}")
        return "\n".join(lines)
