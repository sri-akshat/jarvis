"""LLM-driven tool orchestration."""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List

from jarvis.agent.base import AgentResponse, ToolCallRecord, ToolExecutor, ToolResult, ToolSpec
from jarvis.knowledge.finance_graph import OllamaLLMClient

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
        llm_client: OllamaLLMClient,
        config: OrchestratorConfig | None = None,
    ) -> None:
        self.specs = specs
        self.executor = executor
        self.llm_client = llm_client
        self.config = config or OrchestratorConfig()

    def run(self, question: str, chat_history: List[tuple[str, str]] | None = None) -> AgentResponse:
        transcript: List[ToolCallRecord] = []
        pending_action: Dict[str, Any] | None = None
        structured_tools = {"finance_payments", "lab_results", "medical_events"}
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
            if (
                tool_name in structured_tools
                and result.success
                and self._is_empty_result(result.data)
                and not any(call.tool == "semantic_search" for call in transcript)
                and "semantic_search" in self.specs
            ):
                semantic_params = {"query": question}
                auto_result = self.executor.execute("semantic_search", semantic_params)
                transcript.append(
                    ToolCallRecord(
                        tool="semantic_search",
                        params=semantic_params,
                        result=auto_result,
                    )
                )
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
            "Important reasoning rules:\n"
            "  1. Use only the tools listed above.\n"
            "  2. If you call a structured tool (finance_payments, lab_results, medical_events) and its 'results' field is empty or missing, you MUST immediately call 'semantic_search' with a relevant query before considering action 'final'.\n"
            "  3. Do not return action 'final' unless semantic_search has been tried in this turn, or you already obtained meaningful data from another tool.\n"
            "  4. If none of the tools apply after following the above rules, respond with action 'final' and explain the limitation.\n\n"
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
            "Rules to follow now:\n"
            "  • Use only tools from the list provided earlier.\n"
            "  • If the most recent tool produced no data (empty 'results'), you MUST call 'semantic_search' next unless it has already been tried this turn.\n"
            "  • Only return action 'final' after semantic_search has been attempted, or when you already have sufficient data from a previous tool.\n"
            "  • If nothing applies, respond with action 'final' and explain the limitation.\n\n"
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
            kv_pairs: Dict[str, Any] = {}
            for line in text.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                if ":" not in stripped:
                    kv_pairs.setdefault("notes", []).append(stripped)
                    continue
                key, _, value = stripped.partition(":")
                key = key.strip().lower()
                value = value.strip()
                if key.startswith("next tool") or key.startswith("tool requested"):
                    kv_pairs["tool"] = value
                elif key.startswith("note"):
                    kv_pairs.setdefault("notes", []).append(value)
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
                    for note in kv_pairs.get("notes", []):
                        lowered = note.lower()
                        if lowered.startswith("tool") and ":" in note:
                            _, _, candidate = note.partition(":")
                            tool = candidate.strip()
                            break
                        if "semantic_search" in lowered:
                            tool = "semantic_search"
                            break
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

    @staticmethod
    def _is_empty_result(data: Any) -> bool:
        if not data:
            return True
        if isinstance(data, dict):
            for key in ("results", "mentions", "events"):
                value = data.get(key)
                if isinstance(value, list):
                    return len(value) == 0
            totals = data.get("totals")
            if isinstance(totals, dict) and not totals:
                return True
        return False
