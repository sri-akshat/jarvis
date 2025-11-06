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
                and "semantic_search" in self.specs
                and not any(r.tool == "semantic_search" for r in transcript)
            ):
                logger.debug("Structured tool %s returned empty; auto-invoking semantic_search.", tool_name)
                auto_params = {"query": question}
                auto_result = self.executor.execute("semantic_search", auto_params)
                transcript.append(
                    ToolCallRecord(
                        tool="semantic_search",
                        params=auto_params,
                        result=auto_result,
                    )
                )
                result = auto_result
            if (
                tool_name == "semantic_search"
                and result.success
                and isinstance(result.data, dict)
                and "fetch_message_context" in self.specs
            ):
                fetched_ids: set[str] = set()
                for item in result.data.get("results", []) or []:
                    message_id = item.get("message_id")
                    if not message_id or message_id in fetched_ids:
                        continue
                    fetch_params = {
                        "message_id": message_id,
                        "thread_window": params.get("thread_window", 5),
                        "include_body": True,
                    }
                    fetch_result = self.executor.execute("fetch_message_context", fetch_params)
                    transcript.append(
                        ToolCallRecord(
                            tool="fetch_message_context",
                            params=fetch_params,
                            result=fetch_result,
                        )
                    )
                    fetched_ids.add(message_id)
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
            "  1. Use only the tools listed above (exact names, case-sensitive). Never invent or guess a new tool name.\n"
            "  2. If a structured tool (finance_payments, lab_results, medical_events) cannot directly answer the user's question—either because it returns no rows or because the fields provided do not resolve the question—you SHOULD plan to call 'semantic_search' next with a precise query. When a structured tool yields zero rows, the only valid follow-up is 'semantic_search'.\n"
            "  3. When 'semantic_search' returns results that include a 'message_id', you must call 'fetch_message_context' for the relevant message(s) before producing a final answer. This ensures the answer is grounded in the full email/thread.\n"
            "  4. Do not return action 'final' unless semantic_search has been attempted (or you already have sufficient grounded data) and any cited message IDs have been expanded via fetch_message_context.\n"
            "  5. Construct every tool call using the current user question. Do not reuse query strings or parameters from earlier turns unless the user explicitly repeats the same request.\n"
            "  6. If none of the tools apply after following the above rules, respond with action 'final' and explain the limitation.\n\n"
            "Examples of good tool selection:\n"
            "  • Question: \"How much have I paid to Dezignare?\" → Call finance_payments first. If totals appear, return them as the answer.\n"
            "  • Question: \"What was my last conversation with Adarsh?\" → Use semantic_search with a focused query, then call fetch_message_context on the returned message_id(s) to summarise the actual emails.\n"
            "  • Question: \"What is Dezignare's bank account number?\" → finance_payments does not list account numbers, so immediately follow up with semantic_search (and then fetch_message_context if message IDs are returned) to inspect source emails.\n\n"
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
            "  • Use only tools from the list provided earlier (exact names, no new tools).\n"
            "  • Evaluate whether the latest tool output genuinely answers the user's question. If it does not (for example, missing relevant fields or empty results), the correct next action is to call 'semantic_search' with a focused query (unless it has already been tried this turn).\n"
            "  • If the latest tool returned zero rows (e.g., empty 'mentions', 'results', or 'events'), respond with action 'call_tool' and set 'tool' to 'semantic_search'.\n"
            "  • When semantic_search returns hits containing 'message_id', call 'fetch_message_context' next to retrieve the full email/thread before summarising.\n"
            "  • Only return action 'final' after semantic_search has been attempted (when applicable) and all cited message IDs have been expanded via fetch_message_context, or when you already have sufficient data from a previous tool.\n"
            "  • Always base follow-up tool parameters on the current user question; do not reuse prior queries unless the user repeats the same request.\n"
            "  • After semantic_search (and message expansion), do not call additional tools unless they are on the list and clearly required. If the answer is still unavailable, return action 'final' and clearly explain the limitation using the evidence you have.\n"
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
            if isinstance(totals, dict) and len(totals) == 0:
                return True
        return False
