"""Tool handlers for the agent."""
from jarvis.agent.tools.finance import finance_payments_tool, register_finance_tool
from jarvis.agent.tools.lab import register_lab_tool
from jarvis.agent.tools.medical import register_medical_tool
from jarvis.agent.tools.semantic import register_semantic_tool

__all__ = [
    "register_finance_tool",
    "register_medical_tool",
    "register_lab_tool",
    "register_semantic_tool",
    "finance_payments_tool",
]
