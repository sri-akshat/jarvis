"""Tool registry management."""
from __future__ import annotations

from typing import Dict

from jarvis.agent.base import ToolSpec
from jarvis.agent.tools.finance import register_finance_tool
from jarvis.agent.tools.lab import register_lab_tool
from jarvis.agent.tools.medical import register_medical_tool
from jarvis.agent.tools.semantic import register_semantic_tool


def load_default_registry() -> Dict[str, ToolSpec]:
    specs = [
        register_finance_tool(),
        register_lab_tool(),
        register_medical_tool(),
        register_semantic_tool(),
    ]
    return {spec.name: spec for spec in specs}
