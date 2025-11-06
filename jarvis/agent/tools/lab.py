"""Lab results tool handler."""
from __future__ import annotations

from typing import Any, Dict

from jarvis.agent.base import ToolContext, ToolParameter, ToolResult, ToolSpec
from jarvis.knowledge.queries.lab import fetch_lab_results, LabResult


def lab_result_to_dict(result: LabResult) -> Dict[str, Any]:
    return {
        "test_name": result.test_name,
        "value_text": result.value,
        "value_numeric": result.value_numeric,
        "units": result.units,
        "reference_range": result.reference_range,
        "patient": result.patient,
        "date": result.date,
        "subject": result.subject,
        "filename": result.filename,
        "message_id": result.message_id,
        "attachment_id": result.attachment_id,
    }


def lab_results_tool(context: ToolContext, params: Dict[str, Any]) -> ToolResult:
    extractor = params.get("extractor") or context.defaults.get("lab_extractor") or "llm:mistral"
    test_filter = params.get("test")
    patient_filter = params.get("patient")
    limit = params.get("limit", 20)
    results = fetch_lab_results(
        str(context.database_path),
        extractor=extractor,
        test_filter=test_filter,
        patient_filter=patient_filter,
        limit=limit,
    )
    data = {
        "extractor": extractor,
        "test_filter": test_filter,
        "patient_filter": patient_filter,
        "results": [lab_result_to_dict(r) for r in results],
    }
    return ToolResult.success_result(data)


def register_lab_tool() -> ToolSpec:
    return ToolSpec(
        name="lab_results",
        description="Fetch lab test results from the knowledge base (e.g., creatinine trends).",
        parameters=[
            ToolParameter(
                name="patient",
                description="Optional patient name substring to filter results.",
                required=False,
            ),
            ToolParameter(
                name="test",
                description="Optional lab test name substring (e.g., 'creatinine').",
                required=False,
            ),
            ToolParameter(
                name="limit",
                description="Maximum number of rows to return (default 20).",
                required=False,
            ),
            ToolParameter(
                name="extractor",
                description="Extractor identifier to use (default llm:mistral).",
                required=False,
            ),
        ],
        handler=lab_results_tool,
    )
