"""Medical-related tool handlers."""
from __future__ import annotations

from typing import Any, Dict

from jarvis.agent.base import ToolContext, ToolParameter, ToolResult, ToolSpec
from jarvis.knowledge.queries.medical import MedicalEvent, fetch_medical_events


def medical_event_to_dict(event: MedicalEvent) -> Dict[str, Any]:
    return {
        "event_type": event.event_type,
        "description": event.description,
        "attributes": event.attributes,
        "patient": event.patient,
        "clinician": event.clinician,
        "facility": event.facility,
        "date": event.date,
        "subject": event.subject,
        "filename": event.filename,
        "message_id": event.message_id,
        "attachment_id": event.attachment_id,
    }


def medical_events_tool(context: ToolContext, params: Dict[str, Any]) -> ToolResult:
    extractor = params.get("extractor") or context.defaults.get("medical_extractor") or "llm:mistral"
    patient = params.get("patient")
    event_type = params.get("event_type")
    limit = params.get("limit", 20)
    events = fetch_medical_events(
        str(context.database_path),
        extractor=extractor,
        event_type=event_type,
        patient_filter=patient,
        limit=limit,
    )
    data = {
        "extractor": extractor,
        "patient_filter": patient,
        "event_type": event_type,
        "events": [medical_event_to_dict(e) for e in events],
    }
    return ToolResult.success_result(data)


def register_medical_tool() -> ToolSpec:
    return ToolSpec(
        name="medical_events",
        description="Fetch structured medical events (medications, diagnoses, procedures) from the knowledge base.",
        parameters=[
            ToolParameter(
                name="patient",
                description="Optional patient name substring to filter events.",
                required=False,
            ),
            ToolParameter(
                name="event_type",
                description="Optional event type (e.g. MEDICATION, DIAGNOSIS).",
                required=False,
            ),
            ToolParameter(
                name="limit",
                description="Maximum number of events to return (default 20).",
                required=False,
            ),
            ToolParameter(
                name="extractor",
                description="Extractor identifier to use (default llm:mistral).",
                required=False,
            ),
        ],
        handler=medical_events_tool,
    )
