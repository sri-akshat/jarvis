"""Medical knowledge domain components."""

from jarvis.knowledge.domains.medical.facts import MedicalFactBuilder
from jarvis.knowledge.domains.medical.queries import fetch_medical_events

__all__ = ["MedicalFactBuilder", "fetch_medical_events"]
