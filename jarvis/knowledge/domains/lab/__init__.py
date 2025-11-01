"""Laboratory knowledge domain components."""

from jarvis.knowledge.domains.lab.facts import LabFactBuilder
from jarvis.knowledge.domains.lab.queries import fetch_lab_results

__all__ = ["LabFactBuilder", "fetch_lab_results"]
