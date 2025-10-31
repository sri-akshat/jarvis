"""High-level query helpers."""

from .lab import fetch_lab_results
from .financial import fetch_financial_records
from .medical import fetch_medical_events

__all__ = [
    "fetch_lab_results",
    "fetch_financial_records",
    "fetch_medical_events",
]
