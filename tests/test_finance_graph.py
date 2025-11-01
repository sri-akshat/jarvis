from __future__ import annotations

import pytest

from jarvis.knowledge.finance_graph import _normalise_currency, _parse_amount


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Rs. 200000.00", 200000.0),
        ("USD 3,955", 3955.0),
        ("Amount Paid: 12.50", 12.5),
        ("No digits here", None),
        ("Rs. 5 lakhs", 500000.0),
        ("Amount: 2 lakh", 200000.0),
        ("1 crore", 10000000.0),
    ],
)
def test_parse_amount(text, expected):
    assert _parse_amount(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Rs 1000", "INR"),
        ("Amount (INR)", "INR"),
        ("USD 45", "USD"),
        ("€25", "EUR"),
        ("£10", "GBP"),
        ("Unknown", None),
    ],
)
def test_normalise_currency(text, expected):
    assert _normalise_currency(text) == expected
