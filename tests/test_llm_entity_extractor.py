from __future__ import annotations

import json

import pytest

from jarvis.knowledge.entity_extractor import LLMEntityExtractor


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def test_llm_entity_extractor_parses_response(monkeypatch):
    payload = {
        "response": json.dumps(
            [
                {
                    "text": "HbA1c",
                    "label": "LAB_TEST",
                    "start": 0,
                    "end": 5,
                    "confidence": 0.9,
                    "attributes": {"attributes": {"value": "ignored"}},
                }
            ]
        )
    }

    def fake_post(url, *, json, timeout):
        assert json["model"] == "mock-model"
        return DummyResponse(payload)

    monkeypatch.setattr(
        "jarvis.knowledge.entity_extractor.requests.post",
        fake_post,
    )

    extractor = LLMEntityExtractor(model="mock-model", endpoint="http://llm")
    mentions = list(extractor.extract("HbA1c result recorded."))

    assert len(mentions) == 1
    mention = mentions[0]
    assert mention.text == "HbA1c"
    assert mention.label == "LAB_TEST"
    assert mention.start_char == 0
    assert mention.end_char == 5
    # Metadata should round-trip the attributes dictionary
    assert mention.metadata == {"attributes": {"value": "ignored"}}


def test_llm_entity_extractor_falls_back_to_span_search(monkeypatch):
    fenced_output = """```json
    [
        {
            "text": "5.6 percent",
            "label": "MEASUREMENT",
            "attributes": {"value": "5.6", "units": "%"}
        }
    ]
    ```"""

    payload = {"output": fenced_output}

    def fake_post(url, *, json, timeout):
        return DummyResponse(payload)

    monkeypatch.setattr(
        "jarvis.knowledge.entity_extractor.requests.post",
        fake_post,
    )

    extractor = LLMEntityExtractor(model="mock-model", endpoint="http://llm")
    text = "Latest HbA1c value was approximately 5.6 percent last year."
    mentions = list(extractor.extract(text))

    assert len(mentions) == 1
    mention = mentions[0]
    assert mention.text == "5.6 percent"
    assert mention.label == "MEASUREMENT"
    assert mention.start_char == text.lower().find("5.6 percent")
    assert mention.metadata == {"value": "5.6", "units": "%"}


def test_llm_entity_extractor_handles_invalid_json(monkeypatch, caplog):
    payload = {"choices": [{"message": {"content": "not-json"}}]}

    def fake_post(url, *, json, timeout):
        return DummyResponse(payload)

    monkeypatch.setattr(
        "jarvis.knowledge.entity_extractor.requests.post",
        fake_post,
    )

    extractor = LLMEntityExtractor(model="mock-model", endpoint="http://llm")

    with caplog.at_level("WARNING"):
        mentions = list(extractor.extract("Nothing to see here."))

    assert mentions == []
    assert "non-JSON payload" in caplog.text

