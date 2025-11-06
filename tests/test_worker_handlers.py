from __future__ import annotations

from dataclasses import dataclass

from jarvis.knowledge import task_queue
from jarvis.ingestion.workers.processing import (
    handle_entity_task,
    handle_financial_task,
    handle_lab_task,
    handle_medical_task,
    handle_semantic_task,
)


class StubIndexer:
    def __init__(self, should_process: bool):
        self.should_process = should_process
        self.calls: list[str] = []

    def process_content_id(self, content_id: str) -> bool:
        self.calls.append(content_id)
        return self.should_process


class StubBuilder:
    def __init__(self, processed_segments: int = 1):
        self.processed_segments = processed_segments
        self.calls: list[str] = []
        self.extractor = "stub"

    def run(self, content_id: str) -> int:
        self.calls.append(content_id)
        return self.processed_segments


def test_handle_semantic_task_processes(monkeypatch):
    enqueued = []
    monkeypatch.setattr(
        task_queue,
        "enqueue_task",
        lambda db, task_type, payload: enqueued.append((task_type, payload)),
    )
    indexer = StubIndexer(should_process=True)
    handle_semantic_task(indexer, {"content_id": "cid"}, "db.sqlite")
    assert indexer.calls == ["cid"]
    assert enqueued == [("entity_extract", {"content_id": "cid"})]


def test_handle_semantic_task_skips(monkeypatch):
    monkeypatch.setattr(task_queue, "enqueue_task", lambda *args, **kwargs: None)
    indexer = StubIndexer(should_process=False)
    handle_semantic_task(indexer, {"content_id": "missing"}, "db.sqlite")
    assert indexer.calls == ["missing"]


def test_handle_entity_task_enqueues_followups(monkeypatch):
    calls = []
    monkeypatch.setattr(
        task_queue,
        "enqueue_task",
        lambda db, task_type, payload: calls.append((task_type, payload)),
    )
    builder = StubBuilder(processed_segments=1)
    handle_entity_task(
        builder,
        extractor_name="lab",
        payload={"content_id": "cid"},
        queue_target="db.sqlite",
        financial_extractor="fin",
        medical_extractor="med",
    )
    assert builder.calls == ["cid"]
    task_types = [task for task, _ in calls]
    assert task_types == ["lab_results", "financial_records", "medical_events"]


def test_handle_entity_task_no_work(monkeypatch):
    monkeypatch.setattr(task_queue, "enqueue_task", lambda *args, **kwargs: None)
    builder = StubBuilder(processed_segments=0)
    handle_entity_task(
        builder,
        extractor_name="lab",
        payload={"content_id": "cid"},
        queue_target="db.sqlite",
        financial_extractor="fin",
        medical_extractor="med",
    )
    assert builder.calls == ["cid"]


def test_handle_fact_tasks(monkeypatch):
    calls = []

    class Builder:
        def __init__(self, extractor: str):
            self.extractor = extractor
            self.calls = []

        def run(self, content_id):
            self.calls.append((self.extractor, content_id))

    lab_builder = Builder(extractor="lab")
    handle_lab_task(lab_builder, {"content_id": "cid", "extractor": "lab"})
    assert lab_builder.calls == [("lab", "cid")]

    financial_builder = Builder(extractor="fin")
    handle_financial_task(
        financial_builder, {"content_id": "cid", "extractor": "fin"}
    )
    assert financial_builder.calls == [("fin", "cid")]

    medical_builder = Builder(extractor="med")
    handle_medical_task(medical_builder, {"content_id": "cid", "extractor": "med"})
    assert medical_builder.calls == [("med", "cid")]
