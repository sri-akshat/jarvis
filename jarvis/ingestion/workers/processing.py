"""Long-running worker that processes queued ingestion tasks."""
from __future__ import annotations

import argparse
import logging
import time

from jarvis.cli import configure_runtime
from jarvis.knowledge import task_queue
from jarvis.knowledge.domains.financial import FinancialFactBuilder
from jarvis.knowledge.domains.lab import LabFactBuilder
from jarvis.knowledge.domains.medical import MedicalFactBuilder
from jarvis.knowledge.entity_extractor import (
    KnowledgeGraphBuilder,
    LLMEntityExtractor,
    SpacyEntityExtractor,
)
from jarvis.knowledge.semantic_indexer import SemanticIndexer

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--task-types",
        nargs="*",
        default=[
            "semantic_index",
            "entity_extract",
            "lab_results",
            "financial_records",
            "medical_events",
        ],
        help="Subset of task types to process",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Seconds to wait when no tasks are available (default: 5)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Process at most one task and exit",
    )
    parser.add_argument(
        "--entity-backend",
        choices=["spacy", "llm"],
        default="llm",
        help="Backend to use for entity extraction tasks (default: llm)",
    )
    parser.add_argument(
        "--spacy-model",
        default="en_core_web_sm",
        help="spaCy model name when --entity-backend=spacy",
    )
    parser.add_argument(
        "--llm-model",
        default="mistral",
        help="LLM model identifier when --entity-backend=llm",
    )
    parser.add_argument(
        "--llm-endpoint",
        default="http://localhost:11434/api/generate",
        help="LLM HTTP endpoint",
    )
    parser.add_argument(
        "--llm-timeout",
        type=int,
        default=120,
        help="Timeout for LLM requests in seconds",
    )
    parser.add_argument(
        "--lab-extractor",
        default="llm:mistral",
        help="Extractor identifier for lab fact aggregation",
    )
    parser.add_argument(
        "--financial-extractor",
        default="llm:mistral",
        help="Extractor identifier for financial aggregation",
    )
    parser.add_argument(
        "--medical-extractor",
        default="llm:mistral",
        help="Extractor identifier for medical aggregation",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (e.g. INFO, DEBUG)",
    )
    return parser.parse_args()


def build_entity_extractor(args: argparse.Namespace, database_path: str):
    if args.entity_backend == "spacy":
        extractor = SpacyEntityExtractor(model=args.spacy_model)
        extractor_name = f"spacy:{args.spacy_model}"
    else:
        extractor = LLMEntityExtractor(
            model=args.llm_model,
            endpoint=args.llm_endpoint,
            timeout=args.llm_timeout,
        )
        extractor_name = f"llm:{args.llm_model}"
    builder = KnowledgeGraphBuilder(
        database_path=database_path,
        extractor=extractor,
        extractor_name=extractor_name,
    )
    return builder, extractor_name


def handle_semantic_task(indexer: SemanticIndexer, payload: dict, database: str) -> None:
    content_id = payload.get("content_id")
    if not content_id:
        raise ValueError("semantic_index task missing content_id")
    logger.info("[semantic_index] Processing %s", content_id)
    processed = indexer.process_content_id(content_id)
    if not processed:
        logger.info("[semantic_index] Skipped %s: no content available", content_id)
        return
    task_queue.enqueue_task(
        database,
        "entity_extract",
        {"content_id": content_id},
    )


def handle_entity_task(
    builder: KnowledgeGraphBuilder,
    extractor_name: str,
    payload: dict,
    database: str,
    *,
    financial_extractor: str,
    medical_extractor: str,
) -> None:
    content_id = payload.get("content_id")
    if not content_id:
        raise ValueError("entity_extract task missing content_id")
    logger.info("[entity_extract] Processing %s via %s", content_id, extractor_name)
    processed = builder.run(content_id=content_id)
    if processed:
        task_queue.enqueue_task(
            database,
            "lab_results",
            {"content_id": content_id, "extractor": extractor_name},
        )
        task_queue.enqueue_task(
            database,
            "financial_records",
            {"content_id": content_id, "extractor": financial_extractor},
        )
        task_queue.enqueue_task(
            database,
            "medical_events",
            {"content_id": content_id, "extractor": medical_extractor},
        )


def handle_lab_task(builder: LabFactBuilder, payload: dict) -> None:
    extractor = payload.get("extractor", builder.extractor)
    content_id = payload.get("content_id")
    builder.extractor = extractor
    logger.info("[lab_results] Building facts for %s (extractor=%s)", content_id, extractor)
    produced = builder.run(content_id=content_id)
    logger.info("[lab_results] Produced %s row(s) for %s", produced, content_id)


def handle_financial_task(builder: FinancialFactBuilder, payload: dict) -> None:
    extractor = payload.get("extractor", builder.extractor)
    content_id = payload.get("content_id")
    builder.extractor = extractor
    logger.info(
        "[financial_records] Building facts for %s (extractor=%s)",
        content_id,
        extractor,
    )
    produced = builder.run(content_id=content_id)
    logger.info("[financial_records] Produced %s row(s) for %s", produced, content_id)


def handle_medical_task(builder: MedicalFactBuilder, payload: dict) -> None:
    extractor = payload.get("extractor", builder.extractor)
    content_id = payload.get("content_id")
    builder.extractor = extractor
    logger.info(
        "[medical_events] Building facts for %s (extractor=%s)",
        content_id,
        extractor,
    )
    produced = builder.run(content_id=content_id)
    logger.info("[medical_events] Produced %s row(s) for %s", produced, content_id)


def run(args: argparse.Namespace) -> None:
    config = configure_runtime(args.database, args.log_level)
    db_path = str(config.database_path)
    indexer = SemanticIndexer(db_path)
    entity_builder, extractor_name = build_entity_extractor(args, db_path)
    lab_builder = LabFactBuilder(db_path, extractor=args.lab_extractor)
    financial_builder = FinancialFactBuilder(db_path, extractor=args.financial_extractor)
    medical_builder = MedicalFactBuilder(db_path, extractor=args.medical_extractor)

    while True:
        task = task_queue.fetch_and_lock_task(db_path, task_types=args.task_types)
        if not task:
            if args.run_once:
                break
            time.sleep(args.poll_interval)
            continue
        logger.info(
            "Picked task %s (%s) with payload %s",
            task.task_id,
            task.task_type,
            task.payload,
        )
        try:
            if task.task_type == "semantic_index":
                handle_semantic_task(indexer, task.payload, db_path)
            elif task.task_type == "entity_extract":
                handle_entity_task(
                    entity_builder,
                    extractor_name,
                    task.payload,
                    db_path,
                    financial_extractor=args.financial_extractor,
                    medical_extractor=args.medical_extractor,
                )
            elif task.task_type == "lab_results":
                handle_lab_task(lab_builder, task.payload)
            elif task.task_type == "financial_records":
                handle_financial_task(financial_builder, task.payload)
            elif task.task_type == "medical_events":
                handle_medical_task(medical_builder, task.payload)
            else:
                raise ValueError(f"Unsupported task type: {task.task_type}")
        except Exception as exc:  # pragma: no cover - worker runtime
            logger.exception("Task %s failed: %s", task.task_id, exc)
            task_queue.fail_task(
                db_path,
                task.task_id,
                error=repr(exc),
            )
        else:
            task_queue.complete_task(db_path, task.task_id)
            logger.info("Completed task %s (%s)", task.task_id, task.task_type)
        if args.run_once:
            break


def main() -> None:
    args = parse_args()
    run(args)
