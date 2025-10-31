"""CLI entry point for entity extraction and knowledge graph updates."""
from __future__ import annotations

import argparse
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.entity_extractor import (
    KnowledgeGraphBuilder,
    LLMEntityExtractor,
    SpacyEntityExtractor,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help="Path to the SQLite database (fallback: JARVIS_DATABASE or data/messages.db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of text chunks to process in this run",
    )
    parser.add_argument(
        "--spacy-model",
        default="en_core_web_sm",
        help="spaCy model to use for entity extraction (default: en_core_web_sm)",
    )
    parser.add_argument(
        "--backend",
        choices=["spacy", "llm"],
        default="spacy",
        help="Extraction backend to use (default: spacy)",
    )
    parser.add_argument(
        "--llm-model",
        default="mistral",
        help="LLM model identifier when --backend=llm (e.g., mistral, phi3:mini)",
    )
    parser.add_argument(
        "--llm-endpoint",
        default="http://localhost:11434/api/generate",
        help="LLM HTTP endpoint (default: Ollama generate API)",
    )
    parser.add_argument(
        "--llm-timeout",
        type=int,
        default=60,
        help="Timeout in seconds for LLM requests (default: 60)",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (e.g. INFO, DEBUG)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = configure_runtime(args.database, args.log_level)
    if args.backend == "llm":
        extractor = LLMEntityExtractor(
            model=args.llm_model,
            endpoint=args.llm_endpoint,
            timeout=args.llm_timeout,
        )
        extractor_name = f"llm:{args.llm_model}"
    else:
        extractor = SpacyEntityExtractor(model=args.spacy_model)
        extractor_name = f"spacy:{args.spacy_model}"
    builder = KnowledgeGraphBuilder(
        database_path=str(config.database_path),
        extractor=extractor,
        extractor_name=extractor_name,
    )
    processed = builder.run(limit=args.limit)
    logger.info("Entity extraction completed for %s text segment(s).", processed)


if __name__ == "__main__":
    main()
