"""Query Neo4j for payments to a counterparty and summarise via LLM."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging

from jarvis.cli import configure_runtime
from jarvis.knowledge.finance_graph import (
    MistralLLMClient,
    ask_finance_question,
)
from jarvis.knowledge.neo4j_exporter import Neo4jConnectionConfig

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--counterparty",
        required=True,
        help="Canonical name of the counterparty (e.g. 'dezignare india')",
    )
    parser.add_argument(
        "--neo4j-uri",
        default="bolt://localhost:7687",
        help="Neo4j Bolt URI (default: bolt://localhost:7687)",
    )
    parser.add_argument(
        "--neo4j-user",
        default="neo4j",
        help="Neo4j username (default: neo4j)",
    )
    parser.add_argument(
        "--neo4j-password",
        required=True,
        help="Neo4j password",
    )
    parser.add_argument(
        "--neo4j-database",
        default=None,
        help="Optional Neo4j database name",
    )
    parser.add_argument(
        "--llm-model",
        default="mistral",
        help="LLM model identifier for Ollama/OpenAI-compatible endpoint",
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
        help="LLM request timeout in seconds (default: 60)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of payment mentions to fetch",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        help="Optional log level override (e.g. INFO, DEBUG)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = configure_runtime(database=None, log_level=args.log_level)
    neo4j_config = Neo4jConnectionConfig(
        uri=args.neo4j_uri,
        user=args.neo4j_user,
        password=args.neo4j_password,
        database=args.neo4j_database,
    )
    llm_client = MistralLLMClient(
        model=args.llm_model,
        endpoint=args.llm_endpoint,
        timeout=args.llm_timeout,
    )
    mentions, totals, answer = ask_finance_question(
        neo4j_config,
        counterparty=args.counterparty,
        llm_client=llm_client,
        limit=args.limit,
    )
    logger.info("LLM totals sanity-check: %s", totals)
    print("=== Mistral response ===")
    print(answer.strip())
    print("\n=== Payments considered ===")
    for mention in mentions:
        print(
            f"- {mention.amount_text} "
            f"(numeric={mention.amount_value}, currency={mention.currency}, "
            f"subject={mention.subject}, file={mention.filename})"
        )


if __name__ == "__main__":
    main()
