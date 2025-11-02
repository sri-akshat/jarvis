"""Ask a natural language question using the tool-enabled agent."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse
import logging

from jarvis.agent import ToolContext, ToolExecutor, ToolOrchestrator, load_default_registry
from jarvis.agent.orchestrator import OrchestratorConfig
from jarvis.cli import configure_runtime
from jarvis.knowledge.finance_graph import MistralLLMClient
from jarvis.knowledge.neo4j_exporter import Neo4jConnectionConfig

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question", help="Natural language question for the agent")
    parser.add_argument("--database", default=None, help="Path to SQLite database (fallback: JARVIS_DATABASE)")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j Bolt URI")
    parser.add_argument("--neo4j-user", default="neo4j", help="Neo4j username")
    parser.add_argument("--neo4j-password", help="Neo4j password")
    parser.add_argument("--neo4j-database", default=None, help="Optional Neo4j database name")
    parser.add_argument("--llm-model", default="mistral:latest", help="LLM model tag (default mistral:latest)")
    parser.add_argument("--llm-endpoint", default="http://localhost:11434/api/generate", help="LLM HTTP endpoint")
    parser.add_argument("--llm-timeout", type=int, default=60, help="LLM request timeout (seconds)")
    parser.add_argument("--log-level", default=None, help="Optional log level override")
    parser.add_argument("--max-loops", type=int, default=3, help="Maximum tool iterations")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = configure_runtime(args.database, args.log_level)

    neo4j_config = None
    if args.neo4j_password:
        neo4j_config = Neo4jConnectionConfig(
            uri=args.neo4j_uri,
            user=args.neo4j_user,
            password=args.neo4j_password,
            database=args.neo4j_database,
        )

    registry = load_default_registry()
    context = ToolContext(
        database_path=str(config.database_path),
        neo4j_config=neo4j_config,
        defaults={"medical_extractor": "llm:mistral"},
    )
    executor = ToolExecutor(context, registry)
    llm_client = MistralLLMClient(model=args.llm_model, endpoint=args.llm_endpoint, timeout=args.llm_timeout)
    orchestrator = ToolOrchestrator(
        specs=registry,
        executor=executor,
        llm_client=llm_client,
        config=OrchestratorConfig(max_loops=args.max_loops),
    )

    response = orchestrator.run(args.question)
    print("=== Answer ===")
    print(response.answer.strip())
    if response.tool_calls:
        print("\n=== Tool Calls ===")
        for record in response.tool_calls:
            print(f"- {record.tool} -> success={record.result.success}")
            if record.result.data:
                print(f"  data: {record.result.data if len(str(record.result.data)) < 400 else '(data trimmed)'}")
            if record.result.error:
                print(f"  error: {record.result.error}")
if __name__ == "__main__":
    main()
