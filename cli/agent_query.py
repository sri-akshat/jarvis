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
from jarvis.knowledge.finance_graph import OllamaLLMClient
from jarvis.knowledge.neo4j_exporter import Neo4jConnectionConfig

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question", nargs="?", help="Natural language question for the agent")
    parser.add_argument("--database", default=None, help="Path to SQLite database (fallback: JARVIS_DATABASE)")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687", help="Neo4j Bolt URI")
    parser.add_argument("--neo4j-user", default="neo4j", help="Neo4j username")
    parser.add_argument("--neo4j-password", help="Neo4j password")
    parser.add_argument("--neo4j-database", default=None, help="Optional Neo4j database name")
    parser.add_argument("--llm-model", default="qwen2.5:7b", help="LLM model tag (default qwen2.5:7b)")
    parser.add_argument("--llm-endpoint", default="http://localhost:11434/api/generate", help="LLM HTTP endpoint")
    parser.add_argument("--llm-timeout", type=int, default=60, help="LLM request timeout (seconds)")
    parser.add_argument("--log-level", default=None, help="Optional log level override")
    parser.add_argument("--max-loops", type=int, default=3, help="Maximum tool iterations")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Start an interactive multi-turn chat session",
    )
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
        defaults={
            "medical_extractor": "llm:mistral",
            "lab_extractor": "llm:mistral",
        },
    )
    executor = ToolExecutor(context, registry)
    llm_client = OllamaLLMClient(model=args.llm_model, endpoint=args.llm_endpoint, timeout=args.llm_timeout)
    orchestrator = ToolOrchestrator(
        specs=registry,
        executor=executor,
        llm_client=llm_client,
        config=OrchestratorConfig(max_loops=args.max_loops),
    )

    history: list[tuple[str, str]] = []

    def handle_question(user_question: str) -> None:
        response = orchestrator.run(user_question, chat_history=history)
        answer = response.answer.strip()
        print("=== Answer ===")
        print(answer)
        if response.tool_calls:
            print("\n=== Tool Calls ===")
            for record in response.tool_calls:
                print(f"- {record.tool} -> success={record.result.success}")
                if record.result.data:
                    payload = record.result.data
                    snippet = str(payload)
                    print(f"  data: {snippet if len(snippet) < 400 else snippet[:400] + '…'}")
                if record.result.error:
                    print(f"  error: {record.result.error}")
        history.append((user_question, answer))

    if args.interactive or not args.question:
        print("Entering interactive mode. Type 'exit' or press Ctrl+D to quit.")
        try:
            while True:
                user_input = input("You> ").strip()
                if not user_input or user_input.lower() in {"exit", "quit"}:
                    break
                handle_question(user_input)
        except (EOFError, KeyboardInterrupt):
            print()  # move to new line
        return

    handle_question(args.question)
if __name__ == "__main__":
    main()
