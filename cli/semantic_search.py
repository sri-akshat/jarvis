"""CLI helper to run semantic searches against attachment embeddings."""
from __future__ import annotations

try:  # pragma: no cover
    from cli._bootstrap import ensure_project_root
except ModuleNotFoundError:  # pragma: no cover
    from _bootstrap import ensure_project_root

ensure_project_root()

import argparse

from jarvis.knowledge.retriever import SemanticRetriever


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="Natural language query to search for")
    parser.add_argument(
        "--database",
        default="data/messages.db",
        help="Path to the SQLite database (default: data/messages.db)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of results to return (default: 5)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    retriever = SemanticRetriever(args.database)
    results = retriever.search(args.query, top_k=args.top_k)
    if not results:
        print("No semantic matches found.")
        return

    for rank, result in enumerate(results, start=1):
        print(f"#{rank} | score={result.score:.3f} | content={result.content_id}")
        if result.attachment_filename:
            print(f"    file: {result.attachment_filename} (page {result.page})")
        if result.subject:
            print(f"    subject: {result.subject}")
        print(f"    text: {result.text[:400]}")
        print()


if __name__ == "__main__":
    main()
