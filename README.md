# Jarvis: Messaging Ingestion & Knowledge Graph

Jarvis ingests Gmail messages (and other documents), normalises them into an SQLite datastore, extracts semantic entities, and materialises structured fact tables that are easy to query or feed into downstream LLMs. The project combines semantic search, knowledge-graph style storage, and batch/queue workers so personal data (lab reports, invoices, prescriptions, chat threads) stay grounded and queryable.

## Features

- **Gmail ingestion** with OAuth token reuse and configurable search queries.
- **Local document ingestion** (PDF/TXT/Markdown) with SHA-based deduplication.
- **Semantic indexing** that extracts text chunks and embeddings for retrieval.
- **Knowledge graph extraction** (spaCy or LLM backend) with entity mentions + provenance.
- **Structured fact builders** for lab results, financial records, and medical events.
- **Queue-driven worker** that orchestrates semantic indexing → entity extraction → fact aggregation.
- **CLI utilities** for querying structured tables and cleaning up extractor-specific data.
- **Pytest suite** (≥80% coverage target) and GitHub Actions workflow for automated testing.

## Repository Layout

```
cli/                          # Entry points wrapping ingestion/knowledge modules
jarvis/
  ingestion/
    common/                   # Datastore, Gmail pipelines, local file enqueue helpers
    workers/                  # Long-running task processors (semantic/entity/fact builders)
    gmail/                    # Gmail API integration helpers
  knowledge/
    domains/
      financial/              # Financial fact builder + queries
      lab/                    # Laboratory fact builder + queries
      medical/                # Medical fact builder + queries
    entity_extractor.py       # spaCy/LLM extraction backends and graph builder
    semantic_indexer.py       # Text chunking and embedding storage
    task_queue.py             # Lightweight SQLite-backed work queue
tests/                        # Pytest suite covering ingestion + knowledge layers
.github/workflows/            # CI configuration (lint/test/coverage)
```

## Requirements

- Python 3.9+
- Pip packages listed in `requirements.txt` (core) and `requirements-dev.txt` (testing; requires network access to install).
- Optional: [Ollama](https://ollama.com) or another OpenAI-compatible endpoint if using the LLM extractor.

## Installation

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For development/testing:

```sh
pip install -r requirements-dev.txt
```

## Gmail Ingestion

```sh
python cli/fetch_gmail_messages.py "subject:Meera report" --credentials /path/to/credentials.json
```

Flags:
- `--token`: path for the OAuth token cache (default `~/.gmail-token.json`).
- `--database`: overrides the SQLite path (defaults to `data/messages.db` or `JARVIS_DATABASE`).

## Semantic Indexing & Entity Extraction

```sh
# Extract text + embeddings
python cli/run_semantic_indexer.py --database data/messages.db

# spaCy backend
python cli/run_entity_extraction.py --database data/messages.db

# LLM backend (e.g. using Ollama)
ollama serve
python cli/run_entity_extraction.py --backend llm --llm-model mistral --database data/messages.db
```

Environment variables:
- `JARVIS_DATABASE`: default database path for CLIs/worker.
- `JARVIS_LOG_LEVEL`: default log level (e.g. `DEBUG`).

## Structured Facts & Queries

```sh
python cli/build_lab_results.py --database data/messages.db --extractor llm:mistral
python cli/build_financial_records.py --database data/messages.db --extractor llm:mistral
python cli/build_medical_events.py --database data/messages.db --extractor llm:mistral

python cli/query_lab_results.py --test "hba1c" --limit 5
python cli/query_financial_records.py --counterparty "Dezignare" --limit 5
python cli/query_medical_events.py --event-type medication --limit 5
```

Each `build_*` command materialises structured tables (`lab_results`, `financial_records`, `medical_events`) with provenance metadata (message/attachment IDs, filenames). The `query_*` helpers surface those rows for interactive inspection.

## Local File Ingestion & Worker Queue

```sh
python cli/enqueue_local_files.py /path/to/staging --recursive

ollama serve  # optional, only needed for LLM backend
python cli/processing_worker.py --database data/messages.db --entity-backend llm --llm-model mistral
```

The worker continuously drains `task_queue`, chaining:
1. `semantic_index` → store text + embeddings.
2. `entity_extract` → populate entity mentions + graph relations.
3. `lab_results`, `financial_records`, `medical_events` → build structured tables.

Failed tasks are retried automatically (with exponential backoff); missing content is logged and skipped.

## Cleanup Utilities

Remove spaCy-derived mentions and orphaned graph nodes:

```sh
python cli/cleanup_spacy_data.py --database data/messages.db
```

## Testing & Coverage

```sh
pip install -r requirements.txt -r requirements-dev.txt
coverage run -m pytest
coverage report --fail-under=80
```

GitHub Actions mirrors the same steps (see `.github/workflows/tests.yml`) and uploads `coverage.xml` artifacts.

## Project Roadmap

See [`TODO.md`](TODO.md) for planned enhancements, including LLM-based answering, voice-call ingestion, and infrastructure improvements.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
