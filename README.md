# Jarvis: Messaging Ingestion & Knowledge Graph

[![CI](https://github.com/sri-akshat/jarvis/actions/workflows/tests.yml/badge.svg?branch=master)](https://github.com/sri-akshat/jarvis/actions/workflows/tests.yml)
[![Coverage](https://img.shields.io/badge/coverage-81%25-brightgreen.svg)](#testing--coverage)

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

## Prerequisites

- Python 3.9+ with `pip`
- Google Cloud project with Gmail API enabled and OAuth client credentials JSON
- SQLite (ships with Python) and command-line `sqlite3` for ad-hoc inspection
- spaCy English model (`python -m spacy download en_core_web_sm`) if you want the spaCy extractor
- Optional local LLM backend such as [Ollama](https://ollama.com) for `--backend llm`
- Optional Neo4j instance (Docker works great) for visualising the graph

## Setup

```sh
git clone https://github.com/sri-akshat/jarvis.git
cd jarvis

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt  # optional but recommended for testing

# Download spaCy model if not already available on the machine
python -m spacy download en_core_web_sm
```

Environment variables the CLIs understand:

- `JARVIS_DATABASE` – default path to the SQLite datastore (fallback: `data/messages.db`)
- `JARVIS_LOG_LEVEL` – default logging level (`INFO`, `DEBUG`, ...)

> **Note** All CLI examples below assume you run them from the repository root (e.g. `python cli/fetch_gmail_messages.py ...`). The CLI module now self-configures `sys.path`, so the commands work whether you execute them as scripts or with `python -m`.

## Ingestion Workflow

### 1. Fetch Gmail Messages

```sh
python cli/fetch_gmail_messages.py "subject:Meera report" --credentials /path/to/credentials.json
```

Writes:
- `messages`, `attachments`, `content_registry` tables
- pending jobs in `task_queue` (one per message + attachment)

Verify:
- `sqlite3 data/messages.db ".tables"`
- `sqlite3 data/messages.db "SELECT id, subject FROM messages ORDER BY received_at DESC LIMIT 5;"`
- `sqlite3 data/messages.db "SELECT task_type, status FROM task_queue ORDER BY created_at DESC LIMIT 5;"`

### 2. Register Local Files (optional)

```sh
python cli/enqueue_local_files.py /path/to/staging --recursive
```

Writes:
- `content_registry` (one row per file)
- `local_files` metadata and `task_queue` entries (`semantic_index` tasks)

Verify:
- `sqlite3 data/messages.db "SELECT content_id, path FROM local_files LIMIT 5;"`

### 3. Process the Queue (recommended)

The ingestion steps above enqueue downstream tasks (`semantic_index`, `entity_extract`, and fact builders). The most consistent way to handle the pipeline is to let the worker drain the queue:

```sh
# Start Ollama only if you plan to use the LLM backend.
ollama serve

python cli/processing_worker.py \
  --database data/messages.db \
  --entity-backend llm \
  --llm-model mistral
```

This daemon loops through:

1. `semantic_index` → populates `attachment_texts` and `embeddings`
2. `entity_extract` → writes to `entity_mentions`, `graph_entities`, `graph_relations`
3. `lab_results`, `financial_records`, `medical_events` → materialises fact tables

Watch the logs for progress; stop the worker with `Ctrl+C`. Use `--run-once` to process one task batch and exit.

> Need to reprocess a specific stage manually? See `docs/semantic_pipeline.md` for advanced commands that bypass the worker.

### 4. Query Structured Facts & Embeddings

```sh
python cli/query_lab_results.py --test "hba1c" --limit 5
python cli/query_financial_records.py --counterparty "Dezignare" --limit 5
python cli/query_medical_events.py --event-type medication --limit 5

python cli/semantic_search.py "vitamin d levels" --top-k 3 --database data/messages.db
```

These commands read from the structured tables and `embeddings`/`attachment_texts` to provide interactive results.

### 5. Cleanup (optional)

```sh
python cli/cleanup_spacy_data.py --database data/messages.db
```

Removes spaCy-derived mentions/relations so you can regenerate with a different backend.

## Optional LLM Backend (Ollama)

If you want to run entity extraction with an on-device LLM:

1. Install Ollama (macOS/Linux):
   ```sh
   curl -fsSL https://ollama.com/install.sh | sh
   ```
   or download the desktop app from [ollama.com](https://ollama.com/download).
2. Pull the model you plan to use (for example):
   ```sh
   ollama pull mistral
   ```
3. Start the service before running the worker in LLM mode:
   ```sh
   ollama serve
   python cli/processing_worker.py --entity-backend llm --llm-model mistral
   ```

You can point the worker at another OpenAI-compatible endpoint with `--llm-endpoint` and `--llm-timeout`.

### LLM-powered payment summary (experimental)

Once Neo4j contains your graph (see [Neo4j Visualisation](#neo4j-visualisation)), you can ask an on-device LLM (e.g., Mistral via Ollama) to reason over the payment mentions:

```sh
python cli/ask_finance_summary.py \
  --counterparty "dezignare india" \
  --neo4j-password neo4j-password
```

The script fetches all `MONEY` mentions tied to the counterparty, computes numeric totals for sanity, and asks the LLM to produce a concise explanation. Adjust `--counterparty`, `--llm-model`, or `--limit` as needed.

### General agent (experimental)

An interactive agent can automatically select tools (finance, medical, etc.) and use Mistral to craft an answer:

```sh
python cli/agent_query.py \
  "How much have I paid Dezignare India?" \
  --neo4j-password neo4j-password
```

The agent evaluates the question, invokes the relevant tool(s), and feeds the structured output back to the LLM before responding. Use `--llm-model`, `--max-loops`, or custom defaults to tune behaviour.

## Neo4j Visualisation

Spin up Neo4j locally (using Docker):

```sh
docker run --name jarvis-neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/neo4j-password \
  -e NEO4J_PLUGINS='["apoc"]' \
  -v neo4j_data:/data \
  neo4j:5.18
```

Then push the SQLite knowledge graph into Neo4j:

```sh
python cli/push_neo4j.py \
  --database data/messages.db \
  --uri bolt://localhost:7687 \
  --user neo4j \
  --password neo4j-password \
  --clear-existing
```

Open http://localhost:7474 in your browser to explore the graph visually (e.g., `MATCH (p:PATIENT)-[r:MENTIONED_IN]->(c:Content) RETURN p,r,c LIMIT 50;`).

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
