# Semantic Index Prototype

The prototype semantic pipeline extends the ingestion flow with a lightweight content registry, text extraction step, and embedding index. It is designed so downstream components can plug in richer extractors, embedding models, or knowledge-graph builders without rewriting the plumbing.

## Flow

0. **Local Staging (optional)** – Collect Gmail/WhatsApp exports or other documents in a staging folder and register them via `enqueue_local_files.py`, which deduplicates by SHA and schedules processing tasks.
1. **Content Registration** – When `SQLiteDataStore.save_messages` persists an email, each message body and attachment is registered in `content_registry` with IDs such as `message:<message_id>` or `attachment:<message_id>:<attachment_id>`. The registry captures provenance, MIME type, hashes, and metadata needed for downstream processing.
2. **Text Extraction (`run_semantic_indexer.py`)** – The indexer queries for attachments lacking extracted text, runs MIME-aware extractors (currently PDF and plain-text), and stores normalized chunks in `attachment_texts`.
3. **Embedding Generation** – Extracted chunks are embedded via `SimpleEmbeddingGenerator`, a deterministic hashed bag-of-words encoder that produces 128-dimension vectors stored in the `embeddings` table. Each embedding row keeps the content reference, chunk index, source filename, and timestamp.
4. **Entity Extraction (`run_entity_extraction.py`)** – A spaCy- or LLM-backed worker scans unprocessed text chunks, discovers entities, and persists them as nodes (`graph_entities`) plus `MENTIONED_IN` relations with provenance in `entity_mentions`.
5. **Task Queue** – Every new content item enqueues downstream jobs in `task_queue`. Long-running workers (`jarvis.ingestion.workers.processing`) drain `semantic_index`, `entity_extract`, and `lab_results` tasks to provide resiliency and horizontal scale.
6. **Structured Fact Tables** – Task handlers populate `lab_results`, `financial_records`, and `medical_events` so agents can answer analytical or planning questions without re-scanning raw text.
7. **Knowledge-Graph Stubs** – `graph_entities` and `graph_relations` now hold both discovered entities and content nodes so downstream components can execute structured queries alongside semantic retrieval.

## Usage

```bash
python fetch_gmail_messages.py "subject:Jarvis Update" --credentials path/to/credentials.json
python enqueue_local_files.py /path/to/staging --recursive  # optional local docs
python cli/processing_worker.py --database data/messages.db --entity-backend llm --llm-model mistral
python build_lab_results.py --database data/messages.db --extractor llm:mistral
python build_financial_records.py --database data/messages.db --extractor llm:mistral
python build_medical_events.py --database data/messages.db --extractor llm:mistral
```

The second command processes any new attachments and is safe to rerun; entries are upserted based on `content_id` and chunk indices.

## Extensibility Hooks

- Swap `SimpleEmbeddingGenerator` for a SentenceTransformer or API-based model without changing database schema.
- Implement additional extractors for images, spreadsheets, or audio by extending `_extract_text`.
- Swap `SpacyEntityExtractor` for the built-in `LLMEntityExtractor` (or a custom backend) and add new task handlers under `jarvis.ingestion.workers.processing` for other pipelines.
- Extend `LabFactBuilder` to emit additional structured tables (e.g., invoices, medications) by pattern matching on other entity labels.
- Populate higher-order relations or aggregate facts (labs, invoices) on top of the per-mention graph entries to unlock analytics-style queries.
