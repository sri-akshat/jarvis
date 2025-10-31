# TODO

## Short Term
- [ ] Achieve ≥80% automated test coverage across business logic modules (`jarvis.messaging`, `jarvis.knowledge`, task queue).
- [ ] Configure GitHub Actions workflow to run lint/test + coverage gate (`coverage report --fail-under=80`).
- [ ] Refactor CLI/worker scripts into importable modules with shared logging/config management.
- [ ] Introduce configuration layer (env/ini) and structured logging for long-running workers.

## Natural Language Answers
- [ ] Implement retrieval + prompting layer that feeds structured tables (`lab_results`, `financial_records`, `medical_events`) and snippets into an LLM for grounded responses.
- [ ] Add citation enforcement (message/attachment IDs) and validation before returning answers.

## Multimodal Extensions
- [ ] Build speech-to-text ingestion for voice calls (transcripts + diarization metadata).
- [ ] Extend entity extraction pipelines with call-specific prompts/models and create `call_events` fact builder.
- [ ] Add manual review queue for low-confidence audio-derived facts.

## Future Enhancements
- [ ] Migrate optional storage to Postgres/Neo4j once dataset outgrows SQLite; provide migration scripts.
- [ ] Add data retention/cleanup tooling for embeddings and processed content.
- [ ] Prepare deployment scripts/container (worker + scheduler) for continuous ingestion.
