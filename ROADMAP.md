# Jarvis Roadmap — Q4 2025 & Mission

## 5-Month Mission (Q4 2025 – Q1 2026)

“Turn Jarvis into a dependable, privacy-first personal agent that can identify the tasks that truly need me and autonomously take the first step toward completing them—confidently grounded in my data and transparent about every action.”

The full SMART OKRs are documented in [`docs/okr_2025-26.md`](docs/okr_2025-26.md). The summary is repeated here for quick reference, and each roadmap track below lists the Objective(s)/Key Result(s) it supports.

| Objective | Key Result (target by Mar 31, 2026) | Status |
|-----------|-------------------------------------|--------|
| **O1. Achieve dependable ingestion and retrieval across life streams** | KR1. Integrate ≥3 additional channels (e.g., WhatsApp, calls, calendar) with ≥95% success + provenance tags. | In progress |
| | KR2. Hybrid retriever answers a curated 200-query benchmark with ≥90% precision and 0 critical hallucinations. | In progress |
| **O2. Enable safe automation for recurring personal tasks** | KR3. Toolbox covers 5 recurring errands with approval workflows; ≥20 successful assisted actions. | Planned |
| | KR4. Automation policy shell with observe → suggest → execute levels, logging 100% of actions for audit. | Planned |
| **O3. Deliver reliable, observable infrastructure** | KR5. Containerised worker stack with runbooks; <5% queue failure rate across 30 consecutive days. | Planned |
| | KR6. Configurable alerts/metrics for ingestion lag & automation actions; >90% of alerts resolved within SLA. | Planned |

The Q4 roadmap below maps to these OKRs:
- Track labels note primary OKR contributions (e.g., **O1**, **O2**).
- Stretch items support long-term mission and may roll into Q1 if capacity shifts.

---

## 1. Platform Foundations
- **Config & Observability** (**O3-KR6**) — extend new config/logging layer with metrics/alerting for worker throughput and queue backlogs. *(config/logging done; metrics pending)*
- **Redis + Worker Hardening** (**O3-KR5/KR6**) — sandbox queue operations, add retry/backoff, build health dashboards (Grafana/Prometheus or statsd).
- **Storage Upgrades** (**O3-KR5**) — draft Postgres/Neo4j migration scripts; add retention tooling for embeddings/processed items.

## 2. Ingestion & Knowledge Graph
- **Channel Expansion** (**O1-KR1**) — add WhatsApp + calendar; lay groundwork for voice transcript ingestion with provenance.
- **Audio Insights Pilot** (**O1-KR1** + **O2-KR3**) — design STT pipeline for recurring calls (ironing/gas/landscaper); include manual review for low-confidence segments.
- **Graph Enrichment** (**O1-KR2**) — extend fact builders with payment reconciliation and richer medical ontologies; enforce message ID citations for all facts.

## 3. Retrieval & Grounding
- **Hybrid Retrieval** (**O1-KR2**) — continue tuning BM25 + embeddings + participant re-ranking, measure against curated benchmark.
- **Conversation Summaries** (**O1-KR2**) — store thread-level embeddings/cached snippets for rapid “last contact” answers.
- **Citation Enforcement** (**O1-KR2**) — verify responses and require message IDs in every final answer.

## 4. Agent Automation
- **Task Toolkit** (**O2-KR3**) — implement tools for recurring errands (call/text scripting, payment draft generation) with approval workflows and usage tracking.
- **Policy Shell** (**O2-KR4**) — define automation levels (observe → suggest → execute) and redline rules (never share data, require approvals).
- **UI Surfaces** (**O2-KR3/KR4**) — prototype “Attention Center” dashboard + mobile companion for notifications and quick approvals.

## 5. Deployment & Ops
- **Containerisation** (**O3-KR5**) — package worker + scheduler for deployable stack, support mini PC / Pi setups.
- **LLM Flexibility** (**O3-KR5**) — ensure the reasoning layer is adapter-driven (Qwen/Mistral/next-gen).
- **Security Review** (**O2-KR4 / O3-KR6**) — threat-model Jarvis, ensure encryption at rest/in transit, and bake audit logging into every action.

### Stretch Goals
- Integrate a safe Python execution tool for scratchpad computations.
- Prototype a UI DSL or schema approach for dynamic responses (e.g., trend graphs).
- Explore ambient listening hardware concepts once policy and privacy layers are in place.

This roadmap keeps us focused on clean data, trustworthy retrieval, and incremental automation—the pillars required for Jarvis to graduate into the loyal, always-on advocate envisioned in the Jarvis Vision document.
