# Roadmap

All implementation stages below are pending.

## 0 — Public project foundation

Define the project, scope, architecture and validation approach in a public repository.

## 1 — Reproducible local research MVP

- Source adapter interface and synthetic fixture adapter.
- Validated event schema and rule/LLM parser interfaces.
- SQLite/PostgreSQL repository abstraction.
- Historical event study with documented windows and return definitions.
- Simple explainable signal policy and independent risk checks.
- Backtest skeleton with a replay clock and simulated execution.
- CLI example and meaningful basic tests.

Acceptance: a fresh checkout can run an offline example from source records to events, research output and simulated decisions, with no credentials.

## 2 — Point-in-time data and evaluation

Choose one event family and a small instrument universe. Add an authorized source and market data, record receipt times and revisions, compare rule-only and LLM-assisted baselines, and evaluate chronologically with transaction-cost assumptions.

Acceptance: reproducible results with provenance, explicit limitations and no future-data access.

## 3 — cTrader demo integration

Authenticate a demo account, discover instruments, subscribe to quotes, submit risk-approved demo orders, reconcile fills, recover from disconnects, and track event-driven exits.

Acceptance: an auditable end-to-end demo lifecycle, including duplicate-message handling, rejected orders, stale data and an emergency stop.

## 4 — Broader event coverage

Expand social and geopolitical sources, connect follow-up events to position theses, and evaluate multi-instrument exposure. Consider fine-tuning only after an evaluation dataset identifies a persistent model limitation.

## 5 — Live readiness review

Live execution is outside the initial implementation scope. Any later transition requires an explicit decision after reviewing forward-test evidence, operational reliability and execution constraints.
