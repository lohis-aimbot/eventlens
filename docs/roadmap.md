# Roadmap

## Completed — Public foundation and offline MVP

- Public repository, English README and architecture.
- JSONL ingestion protocol and timezone-aware event schema.
- Conservative rule parser and validated injectable LLM parser interface.
- SQLite/PostgreSQL repository with idempotent event insertion.
- Historical analogue filter and descriptive event-return study.
- Simple EUR/USD stance signal, risk checks and single-position paper replay.
- CLI, synthetic fixtures, tests and database CI configuration.

## Next — Real FOMC research

- Ingest official statements and compare changes against the preceding statement.
- Select an LLM provider and build a manually reviewed extraction evaluation set.
- Obtain timestamped EUR/USD bid/ask data and valid point-in-time expectations.
- Compare rule-only and LLM-assisted policies chronologically, including measured delays and costs.
- Keep small FOMC sample sizes and historical model knowledge explicit in conclusions.

## Then — Follow-up information and cTrader demo

- Ingest incremental press-conference information with measured availability times.
- Validate episode linking and thesis updates on real messages.
- Authenticate a Pepperstone cTrader demo account, discover symbols, and reconcile orders and fills.
- Add broker-aware sizing, margin checks, durable state, reconnect handling and an emergency stop.

## Later — Broader coverage

Use USD/JPY as a comparison market, then evaluate gold and oil. Add authorized social and geopolitical feeds, semantic novelty and cross-asset exposure models. Consider fine-tuning only when a labeled evaluation set demonstrates a persistent limitation.

Live trading remains outside the current scope and requires a separate decision.
