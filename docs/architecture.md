# Architecture — v0.1

The monorepo has one installable Python package with explicit module boundaries. Separate distributions and services can be extracted when needed; the first version avoids inter-package deployment overhead.

```text
apps/cli/                          CLI entry-point documentation
packages/eventlens/src/eventlens/
  core.py                          Validated shared schemas
  ingestion.py                     Source protocol and JSONL adapter
  parsing.py                       Rule parser and injectable LLM parser
  storage.py                       SQLite / PostgreSQL event repository
  research.py                      Point-in-time analogues and event returns
  signals.py                       Uncalibrated EUR/USD stance baseline
  risk.py                          Deterministic risk configuration and checks
  backtesting.py                    Single-position paper replay
  execution.py                      Future broker contract; cTrader disabled
  cli.py                           Installed eventlens command
examples/                          Synthetic messages and bid/ask quotes
tests/                            Behavioral and repository tests
.github/workflows/tests.yml         SQLite / PostgreSQL CI
```

## Time and evidence

Raw records carry publication time, receipt time, source identity, revision and episode identity. Events add parsing availability time and parser version. All input timestamps require an explicit timezone. The CLI models parsing latency; a future live adapter must measure actual completion time.

The caller is currently responsible for assigning records to FOMC episodes and identifying their source. Entity linking is restricted to US.FOMC and EURUSD; it is not general-purpose entity recognition. The rule parser uses a few exact phrases and abstains on unsupported or conflicting language. It does not compare successive official statements or estimate market expectations. The LLM parser accepts an injected client and validates JSON and verbatim evidence; no network provider is bundled.

## Storage

`events_v1` stores the complete validated event as JSON under a deterministic ID. Insertion uses database-native conflict handling for SQLite and PostgreSQL. Raw provenance and revisions are retained in each payload. A matching ID is immutable: first write wins. `list(as_of=...)` filters by parsing availability. Filtering currently happens in Python and is intended for small research datasets; indexed timestamp columns and migrations are future work.

## Decisions and fills

Hawkish maps to a EUR/USD short, dovish to a long. This is a deliberately simplistic baseline, not an empirically validated forecast. Neutral or unknown means abstain; it does not silently close an existing position. A directional reversal in the same episode closes the position without immediately reversing it. Signals from unrelated episodes cannot replace an open position.

Replay waits for a quote strictly after event availability. Repeated content within an episode and event kind is skipped. If multiple updates for the same episode arrive before a quote, the latest supersedes earlier ones. Simultaneous distinct events and duplicate quote times are rejected until an aggregation policy exists. A signal expires rather than filling much later after a data gap.

There is at most one fixed-size EUR/USD position. Entry uses bid/ask plus adverse slippage; exits use the executable opposite side plus adverse slippage. Commission is charged on both legs. P&L is in USD. Stop, holding-time, loss-limit and same-episode reversal exits are evaluated on available quotes; stops can gap and do not guarantee a fill at the stop level. The loss limit is cumulative over the replay, not a daily reset. Open positions are liquidated on the final quote and that reason is recorded.

Not modeled: margin, swaps, order-book depth, partial fills, asynchronous broker acknowledgements, continuously persisted risk state, or real execution latency beyond configured parsing and quote timing. Sparse or missing quotes reduce fidelity. Do not use this replay as a live executor.

## Research

Event studies use mid-price percentage returns in basis points, from the first quote at/after availability within one minute to the first quote at/after a 15-minute horizon within one minute. Both prices must be available by `as_of`. Missing windows are excluded, with sample count reported. These are descriptive, non-causal, overlapping observations; no benchmark adjustment, significance test or profitability claim is made.

Analogue retrieval currently matches kind and stance, excludes the current episode, and uses only earlier available events. It is a baseline filter, not an embedding search. The signal engine does not learn from the event-study output, avoiding a hidden full-sample fitting step.

## Future execution

The first broker target is Pepperstone cTrader demo through Open API. `CTraderAdapter` deliberately raises `NotImplementedError`; no credentials or live order routes exist. Account-specific symbols, contract sizes and trading rules must be discovered before this integration. A real adapter also needs idempotent order handling, fill reconciliation and reconnect recovery.
