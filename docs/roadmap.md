# Intraday roadmap

Each stage needs its own acceptance result before progressing. This is a plan, not
permission to launch collectors, spend API credits or connect a broker automatically.

**Stage 0 completed locally on October 15, 2026:** 205 offline tests, lint, format,
strict type checks, relative Markdown links, command smoke checks and isolated package
build passed. All 16 moved modules retain their non-import logic; pre-move research
fingerprints match. No model, news or broker API was called during this reorganization.
**Stage 1A now implements source capture:** news/calendar polling, raw payloads,
revision history and post-commit availability markers. See [acceptance](observer-acceptance.md).
**Stage 1B adds bounded Demo EURUSD quote capture and as-of news alignment.** Its
[short observation](live-quotes-acceptance.md) passed storage/timing checks but exposed
29/33 zero-spread samples and a stale quote at the news cutoff. Representative quote
quality, extended/important-release sampling and the rest of Stage 1 acceptance remain
pending; Stages 2–5 remain planned.

| Stage | Work | Acceptance |
|---|---|---|
| 0 — Repository organization | Separate sources, historical tools and trading interfaces; group commands; preserve old commands and research identity | Existing offline checks pass; pinned prompts/schemas/configs/experiment IDs unchanged; docs identify real capabilities |
| 1 — Realtime observation | Select a suitable news/calendar source, capture immutable revisions and relevant quotes, retain original/display/receipt clocks | Sample normal periods and an important release; inspect duplicates, gaps, availability and latency; don't substitute publisher time for unknown original time |
| 2 — Structured LLM observation | Repair source-evidence references, add bounded intraday context, compare one-call and analysis/decision roles using identical input cutoffs | Inspect failures and unsupported claims; report validation rate, cost and latency distribution; new 30–60-second target measured separately from legacy rules |
| 3 — Replay and shadow ledger | Reuse costs/accounting, add explicit entry/exit rules and no-order forward observations | Time-separated evaluation including ignored/failed cases; real Bid/Ask, costs, missingness, delayed-entry sensitivity and simple baselines; no after-the-fact event selection |
| 4 — Independent protection | Implement position sizing, loss limits, spread/freshness checks, timed exits, daily close, restart reconciliation and kill switch | Fault tests show no duplicate exposure; no new orders on bad inputs; protection continues without the LLM; exits are confirmed rather than assumed |
| 5 — cTrader Demo | Connect only an explicitly verified Demo account through an isolated adapter | Reconcile intents/orders/fills/positions, measured costs and latency; compare Demo and shadow outcomes; never use live credentials |

WallStreetCN website news/calendar endpoints are now an initial capture adapter.
Long-term provider suitability and delivery latency still need evaluation. Social media can
be added behind the same adapter boundary after provenance and duplicate handling are
defined. Broad news collection does not require an unlimited initial instrument list.

Thesis memory remains useful but should supply relevant event/position context rather
than the full archive. Do not remove existing revision or evidence history to reduce
prompt length. Additional AI risk review is optional; hard limits stay deterministic.

No profitability, forecast calibration or live readiness has been established. A valid
JSON reply, a good-looking replay or a positive simulated reward is not an execution
gate. Small live trading is outside this roadmap until separately authorized and justified.
