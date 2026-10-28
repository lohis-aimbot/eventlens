# Architecture and module map — v0.15

The target is a small LLM-driven intraday system: financial events plus expectations,
quotes, positions and relevant thesis memory lead to a position proposal, independent
hard risk and eventually shadow/Demo execution. News categories and instruments are
configurable research choices. Existing FOMC experiments retain their original scope.

## Current boundaries

| Location | Implemented behavior |
|---|---|
| `contracts.py` | Immutable typed events, UTC clocks, theses, market/portfolio snapshots, decisions and risk/execution records |
| `deepseek.py` | Provider-neutral JSON client contract and DeepSeek HTTP adapter |
| `memory.py`, `sqlite_memory.py` | Memory contract, raw event evidence, immutable revisions, atomic commits, conflict and recovery checks |
| `reasoning.py` | Validated create/update/ignore memory decisions; no portfolio targets |
| `observer.py` | Provider-neutral capture contracts, dedicated SQLite payload/revision/enrichment storage and strict `as_of` cutoff-aware history |
| `sources/english_tier1.py` | Default Tier-1 English FX/macro intake (`investingLive`, `ECB`, `Fed`, `US BEA`, `Forex Factory`), independent per-stream persistence/publication, non-regressing full-text enrichment, `EURUSDEventFilter` and simultaneous event bundling |
| `sources/wallstreetcn.py` | Bounded news/calendar website polling, normalization, rejection records and retry/pacing |
| `quotes.py`, `quotes_cli.py` | Dedicated Demo quote store, provenance/clock preservation (`source_at`, `bridge_at`, `received_at`, `available_at`), unified `QuoteQualityPolicy` (`validate_market_quote`), quality reporting and retrospective news/quote cutoff checks |
| `sources/ctrader_demo.py` | Fresh local-file handshake, bounded tailing and quote validation; no order methods |
| `ctrader/EventLensQuoteBridge.cs` | Sampled Demo EURUSD real-time snapshots, sandbox files and automatic stop |
| `sources/events.py` | General source/filter interfaces only |
| `sources/fed.py` | Official Fed polling, URL validation, retry/pacing, durable inbox and optional memory reasoning |
| `research/` | Historical ticks, source/capture provenance, cutoff selection, replay labels, fixed-position costs, opt-in model replay and durable call journal |
| `trading/` | `SinglePassTradingAgent` (`IGNORE_EVENT`/`HOLD`/`SET_TARGET`/`CLOSE`, `QuoteUnavailableForDecisionError`), `DeterministicTargetValidator`, `DeterministicHardRiskEngine` (`hard-risk-v2`), and `SQLiteShadowLedger` (causal fills, order lifecycle, DST-aware daily close, and deferred protective exits) |
| `runtime.py`, `shadow_cli.py` | `IntradayEventRuntime` (post-LLM market/portfolio refresh, quote-wait validity window vs model retry budget, persistent state machine, durable `runtime_event_bundles` crash recovery), decoupled single-process `ShadowRunner`, and `eventlens shadow` CLI |
| `cli.py`, `*_cli.py` | Grouped dispatcher and existing explicit commands |

Shared modules, sources and trading interfaces do not import research modules. Research
may reuse shared schemas/model/memory logic. Importing a package or requesting top-level
help starts no watcher, database, model or broker. CLI dispatch imports only the selected
command. Two LLM roles can be ordinary isolated calls; no agent platform is needed.

## What was moved

| Former root module | Canonical location |
|---|---|
| `events`, `fed` | `eventlens.sources.<name>` |
| `agent`, `portfolio`, `risk`, `execution` | `eventlens.trading.<name>` |
| `market_data`, `ctrader_ticks`, `market_quality`, `replay_labels` | `eventlens.research.<name>` |
| `research_data`, `research_replay`, `decision_protocol`, `historical_context` | `eventlens.research.<name>` |
| `replay_inference`, `series_replay` | `eventlens.research.<name>` |

Old Python imports of moved modules must be updated; no compatibility module aliases
are installed. The eight older console entrypoints and `python -m` CLI modules keep
their arguments and behavior. The grouped command tree is:

```text
eventlens observe <capture, demo, report or history>
eventlens observe quotes <watch, report or align>
eventlens shadow <start, stop, status, report or recover>
eventlens memory <existing memory arguments>
eventlens source fed <existing collector arguments>
eventlens research accounting <existing eventlens-research arguments>
eventlens research market <existing eventlens-market arguments>
eventlens research labels <existing eventlens-replay arguments>
eventlens research decision <existing eventlens-decision arguments>
eventlens research infer <existing eventlens-infer arguments>
eventlens research series <existing eventlens-series arguments>
```

The observer has a separate SQLite format and does not migrate memory/replay databases.
Its `CaptureSource` boundary returns source observations rather than mislabeling future
calendar schedules as published `Event` objects. Read [observer timing](observer.md). The separate [Demo quote path](live-quotes.md) does not
change existing historical databases or grant execution approval.

Config paths stay under `configs/` to preserve existing commands and local workflows.
The organization step changed no existing database format, prompt, schema, cost/latency
rule or historical experiment. The new observer likewise leaves those contracts unchanged. Proposal schemas and experiment identities
are checked against a pre-move snapshot. Evidence and data stay out of Git.

## Runtime, risk, and shadow accounting rules (`v0.15.3`)

An adapter persists source evidence before inference. Macro releases need a pre-release
forecast snapshot and explicit actual/previous/revised values; zero is a value, not
missing data. Related headlines are grouped without losing their individual provenance
or arrival times. Publisher display time is not assumed to be first public release.

- **Explicit action semantics (`single-pass-agent-v2`)**: Proposals must specify
  `portfolio_action` in `{"IGNORE_EVENT", "HOLD", "SET_TARGET", "CLOSE"}`. Uncertain,
  low-confidence, or irrelevant events map to `IGNORE_EVENT` or `HOLD` (`target=None`) and
  never liquidate an open position. `CLOSE` requires an open position, a verbatim
  `evidence_quote`, and explicit `target_thesis_ids` covering `EUR/USD`.
- **Unified quote quality (`QuoteQualityPolicy`)**: Quotes retain `source_at`, `bridge_at`,
  `received_at`, `available_at`, `source_provider`, and `basis`. Freshness is evaluated
  against `source_at` (`max_quote_age_ms=2000`) while recording `receipt_lag_ms`. Quotes
  with missing/unverified provenance, future timestamps, stale source timestamps, zero
  spread (`allow_zero_spread=False` by default), or spread above `2.0 pips` are rejected
  across model input, hard risk, shadow fills, protective exits, and mark-to-market
  valuation. Live/shadow paths never apply a synthetic spread floor (`spread_floor_pips=0`)
  and never fall back to older valid quotes when the latest quote is invalid.
- **Post-LLM state refresh**: `IntradayEventRuntime` records the pre-LLM `AgentContext`
  for audit, then fetches a fresh `MarketSnapshot` and `PortfolioSnapshot` after the LLM
  call completes. If open positions or pending orders changed during inference, the target
  is rejected (`portfolio_state_changed_during_inference`) without silently rewriting
  `based_on_snapshot_id`.
- **Intraday session & daily loss definition (`hard-risk-v2`)**: Trading days are bounded
  at `17:00 America/New_York` (`session_rollover_utc`, DST-aware via `ZoneInfo`), with a
  daily close cutoff window from `16:55` to `17:00 America/New_York`. Daily net PnL is
  defined as `current_equity_usd - baseline_equity_usd`, which includes realized PnL,
  unrealized mark-to-market PnL (valued at bid for long, ask for short), and cumulative
  transaction fees. Both `baseline_equity_usd` and `loss_limit_triggered` are persisted in
  `shadow_daily_state` across restarts. Risk-increasing actions (opening, adding, or
  reversing a position) are blocked when `kill_switch_active`, `daily_loss_limit_exceeded`,
  `equity_below_minimum_floor`, `unreliable_position_valuation`, `protective_exit_pending`,
  or `daily_close_cutoff_active`, whereas purely risk-reducing actions (`CLOSE` or partial
  reduction) remain permitted.
- **Order lifecycle & deferred protective exits**: Shadow orders track explicit
  `order_status` (`pending`, `filled`, `expired`, `cancelled`, `rejected`) and re-verify
  decision/risk expiry, causal quote ordering (`received_at >= max(decision.completed_at, recorded_at)`),
  kill switch, daily loss limit, and daily close cutoff before filling. If a protective exit
  (stop-loss, daily loss limit, max holding horizon, or daily close) triggers while no valid
  quote is available, `SQLiteShadowLedger` cancels pending risk-increasing orders, persists
  `pending_exit_reason` and `pending_exit_since` on `shadow_account`, emits a
  `protective_exit_deferred_bad_quote` fault record, and executes the exit only when a valid
  quote arrives—never fabricating a fill on an invalid or missing quote.
- **Durable bundle crash recovery (`v0.15.4`, `runtime_event_bundles`)**: Simultaneous macro or
  burst news constituents are atomically bound to their `bundle:...` event before model
  invocation or shadow execution. Post-commit crashes, partial member-marking crashes, process
  restarts, late-arriving siblings, and subsequent `r2` member revisions never regroup already-bound
  constituents into a new bundle or generate duplicate LLM calls, orders, or fills.
- **Quote-waiting vs. model retry separation (`v0.15.4`)**: Missing or stale pre-LLM quotes
  raise `QuoteUnavailableForDecisionError` and hold the event in `awaiting_market_quote` with
  `attempts` unchanged (`increment_attempts=False`) up to `max_quote_wait_age` (`--max-quote-wait-seconds`,
  default `60s`), transitioning to terminal `expired` without model calls if the outage outlasts
  the event's validity window, while model timeouts and invalid outputs count toward
  `max_reasoning_attempts` separately—without relaxing the `2.0s` quote freshness gate or
  fabricating quotes.
- **Decoupled fast/slow stream ingestion (`v0.15.4`)**: `Tier1EnglishSource.poll_due_streams`
  commits and publishes each stream independently upon completion and notifies `ShadowRunner`'s
  decoupled `_event_worker` via wakeup event while `_news_poll_worker`, `_quote_worker`, and
  `_protection_worker` run independently.

Read [shadow acceptance and forward observation](shadow-acceptance.md) for current offline
verification and live forward observation status.

## Preserved research and evidence

`research/decision_protocol.py` still combines the existing proposal contract and pinned
EUR/USD–FOMC v1 rules. A future live schema should be separated deliberately rather than
silently broadening this experiment. No execution approval is granted by schema checks.

Read [memory](memory.md), [reasoning](reasoning.md) and [Fed intake](fed-intake.md) for
existing operations; [research replay](research/research-replay.md),
[historical context](research/historical-context.md),
[series recovery](research/chronological-replay.md) and
[acceptance](research/fomc-series-acceptance.md) for offline behavior. The former README
is retained as a [historical snapshot](research/implementation-history.md).
