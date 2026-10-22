# Shadow loop reliability fixes and forward observation (`v0.15.3`)

This document records the `v0.15.3` Shadow loop reliability fixes, the daily loss and
intraday cutoff definitions, the offline/synthetic regression verification, and the
bounded live forward Shadow observation.

> [!IMPORTANT]
> **Honest acceptance status**: All eight reliability defect categories in `v0.15.3`
> (plus the three follow-up audit fixes for non-blocking startup recovery, hung-LLM
> event injection tests, and cross-day unconfirmed daily loss baseline protection) are
> fixed and verified by **308 offline/synthetic regression tests**, and the
> single-process automated `ShadowRunner` (`eventlens shadow`) has been verified in
> bounded live forward observations against public WallStreetCN news/calendar endpoints
> and the live DeepSeek API (`DEEPSEEK_API_KEY` via local gitignored `.env`), with raw
> SQLite and JSON artifacts persisted in `data/shadow-observation-20261022/`.
> **However, live forward Shadow fill acceptance is NOT yet complete** because no live
> positive-spread Demo EUR/USD quote bridge (`CTRADER_*`) was actively streaming during
> the observation window (historical local `data/live-quotes.sqlite` samples ended on
> `2026-10-15T05:58:10Z` and had `29/33` zero-spread ticks, causing the un-relaxed 2-second
> quote freshness gate to reject stale quotes with `stale_market_quote` and `0` fills).
> Stage 5 cTrader Demo order execution remains blocked until live forward Shadow acceptance
> passes under a concurrent live positive-spread Demo quote stream and live arrivals.

---

## 1. Reliability fixes implemented (`v0.15.3`)

| Area | Previous defect | Implemented fix |
|---|---|---|
| **1. Automated Shadow loop & non-blocking startup recovery** | `IntradayEventRuntime` stopped after recording a pending receipt; `ShadowRunner.run()` awaited `recover_incomplete()` before starting `_protection_worker`, `_quote_worker`, stop control, and `duration_seconds`. | Added `ShadowRunner` (`src/eventlens/runtime.py`) and `eventlens shadow {start,stop,status,report,recover}` (`src/eventlens/shadow_cli.py`). Startup recovery (`recover_incomplete()`) runs inside `_news_and_event_worker()` concurrently with `_protection_worker()`, `_quote_worker()`, stop control, and `duration_seconds`, and enforces event staleness (`max_recovery_event_age_seconds`), retry caps (`max_reasoning_attempts`), LLM timeout (`llm_timeout_seconds`), and cancellation (`stop_event` / `is_stop_requested`). |
| **2. Unified quote quality & clocks** | `Quote` dropped `source_at`, `bridge_at`, and `available_at`; `RiskPolicy` allowed zero spread (`allow_zero_spread=True`); `ShadowCostModel` applied a `0.1 pip` synthetic spread floor by default. | Extended `Quote` with `source_at`, `bridge_at`, `received_at`, `available_at`, `source_provider`, `basis`, `effective_source_at`, `receipt_lag_ms`, and `has_verified_provenance`. Added unified `QuoteQualityPolicy` and `validate_market_quote()` (`allow_zero_spread=False` by default, `max_quote_age_ms=2000` based on `source_at`, `max_spread_pips=2.0`). Disabled default synthetic spread floor (`spread_floor_pips=0`, `allow_synthetic_spread_floor=False`). |
| **3. Post-LLM state refresh** | `IntradayEventRuntime` reused the pre-LLM `MarketSnapshot` and `PortfolioSnapshot` after multi-second LLM calls, causing valid proposals to fail the 2s quote freshness gate. | Preserved pre-LLM `AgentContext` in `runtime_events.context_json` for audit, refreshed `MarketSnapshot` and `PortfolioSnapshot` after `agent.decide()` returns, and rejected proposals with `portfolio_state_changed_during_inference` if open positions or pending orders changed during inference (never silently rewriting `based_on_snapshot_id`). |
| **4. Pending order expiry & pre-fill re-verification** | `fill_pending()` did not re-check decision/risk expiry, kill switch, daily loss limit, or causal quote ordering, and used `filled=1` to mask cancelled orders. | Added `order_status` (`pending`, `filled`, `expired`, `cancelled`, `rejected`), `terminal_at`, and `terminal_reason` to `shadow_receipts`. `fill_pending()` expires stale orders first, enforces causal quote ordering (`quote.received_at >= max(decision.completed_at, recorded_at)` and `quote.effective_source_at >= decision.completed_at`), and re-verifies `based_on_snapshot_id`, `kill_switch_active`, `daily_loss_limit`, `daily_baseline_valid`, `min_equity_usd`, `max_position_weight`, and `daily_close_cutoff`. |
| **5. Explicit action semantics** | `target_direction="flat"` generated a liquidation target even when uncertain or abstaining; fallback `next(iter(effective))` attached arbitrary thesis IDs to closes. | Added `portfolio_action` (`IGNORE_EVENT`, `HOLD`, `SET_TARGET`, `CLOSE`) to `SinglePassProposal` (`single-pass-agent-v2`) and `AgentDecision`. `IGNORE_EVENT` and `HOLD` always produce `target=None`. Removed arbitrary thesis fallback; `CLOSE` requires an open position, verbatim `evidence_quote`, and explicit EUR/USD `target_thesis_ids`. |
| **6. Intraday risk, DST-aware daily close & conservative daily baseline confirmation** | Only a static `min_equity_usd` floor existed; daily close checked `hour == 21` in UTC without DST awareness; cross-day rollover with an open position and missing quotes wrote raw `cash_usd` as a confirmed baseline. | Added `shadow_daily_state` tracking `trading_day`, `session_start_utc`, `daily_close_utc` (`16:55 America/New_York`), `session_rollover_utc` (`17:00 America/New_York`), `baseline_equity_usd`, `min_equity_seen_usd`, `loss_limit_triggered`, `baseline_valid`, and `baseline_reason`, plus `last_valid_equity_usd` on `shadow_account`. When rolling over with an open position and no valid quote, `baseline_valid=0` (`unconfirmed_open_position_without_valid_quote`) blocks risk-increasing actions (`daily_baseline_unconfirmed`) and carries a provisional reference $\max(\text{last\_valid\_equity\_usd}, \text{prior\_baseline\_equity\_usd})$; when valid quotes recover, the baseline is confirmed conservatively via $\max(\text{provisional}, \text{carried\_ref}, \text{current\_equity})$ so losses during the outage are never masked. |
| **7. Persistent deduplication, recovery & audit** | Event deduplication used only in-memory `_seen`; transient failures could not be retried; crash after `memory.commit()` left unrecorded shadow orders. | Added `SQLiteRuntimeStore` (`runtime_events`, `runtime_audit_log`, `shadow_runner_state`). Non-terminal `reasoning_failed` unmarks `WallStreetCNFilter` so transient failures retry up to `max_reasoning_attempts`. `recover_incomplete()` reconciles `memory_committed` decisions after a crash and enforces staleness (`recovery_event_stale`) and retry limits (`recovery_max_attempts_exceeded`) on pre-commit events. All SQLite stores use `RLock` + `check_same_thread=False`. |

---

## 2. Intraday risk, daily loss, and bad-quote protection definitions

1. **Trading session & daily close boundary (`America/New_York`, DST-aware)**:
   - Each FX trading day runs from `17:00 America/New_York` (`session_start_utc`) to
     `17:00 America/New_York` the next calendar day (`session_rollover_utc`), using
     `zoneinfo.ZoneInfo("America/New_York")` (`21:00 UTC` in EDT, `22:00 UTC` in EST).
   - The **daily close cutoff window** runs from `16:55 America/New_York` (`daily_close_utc`)
     to `17:00 America/New_York`. During this window (or if a position remains open from a
     prior session whose `daily_close_utc` was missed due to downtime or bad quotes),
     `evaluate_protective_exits()` triggers `protective_daily_close` and new risk-increasing
     entries are blocked (`daily_close_cutoff_active`).
2. **Daily loss calculation (`max_daily_loss_usd = 200 USD`)**:
   - At the start of each `trading_day`, `baseline_equity_usd` is persisted in
     `shadow_daily_state`.
   - Current equity is computed from the latest valid quote (bid for long, ask for short):
     $$\text{Current Equity USD} = \text{Cash USD} + \text{Unrealized Mark-to-Market PnL USD}$$
     where $\text{Cash USD}$ already incorporates realized PnL minus cumulative transaction
     fees (`commission_bps` + `slippage_bps` + `fixed_fee_usd`).
   - Daily net PnL is:
     $$\text{Daily Net PnL USD} = \text{Current Equity USD} - \text{Baseline Equity USD}$$
     It therefore **includes realized PnL, unrealized mark-to-market PnL, and transaction fees**.
   - When $\text{Daily Net PnL USD} \le -\text{max\_daily\_loss\_usd}$ (`-200 USD`),
     `loss_limit_triggered = 1` is durably persisted in `shadow_daily_state` across process
     restarts, protective exit `protective_daily_loss_limit` is triggered for any open
     position, and all subsequent risk-increasing targets in that trading day are rejected.
   - **Cross-day rollover with open position & missing quotes (`daily_baseline_valid`)**:
     - If a new `trading_day` (`17:00 America/New_York`) begins while `quantity_eur != 0` and no
       valid quote is available (`valuation_valid = False`), raw `cash_usd` is **never** written
       as a confirmed baseline. Instead, `shadow_daily_state` sets `baseline_valid = 0`,
       `baseline_reason = "unconfirmed_open_position_without_valid_quote"`, and carries a
       provisional reference $\text{carried\_ref} = \max(\text{last\_valid\_equity\_usd}, \text{prior\_baseline\_equity\_usd})$.
     - While `daily_baseline_valid = False`, `DeterministicHardRiskEngine` rejects all
       risk-increasing actions with `daily_baseline_unconfirmed` (and `fill_pending()` rejects
       risk-increasing fills with `daily_baseline_unconfirmed_at_fill`), while still allowing
       risk-reducing `CLOSE` / partial reduction.
     - Once a valid quote restores reliable mark-to-market valuation (`valuation_valid = True`),
       the day's baseline is confirmed conservatively:
       $$\text{Confirmed Baseline Equity USD} = \max(\text{provisional\_baseline}, \text{carried\_ref}, \text{current\_equity\_usd})$$
       with `baseline_valid = 1` and `baseline_reason = "confirmed_after_quote_recovery_conservative"`.
       This guarantees that a lower post-recovery equity can never mask losses incurred prior to
       or during the quote outage.
3. **Unreliable valuation & deferred protective exits**:
   - When a position is open (`quantity_eur != 0`) and the latest quote is missing, stale,
     future-dated, zero-spread, or wide-spread, `PortfolioSnapshot.valuation_valid` is set to
     `False` and `DeterministicHardRiskEngine` rejects any risk-increasing proposal with
     `unreliable_position_valuation` (never treating unrealized loss as zero).
   - When a protective exit (`protective_stop_loss`, `protective_daily_loss_limit`,
     `protective_max_holding_horizon`, or `protective_daily_close`) triggers while no valid
     quote is available, `SQLiteShadowLedger`:
     1. Cancels all pending risk-increasing orders (`cancelled_by_protective_exit`).
     2. Persists `pending_exit_reason` and `pending_exit_since` on `shadow_account`.
     3. Records a `protective_exit_deferred_bad_quote` fault in `shadow_faults`.
     4. Blocks all new risk-increasing proposals (`protective_exit_pending`).
     5. Executes the protective exit fill only when a valid causal quote arrives.

---

## 3. Bounded live forward Shadow observations (`2026-10-22`)

Raw SQLite databases and JSON reports for the latest forward runs are stored persistently in
the gitignored directory `data/shadow-observation-20261022/` (never committed to GitHub):

- `data/shadow-observation-20261022/live-poll-shadow.sqlite3`, `live-poll-memory.sqlite3`,
  `live-poll-observer.sqlite3`, `live-poll-report.json`:
  - Command:
    ```bash
    .venv/bin/eventlens shadow \
      --shadow-db data/shadow-observation-20261022/live-poll-shadow.sqlite3 \
      --memory-db data/shadow-observation-20261022/live-poll-memory.sqlite3 \
      --observer-db data/shadow-observation-20261022/live-poll-observer.sqlite3 \
      start --duration 45 --poll-news --call-model \
      --news-interval 15 --event-interval 1 --quote-interval 1 --protection-interval 1
    ```
  - Window (UTC): `2026-10-22T07:13:43.829194+00:00` → `2026-10-22T07:14:28.832478+00:00` (`45.0 s`).
  - Result: `6` live HTTPS polls (`3` `news/global` + `3` `calendar/2026-10-22`, `0` failures),
    `57` baseline revisions classified and excluded from live execution (`allow_baseline_revisions=False`,
    including `1` `baseline_snapshot_not_live_arrival`), `0` new arrivals during the 45s window,
    clean shutdown at `45.0 s`.
- `data/shadow-observation-20261022/real-llm-shadow.sqlite3`, `real-llm-memory.sqlite3`,
  `real-llm-observer.sqlite3`, `real-llm-report.json`:
  - Command:
    ```bash
    .venv/bin/eventlens shadow \
      --shadow-db data/shadow-observation-20261022/real-llm-shadow.sqlite3 \
      --memory-db data/shadow-observation-20261022/real-llm-memory.sqlite3 \
      --observer-db data/shadow-observation-20261022/real-llm-observer.sqlite3 \
      start --duration 15 --call-model --allow-baseline \
      --event-interval 1 --quote-interval 1 --protection-interval 1
    ```
  - Window (UTC): `2026-10-22T07:15:17.743719+00:00` → `2026-10-22T07:15:32.748275+00:00` (`15.0 s`).
  - Result: `56` irrelevant/schedule items filtered out; `1` real WallStreetCN EUR/USD macro
    revision (`wallstreetcn:news:3183235:r1`) accepted by `WallStreetCNFilter`, entered
    `SinglePassTradingAgent` (`--call-model`), and was rejected by the un-relaxed 2-second quote
    freshness gate (`ValueError: Missing or stale EUR/USD market quote for agent decision: stale_market_quote`,
    capped at `attempts = 3`) because `data/live-quotes.sqlite` had no live cTrader quote bridge
    running (`last_received_at = 2026-10-15T05:58:10.313021Z`). `0` fills fabricated.
- `data/shadow-observation-20261022/deepseek-api-probe.json`:
  - Live `DeepSeekClient.from_environment().complete_json()` + `SinglePassProposal` schema
    verification on `wallstreetcn:news:3183235:r1`: `elapsed_ms = 4835`,
    `model_id = "deepseek-flash"`, `input_tokens = 1582`, `output_tokens = 98`,
    `portfolio_action = "IGNORE_EVENT"`.

### 3.1 Earlier 15-minute live news polling observation (`900.0 s`)

- **Started at (UTC)**: `2026-10-22T05:36:39.297247+00:00` (`2026-10-22T13:36:39+08:00`)
- **Stopped at (UTC)**: `2026-10-22T05:51:39.302365+00:00` (`2026-10-22T13:51:39+08:00`)
- **Duration**: `900.0 s` (`60` polls: `30` `news/global` + `30` `calendar/2026-10-22`, `0` failures, `57` baseline revisions excluded, `6` live arrivals observed and classified).
- **Quote quality (`data/live-quotes.sqlite`)**: `33` stored Demo quotes (`29` zero-spread, `4` positive-spread `0.00001`), last received `2026-10-15T05:58:10.313021Z`. Under `QuoteQualityPolicy(max_quote_age_ms=2000, allow_zero_spread=False)`, `0` fresh positive-spread quotes were available during the `2026-10-22` window.
- **Shadow ledger & ending position state**:
  - `receipt_count = 0`, `fill_count = 0`, `fault_count = 0`, `order_status_counts = {}`.
  - `open_quantity_eur = "0"`, `cash_usd = "10000"`, `equity = "10000.00"`, `daily_pnl_usd = "0.00"`, `daily_loss_limit_triggered = false`, `pending_exit_reason = null`, `pending_order_ids = []`.

---

## 4. Acceptance summary: passed vs. still blocked

1. **Passed (Offline / synthetic regression verification)**:
   - All 8 reliability requirements (`v0.15.3`) plus the 3 follow-up audit fixes (non-blocking startup recovery with staleness/retry/timeout/cancellation controls, real event injection into hung-LLM tests, and conservative cross-day daily loss baseline confirmation) are implemented and covered by **308 passing offline tests** (`pytest -q`), strict type checking (`mypy src scripts`), and lint/format checks (`ruff check .`, `ruff format --check .`).
2. **Passed (Bounded live forward observation — infrastructure, news polling & DeepSeek API)**:
   - Single-process `ShadowRunner` (`eventlens shadow start/status/report/stop/recover`) ran with live WallStreetCN news & calendar polling (`0` failures), separated baseline snapshot items from live arrivals, verified live DeepSeek JSON structured output (`single-pass-agent-v2`), enforced the un-relaxed 2-second positive-spread quote gate (`stale_market_quote`, `attempts=3`), and saved persistent evidence in `data/shadow-observation-20261022/`.
3. **Not yet passed / Blocked (Live forward Shadow fill acceptance)**:
   - **Blocker — Live positive-spread Demo quotes**: No cTrader Demo bridge (`CTRADER_*`) was actively streaming fresh quotes during the `2026-10-22` window, and the prior `2026-10-15` session had `29/33` (`87.9%`) zero-spread ticks. Under `allow_zero_spread=False` and `max_quote_age_ms=2000`, live Shadow orders cannot be approved or filled without an active positive-spread Demo quote stream.
   - **Conclusion**: **Live forward Shadow trade/fill acceptance could not be completed in this run.** Stage 5 cTrader Demo order execution remains blocked until a bounded forward run with a live positive-spread cTrader Demo quote stream completes.
