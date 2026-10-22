# Shadow loop reliability fixes and forward observation (`v0.15.3`)

This document records the `v0.15.3` Shadow loop reliability fixes, the daily loss and
intraday cutoff definitions, the offline/synthetic regression verification, and the
bounded live forward Shadow observation.

> [!IMPORTANT]
> **Honest acceptance status**: All eight reliability defect categories in `v0.15.3`
> are fixed and verified by **306 offline/synthetic regression tests**, and the
> single-process automated `ShadowRunner` (`eventlens shadow`) has been verified in a
> bounded 15-minute (`900.0 s`) live forward observation against public WallStreetCN
> news/calendar endpoints. **However, live forward Shadow fill acceptance is NOT yet
> complete** because (1) no live positive-spread Demo EUR/USD quote bridge (`CTRADER_*`)
> was streaming during the observation window (historical local `data/live-quotes.sqlite`
> samples ended on `2026-10-15T05:58:10Z` and had `29/33` zero-spread ticks), and
> (2) `DEEPSEEK_API_KEY` was not configured in the local shell environment. Stage 5
> cTrader Demo order execution remains blocked until live forward Shadow acceptance
> passes under fresh positive-spread Demo quotes and live LLM inference.

---

## 1. Reliability fixes implemented (`v0.15.3`)

| Area | Previous defect | Implemented fix |
|---|---|---|
| **1. Automated Shadow loop** | `IntradayEventRuntime` stopped after recording a pending receipt; `fill_pending()` and `evaluate_protective_exits()` were only called manually by tests. | Added `ShadowRunner` (`src/eventlens/runtime.py`) and `eventlens shadow {start,stop,status,report,recover}` (`src/eventlens/shadow_cli.py`) running three concurrent single-process loops (news/event cycle, quote/fill/valuation cycle, independent protection timer) with hard `duration_seconds` cutoff and clean shutdown. |
| **2. Unified quote quality & clocks** | `Quote` dropped `source_at`, `bridge_at`, and `available_at`; `RiskPolicy` allowed zero spread (`allow_zero_spread=True`); `ShadowCostModel` applied a `0.1 pip` synthetic spread floor by default. | Extended `Quote` with `source_at`, `bridge_at`, `received_at`, `available_at`, `source_provider`, `basis`, `effective_source_at`, `receipt_lag_ms`, and `has_verified_provenance`. Added unified `QuoteQualityPolicy` and `validate_market_quote()` (`allow_zero_spread=False` by default, `max_quote_age_ms=2000` based on `source_at`, `max_spread_pips=2.0`). Disabled default synthetic spread floor (`spread_floor_pips=0`, `allow_synthetic_spread_floor=False`). |
| **3. Post-LLM state refresh** | `IntradayEventRuntime` reused the pre-LLM `MarketSnapshot` and `PortfolioSnapshot` after multi-second LLM calls, causing valid proposals to fail the 2s quote freshness gate. | Preserved pre-LLM `AgentContext` in `runtime_events.context_json` for audit, refreshed `MarketSnapshot` and `PortfolioSnapshot` after `agent.decide()` returns, and rejected proposals with `portfolio_state_changed_during_inference` if open positions or pending orders changed during inference (never silently rewriting `based_on_snapshot_id`). |
| **4. Pending order expiry & pre-fill re-verification** | `fill_pending()` did not re-check decision/risk expiry, kill switch, daily loss limit, or causal quote ordering, and used `filled=1` to mask cancelled orders. | Added `order_status` (`pending`, `filled`, `expired`, `cancelled`, `rejected`), `terminal_at`, and `terminal_reason` to `shadow_receipts`. `fill_pending()` expires stale orders first, enforces causal quote ordering (`quote.received_at >= max(decision.completed_at, recorded_at)` and `quote.effective_source_at >= decision.completed_at`), and re-verifies `based_on_snapshot_id`, `kill_switch_active`, `daily_loss_limit`, `min_equity_usd`, `max_position_weight`, and `daily_close_cutoff`. |
| **5. Explicit action semantics** | `target_direction="flat"` generated a liquidation target even when uncertain or abstaining; fallback `next(iter(effective))` attached arbitrary thesis IDs to closes. | Added `portfolio_action` (`IGNORE_EVENT`, `HOLD`, `SET_TARGET`, `CLOSE`) to `SinglePassProposal` (`single-pass-agent-v2`) and `AgentDecision`. `IGNORE_EVENT` and `HOLD` always produce `target=None`. Removed arbitrary thesis fallback; `CLOSE` requires an open position, verbatim `evidence_quote`, and explicit EUR/USD `target_thesis_ids`. |
| **6. Intraday risk & DST-aware daily close** | Only a static `min_equity_usd` floor existed; daily close checked `hour == 21` in UTC without timezone/DST awareness or missed-cutoff handling. | Added `shadow_daily_state` tracking `trading_day`, `session_start_utc`, `daily_close_utc` (`16:55 America/New_York`), `session_rollover_utc` (`17:00 America/New_York`), `baseline_equity_usd`, `min_equity_seen_usd`, and `loss_limit_triggered`. `DeterministicHardRiskEngine` blocks risk-increasing actions (open, add, reversal) when kill switch, daily loss limit, unreliable valuation, pending exit, or daily close window is active, while permitting risk-reducing actions (`CLOSE` or partial reduction). |
| **7. Persistent deduplication, recovery & audit** | Event deduplication used only in-memory `_seen`; transient failures could not be retried; crash after `memory.commit()` left unrecorded shadow orders. | Added `SQLiteRuntimeStore` (`runtime_events`, `runtime_audit_log`, `shadow_runner_state`). Non-terminal `reasoning_failed` unmarks `WallStreetCNFilter` so transient failures retry. `recover_incomplete()` reconciles `memory_committed` decisions after a crash against current market/portfolio state and expiry. All SQLite stores use `RLock` + `check_same_thread=False`. |

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

## 3. Bounded live forward Shadow observation (`2026-10-22`)

### 3.1 Command and observation window

```bash
.venv/bin/eventlens shadow start \
  --duration 900 \
  --poll-news \
  --news-interval 30 \
  --event-interval 1 \
  --quote-interval 1 \
  --protection-interval 1 \
  --shadow-db /tmp/eventlens-forward-obs-20261021/shadow.sqlite3 \
  --memory-db /tmp/eventlens-forward-obs-20261021/memory.sqlite3 \
  --observer-db /tmp/eventlens-forward-obs-20261021/observer.sqlite3 \
  --quotes-db data/live-quotes.sqlite
```

- **Started at (UTC)**: `2026-10-22T05:36:39.297247+00:00` (`2026-10-22T13:36:39+08:00`)
- **Stopped at (UTC)**: `2026-10-22T05:51:39.302365+00:00` (`2026-10-22T13:51:39+08:00`)
- **Duration**: `900.0 s` (15 minutes, exited cleanly with `status="stopped"`, 0 leftover background tasks)
- **Environment**:
  - `DEEPSEEK_API_KEY`: Not present in environment (in accordance with security rules, no keys were scraped from chat history or clipboard; `--call-model` was not enabled).
  - `data/live-quotes.sqlite`: Existing local Demo quote store (`33` historical quotes from session `d90a02d3-6d25-4b30-964a-248a752b10f9` between `2026-10-15T05:57:12Z` and `2026-10-15T05:58:10Z`, with `29/33` zero-spread quotes and `4/33` positive-spread quotes; no active cTrader bridge process writing fresh quotes on `2026-10-22`).
  - Public WallStreetCN live polling (`--poll-news`): Active over HTTPS every 30 seconds (`allow_baseline_revisions=False` so initial page baseline items are never mislabeled as live breaking arrivals).

### 3.2 News capture and filtering summary

- **Polls executed**: `60` (`30` `news/global` polls + `30` `calendar/2026-10-22` polls), `0` failures, `0` unpublished polls, `1,710` total sightings across `63` unique revisions.
- **Initial baseline snapshot (`57` revisions)**: `25` news + `32` calendar items present at `t=0` were classified and excluded from live execution (`allow_baseline_revisions=False`):
  - `baseline_unreleased_calendar_schedule`: `25`
  - `baseline_below_news_importance_threshold`: `15`
  - `baseline_no_eurusd_macro_relevance`: `7`
  - `baseline_unrelated_calendar_country`: `7`
  - `baseline_snapshot_not_live_arrival` (would have matched filter if live): `2`
  - `baseline_schedule_reminder_digest`: `1`
- **Live arrivals during the 15-minute window (`6` revisions)**:
  1. `wallstreetcn:news:3183201:r1` (`received_at=2026-10-22T05:41:46.735022Z`, importance `2`, KOSPI equity move) → `filtered_out` (`no_eurusd_macro_relevance`).
  2. `wallstreetcn:news:3183206:r1` (`received_at=2026-10-22T05:45:50.948552Z`, importance `1`, Middle East geopolitical headline) → `filtered_out` (`below_news_importance_threshold`).
  3. `wallstreetcn:news:3183194:r2` (`received_at=2026-10-22T05:46:21.652513Z`, importance `2`, A-share index revision) → `filtered_out` (`no_eurusd_macro_relevance`).
  4. `wallstreetcn:news:3183207:r1` (`received_at=2026-10-22T05:46:21.652513Z`, importance `1`, A-share PCB sector) → `filtered_out` (`below_news_importance_threshold`).
  5. `wallstreetcn:news:3183209:r1` (`received_at=2026-10-22T05:47:53.435617Z`, importance `2`, `"美元兑日元下跌0.5%至157.34。"`) → **Accepted by `WallStreetCNFilter`**, persisted in `raw_events`, assembled context (`memory_v=0`, `pre_snap=shadow-snapshot-0`), and recorded `reasoning_failed` (`RuntimeError: Model calls are disabled; pass --call-model to enable DeepSeek reasoning`) because `DEEPSEEK_API_KEY` was not configured in the environment.
  6. `wallstreetcn:news:3183211:r1` (`received_at=2026-10-22T05:49:55.679915Z`, importance `2`, STAR 50 index move) → `filtered_out` (`no_eurusd_macro_relevance`).

### 3.3 Quote quality, model calls, latency, and final portfolio state

- **Quote quality (`data/live-quotes.sqlite`)**: `33` stored Demo quotes (`29` zero-spread, `4` positive-spread `0.00001`), last received `2026-10-15T05:58:10.313021Z`. Under `QuoteQualityPolicy(max_quote_age_ms=2000, allow_zero_spread=False)`, `0` fresh positive-spread quotes were available during the `2026-10-22` window.
- **Model calls & tokens**: `model_calls = 0`, `total_input_tokens = 0`, `total_output_tokens = 0`.
- **Latency (`receipt_to_decision_ms` / `receipt_to_fill_ms`)**: `0` live samples (`receipt_to_decision_samples = 0`, `receipt_to_fill_samples = 0`).
- **Shadow ledger & ending position state**:
  - `receipt_count = 0`, `fill_count = 0`, `fault_count = 0`, `order_status_counts = {}`.
  - `open_quantity_eur = "0"`, `cash_usd = "10000"`, `equity = "10000.00"`, `daily_pnl_usd = "0.00"`, `daily_loss_limit_triggered = false`, `pending_exit_reason = null`, `pending_order_ids = []`.

---

## 4. Acceptance summary: passed vs. still blocked

1. **Passed (Offline / synthetic regression verification)**:
   - All 8 reliability requirements (`v0.15.3`) are implemented and covered by **306 passing offline tests** (`pytest -q`), strict type checking (`mypy src scripts`), and lint/format checks (`ruff check .`, `ruff format --check .`).
2. **Passed (Bounded live forward observation — infrastructure & source loop)**:
   - Single-process `ShadowRunner` (`eventlens shadow start/status/report/stop/recover`) ran for 15 minutes (`900.0 s`) with live WallStreetCN news & calendar polling (`60` polls, `0` failures), separated baseline snapshot items (`57`) from live arrivals (`6`), persisted event states and audit records in SQLite without thread/lock conflicts, and exited cleanly at the duration limit with zero background processes left behind.
3. **Not yet passed / Blocked (Live forward Shadow fill acceptance)**:
   - **Blocker 1 — Live positive-spread Demo quotes**: No cTrader Demo bridge was actively streaming fresh quotes during the `2026-10-22` window, and the prior `2026-10-15` session had `29/33` (`87.9%`) zero-spread ticks. Under `allow_zero_spread=False` and `max_quote_age_ms=2000`, live Shadow orders cannot be approved or filled without an active positive-spread Demo quote stream.
   - **Blocker 2 — Environment `DEEPSEEK_API_KEY`**: `DEEPSEEK_API_KEY` was not set in the shell environment during the forward run (`--call-model` requires `DEEPSEEK_API_KEY` in `os.environ`).
   - **Conclusion**: **Live forward Shadow trade/fill acceptance could not be completed in this run.** Stage 5 cTrader Demo order execution remains blocked until a bounded forward run with both `DEEPSEEK_API_KEY` and a live positive-spread cTrader Demo quote stream completes.
