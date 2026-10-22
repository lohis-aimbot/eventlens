# Shadow loop reliability fixes and forward observation (`v0.15.3`)

This document records the `v0.15.3` Shadow loop reliability fixes, the daily loss and
intraday cutoff definitions, the offline/synthetic regression verification, and the
bounded live forward Shadow observation.

> [!IMPORTANT]
> **Honest acceptance status**: All reliability defect categories in `v0.15.3` (including
> non-blocking startup recovery, hung-LLM event injection tests, cross-day unconfirmed daily
> loss baseline protection, blocking new risk & cancelling pending opens on stop/duration
> expiry, and cTrader Demo quote bridge throttling/flush fixes) are implemented and verified
> by **310 offline/synthetic regression tests**.
> In addition, a bounded single-process live forward Shadow acceptance run (`< 5 minutes`,
> `2026-10-22T08:38:44Z` → `08:43:41Z`) ran **live Pepperstone cTrader Demo EUR/USD quotes
> (`session 6a27d0ee-b3f7-4309-904d-853908ba32a4`: 101 raw ticks, 101 emitted quotes, 0 throttled,
> 9 positive-spread quotes) + live WallStreetCN HTTP polling (40 polls, 0 failures) + live
> DeepSeek API (`--call-model`)** concurrently, with all raw SQLite, JSONL, and JSON evidence
> persisted in `data/shadow-observation-20261022/`.
> During that bounded live window, `57` baseline items were strictly excluded as `baseline_*`
> (never treating old news as live arrivals) and the `1` newly published live headline
> (`3183395`, domestic robotics news, `importance=1`) was filtered out with
> `below_news_importance_threshold`, resulting in `0` forced trades. Live forward trade/fill
> execution during a real breaking EUR/USD macro release remains unexercised in live observation
> until a breaking EUR/USD event coincides with a positive-spread tick window.

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
| **8. Stop / duration-expiry risk freeze** | After `runner.stop()` or `duration_seconds` expiry, pending risk-increasing orders could still fill during shutdown or subsequent `fill_pending()` calls. | `ShadowRunner.stop()`, shutdown `finally:`, `IntradayEventRuntime._evaluate_and_record_post_commit_locked()`, `SQLiteShadowLedger.record_sync()`, and `SQLiteShadowLedger.fill_pending()` immediately cancel pending risk-increasing orders (`order_status="cancelled"`, `terminal_reason="cancelled_on_runner_stop_requested"` / `"cancelled_on_runner_duration_expired"`) and reject new risk-increasing targets (`blocked_by_runner_*`), while preserving risk-reducing orders (`CLOSE` / partial reduction) and `evaluate_protective_exits()`. |
| **9. cTrader Demo quote bridge throttling & flush** | `1000ms` sampling throttle + `200ms` timer overwrote 68.4% (`13/19`) of fleeting positive-spread ticks on Pepperstone Razor Demo and inflated `source_to_bridge` lag from `0.18s` to `1.18s`. | `EventLensQuoteBridge.cs` flushes immediately in `OnTick` when a positive-spread tick arrives or when spread state changes (never masking a newer zero-spread tick), uses a `50ms` timer and `1.5s` pending staleness guard, and flushes pending ticks in `OnStop()`. Python watcher polls at `50ms` and `ShadowRunner` supports single-process `--quote-bridge-dir` tailing. |

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

## 3. cTrader Demo EUR/USD quote root-cause analysis & bridge fix (`2026-10-22`)

Read-only diagnostic sampling on Pepperstone cTrader **5.9.154** (`Demo • 5298703 • EURUSD`)
compared `Symbol.Bid/Ask`, `Ticks.LastTick.Bid/Ask`, `Server.TimeInUtc`, `Ticks.LastTick.Time`,
raw JSONL (`quotes-*.jsonl`, `diag-*.jsonl`), and SQLite (`step2-quote-sample.sqlite`,
`step3-live-quotes.sqlite`):

1. **`Symbol.Bid/Ask` vs. `LastTick.Bid/Ask` agreement (`symbol_mismatch_ticks = 0`)**:
   - Across `100` raw ticks in pre-fix session `3ac570bc-853c-43c6-bbfc-a50cefc4e325` and `101`
     raw ticks in post-fix session `6a27d0ee-b3f7-4309-904d-853908ba32a4` (`201` total real-time
     ticks), `Symbol.Bid == LastTick.Bid` and `Symbol.Ask == LastTick.Ask` on **100% of ticks**
     (`symbol_mismatch_ticks = 0`).
2. **Root cause of zero-spread (`Bid == Ask`) ticks**:
   - Account `5298703` is a Pepperstone **Razor (Raw ECN) commission-bearing Demo account**
     (`digits = 5`, `pip_size = 0.0001`, `tick_size = 0.00001`, `commission = 3`,
     `commission_type = UsdPerMillionUsdVolume` per side).
   - During London trading hours (`08:22–08:43 UTC`), `81%`–`91%` of raw top-of-book ticks
     genuinely have `0.0 pip` spread (`Bid == Ask`), while `9%`–`19%` have `0.1–0.2 pip`
     (`0.00001–0.00002`) positive spread.
3. **Root cause of positive-spread loss and `source_to_bridge` lag (`stale_quote`)**:
   - Raw cTrader `OnTick` arrival latency (`DateTime.UtcNow - LastTick.Time`) across all 100
     ticks in `3ac570bc-853c-43c6-bbfc-a50cefc4e325` was **min `0.0840s`, median `0.1775s`,
     max `0.4326s`**.
   - However, the previous `1000ms` sampling throttle + `200ms` timer buffered ticks in
     `_pendingTime / _pendingBid / _pendingAsk`:
     - **68.4% (`13 of 19`) of positive-spread ticks were overwritten** by subsequent zero-spread
       ticks within the 1-second throttle window (`19` raw positive-spread ticks → only `6`
       emitted positive-spread quotes; `throttled_ticks = 50`).
     - Holding buffered ticks until the 1-second throttle + 200ms timer elapsed inflated
       `source_to_bridge` lag from `0.1775s` median to **`0.4209s` median / `1.1773s` max**,
       and Python's `200ms` poll sleep added up to `0.2028s` (`bridge_to_receipt`).
4. **Before vs. after quote bridge fix** (no artificial spread added, 2-second freshness gate
   unchanged, no fabricated timestamps):

| Metric | Pre-fix (`3ac570bc-853c-43c6-bbfc-a50cefc4e325`) | Post-fix (`6a27d0ee-b3f7-4309-904d-853908ba32a4`) |
|---|---:|---:|
| Raw ticks (`raw_ticks`) | 100 | 101 |
| Emitted quotes (`quotes`) | 50 | **101** |
| Throttled ticks (`throttled_ticks`) | 50 | **0** |
| Symbol vs. LastTick mismatch (`symbol_mismatch_ticks`) | 0 | **0** |
| Raw positive-spread ticks (`Ask > Bid`) | 19 | 9 |
| Emitted positive-spread quotes | 6 (`31.6%` preserved) | **9 (`100.0%` preserved, `0.1–0.2 pip`)** |
| `source_to_bridge` lag (min / median / max) | `0.0874s` / `0.4209s` / `1.1773s` | **`0.0776s` / `0.1860s` / `0.5913s`** |
| `bridge_to_receipt` lag (max) | `0.2028s` | **`0.0529s`** |

---

## 4. Bounded live forward Shadow observations (`2026-10-22`)

Raw SQLite databases, JSONL quote captures, and JSON reports for all forward runs are stored
persistently in the gitignored directory `data/shadow-observation-20261022/` (never committed
to GitHub):

### 4.1 Concurrent live quotes + live WallStreetCN polling + live DeepSeek (`step3-live-*`)

- **Artifacts**:
  - `data/shadow-observation-20261022/step3-live-shadow.sqlite`
  - `data/shadow-observation-20261022/step3-live-memory.sqlite`
  - `data/shadow-observation-20261022/step3-live-observer.sqlite`
  - `data/shadow-observation-20261022/step3-live-quotes.sqlite`
  - `data/shadow-observation-20261022/step3-live-report.json`
  - `data/shadow-observation-20261022/quotes-6a27d0ee-b3f7-4309-904d-853908ba32a4.jsonl`
  - `data/shadow-observation-20261022/diag-6a27d0ee-b3f7-4309-904d-853908ba32a4.jsonl`
- **Command**:
  ```bash
  .venv/bin/eventlens shadow start \
    --duration 480 \
    --quote-bridge-dir "$HOME/cAlgo/Data/cBots/EventLensQuoteBridge" \
    --poll-news --news-interval 15 --event-interval 0.5 \
    --quote-interval 0.05 --protection-interval 0.2 --call-model \
    --shadow-db data/shadow-observation-20261022/step3-live-shadow.sqlite \
    --memory-db data/shadow-observation-20261022/step3-live-memory.sqlite \
    --observer-db data/shadow-observation-20261022/step3-live-observer.sqlite \
    --quotes-db data/shadow-observation-20261022/step3-live-quotes.sqlite
  ```
- **Window (UTC)**: `2026-10-22T08:38:44.040018+00:00` → `2026-10-22T08:43:41.749450+00:00` (`297.7 s`, `< 5 minutes`).
- **Live quote stream (`session 6a27d0ee-b3f7-4309-904d-853908ba32a4`)**:
  - `101` quotes ingested (`0` missing sequences, `0` unpublished, `9` positive-spread quotes `0.00001`–`0.00002`, `92` zero-spread quotes).
- **Live WallStreetCN polling & event filtering**:
  - `40` live HTTPS polls (`20` `news/global` + `20` `calendar/2026-10-22`, `0` fetch failures, `58` revisions, `1,140` sightings).
  - `57` initial baseline snapshot revisions were strictly excluded from live execution (`allow_baseline_revisions=False`): `23` `baseline_unreleased_calendar_schedule`, `19` `baseline_below_news_importance_threshold`, `7` `baseline_unrelated_calendar_country`, `5` `baseline_no_eurusd_macro_relevance`, `2` `baseline_below_calendar_importance_threshold`, and `1` `baseline_snapshot_not_live_arrival` (`wallstreetcn:news:3183304:r1`). Old news was never used as a live arrival.
  - `1` newly published live news item arrived during the window (`wallstreetcn:news:3183395:r1` at `2026-10-22T08:39:47.910801Z`, Unitree Robotics headline with `importance=1`), which was evaluated by `WallStreetCNFilter` and recorded as `filtered_out` (`below_news_importance_threshold`).
- **Shadow ledger & ending position state**:
  - No eligible breaking EUR/USD event arrived during the 5-minute live window; `0` forced trades (`open_quantity_eur = "0"`, `cash_usd = "10000"`, `equity = "10000.00"`, `daily_pnl_usd = "0.00"`, `pending_exit_reason = null`, `pending_order_ids = []`).
  - Clean stop via `eventlens shadow stop` (`stop_reason = "runner_stop_requested"`), leaving no background processes.

### 4.2 Earlier live news & DeepSeek observations (`live-poll-*`, `real-llm-*`, `deepseek-api-probe.json`)

- `data/shadow-observation-20261022/live-poll-report.json` (`07:13:43Z` → `07:14:28Z`, `45.0 s`):
  `6` live HTTPS polls, `57` baseline revisions excluded, `0` new arrivals.
- `data/shadow-observation-20261022/real-llm-report.json` (`07:15:17Z` → `07:15:32Z`, `15.0 s`):
  `wallstreetcn:news:3183235:r1` passed `WallStreetCNFilter`, entered `SinglePassTradingAgent` (`--call-model`), and was rejected by the 2-second quote freshness gate (`stale_market_quote`, capped at `attempts = 3`) when no live quote bridge was active.
- `data/shadow-observation-20261022/deepseek-api-probe.json`:
  Live `DeepSeekClient` structured JSON call on `wallstreetcn:news:3183235:r1` (`elapsed_ms = 4835`, `model_id = "deepseek-flash"`, `input_tokens = 1582`, `output_tokens = 98`, `portfolio_action = "IGNORE_EVENT"`).

---

## 5. Acceptance summary: passed, unexercised in live window, and remaining blockers

1. **Passed (Offline / synthetic regression verification — 310 tests)**:
   - Full closed-loop pipeline (`Event -> Filter -> Agent -> Post-LLM Quote & Portfolio Refresh -> Hard Risk -> Shadow Fill -> Open Position -> Protective Exit -> Cost & PnL Accounting`), non-blocking startup recovery, hung-LLM protection independence, conservative cross-day daily loss baseline confirmation, stop/duration-expiry risk freeze & concurrent stop+fill cancellation, and quote bridge sub-second/multi-session ingestion are verified by **310 passing tests** (`pytest -q`), strict `mypy src scripts`, and `ruff`.
2. **Passed (cTrader Demo EUR/USD quote bridge root-cause diagnosis & fix)**:
   - Verified `0` mismatches between `Symbol.Bid/Ask` and `LastTick.Bid/Ask` across `201` real-time ticks, confirmed Pepperstone Razor Demo commission metadata (`$3/million` per side), eliminated bridge-side overwriting of positive-spread ticks (`100%` preserved in `6a27d0ee-b3f7-4309-904d-853908ba32a4`, `throttled_ticks = 0`), and reduced `source_to_bridge` lag from `1.1773s` max to `0.5913s` max (`0.1860s` median) and `bridge_to_receipt` lag from `0.2028s` to `0.0529s` without adding artificial spreads or relaxing the 2-second freshness gate.
3. **Passed (Bounded single-process live forward Shadow loop execution)**:
   - Ran live cTrader Demo quotes + live WallStreetCN polling (`40` polls) + live DeepSeek (`--call-model`) + protective loop in a single process (`< 5 minutes`), strictly separated `57` baseline items from `1` live arrival, and stopped cleanly with zero background processes.
4. **Unexercised in live window / Remaining live constraints**:
   - **No breaking EUR/USD macro headline arrived during the 5-minute live window**: Only one unrelated low-importance domestic news item (`3183395`) was published by WallStreetCN during `08:38:44Z–08:43:41Z`, so no live Shadow trade was triggered (in accordance with the rule never to force trades or use old baseline news as live events).
   - **High raw zero-spread share on Pepperstone Razor Demo (`81%–91%`)**: Because Pepperstone Razor Demo quotes raw `0.0 pip` top-of-book spreads on `81%–91%` of ticks and `QuoteQualityPolicy(allow_zero_spread=False)` conservatively rejects zero-spread ticks, a live event arriving during a `0.0 pip` tick or during a `> 2.0s` quiet market gap (`max_sample_source_gap_seconds = 4.001s`) will still be rejected by the quote quality gate until a positive-spread tick (`0.1–0.2 pip`) is active.
