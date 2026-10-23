# Shadow loop reliability fixes and forward observation (`v0.15.3`)

This document records the `v0.15.3` Shadow loop reliability fixes, the daily loss and
intraday cutoff definitions, the offline/synthetic regression verification, and the
bounded live forward Shadow observation.

> [!IMPORTANT]
> **Honest acceptance status**: All reliability defect categories in `v0.15.3` (including
> non-blocking startup recovery, hung-LLM event injection tests, cross-day unconfirmed daily
> loss baseline protection, blocking new risk & cancelling pending opens on stop/duration
> expiry, and cTrader Demo quote bridge throttling/flush + Pepperstone Razor ECN commission-bearing
> zero-spread verification) are implemented and verified by **311 offline/synthetic regression tests**.
> In addition, bounded single-process live forward Shadow acceptance runs on `2026-10-22` and
> `2026-10-23` exercised the **complete real-time closed loop (`Real Event -> DeepSeek -> Hard Risk
> -> Shadow Fill -> Open Position -> Protective Exit -> Cost & PnL Accounting`)** during the live
> October Flash PMI releases for France (`07:15 UTC`) and Germany (`07:30 UTC`), ingesting `1,786`
> real-time Pepperstone Razor EUR/USD quotes (`sessions e98ba345-f77c-4492-89f3-71c3d3dca1b3` and
> `ca74f379-51b5-4b58-b7cb-245a2241069e`), executing `6` live DeepSeek decisions (`memory_v=1..5`),
> filling `2` target short entries (`-892 EUR` at `1.120102` and `-446 EUR` at `1.119292`) and `1`
> automatic timed protective exit (`+892 EUR` at `1.119448`, `protective_max_holding_horizon`,
> locking in `+$0.58` realized PnL / `+$0.52` net after commission), with all raw SQLite, JSONL, and
> JSON evidence persisted in `data/shadow-observation-20261022/` and `data/shadow-observation-20261023/`.

---

## 1. Reliability fixes implemented (`v0.15.3`)

| Area | Previous defect | Implemented fix |
|---|---|---|
| **1. Automated Shadow loop & non-blocking startup recovery** | `IntradayEventRuntime` stopped after recording a pending receipt; `ShadowRunner.run()` awaited `recover_incomplete()` before starting `_protection_worker`, `_quote_worker`, stop control, and `duration_seconds`. | Added `ShadowRunner` (`src/eventlens/runtime.py`) and `eventlens shadow {start,stop,status,report,recover}` (`src/eventlens/shadow_cli.py`). Startup recovery (`recover_incomplete()`) runs inside `_news_and_event_worker()` concurrently with `_protection_worker()`, `_quote_worker()`, stop control, and `duration_seconds`, and enforces event staleness (`max_recovery_event_age_seconds`), retry caps (`max_reasoning_attempts`), LLM timeout (`llm_timeout_seconds`), and cancellation (`stop_event` / `is_stop_requested`). |
| **2. Unified quote quality, clocks & Razor ECN commission verification** | `Quote` dropped `source_at`, `bridge_at`, `available_at`, and session `commission` metadata; `ShadowCostModel` applied a `0.1 pip` synthetic spread floor by default, while `validate_market_quote()` rejected genuine `0.0 pip` Razor ECN quotes without checking the account's commission model. | Extended `Quote` and `QuoteObservation` with `source_at`, `bridge_at`, `received_at`, `available_at`, `source_provider`, `basis`, `commission`, `commission_type`, `effective_source_at`, `receipt_lag_ms`, `has_verified_provenance`, and `has_verified_commission`. `validate_market_quote()`, `align_news()`, and `ShadowCostModel` reject unverified zero-spread quotes (`allow_zero_spread=False` when `commission` is missing/zero) while accepting genuine `0.0 pip` quotes from verified commission-bearing ECN sessions (`commission > 0` and `commission_type == "UsdPerMillionUsdVolume"`, charging explicit commission + slippage with `synthetic_assumption=False`). |
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

### 4.2 Full closed-loop live PMI Shadow acceptance (`2026-10-23`, `data/shadow-observation-20261023/`)

- **Artifacts**:
  - `data/shadow-observation-20261023/pmi-live-shadow.sqlite`
  - `data/shadow-observation-20261023/pmi-live-memory.sqlite`
  - `data/shadow-observation-20261023/pmi-live-observer.sqlite`
  - `data/shadow-observation-20261023/pmi-live-quotes.sqlite`
  - `data/shadow-observation-20261023/pmi-live-report-run1.json`
  - `data/shadow-observation-20261023/pmi-live-report.json`
  - `data/shadow-observation-20261023/quotes-e98ba345-f77c-4492-89f3-71c3d3dca1b3.jsonl`
  - `data/shadow-observation-20261023/quotes-ca74f379-51b5-4b58-b7cb-245a2241069e.jsonl`
- **Windows (UTC)**:
  - **Window A (France October Flash PMI release)**: `2026-10-23T07:14:56Z` → `07:23:46Z` (`530.0 s`, `< 9 minutes`).
  - **Window B (Protective exit + Germany October Flash PMI release)**: `2026-10-23T07:28:50Z` → `07:37:10Z` (`500.0 s`, `< 9 minutes`).
- **Live quote stream (`1,786` real-time quotes across 2 sessions, `0` failures, `0` unpublished)**:
  - `session e98ba345-f77c-4492-89f3-71c3d3dca1b3`: `947` quotes ingested (`658` Razor `0.0 pip` + `289` positive-spread `0.1–0.5 pip`).
  - `session ca74f379-51b5-4b58-b7cb-245a2241069e`: `839` quotes ingested (`594` Razor `0.0 pip` + `245` positive-spread `0.1–0.6 pip`).
- **End-to-end live event → DeepSeek → Hard Risk → Shadow Fill → Position → Protective Exit → Cost/PnL trace**:
  1. **`07:15:18Z` — France October Flash PMI release (`3` live calendar revisions)**:
     - `wallstreetcn:calendar:1596052@1792739700:r2` (France Manufacturing PMI `49.3` vs `50.5` forecast): passed `WallStreetCNFilter`, invoked live DeepSeek (`decision_id = fdcc8f84-ba54-48bc-9058-46ed91c5c607`, `4,650 ms`), committed thesis `22d130e6-2b86-4571-896f-0a2278b83f2f` (`memory_v = 1`), proposed `SET_TARGET` (`weight = -0.10` short EUR/USD), passed `DeterministicHardRiskEngine` (`assessment_id = 18df3992-86d2-4a92-abac-cf89d289c7d0`), and filled `312 ms` later at `07:15:23.706Z` (`fill_id = 023633d8-c710-4e7b-9ae7-ced9953bc306`, **`-892 EUR` at `1.120102`**, `commission_usd = $0.03`, `synthetic_assumption = false`).
     - `wallstreetcn:calendar:1596053@1792739700:r2` (France Services PMI `48.4` vs `51.2`): arrived while the first order was still pending during those `312 ms`; safely rejected with `Cannot validate target while pending orders are outstanding`, preventing duplicate exposure.
     - `wallstreetcn:calendar:1596054@1792739700:r2` (France Composite PMI `48.4` vs `50.3`): DeepSeek observed the open `-892 EUR` short position in `shadow-snapshot-1`, advanced thesis to `revision = 2` (`memory_v = 2`), and selected `portfolio_action = "HOLD"` (`no_target`).
  2. **`07:30:23.722Z` — Automatic timed protective exit (`protective_max_holding_horizon`)**:
     - Exactly `900.0 s` after opening at `07:15:23.706Z`, `_protection_worker` triggered `protective_max_holding_horizon` and filled `+892 EUR` at `1.119448` (`fill_id = b3df9ebc-867b-4ab1-b545-55ec005d62ed`, `new_quantity = 0`, `commission_usd = $0.03`, **`realized_pnl_usd = +$0.58`**, **`equity_after_usd = $10,000.52`**).
  3. **`07:30:26Z` — Germany October Flash PMI release (`3` live calendar revisions)**:
     - `wallstreetcn:calendar:1596055@1792740600:r2` (Germany Manufacturing PMI beat): DeepSeek recorded contradicting evidence (`memory_v = 3`), lowered thesis confidence, and chose `HOLD` (stay flat).
     - `wallstreetcn:calendar:1596056@1792740600:r2` (Germany Services PMI miss): DeepSeek updated thesis (`memory_v = 4`) and remained flat (`HOLD`).
     - `wallstreetcn:calendar:1596057@1792740600:r2` (Germany Composite PMI miss): DeepSeek updated thesis (`memory_v = 5`), proposed a smaller `SET_TARGET` (`weight = -0.05` short EUR/USD), passed `DeterministicHardRiskEngine`, and filled at `07:30:40.306Z` (`fill_id = 6385ca26-e1b5-45d9-bca6-f6e50149c032`, **`-446 EUR` at `1.119292`**, `commission_usd = $0.01`).
- **Ending ledger & accounting state (`2026-10-23T07:37:10Z`)**:
  - `cash_usd = "10000.51"`, `realized_pnl_usd = "0.58"`, `total_fees_usd = "0.07"`, `unrealized_pnl_usd = "-0.16"`, `equity = "10000.35"`, `daily_pnl_usd = "0.35"`.

### 4.3 Earlier live news & DeepSeek observations (`live-poll-*`, `real-llm-*`, `deepseek-api-probe.json`)

- `data/shadow-observation-20261022/live-poll-report.json` (`07:13:43Z` → `07:14:28Z`, `45.0 s`):
  `6` live HTTPS polls, `57` baseline revisions excluded, `0` new arrivals.
- `data/shadow-observation-20261022/real-llm-report.json` (`07:15:17Z` → `07:15:32Z`, `15.0 s`):
  `wallstreetcn:news:3183235:r1` passed `WallStreetCNFilter`, entered `SinglePassTradingAgent` (`--call-model`), and was rejected by the 2-second quote freshness gate (`stale_market_quote`, capped at `attempts = 3`) when no live quote bridge was active.
- `data/shadow-observation-20261022/deepseek-api-probe.json`:
  Live `DeepSeekClient` structured JSON call on `wallstreetcn:news:3183235:r1` (`elapsed_ms = 4835`, `model_id = "deepseek-flash"`, `input_tokens = 1582`, `output_tokens = 98`, `portfolio_action = "IGNORE_EVENT"`).

---

## 5. Acceptance summary: passed items and operational characteristics

1. **Passed (Offline / synthetic regression verification — 311 tests)**:
   - Full closed-loop pipeline (`Event -> Filter -> Agent -> Post-LLM Quote & Portfolio Refresh -> Hard Risk -> Shadow Fill -> Open Position -> Protective Exit -> Cost & PnL Accounting`), non-blocking startup recovery, hung-LLM protection independence, conservative cross-day daily loss baseline confirmation, stop/duration-expiry risk freeze & concurrent stop+fill cancellation, and Pepperstone Razor commission-bearing ECN `0.0 pip` quote verification are verified by **311 passing tests** (`pytest -q`), strict `mypy src scripts`, and `ruff`.
2. **Passed (cTrader Demo EUR/USD quote bridge root-cause diagnosis & fix)**:
   - Verified `0` mismatches between `Symbol.Bid/Ask` and `LastTick.Bid/Ask` across real-time ticks, confirmed Pepperstone Razor Demo commission metadata (`commission = 3`, `commission_type = UsdPerMillionUsdVolume` per side), propagated verified session commission metadata into `Quote` (`has_verified_commission`), eliminated bridge-side tick throttling, and reduced `source_to_bridge` lag to `0.1860s` median and `bridge_to_receipt` lag to `0.0529s` max without adding artificial spreads or relaxing the 2-second freshness gate.
3. **Passed (Bounded single-process live forward Shadow closed-loop acceptance)**:
   - Verified the complete real-time closed loop (`Real Event -> DeepSeek -> Hard Risk -> Shadow Fill -> Open Position -> Protective Exit -> Cost & PnL Accounting`) during the `2026-10-23` France (`07:15 UTC`) and Germany (`07:30 UTC`) October Flash PMI releases, with all raw SQLite, JSONL, and JSON evidence saved in `data/shadow-observation-20261022/` and `data/shadow-observation-20261023/`.
4. **Operational characteristics observed in live runs**:
   - **Quiet market inter-tick gaps (`> 2.0s`)**: Because `EventLensQuoteBridge` emits only on real market ticks (never fabricating heartbeat quotes) and the 2-second quote freshness gate (`max_quote_age_ms = 2000`) is strictly enforced, protective monitor ticks during quiet market intervals (`> 2.0s` without a new price tick) log `protective_monitor_quote_unavailable` and resume immediately when the next real tick arrives.
