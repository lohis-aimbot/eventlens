# Shadow loop reliability fixes and forward observation (`v0.15.3`)

This document records the `v0.15.3` Shadow loop reliability fixes, the daily loss and
intraday cutoff definitions, the offline/synthetic regression verification, and the
bounded live forward Shadow observation.

> [!IMPORTANT]
> **Honest acceptance status**: All reliability defect categories in `v0.15.3` (including
> non-blocking startup recovery, hung-LLM event injection tests, cross-day unconfirmed daily
> loss baseline protection, blocking new risk & cancelling pending opens on stop/duration
> expiry, cTrader Demo quote bridge throttling/flush + Pepperstone Razor ECN commission-bearing
> zero-spread verification, and the `pepperstone_razor_200` `$200 USD / 200x` leverage / `0.01..100`
> lot standard) are implemented and verified by **315 offline/synthetic regression tests**.
> In addition, bounded single-process live forward Shadow acceptance runs on `2026-10-22` and
> `2026-10-23` exercised the **complete real-time closed loop (`Real Event -> DeepSeek -> Hard Risk
> -> Shadow Fill -> Open Position -> Protective Exit -> Cost & PnL Accounting`)** during the live
> October Flash PMI releases for France (`07:15 UTC`) and Germany (`07:30 UTC`), ingesting `1,786`
> real-time Pepperstone Razor EUR/USD quotes (`sessions e98ba345-f77c-4492-89f3-71c3d3dca1b3` and
> `ca74f379-51b5-4b58-b7cb-245a2241069e`), executing `6` live DeepSeek decisions (`memory_v=1..5`),
> and locking in `+6.54 pips` on the France PMI short (`+$0.58` realized PnL on 1x unleveraged
> `-892 EUR`, or **`+$2.62` realized PnL / `+$2.36` net after commission (`+1.18%` account return in
> 15 minutes) on `-0.04 lots` (`-4,000 EUR`) under the `pepperstone_razor_200` `$200 USD / 200x`
> standard**), with all raw SQLite, JSONL, and JSON evidence persisted in
> `data/shadow-observation-20261022/` and `data/shadow-observation-20261023/`.

---

## 1. Reliability fixes implemented (`v0.15.3`)

| Area | Previous defect | Implemented fix |
|---|---|---|
| **1. Automated Shadow loop & non-blocking startup recovery** | `IntradayEventRuntime` stopped after recording a pending receipt; `ShadowRunner.run()` awaited `recover_incomplete()` before starting `_protection_worker`, `_quote_worker`, stop control, and `duration_seconds`. | Added `ShadowRunner` (`src/eventlens/runtime.py`) and `eventlens shadow {start,stop,status,report,recover}` (`src/eventlens/shadow_cli.py`). Startup recovery (`recover_incomplete()`) runs inside `_news_and_event_worker()` concurrently with `_protection_worker()`, `_quote_worker()`, stop control, and `duration_seconds`, and enforces event staleness (`max_recovery_event_age_seconds`), retry caps (`max_reasoning_attempts`), LLM timeout (`llm_timeout_seconds`), and cancellation (`stop_event` / `is_stop_requested`). |
| **2. Unified quote quality, clocks & explicit Razor ECN zero-spread exception policy** | `Quote` dropped `source_at`, `bridge_at`, `available_at`, and session `commission` metadata; `ShadowCostModel` applied a `0.1 pip` synthetic spread floor by default, while `validate_market_quote()` rejected genuine `0.0 pip` Razor ECN quotes without checking the account's commission model. | Extended `Quote` and `QuoteObservation` with `source_at`, `bridge_at`, `received_at`, `available_at`, `source_provider`, `basis`, `commission`, `commission_type`, `effective_source_at`, `receipt_lag_ms`, `has_verified_provenance`, and `has_verified_commission`. Zero-spread quotes (`Bid == Ask`) are rejected by default (`allow_commission_bearing_zero_spread=False`) and accepted **only** when explicitly opted in by policy (`allow_commission_bearing_zero_spread=True`) **and** originating from an allowlisted verified source (`source_provider == "ctrader_algo"`, `basis == "live_poll"`, full clock provenance, `commission > 0`, `commission_type == "UsdPerMillionUsdVolume"`). Arbitrary sources are never trusted solely because `commission > 0`. Raw `Bid/Ask` (`raw_bid`, `raw_ask`) are preserved on every `ShadowFill` without artificial spread padding or relaxed freshness gates (`max_quote_age_ms = 2000`). |
| **3. Post-LLM state refresh** | `IntradayEventRuntime` reused the pre-LLM `MarketSnapshot` and `PortfolioSnapshot` after multi-second LLM calls, causing valid proposals to fail the 2s quote freshness gate. | Preserved pre-LLM `AgentContext` in `runtime_events.context_json` for audit, refreshed `MarketSnapshot` and `PortfolioSnapshot` after `agent.decide()` returns, and rejected proposals with `portfolio_state_changed_during_inference` if open positions or pending orders changed during inference (never silently rewriting `based_on_snapshot_id`). |
| **4. Pending order expiry & pre-fill re-verification** | `fill_pending()` did not re-check decision/risk expiry, kill switch, daily loss limit, or causal quote ordering, and used `filled=1` to mask cancelled orders. | Added `order_status` (`pending`, `filled`, `expired`, `cancelled`, `rejected`), `terminal_at`, and `terminal_reason` to `shadow_receipts`. `fill_pending()` expires stale orders first, enforces causal quote ordering (`quote.received_at >= max(decision.completed_at, recorded_at)` and `quote.effective_source_at >= decision.completed_at`), and re-verifies `based_on_snapshot_id`, `kill_switch_active`, `daily_loss_limit`, `daily_baseline_valid`, `min_equity_usd`, `max_position_weight`, and `daily_close_cutoff`. |
| **5. Explicit action semantics** | `target_direction="flat"` generated a liquidation target even when uncertain or abstaining; fallback `next(iter(effective))` attached arbitrary thesis IDs to closes. | Added `portfolio_action` (`IGNORE_EVENT`, `HOLD`, `SET_TARGET`, `CLOSE`) to `SinglePassProposal` (`single-pass-agent-v2`) and `AgentDecision`. `IGNORE_EVENT` and `HOLD` always produce `target=None`. Removed arbitrary thesis fallback; `CLOSE` requires an open position, verbatim `evidence_quote`, and explicit EUR/USD `target_thesis_ids`. |
| **6. Intraday risk, DST-aware daily close & conservative daily baseline confirmation** | Only a static `min_equity_usd` floor existed; daily close checked `hour == 21` in UTC without DST awareness; cross-day rollover with an open position and missing quotes wrote raw `cash_usd` as a confirmed baseline. | Added `shadow_daily_state` tracking `trading_day`, `session_start_utc`, `daily_close_utc` (`16:55 America/New_York`), `session_rollover_utc` (`17:00 America/New_York`), `baseline_equity_usd`, `min_equity_seen_usd`, `loss_limit_triggered`, `baseline_valid`, and `baseline_reason`, plus `last_valid_equity_usd` on `shadow_account`. When rolling over with an open position and no valid quote, `baseline_valid=0` (`unconfirmed_open_position_without_valid_quote`) blocks risk-increasing actions (`daily_baseline_unconfirmed`) and carries a provisional reference $\max(\text{last\_valid\_equity\_usd}, \text{prior\_baseline\_equity\_usd})$; when valid quotes recover, the baseline is confirmed conservatively via $\max(\text{provisional}, \text{carried\_ref}, \text{current\_equity})$ so losses during the outage are never masked. |
| **7. Persistent deduplication, recovery & audit** | Event deduplication used only in-memory `_seen`; transient failures could not be retried; crash after `memory.commit()` left unrecorded shadow orders. | Added `SQLiteRuntimeStore` (`runtime_events`, `runtime_audit_log`, `shadow_runner_state`). Non-terminal `reasoning_failed` unmarks `WallStreetCNFilter` so transient failures retry up to `max_reasoning_attempts`. `recover_incomplete()` reconciles `memory_committed` decisions after a crash and enforces staleness (`recovery_event_stale`) and retry limits (`recovery_max_attempts_exceeded`) on pre-commit events. All SQLite stores use `RLock` + `check_same_thread=False`. |
| **8. Two-phase stop/drain, remaining window coverage & unclosed-risk reporting** | After `runner.stop()` or `duration_seconds` expiry, all workers cancelled simultaneously and new risk could be opened near cutoff when remaining runtime/quote-bridge duration was shorter than `max_holding_seconds + exit_buffer_seconds`. | Implemented two-phase stop (`running -> stopping -> draining -> stopped`): `_news_and_event_worker` stops first and risk-increasing pending orders are cancelled, while `_quote_worker` and `_protection_worker` continue running until the position exits (`flat_completed`), explicit handoff is confirmed (`handoff_confirmed_open_position`), or `exit_drain_timeout_seconds` expires (`unclosed_risk=true`, `shutdown_status="unclosed_risk_requires_recovery"`, emitting explicit `alerts` and `recovery_instructions`). Near run or quote-bridge cutoff (`remaining < max_holding_seconds + exit_buffer_seconds`), new risk-increasing entries are blocked (`insufficient_remaining_run_window_for_new_risk`, `insufficient_quote_bridge_window_for_new_risk`). |
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

4. **Explicit zero-spread exception policy & cost provenance**:
   - **Default rejection (`allow_commission_bearing_zero_spread = False`)**: By default across
     `QuoteQualityPolicy`, `RiskPolicy`, `ProtectiveExitPolicy`, `SinglePassTradingAgent`,
     `align_news()`, and `ShadowCostModel`, zero-spread quotes (`Bid == Ask`) are rejected
     (`zero_spread_rejected`), even if `commission > 0`.
   - **Allowlisted verified opt-in (`allow_commission_bearing_zero_spread = True`)**:
     Zero-spread quotes are accepted **only** when explicitly enabled in policy **and** the quote
     satisfies all verified provenance gates:
     1. `source_provider in TRUSTED_COMMISSION_ZERO_SPREAD_PROVIDERS` (`{"ctrader_algo"}`).
     2. `basis == "live_poll"` with full causal clock provenance (`source_at <= bridge_at <= received_at`).
     3. `commission > 0` and `commission_type in TRUSTED_COMMISSION_TYPES` (`{"UsdPerMillionUsdVolume"}`).
     Arbitrary or untrusted data sources are **never** accepted solely because `commission > 0`.
   - **Raw `Bid/Ask` preservation**: Raw `Bid` and `Ask` (`raw_bid`, `raw_ask`) are recorded on
     every `ShadowFill` without adding artificial spreads or relaxing the `2.0s` freshness gate.
   - **Distinction between platform metadata and unverified shadow assumptions**:
     - **Verified platform metadata (`Symbol.Commission`, `Symbol.CommissionType`)**:
       Reported by Pepperstone cTrader (`commission = 3`, `commission_type = "UsdPerMillionUsdVolume"`).
       Literal API formula: $\text{broker\_fee} = |\text{units\_eur}| \times \text{fill\_price} \times \frac{\text{commission}}{1{,}000{,}000}$.
     - **Conservative shadow commission floor (`commission_bps = 0.30`)**:
       Matches Pepperstone Razor's published retail ECN schedule (`$3.00` per `100,000` lot per side,
       or `0.30 bps`). Effective fee is $\max(\text{broker\_fee}, \text{bps\_fee}) + \text{fixed\_fee\_usd}$,
       with `commission_source` explicitly tagged on every fill as
       `"broker_metadata_usd_per_million"`, `"broker_metadata_verified_conservative_bps_floor"`, or
       `"unverified_model_commission_bps"`.
     - **Unverified shadow slippage assumption (`slippage_bps = 0.25`)**:
       Applies `0.25 bps` (`0.025 pip`) adverse price slippage to every shadow fill and is explicitly
       marked on every `ShadowFill` with `slippage_bps_applied = 0.25` and
       `slippage_assumption_unverified = True`.

---

## 3. cTrader Demo EUR/USD quote root-cause analysis & bridge fix (`2026-10-22`)

Read-only diagnostic sampling on Pepperstone cTrader **5.9.154** (`Demo • ***8703 • EURUSD`)
compared `Symbol.Bid/Ask`, `Ticks.LastTick.Bid/Ask`, `Server.TimeInUtc`, `Ticks.LastTick.Time`,
raw JSONL (`quotes-*.jsonl`, `diag-*.jsonl`), and SQLite (`step2-quote-sample.sqlite`,
`step3-live-quotes.sqlite`):

1. **`Symbol.Bid/Ask` vs. `LastTick.Bid/Ask` agreement (`symbol_mismatch_ticks = 0`)**:
   - Across `100` raw ticks in pre-fix session `3ac570bc-853c-43c6-bbfc-a50cefc4e325` and `101`
     raw ticks in post-fix session `6a27d0ee-b3f7-4309-904d-853908ba32a4` (`201` total real-time
     ticks), `Symbol.Bid == LastTick.Bid` and `Symbol.Ask == LastTick.Ask` on **100% of ticks**
     (`symbol_mismatch_ticks = 0`).
2. **Root cause of zero-spread (`Bid == Ask`) ticks**:
   - Account `***8703` is a Pepperstone **Razor (Raw ECN) commission-bearing Demo account**
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
  - **1x Unleveraged (`pmi-live-report.json`, `$10,000` initial cash)**:
    `cash_usd = "10000.51"`, `realized_pnl_usd = "0.58"`, `total_fees_usd = "0.07"`, `unrealized_pnl_usd = "-0.16"`, `equity = "10000.35"`, `daily_pnl_usd = "0.35"`.
  - **Pepperstone Razor `$200 USD / 200x` Standard (`pmi-live-pepperstone-200-report.json`, `--account-profile pepperstone_razor_200`)**:
    - Fill #1 (`07:15:23.706Z`, France Manufacturing PMI `weight = -0.10`): **`-0.04 lots` (`-4,000 EUR`)** at `1.120102`, `commission_usd = $0.13`, `margin_used_usd = $22.40` (`11.2%` of `$200`).
    - Fill #2 (`07:30:23.722Z`, 15m `protective_max_holding_horizon`): **`+0.04 lots` (`+4,000 EUR`)** at `1.119448`, `commission_usd = $0.13`, **`realized_pnl_usd = +$2.62`**, **`equity_after_usd = $202.36`** (**`+1.18%` net return on `$200` in 15 minutes**).
    - Fill #3 (`07:30:40.306Z`, Germany Composite PMI `weight = -0.05`): **`-0.02 lots` (`-2,000 EUR`)** at `1.119292`, `commission_usd = $0.07`, `margin_used_usd = $11.20`, `free_margin_usd = $190.43`.

### 4.3 Pepperstone Razor `$200 USD / 200x` Testing Workflow & Risk-Controlled Profit Standard (`--account-profile pepperstone_razor_200`)

`eventlens shadow` defaults to `--account-profile pepperstone_razor_200` (`RiskPolicy.pepperstone_razor_200()` + `ProtectiveExitPolicy.pepperstone_razor_200()`), aligning Shadow execution and accounting directly with the Pepperstone Razor Demo account (`***8703`):

| Parameter | Standard Value | Purpose (Profit Maximization + Risk Control) |
|---|---|---|
| `initial_cash_usd` | **`$200.00 USD`** | Exact initial Demo account equity |
| `leverage` | **`200` (`200:1`)** | `$40,000 USD` nominal purchasing power at `$200` equity |
| `min_volume_eur` / `volume_step_eur` | **`1,000 EUR` (`0.01 lot`)** | Minimum trade size & lot increment (`ROUND_HALF_UP` on valid signals; non-zero signals clamp to `>= 0.01 lot`) |
| `max_volume_eur` | **`10,000,000 EUR` (`100 lots`)** | Broker maximum trade size |
| `max_position_weight` / `max_gross_weight` | **`0.25` (`25%` margin cap)** | Hard risk cap at `0.08 lots` (`8,000 EUR`, `~$44.80` margin), preserving **`>= 75%` free margin** |
| `max_position_loss_usd` | **`$4.00 USD` (`2%` equity)** | Single-trade deterministic stop-loss (`protective_stop_loss`, ~`10 pips` on `0.04 lots`) |
| `take_profit_usd` | **`$8.00 USD` (`4%` equity)** | Single-trade deterministic take-profit (`protective_take_profit`, `2:1` reward-to-risk, ~`20 pips` on `0.04 lots`) |
| `max_holding_seconds` | **`900 s` (`15 minutes`)** | Captures the primary post-release macro impulse and exits before subsequent data releases |
| `max_daily_loss_usd` | **`$8.00 USD` (`4%` equity)** | Daily circuit breaker (`protective_daily_loss_limit`, halts new risk after 2 full stop-outs) |
| `min_equity_usd` | **`$180.00 USD` (`10%` max DD)** | Hard account equity floor (`equity_below_minimum_floor`) |

Standard live test command around scheduled tier-1 EUR/USD or USD releases (with `duration >= max_holding_seconds + exit_buffer_seconds`):
```bash
.venv/bin/eventlens shadow start \
  --account-profile pepperstone_razor_200 \
  --duration 1500 \
  --close-on-stop \
  --quote-bridge-dir "$HOME/cAlgo/Data/cBots/EventLensQuoteBridge" \
  --poll-news --news-interval 10 --event-interval 0.5 \
  --quote-interval 0.05 --protection-interval 0.2 --call-model
```

### 4.4 Earlier live news & DeepSeek observations (`live-poll-*`, `real-llm-*`, `deepseek-api-probe.json`)

- `data/shadow-observation-20261022/live-poll-report.json` (`07:13:43Z` → `07:14:28Z`, `45.0 s`):
  `6` live HTTPS polls, `57` baseline revisions excluded, `0` new arrivals.
- `data/shadow-observation-20261022/real-llm-report.json` (`07:15:17Z` → `07:15:32Z`, `15.0 s`):
  `wallstreetcn:news:3183235:r1` passed `WallStreetCNFilter`, entered `SinglePassTradingAgent` (`--call-model`), and was rejected by the 2-second quote freshness gate (`stale_market_quote`, capped at `attempts = 3`) when no live quote bridge was active.
- `data/shadow-observation-20261022/deepseek-api-probe.json`:
  Live `DeepSeekClient` structured JSON call on `wallstreetcn:news:3183235:r1` (`elapsed_ms = 4835`, `model_id = "deepseek-flash"`, `input_tokens = 1582`, `output_tokens = 98`, `portfolio_action = "IGNORE_EVENT"`).

### 4.5 Residual Position Live Recovery & Continuous Uninterrupted Shadow Acceptance (`2026-10-27`, `data/shadow-observation-20261027/`)

All pre-recovery backups, recovery receipts, post-recovery ledger reports, raw cTrader JSONL quote files, and continuous Shadow SQLite/JSON artifacts are persisted under gitignored `data/shadow-observation-20261027/`.

#### A. Live-quote recovery & complete reconciliation of `2026-10-23` residual open positions (`09:30:48 UTC`)

Both residual short `EUR/USD` positions left open at the end of `2026-10-23` Window B (`07:37:10Z`) were recovered via `eventlens shadow ... recover --quote-bridge-dir "$HOME/cAlgo/Data/cBots/EventLensQuoteBridge" --wait-for-quote-seconds 20` against fresh live quotes from cTrader Demo (`***8703`) session `a6b89e7e-0353-4cf0-b12e-484fe1283615` (`09:28:12Z` → `09:38:12Z`, `868` emitted quotes). No historical prices were backfilled.

| Ledger & Profile | Trade # | Entry (`UTC`, qty, price, fee) | Exit (`UTC`, reason, raw `Bid/Ask`, fill price, fee) | Realized PnL | Ending Reconciliation |
|---|---:|---|---|---:|---|
| `pmi-live-shadow.sqlite` (`institutional_10k`, `$10,000`, `1x`) | #1 | `2026-10-23T07:15:23.706Z`, `-892 EUR` @ `1.120102`, `$0.03` | `2026-10-23T07:30:23.722Z` (`protective_max_holding_horizon`), `+892 EUR` @ `1.119448`, `$0.03` | `+$0.58` | `cash = equity = $10,002.85` (`$10,000.00 + $2.93 - $0.08`), `open_qty = 0`, `unclosed_risk = false` |
| `pmi-live-shadow.sqlite` (`institutional_10k`, `$10,000`, `1x`) | #2 (recovered) | `2026-10-23T07:30:40.306Z`, `-446 EUR` @ `1.119292`, `$0.01` | `2026-10-27T09:30:48.419Z` (`protective_max_holding_horizon`), `1.114 / 1.114`, `+446 EUR` @ `1.114028`, `$0.01` (`broker_metadata_verified_conservative_bps_floor`, `0.25 bps` unverified slippage) | `+$2.35` | (see above) |
| `pmi-live-pepperstone-200-shadow.sqlite` (`pepperstone_razor_200`, `$200`, `200x`) | #1 | `2026-10-23T07:15:23.706Z`, `-4,000 EUR` (`-0.04 lots`) @ `1.120102`, `$0.13` | `2026-10-23T07:30:23.722Z` (`protective_max_holding_horizon`), `+4,000 EUR` @ `1.119448`, `$0.13` | `+$2.62` | `cash = equity = $212.71` (`$200.00 + $13.11 - $0.40`), `open_qty = 0`, `unclosed_risk = false` |
| `pmi-live-pepperstone-200-shadow.sqlite` (`pepperstone_razor_200`, `$200`, `200x`) | #2 (recovered) | `2026-10-23T07:30:40.306Z`, `-2,000 EUR` (`-0.02 lots`) @ `1.119292`, `$0.07` | `2026-10-27T09:30:48.632Z` (`protective_take_profit`), `1.11402 / 1.11402`, `+2,000 EUR` @ `1.114048`, `$0.07` (`broker_metadata_verified_conservative_bps_floor`, `0.25 bps` unverified slippage) | `+$10.49` | (see above) |

> **Scope note:** Both ledgers represent the same two `2026-10-23` PMI decisions under two sizing profiles and must never be summed as four independent live trades. Trade #2's gain between `2026-10-23T07:30Z` and `2026-10-27T09:30Z` occurred across an offline multi-day gap while no runner was active; it validates restart/recovery accounting on fresh quotes rather than uninterrupted intraday protection or strategy alpha.

#### B. Uninterrupted `2,240.0 s` (`37m 20s`) continuous live forward Shadow acceptance (`12:25:16Z` → `13:02:37Z`)

- **Preliminary `355.0 s` live check (`09:31:54Z` → `09:37:49Z`, `continuous-600s-*`)**: surfaced a `1 ms` wall-clock ordering issue in `ShadowRunner.step_protection()` where `now` was sampled before `_current_market()`, causing `protection_coverage` to classify newly polled snapshots as `future_market_snapshot`. Fixed in commit `817e333` with regression test `test_step_protection_coverage_with_advancing_wall_clock_does_not_flag_future_snapshot`.
- **Primary uninterrupted window (`continuous-*`)**:
  - **Runtime window (UTC)**: `2026-10-27T12:25:16.985627+00:00` → `2026-10-27T13:02:37.015550+00:00` (`duration_seconds = 2240.0`, **`2.33×`** `max_holding_seconds (900s) + exit_buffer_seconds (60s) = 960s`), spanning both the `12:30 UTC` US September Durable Goods Orders and `13:00 UTC` US August FHFA / S&P Case-Shiller Home Price Index releases.
  - **Quote bridge session (`874420b1-2c2a-4898-9b4f-ec81932d84b1`)**: `12:23:31.241Z` → `13:03:31.275Z` (`duration_seconds = 2400`, `3,804` quotes emitted from `3,849` raw ticks, `0` symbol mismatches; `3,517` live quotes ingested during the runner window: `2,670` verified commission-bearing zero-spread + `847` positive-spread `0.1–0.7 pips`, `0` rejected, `0` failures, `max_bridge_to_receipt_seconds = 0.0716s`).
  - **Protection coverage (`protection_coverage`)**: `10,761` total checks (`200 ms` cadence), **`9,653` valid fresh-quote checks (`89.7%`)**, `1,108` quiet-market `stale_market_quote` / startup checks across `154` brief `>2.0 s` inter-tick pauses (`max_outage_seconds = 9.0208`), and **`unavailable_checks_while_position_open = 0`** (`0.0 s` unprotected open-position gap).
  - **Observer & full event accounting (`84` events persisted)**:
    - `411` live WallStreetCN polls (`84` revisions, `10,862` sightings, `1` transient `ConnectTimeout` auto-recovered on the next cycle).
    - `77` `filtered_out`: `53` initial baseline records (`21` `baseline_below_news_importance_threshold`, `20` `baseline_unreleased_calendar_schedule`, `6` `baseline_unrelated_calendar_country`, `4` `baseline_no_eurusd_macro_relevance`, `1` `baseline_below_calendar_importance_threshold`, `1` `baseline_snapshot_not_live_arrival`) plus `24` live low-importance / non-EUR/USD arrivals (`17` `below_news_importance_threshold`, `7` `no_eurusd_macro_relevance`).
    - `5` live `SinglePassTradingAgent` (`deepseek-flash@bc19ba97cbe016479903222bb1578db6`, `10,794` input / `595` output tokens, `receipt_to_decision_ms = [5337, 7225, 6217, 4441, 7868]`) evaluations committed across `memory_version 1..5`, all returning `IGNORE_EVENT` (`no_target`): forecast-bearing US macro prints landed near consensus (`US Durable Goods 1.8% vs 1.9% forecast`, `S&P/CS YoY 2.65% vs 2.6% forecast`, `S&P/CS MoM 0.27% vs 0.3% forecast`), `FHFA MoM` matched its prior reading with consensus expectation missing in the source feed (`0.3% actual vs 0.3% previous`, `reported_forecast = null`), and corporate news (`Corning capex guidance`) was unrelated to `EUR/USD`. No trades were forced.
    - `2` companion `12:30 UTC` Durable Goods revisions (`1596090@1793104200:r2` ex-transport `0.5% vs 0.6%` and `1596091@1793104200:r2` core capital goods `0.5% vs 0.7%`) arrived right as a `>2.0 s` inter-tick pause occurred (`12:30:29.75Z–12:30:36.51Z`) and failed closed (`reasoning_failed`: `Missing or stale EUR/USD market quote for agent decision: stale_market_quote`, `attempts = 3`, `0` model tokens spent, `0` fabricated quotes).
  - **Two-phase shutdown & ending state**: `stop_reason = "runner_duration_expired"`, `shutdown_status = "flat_completed"`, `event_worker_stopped_before_drain = true`, `quote_worker_alive_during_drain = true`, `protection_worker_alive_during_drain = true`, `open_quantity_eur = "0"`, `pending_order_ids = []`, `pending_exit_reason = null`, `unclosed_risk = false`, `equity = "$200.00"`, and `0` lingering background processes.

---

## 5. Acceptance summary: passed items and operational characteristics

1. **Passed (Offline / synthetic regression verification — 316 tests)**:
   - Full closed-loop pipeline (`Event -> Filter -> Agent -> Post-LLM Quote & Portfolio Refresh -> Hard Risk -> Shadow Fill -> Open Position -> Protective Exit -> Cost & PnL Accounting`), non-blocking startup recovery, hung-LLM protection independence, conservative cross-day daily loss baseline confirmation, two-phase stop/drain shutdown (`running -> stopping -> draining -> stopped`), pre-cutoff remaining run & quote-bridge window blocking (`max_holding_seconds + exit_buffer_seconds`), quote outage alert & unclosed-risk reporting, advancing wall-clock protection coverage tracking, explicit zero-spread exception policy (`allow_commission_bearing_zero_spread` opt-in + allowlisted `ctrader_algo` provenance), and the `pepperstone_razor_200` (`$200 USD / 200x / 0.01..100 lot / $4 SL / $8 TP / 900s`) sizing & protection standard are verified by **316 passing tests** (`pytest -q`), strict `mypy src scripts`, and `ruff`.
2. **Passed (cTrader Demo EUR/USD quote bridge root-cause diagnosis & fix)**:
   - Verified `0` mismatches between `Symbol.Bid/Ask` and `LastTick.Bid/Ask` across real-time ticks, confirmed Pepperstone Razor Demo commission metadata (`commission = 3`, `commission_type = UsdPerMillionUsdVolume` per side), propagated verified session commission metadata into `Quote` (`has_verified_commission`), extended bridge/CLI duration bounds up to `86,400 s`, eliminated bridge-side tick throttling, and reduced `source_to_bridge` lag to `0.1860s` median and `bridge_to_receipt` lag to `0.0529s–0.0716s` max without adding artificial spreads or relaxing the 2-second freshness gate.
3. **Passed (Residual position live-quote recovery, complete PnL reconciliation, and uninterrupted `2,240 s` continuous Shadow acceptance)**:
   - Closed both `2026-10-23` residual open positions on fresh `2026-10-27` live quotes with exact cent-level reconciliation (`unclosed_risk = false`), and completed an uninterrupted `2,240.0 s` (`2.33×` `max_holding_seconds + exit_buffer_seconds`) continuous live Shadow session across two US macro release windows (`3,517` live quotes, `9,653` valid protection checks, `0` unprotected open-position gaps, `84` persisted events, clean two-phase `flat_completed` shutdown).
4. **Operational characteristics & remaining boundaries**:
   - **Quiet market inter-tick gaps (`> 2.0s`)**: Because `EventLensQuoteBridge` emits only on real market ticks (never fabricating heartbeat quotes) and the 2-second quote freshness gate (`max_quote_age_ms = 2000`) is strictly enforced, quiet intervals (`> 2.0s` without a new tick) defer LLM evaluation (`stale_market_quote`) and log `unavailable_quote_checks` until the next real tick arrives.
   - **No live fill occurred inside the `2026-10-27` `2,240 s` window because US macro arrivals lacked actionable surprise** (forecast-bearing releases landed near consensus and `FHFA MoM` matched prior `0.3%` with `reported_forecast = null`): All `5` live model decisions returned `IGNORE_EVENT` (`0` forced entries). Continuous open-to-exit holding within a single uninterrupted window is verified deterministically in the offline regression suite, while live entry/exit fills were observed in the `2026-10-23` PMI sessions and `2026-10-27` live-quote recovery. Profitability and broker-order execution readiness remain separate future milestones.
