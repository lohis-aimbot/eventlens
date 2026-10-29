# Errata & Quarantine Notice: October 28, 2026 FOMC Ad-Hoc Replay

> [!WARNING]
> **Quarantine Status**: The ad-hoc local replay script (`scratch/fomc_oct28_replay.py`) and its local SQLite outputs (`scratch/fomc_oct28_run/{shadow,memory,observer}.sqlite`) are **preserved unmodified** as raw audit artifacts, and are **permanently excluded from live forward Shadow acceptance evidence**.

## 1. Identified Provenance & Methodology Defects in `scratch/fomc_oct28_replay.py`

1. **Historical 1-minute bars mislabeled as verified live quotes**:
   - The ad-hoc script fetched Yahoo Finance 1-minute `EURUSD=X` OHLC bars, added a fixed `0.2`-pip symmetric spread (`half_spread = 0.00001`) around the bar Open price, fabricated millisecond clocks (`source_at = ts - 40ms`, `bridge_at = ts - 20ms`), and tagged every quote with `source_provider="ctrader_algo"` and `basis="live_poll"`.
   - As a result, `ShadowFill.synthetic_assumption` evaluated to `false`, and 1-minute bar Open prices up to `59s` old were refreshed into apparent `40ms`-fresh live quotes to bypass the `2,000ms` quote freshness gate.
2. **Sub-minute execution and intrabar protection claims unsupported by 1-minute bars**:
   - One-minute OHLC bars do not contain tick arrival times, spread widening during FOMC releases, or intrabar High/Low ordering. Any second-level latency, fill-price, or intrabar stop-loss/trailing-stop ordering conclusion from 1-minute bars is **not evaluable (`not_evaluable_from_minute_bars` / 不可评估)**.
3. **Post-hoc text & manual `trim_before_marker` slicing do not exclude lookahead bias**:
   - Both the Federal Reserve statement and the `investingLive` Warsh press-conference articles were fetched post-hoc on `2026-10-29`, and the intermediate `18:39:05Z` Q&A snapshot was constructed manually via `trim_before_marker`. Without contemporaneous capture hashes and availability timestamps from `2026-10-28`, lookahead bias **cannot** be claimed excluded (`lookahead_not_excluded_post_hoc_text`).
4. **Manually assigned arrival timestamps**:
   - Event receipt times (`18:00:05Z`, `18:34:05Z`, `18:39:05Z`, `18:55:05Z`) were investigator assumptions rather than observed feed receipts.

## 2. Remediations Implemented in Repository

- **Immutable historical files**: Neither `scratch/fomc_oct28_replay.py` nor `scratch/fomc_oct28_run/*` nor `data/*` was modified or overwritten.
- **Contract-level provenance separation (`src/eventlens/contracts.py`, `src/eventlens/trading/execution.py`)**:
  - Added `basis="historical_bar_replay"`, `bar_interval_seconds`, `synthetic_spread_applied`, and `simulated_receipt` to `Quote`.
  - `Quote` validation forbids combining `basis="live_poll"` with historical bars, synthetic spreads, or simulated receipts, and `Quote.has_verified_provenance` returns `False` for all non-verified quotes.
  - `SQLiteShadowLedger` sets `ShadowFill.synthetic_assumption = True` whenever `synthetic_used` OR `not quote.has_verified_provenance`.
- **Repository tooling & pinned assumptions (`src/eventlens/replay_provenance.py`, `configs/fomc_2026_10_28_replay_assumptions.json`, `scripts/run_historical_bar_replay.py`)**:
  - `build_historical_bar_quote()` anchors `source_at` strictly to `bar_open_at` (never refreshing stale minute bars to `observed_at - 40ms`).
  - `evaluate_bar_replay_assumptions()` emits explicit `data_provenance`, marks `second_level_execution_conclusions` and `intrabar_sl_tp_ordering` as `not_evaluable_from_minute_bars`, sets `lookahead_bias_excluded: false` for post-hoc / manually trimmed articles, and outputs an arrival-time sensitivity grid across `[+0s, +15s, +60s, +180s, +300s]` offsets.
