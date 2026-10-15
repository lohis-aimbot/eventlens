# Offline accounting diagnostics — v0.7

This milestone adds a small EUR/USD research ledger and a bounded Demo data sampler. It prepares the original **Event → Thesis Memory → Position** path for cost and timing checks. It does not connect LLM decisions to positions, implement a full backtester or establish alpha. Portfolio, hard risk and execution remain interfaces.

v0.8 extends the cost contract with explicit spread floors and minimum commissions and uses `research-ledger-v2` case IDs. Default zero floors/minimums preserve earlier numeric accounting. The [cost/path follow-up](cost-path-check.md) records a March 100 ms replay and five assumed cost floors; the original evidence and historical v0.7 acceptance below remain unchanged.

## Evidence and sample selection

`configs/fomc-2025.json` declares all eight scheduled 2025 FOMC statements before inspecting their quote-window results. The [official calendar](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm) and the linked statements identify each release. Times are stored in UTC; 14:00 New York is 19:00 UTC in winter and 18:00 UTC during daylight saving time. The separate August notation vote on strategy is outside this scheduled-statement sample.

| Statement | Source UTC | Current quote evidence |
|---|---|---|
| [January 29](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250129a.htm) | 19:00 | 2,017 Demo sampled snapshots collected |
| [March 19](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250319a.htm) | 18:00 | 2,009 Demo sampled snapshots collected |
| [May 7](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250507a.htm) | 18:00 | 1,985 Demo sampled snapshots collected |
| [June 18](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a.htm) | 18:00 | 2,094 Demo sampled snapshots collected; gaps disclosed below |
| [July 30](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250730a.htm) | 18:00 | 2,014 Demo sampled snapshots collected |
| [September 17](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250917a.htm) | 18:00 | 2,165 Demo sampled snapshots collected |
| [October 29](https://www.federalreserve.gov/newsevents/pressreleases/monetary20251029a.htm) | 18:00 | 1,936 Demo sampled snapshots collected |
| [December 10](https://www.federalreserve.gov/newsevents/pressreleases/monetary20251210a.htm) | 19:00 | 2,078 Demo sampled snapshots collected |

The catalog was declared before these quote results. All eight compact Demo windows are now acquired; the [full data-quality report](fomc-2025-data-quality.md) documents gaps and unresolved cost assumptions. Existing September/November 2024 and June 2025 checkpoints remain usable through the `legacy` adapter. They cannot support reliable path drawdown or all delay/horizon combinations. Broker quotes and detailed local accounting reports remain outside Git; public audit outputs contain derived aggregates and evidence hashes. Public synthetic inputs are invented test fixtures.

## Contracts and accounting

- `ResearchWindow`: event ID, source URL/time, backfill capture time, provider, instrument, observation kind, optional sampling interval and ordered UTC Bid/Ask observations. Capture time is not historical information availability. An evidence hash excludes re-import time; changing evidence changes its hash.
- `LatencyScenario`: assumed ingest, reasoning, signal, order and execution delays. Earliest fill readiness is after all stages. The first observed quote at or after readiness is used only within a two-second default wait; older quotes are never used as fills. Entry must precede its scheduled exit order.
- `CostScenario`: displayed spread through Bid/Ask, adverse slippage, per-fill notional commission and optional fixed fees. Defaults are sensitivity assumptions, not Pepperstone account tariffs.
- `ResearchPolicy`: USD initial collateral, fixed EUR units, a notional cap, quote-wait limit and path-gap threshold. This is a narrow accounting guard, not the platform's future HardRiskEngine.
- `AccountingResult`: reproducible case ID, evidence ID, version, assumptions, simulated timeline/fills, cash, remaining position, valuation time, fees, net PnL and observed-path quality.

Each case starts with USD 10,000 and independently tests 1,000 EUR long, short or flat. Horizons of five and thirty minutes are measured from simulated signal time. Exit readiness adds order/execution delay; the actual observed quote can be later. No winning direction is selected after seeing prices, and cases are not compounded into a strategy track record.

This is a linear, cash-collateralized PnL ledger. Opening deducts entry commission, without transferring spot principal. Closing realizes `signed_EUR_units × (exit_price − entry_price)` and deducts exit commission. Long opens at Ask and closes at Bid; short opens at Bid and closes at Ask. Slippage worsens each side. Spread is already included and must not be deducted a second time. Both long and short notional returns use the absolute entry notional as denominator.

Open liquidation equity equals `cash + unrealized_PnL − estimated_exit_fee`. An estimated closing fee reduces the liquidation mark but is not charged to cash until a simulated close. If no qualifying exit quote exists, the result retains the open units and latest observed valuation with `status=exit_unavailable`; closed PnL remains null. Missing entry quotes produce no fill. Neither missing state becomes an invented flat position or zero-return closed trade.

Drawdown is measured only at available observations, including immediate entry spread/cost loss. `checkpoints` means a sparse path; `sampled_with_gaps` means a sampled path exceeded the default three-second gap threshold. Even `sampled` only describes the observed path, not every tick or guaranteed execution. There is no margin liquidation, swap, currency conversion, partial fill, market impact or liquidity-depth simulation.

## Predeclared sensitivities

| Scenario | Ingest | Reasoning | Signal / order / execution | Earliest entry after source |
|---|---:|---:|---:|---:|
| five_seconds_assumed | 1 s | 3.85 s | 0.05 s each | 5 s |
| poll_plus_reasoning_assumed | 60 s | 4.85 s | 0.05 s each | 65 s |
| slow_reasoning_assumed | 60 s | 59.85 s | 0.05 s each | 120 s |

The 60-second RSS interval does not guarantee 60-second delivery: provider publication delay and polling phase remain unknown. None of these scenarios are live latency measurements.

Cost scenarios are displayed-spread-only; illustrative base (0.5 bps commission and 0.25 bps slippage per fill); illustrative stress (1 bps commission and 0.5 bps slippage per fill). All three directions, two horizons, three delays and three costs are emitted: 54 cases per event. These accounting benchmarks do not estimate an LLM's decision quality.

## Offline commands

```bash
eventlens-research window examples/synthetic-research-window.json
eventlens-research legacy data/fomc-2025-06-18-replay-labels.json --output data/june-accounting.json
eventlens-research probe path/to/log.txt --event-id fed-2025-06-18 --save-window data/june-window.json.gz --output data/june-accounting.json
eventlens-research window data/june-window.json.gz
```

Without `--output` the report goes to stdout. The adapter reads existing old observations, not their stored label values. v0.7 also corrects the legacy short-return formula to `(entry_bid − exit_ask) / entry_bid`; previously generated short labels must be regenerated for comparison. No network, model or broker calls occur in these research commands.

The probe importer requires a matching Demo/EURUSD start marker and focus time, zero probe errors, positive sample interval, matching observation count and ordered quotes inside source minus two minutes through plus thirty-five minutes. Count consistency alone does not prove full duration coverage. Fill coverage and path gaps must still be inspected in the report.

## Collect the next Demo window

1. In cTrader Algo, replace the existing `EventLensTickProbe` source with [the repository source](../../ctrader/EventLensTickProbe.cs), then Build. Use a **Demo** EURUSD instance and its **Backtesting** tab.
2. Select server tick data with the most accurate available mode. Include June 18, 2025 through at least 18:35 UTC in the backtest dates.
3. Set **Focus UTC** to `2025-06-18T18:00:00Z`. Commit the parameter edit before running. Set **Sample interval ms (0 = off)** to `1000`.
4. Run the backtest and locate its log. The start line must say `mode=demo_backtest_only`, `orders=none`, the expected focus and `sample_interval_ms=1000`. Summary must include a nonzero `window_samples` and zero quote/time errors.
5. Import that log with the `probe` command above. Preserve the compact gzip window and report locally; do not commit full logs, account identifiers or broker datasets.

Sampling prints the first valid existing tick snapshot in each interval bucket over the bounded 37-minute window. Empty buckets are omitted. It does not invent one-second bars or infer independent Bid/Ask freshness. Sampling defaults to off for compatibility with compact checkpoint logs. The robot requires Backtesting and a Demo account, requests no external access and contains no order methods.

## Processing latency and acceptance

Memory-only reasoning now reports `ProcessingLatency`, using application-stamped receipt, reasoning-start and completion times, linked to event/decision/model IDs. Duplicate reuse returns the original trace without another model call. `source_age_at_receipt_ms` is the age of source text, **not** measured network latency; replaying a 2024 statement in 2026 is historical input. No new paid model call or fresh-release latency measurement was made for this milestone.

Local acceptance: 71 pytest tests pass; Ruff and mypy pass. Tests cover independent long/short cash calculations, spread counted once, commissions, adverse slippage, every delay stage, post-readiness quote selection, unavailable exits retaining state, provenance/UTC validation, catalog daylight-saving conversion and reproducible cases. Audit tests additionally cover full-second/boundary denominators, duplicate sample buckets, mismatched events and explicit missing-file reporting. The invented fixture yields 36 closed and 18 flat cases. Each existing real checkpoint dataset yields 12 closed, 24 exit-unavailable and 18 flat cases. These are coverage/accounting checks, not trading performance metrics.

The C# source passed a direct reference compile and subsequently a native Build/run in Pepperstone cTrader 5.9.154. The October 14, 2026 acceptance run used the existing **Demo EURUSD** instance, **Backtesting** tab, accurate server trading data, June 18–19 historical dates, focus `2025-06-18T18:00:00Z` and sample interval 1,000 ms. The probe made no orders; cTrader showed zero backtest PnL. This zero is the read-only probe outcome, not a trading strategy result.

### First sampled-window acceptance

| Check | Observed result |
|---|---|
| Bounded window snapshots | 2,094 |
| First / last market time UTC | 2025-06-18 17:58:00.201 / 18:34:59.182 |
| Probe invalid / crossed / backward counters | 0 / 0 / 0 |
| Maximum consecutive sampled-window gap | 5,223 ms |
| Sampled-window gaps over three seconds | 11 |
| Independent cases | 36 closed, 18 flat, 0 unavailable |
| Five-minute observed paths | `sampled`, maximum path gap 2,211 ms |
| Thirty-minute observed paths | `sampled_with_gaps`, maximum path gap 3,803 ms |
| Compact gzip evidence size | 16,318 bytes |
| Evidence SHA256 | `01b2b11a8b3ebc0c4f207a31deaae826f79b48e2b5752451e5d3c0e796bd2179` |

All 36 closed cases were checked using parsed UTC datetimes: entry was at/after readiness and before the scheduled exit order; exit was at/after its readiness. Decimal cash conservation also passed for all closed cases. For example, the assumed 65-second-delay long/five-minute case becomes entry-ready at `18:01:05.000Z` and uses the snapshot at `18:01:05.086Z`; exit-ready is `18:06:05.000Z`, with the snapshot at `18:06:05.032Z`. These are hypothetical observed-quote fills, not broker confirmations.

**Code and first-window endpoint/accounting acceptance pass.** Subsequent runs completed the other seven catalog windows; see the [eight-event quality audit](fomc-2025-data-quality.md). All 288 non-flat scenarios now close and pass independent time/cash checks. Sampling remains incomplete, all thirty-minute paths exceed the current three-second gap threshold and zero spreads require cost verification. Measured live event availability and actual execution costs remain pending. Preserve compact evidence locally under ignored `data/`; raw logs and broker quotes are not published. Resolve cost and path requirements before implementing LLM target proposals and deterministic risk. Positive benchmark PnL, training fit or a single replay must never be treated as evidence for live trading.
