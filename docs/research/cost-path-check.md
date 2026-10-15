# Cost and dense-path verification — v0.8

**Acceptance: deterministic cost safeguards and one diagnostic dense replay pass. Verified live execution costs and continuous-path completeness remain pending.** This milestone does not evaluate LLM decisions, train a model, introduce orders or select winning benchmark directions.

## Zero spread and commission evidence

Zero spread is not sufficient evidence of corrupt data: [Pepperstone's public fee page](https://pepperstone.com/en/trading/costs-and-fees) lists zero minimum EUR/USD spread for Razor and a separate cTrader commission schedule. This establishes that zero is possible, not that our Demo snapshots are independently verified or cost-free. The actual source of the high zero-spread share remains unconfirmed.

The updated Demo/backtest-only probe records symbol fee metadata without requesting account IDs, credentials or orders. In the October 14, 2026 March replay it reported:

| Item | Observed value | Evidence meaning |
|---|---|---|
| Symbol commission | 3 | Demo/backtest symbol metadata, not a charged fill fee |
| Symbol commission type | `UsdPerMillionUsdVolume` | Preserve the declared USD-volume denomination |
| Minimum commission | 0; type `QuoteAsset`, asset USD | No positive minimum in this metadata snapshot |
| Lot size / pip size / digits | 100,000 / 0.0001 / 5 | Symbol unit metadata |
| Native backtest commission setting | 30 USD per million USD volume | Manually configured simulator setting observed in the UI |
| Apply commission automatically | Off | Simulator setting is separate from symbol metadata |

The [cTrader settings contract](https://help.ctrader.com/ctrader-algo/references/Plugin/Backtesting/BacktestingSettings/) distinguishes manually configured commission from automatic application of the symbol value. The [commission-type enum](https://help.ctrader.com/ctrader-algo/references/MarketData/Symbols/SymbolCommissionType/) distinguishes USD-volume rates from per-lot rates. These readings are not interchangeable and neither confirms a historical fee actually paid. The probe made no trades, so no charged commission was observed. Metadata was captured in 2026, not obtained from the 2025 fee schedule.

The Python ledger keeps all cost scenarios explicitly assumed. Its 0.5 bps per-fill USD-notional commission means 50 USD per million USD notional, with additional assumed adverse slippage; it is not automatically replaced by the smaller symbol metadata or the native simulator setting. Resolving the applicable Demo fee schedule and later checking actual Demo fill costs remain separate gates. No live account was accessed.

## Dense replay diagnostic

March was selected because its one-second audit had the largest observed gap, not because of its market direction or PnL. It is a quality investigation, not a ninth independent event or a new evaluation sample. Accurate server tick data and the same focus/date were replayed with 100 ms sampling.

| Check | One-second capture | 100 ms replay |
|---|---:|---:|
| Window snapshots | 2,009 | 4,992 |
| Largest consecutive sampled gap | 10.078 s | 9.845 s |
| Original observations reproduced | — | All 2,009 exact timestamp/Bid/Ask triples |
| Valid window ticks counted by probe | Not recorded in old version | 4,992 |
| Zero-spread ticks | Snapshot audit only | 3,594 |
| Compressed dense evidence | — | 33,446 bytes |

All dense snapshots reconcile to the probe's valid-window tick count, first/last times, maximum gap and zero-spread counter. The 100 ms capture therefore retained every valid OnTick observation delivered to this replay probe. It does not establish every tick in an independent source, side-specific freshness or real-time transport latency.

**Increasing sampling density did not eliminate the gap.** The supplied backtest stream itself has a 9.845-second interval without a new observed valid tick in the window. Whether that reflects a quiet quote period, historical source coverage or backtest delivery cannot be determined from this probe alone. No interpolation, synthetic price or relaxed gap threshold was used to claim a pass. Only March was rerun densely; the other seven paths retain their original limitations.

## Cost model safeguards

`CostScenario` now supports an explicit `spread_floor_pips` and `minimum_commission_usd`. For EUR/USD, the assumed minimum spread is widened symmetrically around the observed midpoint only when larger than the observed Bid/Ask spread. Raw observations and their evidence hashes remain unchanged. Existing wider spreads are preserved. Adverse slippage applies after the widening, on the appropriate buy/sell side. Spread is not charged again as a cash fee.

Per-fill commission is `max(abs(EUR units) × fill price × commission_bps / 10,000, minimum_commission_usd)`, plus any separately configured fixed fee. The default floor and minimum are zero, preserving the prior numeric accounting when these safeguards are unused. All configuration values remain assumptions, including any positive minimum; none is presented as broker confirmation.

Engine version is now `research-ledger-v2` and package version 0.8.0. Case identities change with the engine and complete cost contract. Older local v1 reports remain historical artifacts; do not compare their case IDs directly with v2. The original eight evidence hashes are unchanged.

## Predeclared sensitivity grid

All profiles in [cost-sensitivity.json](../../configs/cost-sensitivity.json) use identical per-fill commission of 0.5 bps and adverse slippage of 0.25 bps. Only the spread floor varies: 0, 0.2, 0.5, 1 and 2 pips. Profiles must start at zero and increase strictly; mixed fees or duplicate names are rejected so the comparison isolates the floor. These levels are sensitivity assumptions, not calibrated FOMC execution spreads or worst-case guarantees.

The full grid contains 8 events × 3 assumed delays × 2 horizons × 2 fixed directions × 5 floors = **480 closed accounting cases**. Each event/profile has 12 cases. No direction or profile is selected after seeing prices; no profitable-case count, compounded portfolio result or forecast accuracy is claimed.

The table shows median **additional cost** versus the identical floor-zero baseline over the 12 cases for each event. Fixed size is 1,000 EUR, and figures below are rounded USD amounts. These numbers are not expected returns or estimates of the cost of a different position size.

| Event | 0.2 pip floor | 0.5 pip floor | 1 pip floor | 2 pip floor |
|---|---:|---:|---:|---:|
| 2025-01-29 | 0.0050 | 0.0325 | 0.0825 | 0.1825 |
| 2025-03-19 | 0.0125 | 0.0425 | 0.0925 | 0.1925 |
| 2025-05-07 | 0.0075 | 0.0350 | 0.0850 | 0.1850 |
| 2025-06-18 | 0.0025 | 0.0225 | 0.0725 | 0.1725 |
| 2025-07-30 | 0.0100 | 0.0375 | 0.0875 | 0.1875 |
| 2025-09-17 | 0.0100 | 0.0250 | 0.0500 | 0.1300 |
| 2025-10-29 | 0.0125 | 0.0425 | 0.0925 | 0.1925 |
| 2025-12-10 | 0.0100 | 0.0375 | 0.0875 | 0.1875 |

Every same-case net PnL was nonincreasing as the spread floor rose; all 480 entries/exits remained available. This is an accounting monotonicity check, not proof of economic value. Endpoint availability does not imply a sufficiently complete intra-position path or guaranteed fill.

## Validation and remaining gate

79 pytest tests pass locally, including zero-spread widening, unchanged wider spreads, minimum commissions, adverse slippage ordering, unchanged evidence, duplicate/unknown diagnostic rejection and full-tick counter reconciliation. Ruff formatting/lint and mypy validate the code. The updated C# source was natively built and its diagnostics were observed in the Demo historical run.

Cost safeguards and the diagnostic are ready for controlled research. Before any claim about executable net alpha or path-dependent risk, resolve the applicable commission/slippage assumptions, obtain independent side/coverage evidence where required, and measure actual event availability. A coarse fixed-horizon LLM replay can be designed later with these limitations explicit; live shadow evidence is still necessary because historical LLM knowledge can leak future outcomes. This stage stops without implementing target decisions, hard-risk execution or trading.

## Reproduce and inspect

Use the installed local EventLens package and ignored datasets, from the repository root:

```bash
python scripts/audit_cost_path.py --output data/cost-path-audit-v1.json
```

The [executed notebook](../../notebooks/cost-path-check.ipynb) runs the same audit and contains aggregate output only. Local inputs are the eight original compact windows plus the March `window-100ms.json.gz` and `100ms-diagnostics.json`. Missing or inconsistent dense evidence fails; there is no synthetic fallback. Detailed raw logs and broker quote rows are not published.

Cost config ID: `f805a3db5543e9e7aad4c9657d7dedab94f77907b691b40d987e8d7b82b9a144`.
March original evidence: `af9760357d57d6dfa81194a3db84664f8c463a30d02aac1d34b2d980a56ea269`.
March dense evidence: `145edb01c1788a65bbc1380df109cd8e4e423f8063033dc787396da50e1bdc83`.
