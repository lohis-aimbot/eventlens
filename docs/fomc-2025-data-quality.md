# FOMC 2025 Demo quote-window quality report

**Acceptance: 8/8 predeclared windows collected; endpoint timing and accounting pass. Path completeness and executable-cost assumptions remain unresolved.** This is a data audit, not an AI strategy backtest, a forecast evaluation or evidence of alpha.

## Dataset and provenance

On October 14, 2026, the read-only `EventLensTickProbe` was run in Pepperstone cTrader 5.9.154 against the existing Demo EURUSD instance, using the Backtesting tab and accurate server trading data. The seven additional runs each used one historical date; the earlier June run used June 18–19. Every run had `orders=none`, a matching UTC focus, a 1,000 ms sample interval and zero invalid/crossed/backward counters. Native UI runs finished with zero backtest PnL because the probe places no orders.

The sample includes all eight scheduled statements in [the predeclared catalog](../configs/fomc-2025.json); none was dropped after inspecting quote quality. Each observation is the first valid existing Bid/Ask snapshot in a sample bucket, not every source tick and not an executable order fill. Units are EUR/USD, pip = 0.0001. Timestamps are UTC. Publication time, assumed decision availability and actual historical capture time remain separate.

There are **16,298 snapshots**, held in eight ignored gzip windows totaling **125,185 bytes** (about 122.3 KiB). Raw logs, account identifiers and quote rows are not published. The [audit script](../scripts/audit_fomc_windows.py) and [executed notebook](../notebooks/fomc-window-quality.ipynb) provide inspectable checks and derived aggregate output.

## Coverage and gaps

Coverage uses 2,220 half-open full-second buckets per event: `[source − 2m, source + 35m)`. An observation exactly at +35m is an additional boundary snapshot, not another expected full second. July has one such boundary snapshot. Missing buckets mean no sample was recorded in that bucket; they do not by themselves establish missing source ticks or a broken feed. Empty buckets are neither filled nor interpolated.

| Event date | Snapshots | Occupied / expected seconds | Coverage | Missing seconds | Largest sampled gap | Gaps >3s |
|---|---:|---:|---:|---:|---:|---:|
| 2025-01-29 | 2,017 | 2,017 / 2,220 | 90.86% | 203 | 5.414 s | 16 |
| 2025-03-19 | 2,009 | 2,009 / 2,220 | 90.50% | 211 | 10.078 s | 20 |
| 2025-05-07 | 1,985 | 1,985 / 2,220 | 89.41% | 235 | 7.677 s | 32 |
| 2025-06-18 | 2,094 | 2,094 / 2,220 | 94.32% | 126 | 5.223 s | 11 |
| 2025-07-30 | 2,014 | 2,013 / 2,220 | 90.68% | 207 | 6.082 s | 24 |
| 2025-09-17 | 2,165 | 2,165 / 2,220 | 97.52% | 55 | 4.923 s | 9 |
| 2025-10-29 | 1,936 | 1,936 / 2,220 | 87.21% | 284 | 7.806 s | 34 |
| 2025-12-10 | 2,078 | 2,078 / 2,220 | 93.60% | 142 | 5.505 s | 14 |

Across the eight windows, 16,297 of 17,760 full-second buckets are occupied (91.76%); 1,463 are empty. There are 160 consecutive sampled gaps over three seconds. The largest is 10.078 seconds in March. The first and last observations bracket each event window to subsecond/approximately one-second boundary lag, as recorded in the notebook; this does not imply complete interior coverage.

## Observed spread profile

Quantiles below use sampled snapshots, not all ticks or a time-weighted spread. P95 uses nearest rank. Zero spread is a valid schema value and is preserved, not replaced or silently dropped.

| Event date | Median spread (pips) | P95 (pips) | Maximum (pips) | Zero-spread snapshots | Zero-spread share |
|---|---:|---:|---:|---:|---:|
| 2025-01-29 | 0.0 | 0.3 | 2.9 | 1,189 | 58.95% |
| 2025-03-19 | 0 | 0.3 | 4.5 | 1,499 | 74.61% |
| 2025-05-07 | 0.1 | 0.3 | 3.9 | 950 | 47.86% |
| 2025-06-18 | 0.1 | 0.3 | 6.8 | 866 | 41.36% |
| 2025-07-30 | 0.0 | 0.3 | 4.0 | 1,252 | 62.16% |
| 2025-09-17 | 0.0 | 1.3 | 3.9 | 1,185 | 54.73% |
| 2025-10-29 | 0.0 | 0.2 | 3.3 | 1,455 | 75.15% |
| 2025-12-10 | 0.0 | 0.2 | 4.2 | 1,512 | 72.76% |

**9,908 of 16,298 snapshots (60.79%) have zero displayed spread.** The audit cannot determine whether this reflects genuine broker prices, rounding, or backtest quote reconstruction. It must not be interpreted as guaranteed zero-cost execution. Independent Bid/Ask validation and Demo/account-specific commission/slippage evidence are required before making after-cost strategy claims. Current base/stress costs remain explicitly illustrative assumptions.

## Endpoint and accounting checks

Every event emits all 54 predeclared combinations of direction, horizon, latency and cost. Each has 36 hypothetical closed cases and 18 flat cases, with no unavailable entries/exits under the current two-second quote-wait limit. Across all events: **288 closed + 144 flat = 432 cases**. These are repeated accounting scenarios over **eight events**, not 432 independent market samples.

For every closed case, the audit independently checked parsed UTC fill ordering, entry before scheduled exit order, both quote waits within the limit, signed-unit price-change PnL minus both commissions, closing cash conservation and zero remaining position. All 288 closed-case checks passed. This validates the ledger and simulated fill chronology; it does not validate complete strategy features, LLM historical knowledge or real information availability.

| Event date | Five-minute path: max gap | Five-minute path quality | Thirty-minute path: max gap | Thirty-minute path quality |
|---|---:|---|---:|---|
| 2025-01-29 | 2.751 s | sampled | 5.414 s | sampled_with_gaps |
| 2025-03-19 | 3.936 s | sampled_with_gaps | 10.078 s | sampled_with_gaps |
| 2025-05-07 | 7.677 s | sampled_with_gaps | 7.677 s | sampled_with_gaps |
| 2025-06-18 | 2.211 s | sampled | 3.803 s | sampled_with_gaps |
| 2025-07-30 | 3.013 s | sampled_with_gaps | 6.082 s | sampled_with_gaps |
| 2025-09-17 | 2.079 s | sampled | 3.406 s | sampled_with_gaps |
| 2025-10-29 | 4.423 s | sampled_with_gaps | 7.806 s | sampled_with_gaps |
| 2025-12-10 | 2.168 s | sampled | 4.202 s | sampled_with_gaps |

Four five-minute event paths satisfy the current three-second gap limit; four do not. All eight thirty-minute paths are `sampled_with_gaps`. Drawdown computed on these observations is an observed-path lower bound, not continuous worst-case drawdown or proof that stops could execute. A `sampled` flag also does not establish every-tick coverage or independent side freshness.

## Findings, impact and next gate

| Finding | Severity / evidence confidence | Impact | Smallest next check |
|---|---|---|---|
| Sample gaps in all full windows and all thirty-minute paths | Medium / high | Intra-position excursions and stop execution may be missed | Preserve gap flags; use denser source ticks for any path-dependent risk evaluation |
| 60.79% zero-spread snapshots; cause unverified | High / high for observed share, unresolved cause | Displayed-spread-only accounting can understate executable cost | Cross-check independent Bid/Ask observations and explicit Demo commission/slippage assumptions |
| Historical availability and stage delays are assumed | High / high | Early hypothetical reactions may precede actual delivery | Measure fresh-event intake/reasoning latency before claiming live applicability |
| Only eight scheduled events; no LLM targets tested | High / high | No reliable claim of predictive or economic value | Predeclare later decision rules and chronological evaluation; retain future shadow evidence |

**Stop at this data milestone.** Window collection, event identity, UTC/schema checks and endpoint accounting pass. Continuous-path risk and executable-cost gates have not passed. Do not relabel this as strategy acceptance, enlarge the sample count using scenario combinations, remove difficult events, train a model or turn on trading. The immediate follow-up is to resolve Bid/Ask/cost assumptions and path requirements before evaluating LLM target decisions.

## Reproduce and identify evidence

With EventLens installed in the local environment and the ignored compact datasets present, run from the repository root:

```bash
python scripts/audit_fomc_windows.py --output data/fomc-2025-window-audit.json
```

The notebook reruns the same script with no network or model calls. Public checkouts retain the captured aggregate audit but omit the broker quotes; missing files are explicitly listed, never replaced with synthetic evidence. `captured_at` is actual backfill import time, not live delivery time. Evidence SHA256 excludes capture time to keep identical reimports stable.

| Event date | Compact bytes | Evidence SHA256 |
|---|---:|---|
| 2025-01-29 | 15,338 | `8ac5b7b678b9d87dd2ae8326ea65729510f6380fed6826b7ef699a0a7a013578` |
| 2025-03-19 | 15,179 | `af9760357d57d6dfa81194a3db84664f8c463a30d02aac1d34b2d980a56ea269` |
| 2025-05-07 | 15,513 | `983ab48e293237a9a5f24d5baaede74193b349af067bdb3fe83040aeb3771ff2` |
| 2025-06-18 | 16,318 | `01b2b11a8b3ebc0c4f207a31deaae826f79b48e2b5752451e5d3c0e796bd2179` |
| 2025-07-30 | 15,521 | `65fcda2e36a201fe5f8b6e0ce0910dd7990ab8b1b102b85724541a53e1721cd2` |
| 2025-09-17 | 17,588 | `14c65e8ad99be4f261ad4ded926ea48d2d918bec5dad13eee64d07ec6c67c22a` |
| 2025-10-29 | 14,200 | `f53a1e8c2107529e1e65eaf549b078009ec79bf96c506faec7d27639190c309f` |
| 2025-12-10 | 15,528 | `5e8ee77742cec8c0650d565722a2b20c236bc0a6292a1c538d046abddd0063b7` |
