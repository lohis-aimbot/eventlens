# Stage 1B acceptance — Demo quotes and news alignment

Checked **2026-10-15**. **Capture/durability/alignment plumbing passed; representative
market-data quality and complete realtime-observer acceptance remain pending.**
No model, order, fill, strategy result or major-release latency claim is part of this check.

## Actual local observation

- Pepperstone cTrader **5.9.154**, visibly verified **Demo EURUSD** instance.
- Separate `EventLensQuoteBridge` C# project: .NET 6 build succeeded, zero warnings/errors.
- Sampling configured at 1000 ms on ticks, bounded to 60 seconds. Python watcher started
  first with a 90-second limit. It received the stop record; cTrader visibly returned to
  the stopped state. No background observer was left running.
- One public financial-news fetch ran during this interval, using the existing observer.
  No LLM, order API, positions query or Open API authentication was invoked.

| Measure | Observed |
|---|---:|
| Sampled quotes saved | 33 |
| First / last Python receipt (UTC) | 05:57:12.340386 / 05:58:10.313021 |
| Rejected / unpublished quotes | 0 / 0 |
| Missing sequence IDs at confirmed stop | 0 |
| Zero-spread samples | **29 of 33** |
| Largest gap between sampled receipts | 5.299672 s |
| Bridge serialization to Python receipt, median / max | 0.085147 / 0.194597 s |
| Local quote database size | 57,344 bytes |

The 1000 ms setting is a minimum interval on actual ticks, not a claim of one sample
every second. Gaps do not prove feed loss; the bridge deliberately samples. Conversely,
complete sequences prove only delivery of the emitted samples, not full market coverage.
The high zero-spread count requires further comparison of broker/platform snapshot
behavior before execution-cost assumptions can rely on this source. Its cause is not
established. No threshold was loosened to turn these observations into accepted trades.

All 33 persisted quote payloads matched their raw decimal prices. Every row satisfied
source ≤ bridge ≤ Python receipt ≤ write start ≤ post-data-commit availability. Before
each availability marker, the corresponding quote was absent from as-of lookup. Read-only
checks left both news and quote database bytes unchanged.

## Actual news alignment result

A news snapshot became available at **2026-10-15T05:57:35.350476Z**. It contained 24
previously unseen IDs and six repeats. These IDs accumulated between the earlier capture
and this run: **24 new-to-database rows are not 24 breaking releases during this minute**.
No edited content or important economic release was confirmed in this short sample.

For the first returned news ID, `3179153`, the exact news-availability cutoff selected a
quote with source time **05:57:31.968Z**, age **3.382476 seconds**, and zero spread.
The result was **`stale_quote`**, with `execution_authorized=false`. It did not use a later
quote or substitute a midpoint as a hypothetical fill.

Read-only root-cause inspection of the stored session (`d90a02d3-6d25-4b30-964a-248a752b10f9`)
established three facts without modifying local database bytes:

1. **No decimal truncation in C# or Python:** Four of the 33 rows (`sequence` 12, 13, 15 and 22)
   preserve a 5-digit `0.00001` (0.1 pip) spread in `raw_line`, matching EURUSD 5-digit pricing;
   the remaining 29 rows were emitted with identical `bid` and `ask` strings by cTrader.
2. **Pre-start cached first tick:** Sequence 1 had `source_at = 05:57:11.244Z` while session
   `started_at` was `05:57:12.206764Z` (`first_source_before_start = true`, source-to-bridge lag
   `0.979168 s` versus `0.273–0.339 s` on sequences 2–33). `EventLensQuoteBridge.cs` now filters
   out pre-start cached `LastTick` timestamps and flushes trailing throttled ticks on its timer.
3. **Clock decomposition at the news cutoff:** The `3.382476 s` quote source age for news `3179153`
   decomposes into `0.274895 s` source-to-bridge delta, `0.131340 s` bridge-to-Python receipt delta,
   and `2.976241 s` elapsed after Python receipt during a `5.295 s` source-tick gap between
   sequences 14 (`05:57:31.968Z`) and 15 (`05:57:37.263Z`).

For completeness, all 19 later emitted-sample availability cutoffs in the same window
were also inspected: 17 returned `zero_spread`; two returned `aligned_observation`.
These are retrospective input audits at those later cutoffs. They do not repair the
failed earlier cutoff, establish news relevance or show what an LLM would have done.
No returns or PnL were computed, and the news's original release time remains unknown.

Inspect the original local database, without making a network request:

```bash
eventlens observe quotes report
eventlens observe quotes align 3179153 --as-of 2026-10-15T05:57:35.350476Z
```

Fresh clones cannot reproduce these exact captures without the original ignored local
data. The automated tests use invented timestamps/prices instead; CI never starts cTrader.

## Automated checks and remaining gate

The full local suite passes **287 tests**; Ruff, strict mypy and package-build checks also
pass. Offline tests cover future/stale/naive clocks, exact decimal prices, malformed and
non-Demo sessions, duplicate/reordered quotes, stop boundaries, sequence gaps, symbol/tick
diagnostics, clock-lag decomposition, publication failure, synthetic separation, file-tail
boundaries, read-only stores, foreign-database rejection and writer locking. They also verify
that alignment cannot use a future quote/news revision or fall back past a newer zero-spread
quote. Frozen historical research inputs and experiment identities remain unchanged.

Still needed: representative continuous sampling, an important release, independent
price/spread comparison, clock-health monitoring, feed outage/restart behavior over time,
verified pre-release expectations and a validated LLM input bridge. The current CLI is
an observation tool, not a running integrated event-to-thesis-to-position agent.
The 30–60-second event-to-decision/order goal is untested. No orders were sent.
