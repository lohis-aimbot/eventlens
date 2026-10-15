# Stage 1B — local Demo quote observation

This path observes **EURUSD** quotes from the already installed cTrader desktop app.
It contains no order calls, account-position queries, model calls, broker secrets,
Open API client or HTTP listener. It is independent of the historical backtesting probe.
A working quote bridge is not completion of the realtime trading agent.

## Small architecture

```text
cTrader Demo, real-time EURUSD ticks
  → EventLensQuoteBridge.cs, bounded sampling
  → new JSONL file in this cBot's local sandbox
  → Python watcher, actual receipt and committed availability
  → dedicated quote SQLite database
  → retrospective as-of alignment with observed news revisions
```

The C# component is a small platform adapter. Contracts, storage, validation and alignment
run in Python. Future independent feeds can receive their own adapters; this first bridge
intentionally accepts only EURUSD on Demo. It does not implement a general multi-broker feed.

The cBot uses `AccessRights.None` and writes only within its sandbox using the
[documented file-operation mechanism](https://help.ctrader.com/ctrader-algo/guides/file-operations/).
It rejects live accounts, backtests, optimization and other symbols before opening a file.
This is a runtime/code restriction, **not** a broker-issued view-only credential scope.
Do not replace the reviewed bridge with arbitrary trading code. No account ID, balance,
position or credential is emitted. Local files are trusted local adapter output, not
cryptographically authenticated market evidence.

## Run a bounded observation

1. In cTrader Algo, create a **C#** cBot named `EventLensQuoteBridge`, replace its generated
   source with [`ctrader/EventLensQuoteBridge.cs`](../ctrader/EventLensQuoteBridge.cs), then Build.
   The tested project targets .NET 6. No new NuGet dependency is required beyond cTrader.Automate.
2. Choose a **local Demo EURUSD** instance. Set duration to 60 seconds and sample interval
   to 1000 ms. This bridge uses the real-time tab, unlike `EventLensTickProbe`.
3. Start Python **before** pressing Start on this specific cBot. On the tested Mac:

   ```bash
   eventlens observe quotes watch "$HOME/cAlgo/Data/cBots/EventLensQuoteBridge" --seconds 180
   ```

   Use the actual sandbox directory on your installation. Python does not create or modify
   that directory. Old `quotes-*.jsonl` files are ignored; start a fresh bridge run after
   the watcher prints `watching`. Only one fresh bridge session is allowed per watch.
4. Press Start on the verified Demo quote bridge. It stops itself after the configured
   monotonic duration, including when ticks stop. Python exits after the stop record or
   its own deadline. A deadline with no stop is explicitly unclosed and returns failure.
   Ending Python early does not stop cTrader; the bridge's own timer remains responsible.
5. Read results locally:

   ```bash
   eventlens observe quotes report
   eventlens observe quotes --database data/live-quotes.sqlite report
   ```

Watch bounds are 10–600 seconds; bridge bounds are 10–600 seconds and 1000–10000 ms
between emitted tick snapshots. Sampling happens **on actual ticks**, not every wall-clock
second: quiet periods have gaps. There is no forward fill, invented heartbeat quote or
backfill at watch startup. This is sampled observation, not a complete tick archive.

## Clocks, durability and quality

| Field | Meaning |
|---|---|
| `source_at` | UTC `Ticks.LastTick.Time` for the sampled Bid/Ask pair |
| `bridge_at` | Local `DateTime.UtcNow` when the bridge serializes that pair |
| `received_at` | Actual Python clock when a complete new JSONL line is read |
| `recorded_at` | Start of Python's database write |
| `available_at` | Marker sampled after the quote data transaction commits |

Bid/Ask come from the same `Ticks.LastTick` snapshot. Separate per-side update times,
order-book depth, executable liquidity and fills are unknown. A source clock ahead of
the local bridge is rejected; UTC formatting alone does not prove clock synchronization.
Receipt must follow bridge time, and storage/availability must follow receipt. A crash
between storing data and publishing its marker leaves an auditable unpublished quote
that cannot be selected by `latest` or alignment.

The dedicated quote database refuses existing news/memory databases, uses a writer lock,
and preserves decimal prices and raw quote lines. Tables are `quote_sessions`,
`quote_observations` and `quote_failures`. Report/align use read-only connections.
`live_poll` here means a locally polled **Demo real-time** file, not live-account trading.
Synthetic fixture sessions cannot enter the default live lookup.

The watcher records malformed/unknown-session/live/backtest handshakes and duplicate,
reordered, future or already-stale (>5 seconds at receipt) quotes as failures. It rejects
linked, replaced, truncated and oversized new files rather than replaying their contents.
A watcher failure never creates a trade. Raw local evidence stays under ignored data paths.

Zero-spread snapshots remain stored and counted; alignment marks them unusable. This
conservative initial policy avoids treating an unvalidated free-spread quote as execution
quality. Report exposes sampled receipt gaps, missing sequence counts, closed sessions,
zero spreads and bridge-to-Python clock deltas. None of these measures source-to-fill latency.

## News/quote alignment

During a watch, a separate bounded news capture can run:

```bash
eventlens observe capture --kind news
eventlens observe quotes align NEWS_SOURCE_ID --as-of 2026-10-15T05:57:35.350476Z
```

Use a source ID and cutoff actually present in your own observation, not the example
clock with a newly created database. `align` retrospectively asks what stored inputs
were observable at that cutoff; it is not a saved contemporaneous LLM decision.
It selects only news revisions and quotes whose committed availability is at/before the
cutoff. It never uses a later quote to price an earlier event and never skips the newest
bad-quality quote to recover an older, nicer price.

Default limits are 2 seconds of quote source age and 60 seconds since the news revision
became available. They are initial observation checks, not validated trading thresholds.
News age here is **local observation age**; original publication time can remain unknown,
and newly discovered news is not necessarily newly published. Baseline captures are not
trading opportunities. Calendar schedules are not confirmed releases and are not inputs
to this news-only alignment command. No causal asset relevance is inferred by pairing
an arbitrary headline with EURUSD.

All outcomes retain `execution_authorized=false`. `aligned_observation` means that two
observable input records meet these freshness checks; it says nothing about importance,
direction, profitability, fees or permission to trade. See the [bounded acceptance](live-quotes-acceptance.md).
