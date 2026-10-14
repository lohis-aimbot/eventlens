# EUR/USD historical tick feasibility

This stage prepares the intraday data path before any event study or strategy backtest. A separate, read-only probe has been run against Pepperstone cTrader **Demo** server historical trading data. It does **not** establish live information latency, trade execution quality or event alpha. The checked-in JSON and probe-log examples remain invented data. See the [three-event timing-label preview](fomc-replay-preview.md) for the first real Demo replay observations and their limits.

## Why Bid and Ask ticks matter

cTrader Open API requests [historical ticks](https://help.ctrader.com/open-api/symbol-data/) separately for Bid and Ask. A request covers at most seven days, and a response can be truncated (`hasMore=true`). The [response schema](https://help.ctrader.com/open-api/messages/) says ticks arrive newest-first: the first timestamp is absolute Unix milliseconds, and later timestamps are backward millisecond differences. The price integer is divided by 100,000 and rounded to the symbol's digits. EventLens decodes each side independently and forms a quote only from the most recent Bid and Ask observed **at or before** the lookup time, subject to a configurable maximum age. It does not pair arrays by row number or fill a stale side indefinitely.

## Current implementation

- `market_data.py`: typed side-specific historical ticks, quote reconstruction and a provider-neutral data-source protocol.
- `ctrader_ticks.py`: pure decoder for normalized cTrader response pages; validates UTC windows, relative timestamps, price and page truncation status.
- `market_quality.py`: side counts, first/last tick, largest consecutive gap, crossed-quote count and quote/spread coverage at event availability and +1s, +5s, +10s, +30s, +1m, +5m, +30m. Out-of-window horizons are explicitly marked unavailable.
- `eventlens-market`: offline JSON inspection command. It never connects to cTrader and cannot submit orders.
- `replay_labels.py`: typed Demo snapshot checkpoints and availability-delay/forward-horizon labels. It rejects event/focus mismatches, duplicate or early checkpoints, late snapshots and invalid probe summaries.
- `eventlens-replay`: offline CLI to turn a local read-only probe log into JSON labels. No broker or LLM call occurs.

Run the invented example:

```bash
eventlens-market examples/synthetic-ctrader-ticks.json
```

The JSON envelope has an explicit `event_source_time`, `available_time`, and `availability_basis` (`measured` or `assumed`), plus exactly one normalized page per side. A page contains `instrument`, `side`, `digits`, `request_start`, `request_end`, `has_more`, and `tick_data`. `tick_data` is the raw cTrader `timestamp`/`tick` sequence. For an actual API response, the adapter must obtain the symbol name and digits from the authenticated account and attach the request bounds; it must never assume the example's EUR/USD symbol ID or precision matches Pepperstone. `complete=false` means at least one page advertised truncation; `complete=true` only means those two responses did not advertise truncation. It does not prove the broker has a continuous full history.

## Pepperstone Demo cTrader Algo probe

`ctrader/EventLensTickProbe.cs` is a C# cBot used only for a historical-data feasibility check. It refuses to run outside cTrader Backtesting (`IsBacktesting` guard) or on a live account (`Account.IsLive` guard), requests no external access, and contains no order calls. In cTrader Mac, create a C# cBot named `EventLensTickProbe`, paste this source, save and build it, select a **Demo** EURUSD instance, open **Backtesting**, and choose **server trading data (accurate)**. Do not use the instance's real-time Start button. The built-in backtester may require a .NET SDK on Mac. Set `Focus UTC` to the verified ISO value such as `2025-06-18T18:00:00Z`, then leave the field to commit it. Inspect the `EVENTLENS_PROBE|start` log line before accepting results; changing the date range alone does not change the focus parameter. The probe prints 16 compact checkpoints at predefined offsets and summary statistics; it does not export a full broker tick stream.

To inspect an **invented** log fixture without cTrader:

```bash
eventlens-replay examples/synthetic-ctrader-probe.log \
  --event-id synthetic-fomc --instrument EUR/USD \
  --source-time 2025-06-18T18:00:00Z
```

For a real Demo log, supply the local cTrader Backtesting `log.txt` path and the official source time. Keep the full output in ignored `data/`. The CLI requires one Demo-backtest start marker, one zero-error summary, and one focus time matching the independently verified source time. An absent or late checkpoint yields an explicit unavailable label; it never fills an old price forward as if a trade occurred. Its `mid_return` and `spread_only_*_return` fields are **indicative quote changes**, not fills, PnL or alpha.

On October 12, 2026, the probe was compiled successfully in Pepperstone cTrader 5.9.154 and replayed June 18–19, 2025 (UTC). The [Federal Reserve statement](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a.htm) was scheduled for June 18 at 2:00 p.m. EDT, or 18:00 UTC. cTrader's log reported:

| Check | Observed Demo replay result |
| --- | ---: |
| EURUSD quote updates over the two days | 206,919 |
| First / last replay timestamp (UTC) | June 18 00:00:00.395 / June 19 23:59:59.963 |
| Maximum consecutive replay gap | 156.996 s |
| Invalid / crossed / backward-time updates | 0 / 0 / 0 |
| Updates in 17:59:00–18:01:00 UTC on June 18 | 338 |
| Last quote before 18:00 UTC | 17:59:59.895, Bid 1.14976 / Ask 1.14993 |
| First quote at or after 18:00 UTC | 18:00:00.108, Bid 1.14970 / Ask 1.14994 |
| Backtest trades | 0 |

The 108 ms gap from scheduled publication to the first later replay quote is a **market-data timestamp gap**, not measured Fed feed receipt, parser latency, order latency or a fill. Some replay updates had zero Bid/Ask spread, so historical execution costs require separate validation. The two-day maximum gap is not a claim about the FOMC minute. The probe emits only compact checkpoints rather than persisting the full tick stream.

## Later Open API gate (Demo, view-only)

1. Register an Open API application in the [cTrader portal](https://openapi.ctrader.com/) and wait for Spotware approval. The [official process](https://help.ctrader.com/open-api/api-application/) requires app approval before integration and then an OAuth redirect URI.
2. Authorize **view-only `accounts` scope** for a Pepperstone **Demo** account only. The [official OAuth guide](https://help.ctrader.com/open-api/account-authentication/) distinguishes that from `trading` scope. Keep the client secret, authorization code, access token and refresh token out of Git and chat.
3. Resolve the account-specific EUR/USD symbol ID and digits, then request a **small** historical interval around a selected FOMC release, with pre-event warm-up and enough post-event time for the 30-minute horizon. Request Bid and Ask separately. Record whether the API actually returned both sides, timestamps and `hasMore` status. If truncated, split the interval or paginate without dropping same-millisecond ticks; never silently mark partial history complete.
4. Compare gaps, spreads and quote coverage with the Algo replay result. Keep broker data and account identifiers locally under ignored `data/`; commit only code, documentation and synthetic fixtures. Strategy backtesting comes only after the point-in-time data gate passes.

Historical backfills contain **market quote timestamps**, not the time EventLens would have received those quotes live. Similarly, the Fed archive's publication time is not a measured live receipt time. The `available_time` used here must be supplied separately from a measured collector receipt (or explicitly labeled a simulated latency assumption). The new labels use **assumed** availability only. Do not interpret a quote-quality or replay-label result as a fill or profitability estimate.

## v0.7 follow-up

The [offline accounting milestone](research-replay.md) adds bounded Demo snapshot sampling and independent long/short/flat cost scenarios. Existing compact checkpoints remain sparse evidence. The revised C# source passed an offline API-reference compile and native Demo Build/backtests, collecting 16,298 snapshots across all eight predeclared 2025 statements. All predeclared entry/exit endpoints were available; sampling gaps and zero-spread cost concerns remain explicitly flagged in the [data-quality audit](fomc-2025-data-quality.md). Defaults keep sampling off. No brokerage connector or orders were added.
