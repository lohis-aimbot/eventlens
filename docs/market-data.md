# EUR/USD historical tick feasibility

This stage prepares the intraday data path before any event study or backtest. It does **not** claim that Pepperstone's account has been queried or that event information has alpha. The checked-in example is invented data.

## Why Bid and Ask ticks matter

cTrader Open API requests [historical ticks](https://help.ctrader.com/open-api/symbol-data/) separately for Bid and Ask. A request covers at most seven days, and a response can be truncated (`hasMore=true`). The [response schema](https://help.ctrader.com/open-api/messages/) says ticks arrive newest-first: the first timestamp is absolute Unix milliseconds, and later timestamps are backward millisecond differences. The price integer is divided by 100,000 and rounded to the symbol's digits. EventLens decodes each side independently and forms a quote only from the most recent Bid and Ask observed **at or before** the lookup time, subject to a configurable maximum age. It does not pair arrays by row number or fill a stale side indefinitely.

## Current implementation

- `market_data.py`: typed side-specific historical ticks, quote reconstruction and a provider-neutral data-source protocol.
- `ctrader_ticks.py`: pure decoder for normalized cTrader response pages; validates UTC windows, relative timestamps, price and page truncation status.
- `market_quality.py`: side counts, first/last tick, largest consecutive gap, crossed-quote count and quote/spread coverage at event availability and +1s, +5s, +10s, +30s, +1m, +5m, +30m. Out-of-window horizons are explicitly marked unavailable.
- `eventlens-market`: offline JSON inspection command. It never connects to cTrader and cannot submit orders.

Run the invented example:

```bash
eventlens-market examples/synthetic-ctrader-ticks.json
```

The JSON envelope has an explicit `event_source_time`, `available_time`, and `availability_basis` (`measured` or `assumed`), plus exactly one normalized page per side. A page contains `instrument`, `side`, `digits`, `request_start`, `request_end`, `has_more`, and `tick_data`. `tick_data` is the raw cTrader `timestamp`/`tick` sequence. For an actual API response, the adapter must obtain the symbol name and digits from the authenticated account and attach the request bounds; it must never assume the example's EUR/USD symbol ID or precision matches Pepperstone. `complete=false` means at least one page advertised truncation; `complete=true` only means those two responses did not advertise truncation. It does not prove the broker has a continuous full history.

## Live-account feasibility gate

1. Register an Open API application in the [cTrader portal](https://openapi.ctrader.com/) and wait for Spotware approval. The [official process](https://help.ctrader.com/open-api/api-application/) requires app approval before integration and then an OAuth redirect URI.
2. Authorize **view-only `accounts` scope** for a suitable Pepperstone cTrader account. The [official OAuth guide](https://help.ctrader.com/open-api/account-authentication/) distinguishes that from `trading` scope. Keep the client secret, authorization code, access token and refresh token out of Git and chat.
3. Resolve the account-specific EUR/USD symbol ID and digits, then request a **small** historical interval around a selected FOMC release, with pre-event warm-up and enough post-event time for the 30-minute horizon. Request Bid and Ask separately. Record whether the API actually returned both sides, timestamps and `hasMore` status. If truncated, split the interval or paginate without dropping same-millisecond ticks; never silently mark partial history complete.
4. Compare gaps, spreads and quote coverage. Keep broker data and account identifiers locally under ignored `data/`; commit only code, documentation and synthetic fixtures. Backtesting comes only after this gate passes on real data.

Historical backfills contain **market quote timestamps**, not the time EventLens would have received those quotes live. Similarly, the Fed archive's publication time is not a measured live receipt time. The `available_time` used here must be supplied separately from a measured collector receipt (or explicitly labeled a simulated latency assumption). Do not interpret this quality report as a return label, fill or profitability estimate.
