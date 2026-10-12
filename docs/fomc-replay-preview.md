# FOMC × EUR/USD Demo replay: first timing labels

This is a **data-alignment acceptance check**, not a strategy backtest or evidence of alpha. On October 12, 2026, the read-only `EventLensTickProbe` ran in Pepperstone cTrader 5.9.154 **Demo Backtesting**, using **server trading data (accurate)**. It made **zero trades**. The checked-in code generated checkpoints; cTrader kept the full log in its local Backtesting directory and the generated JSON stayed in ignored local `data/`. Neither is published here.

The event source times are the Federal Reserve's scheduled statement-release times: [September 18, 2024](https://www.federalreserve.gov/newsevents/pressreleases/monetary20240918a.htm) at 18:00 UTC, [November 7, 2024](https://www.federalreserve.gov/newsevents/pressreleases/monetary20241107a.htm) at 19:00 UTC, and [June 18, 2025](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a.htm) at 18:00 UTC. These are **not** measured EventLens receipt times. The 2024 statements used in the [thesis-memory case](fomc-memory-case.md) were first retrieved for that separate experiment in 2026; this market replay does not retroactively make those 2024 thesis decisions live.

For each event, `available_time = source_time + assumed_delay` for 1, 5, 30 and 60 seconds. The entry checkpoint is the first valid combined Bid/Ask replay snapshot **at or after** that time. Exit targets are `available_time + 60/300/1800 seconds`, again using the first later replay snapshot. Checkpoints more than two seconds late are unavailable. The resulting horizon is measured from assumed availability, so the actual entry-to-exit interval can differ slightly. These combined snapshots do not reveal independent per-side update timestamps or live quote receipt times; the separate Open API tick decoder remains the path for side-specific freshness checks.

| Event | Replay updates | FOMC ±1 min updates | Maximum checkpoint wait | Valid labels |
| --- | ---: | ---: | ---: | ---: |
| 2024-09-18 | 161,622 | 328 | 0.559 s | 12/12 |
| 2024-11-07 | 176,658 | 346 | 0.526 s | 12/12 |
| 2025-06-18 | 206,919 | 338 | 0.496 s | 12/12 |

The table below shows the **5-minute** midpoint change and a hypothetical long-side change using entry Ask and exit Bid. The latter includes displayed spread at those two snapshots only; it excludes commission, slippage, order latency, liquidity, partial fills and any live execution effects. No direction was selected by the LLM or by a strategy.

| Event | Assumed availability delay | Midpoint change | Ask-to-Bid long change |
| --- | ---: | ---: | ---: |
| 2024-09-18 | 1 s | +0.2034% | +0.1795% |
| 2024-09-18 | 5 s | −0.0228% | −0.0340% |
| 2024-09-18 | 30 s | −0.0546% | −0.0654% |
| 2024-09-18 | 60 s | −0.0743% | −0.0806% |
| 2024-11-07 | 1 s | −0.2079% | −0.2176% |
| 2024-11-07 | 5 s | −0.2098% | −0.2121% |
| 2024-11-07 | 30 s | −0.1689% | −0.1703% |
| 2024-11-07 | 60 s | −0.0274% | −0.0287% |
| 2025-06-18 | 1 s | +0.0143% | +0.0078% |
| 2025-06-18 | 5 s | +0.0526% | +0.0496% |
| 2025-06-18 | 30 s | −0.2021% | −0.2082% |
| 2025-06-18 | 60 s | −0.0843% | −0.0860% |

This tiny convenience sample is insufficient for significance, out-of-sample validation or a profitability claim. It does show why **publication time cannot be treated as a fill**: the sign and size of a later price change can change with only a few seconds' delay. A first November run accidentally retained September's focus parameter despite changing the date range; its mismatched log was rejected and the November run was repeated with the correct focus. The CLI enforces the verified source/focus match, complete Demo-backtest log markers and zero invalid/crossed/backward quote counters. That failure is precisely why timestamps and provenance belong in the contract.

Next research gate: collect a larger, predeclared FOMC sample and **measure** official-source receipt and processing latency in shadow mode, then compare across held-out events with realistic execution costs. Until then, no paper orders or live orders follow from these labels.
