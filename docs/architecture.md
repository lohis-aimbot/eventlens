# Proposed architecture

This document defines the intended monorepo layout. Packages will be added when implemented rather than populated with placeholder functionality.

```text
apps/
  cli/                  Research and replay entry point
packages/
  core/                 Event, signal, order and position contracts
  ingestion/            Source adapters and receipt-time tracking
  parsing/              Rule and LLM event parsers
  storage/              Event repository and schema migrations
  research/             Historical retrieval and event studies
  signals/              Decision policies and thesis updates
  risk/                 Position limits and execution gates
  backtesting/          Clock-driven replay and simulated fills
  execution/            Paper and cTrader adapters
tests/                  Unit and integration tests
examples/               Synthetic fixtures and reproducible workflows
docs/                   Design and research methodology
```

## Contracts and boundaries

- Ingestion returns source records with provenance, publication and receipt times, without issuing trading instructions.
- Parsing returns validated structured events with evidence spans and explicit missing or uncertain fields. Source text is data, never executable instructions.
- Storage supports consistent repository operations across SQLite and PostgreSQL.
- Research separates event timing, estimation windows, forward returns and benchmark-adjusted results.
- Signals consume events, point-in-time market state and current positions; abstention is a valid decision.
- Risk checks enforce exposure, sizing, freshness and duplicate-order constraints independently of the LLM.
- Execution reconciles submitted orders with broker acknowledgements, fills and actual positions. Reconnects must not blindly resubmit orders.
- Replay uses a controllable clock and cannot access future source records or revised values before their availability time.

## cTrader boundary

The Python application is intended to connect through cTrader Open API rather than automate the app UI. The first connection will target a Pepperstone cTrader demo account. Instrument identifiers, contract sizes, minimum order sizes, trading sessions and account mode must be discovered from the target account instead of assumed.

cTrader cBot backtesting is an optional later integration. It is not a substitute for collecting and replaying point-in-time news and social data.
