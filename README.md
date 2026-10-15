# EventLens

**News → Analysis → Decision → Hard Risk → Intraday Position**

EventLens is a Python foundation for an LLM-driven intraday trading system using existing model APIs. The intended workflow interprets financial events alongside market expectations, current prices, positions and relevant thesis memory. Positions would last minutes to hours and close within the configured trading day.

**Status: foundation and historical research tooling, v0.13. No automated trading.** This release reorganizes existing code and commands; it adds no live news provider, market connection, trading loop or broker execution.

## Implemented and planned

| Area | Current capability | Still needed |
|---|---|---|
| Information | Official Fed FOMC RSS polling, raw text, publication/receipt clocks, durable inbox | General financial news/calendar adapters; later social media |
| LLM | Replaceable JSON client, DeepSeek adapter, validated memory updates | Reliable realtime analysis and position decisions |
| Memory | SQLite event evidence, immutable thesis revisions and decision history | Bounded selection of relevant intraday context |
| Market data | Offline Bid/Ask decoding and Demo historical-data diagnostics | Live quotes and authoritative position/account state |
| Research | Historical input cutoffs, opt-in LLM replay, cost/delay accounting and recovery | Integrated strategy evaluation and forward shadow observations |
| Trading | Typed portfolio, risk and execution interfaces | Hard risk, stops, daily close, shadow fills and a cTrader Demo adapter |

News types are research candidates, not a fixed CPI/NFP/FOMC whitelist. A future source adapter may cover macro, policy, energy, geopolitical and company events. Historical EUR/USD × FOMC experiments remain narrow, frozen studies; they do not define the future instrument or news universe.

## Intended minimal workflow

```text
News / calendar adapters → deduplicate and group events → LLM analysis and decision
                                                        ↑
                                  current quotes, positions and relevant thesis memory
                                                        ↓
                                     deterministic risk → shadow / Demo execution

Independent market monitor and timer → protective exits and daily close
Every stage → SQLite evidence, decisions, failures and evaluation
```

One process and SQLite are sufficient initially. Analysis and decision may use one model call or two isolated roles; their reliability, latency and cost must be compared before choosing. LLMs have no broker credentials and cannot override operator risk limits. Protective exits must not depend on model availability. No model training, RL, microservices or multi-agent framework is required.

**Timing goal, not a measured guarantee:** where conditions permit, complete the future decision/order workflow within 30–60 seconds of the event's first verified public release. Keep original release time, publisher display time when available, actual receipt, analysis, submission and fill clocks distinct. Unknown original publication time must remain unknown. The existing Fed watcher and research CLIs do not meet or implement this end-to-end target.

## Run existing tools

Python 3.11+:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
eventlens --help
eventlens research decision check examples/synthetic-position-proposal.json
eventlens research infer plan examples/synthetic-inference-case.json
eventlens research series plan examples/synthetic-series-plan.json
```

These `check`/`plan` commands are offline and do not call models, brokers or create trades. Optional model runs still require credentials and the existing explicit opt-in; see [reasoning](docs/reasoning.md) and [research inference](docs/research/replay-inference.md). Never put credentials in chat, source files or Git.

`eventlens memory …` exposes the memory tools. `eventlens source fed --help` describes the existing poll-only collector; running the collector accesses the public Fed source. There is no `observe` command yet. All eight older `eventlens-*` commands and their options remain available. Research config paths, local database formats and frozen experiment content remain unchanged.

## Repository

```text
src/eventlens/
├── contracts.py, deepseek.py, latency.py  # Shared schemas, model adapter and timing
├── memory.py, sqlite_memory.py           # Durable thesis/evidence storage
├── reasoning.py                         # Existing memory-only LLM path
├── sources/                             # Event interfaces and Fed adapter
├── trading/                             # Agent, portfolio, risk, execution interfaces
├── research/                            # Historical quotes, labels, replay and experiments
├── runtime.py                           # Future realtime coordinator interface
├── cli.py                               # Lazy grouped command dispatcher
└── *_cli.py                             # Compatible existing command implementations
tests/                                   # Shared, sources/ and research/ tests
configs/                                 # Existing pinned research configs; paths retained
docs/                                    # Architecture, roadmap, operation and research/
ctrader/                                 # Read-only Demo backtesting probe
scripts/, notebooks/, examples/           # Research utilities and synthetic fixtures
data/                                    # Local ignored evidence; never committed
```

Python imports for relocated modules now use `eventlens.sources.*`, `eventlens.trading.*` and `eventlens.research.*`. Repository callers have been updated; old Python import paths are not aliases. Console commands and `python -m eventlens.<existing>_cli` remain compatible. See the [module map](docs/architecture.md).

## Evidence and next step

The latest historical series completed eight FOMC episodes with restart-safe bookkeeping. Only **3/8** position replies passed evidence checks, all flat; five were rejected. Mean two-stage processing was **7.443 seconds**, excluding live feed and order delivery. These are integration results, not forecast accuracy or profitability. Demo quote gaps, unresolved zero spreads and assumed trading costs also limit the existing evidence. See [acceptance](docs/research/fomc-series-acceptance.md) and [market-data limitations](docs/research/fomc-2025-data-quality.md).

The next milestone is a **realtime observer**: capture broad financial events with quotes, produce evidence-linked decisions without orders, and inspect actual latency and subsequent prices. News/provider selection remains open. It is not implemented by this reorganization. See the [roadmap](docs/roadmap.md), [architecture](docs/architecture.md) and [historical implementation snapshot](docs/research/implementation-history.md).

Local checks: `pytest -q`, `ruff check .`, `ruff format --check .`, `mypy src scripts`. CI uses synthetic/offline checks, never paid model or broker calls. Results must distinguish output validity, predictive quality and after-cost trading performance.
