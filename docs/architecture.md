# Architecture and module map — v0.15

The target is a small LLM-driven intraday system: financial events plus expectations,
quotes, positions and relevant thesis memory lead to a position proposal, independent
hard risk and eventually shadow/Demo execution. News categories and instruments are
configurable research choices. Existing FOMC experiments retain their original scope.

## Current boundaries

| Location | Implemented behavior |
|---|---|
| `contracts.py` | Immutable typed events, UTC clocks, theses, market/portfolio snapshots, decisions and risk/execution records |
| `deepseek.py` | Provider-neutral JSON client contract and DeepSeek HTTP adapter |
| `memory.py`, `sqlite_memory.py` | Memory contract, raw event evidence, immutable revisions, atomic commits, conflict and recovery checks |
| `reasoning.py` | Validated create/update/ignore memory decisions; no portfolio targets |
| `observer.py` | Provider-neutral capture contracts, dedicated SQLite payload/revision storage and cutoff-aware history |
| `sources/wallstreetcn.py` | Bounded news/calendar website polling, normalization, rejection records and retry/pacing |
| `quotes.py`, `quotes_cli.py` | Dedicated Demo quote store, quality reporting and retrospective news/quote cutoff checks |
| `sources/ctrader_demo.py` | Fresh local-file handshake, bounded tailing and quote validation; no order methods |
| `ctrader/EventLensQuoteBridge.cs` | Sampled Demo EURUSD real-time snapshots, sandbox files and automatic stop |
| `sources/events.py` | General source/filter interfaces only |
| `sources/fed.py` | Official Fed polling, URL validation, retry/pacing, durable inbox and optional memory reasoning |
| `research/` | Historical ticks, source/capture provenance, cutoff selection, replay labels, fixed-position costs, opt-in model replay and durable call journal |
| `trading/` | `TradingAgent`, `PortfolioState`, `TargetValidator`, `HardRiskEngine`, `ShadowExecutor` interfaces only |
| `runtime.py` | Future runtime/audit interfaces only; no working event-to-order loop |
| `cli.py`, `*_cli.py` | Grouped dispatcher and existing explicit commands |

Shared modules, sources and trading interfaces do not import research modules. Research
may reuse shared schemas/model/memory logic. Importing a package or requesting top-level
help starts no watcher, database, model or broker. CLI dispatch imports only the selected
command. Two LLM roles can be ordinary isolated calls; no agent platform is needed.

## What was moved

| Former root module | Canonical location |
|---|---|
| `events`, `fed` | `eventlens.sources.<name>` |
| `agent`, `portfolio`, `risk`, `execution` | `eventlens.trading.<name>` |
| `market_data`, `ctrader_ticks`, `market_quality`, `replay_labels` | `eventlens.research.<name>` |
| `research_data`, `research_replay`, `decision_protocol`, `historical_context` | `eventlens.research.<name>` |
| `replay_inference`, `series_replay` | `eventlens.research.<name>` |

Old Python imports of moved modules must be updated; no compatibility module aliases
are installed. The eight older console entrypoints and `python -m` CLI modules keep
their arguments and behavior. The grouped command tree is:

```text
eventlens observe <capture, demo, report or history>
eventlens observe quotes <watch, report or align>
eventlens memory <existing memory arguments>
eventlens source fed <existing collector arguments>
eventlens research accounting <existing eventlens-research arguments>
eventlens research market <existing eventlens-market arguments>
eventlens research labels <existing eventlens-replay arguments>
eventlens research decision <existing eventlens-decision arguments>
eventlens research infer <existing eventlens-infer arguments>
eventlens research series <existing eventlens-series arguments>
```

The observer has a separate SQLite format and does not migrate memory/replay databases.
Its `CaptureSource` boundary returns source observations rather than mislabeling future
calendar schedules as published `Event` objects. Read [observer timing](observer.md). The separate [Demo quote path](live-quotes.md) does not
change existing historical databases or grant execution approval.

Config paths stay under `configs/` to preserve existing commands and local workflows.
The organization step changed no existing database format, prompt, schema, cost/latency
rule or historical experiment. The new observer likewise leaves those contracts unchanged. Proposal schemas and experiment identities
are checked against a pre-move snapshot. Evidence and data stay out of Git.

## Runtime design constraints, not implemented features

An adapter persists source evidence before inference. Macro releases need a pre-release
forecast snapshot and explicit actual/previous/revised values; zero is a value, not
missing data. Related headlines are grouped without losing their individual provenance
or arrival times. Publisher display time is not assumed to be first public release.

Analysis receives only information available at its input cutoff. The decision role
also receives fresh quotes, observed positions/pending orders and bounded relevant
theses. A response is a proposal, never a fill or risk approval. Validate evidence IDs,
model/prompt version, decision expiry and snapshot identity. Invalid/late replies create
no new position; valid beliefs may survive a rejected trade. The existing evidence-quote
interface still needs repair; moving it does not fix the known failures.

LLMs may suggest direction, position changes and bounded risk parameters. They cannot
hold broker credentials, edit operator risk limits or certify their own approval.
Deterministic code must enforce position/loss/spread/freshness limits, order deduplication,
reconciliation, kill switch and session-aware daily close. Protective exits and timers
operate independently of LLM availability. Confirm a close through execution feedback;
an intention to close is not a closed position.

The future 30–60-second goal concerns the release-to-decision/order workflow under
eligible conditions, not a guarantee of fills. Track publication/display/receipt,
analysis, decision, submission, acknowledgement and fill clocks separately. Unknown
original time remains unknown. Current observed research processing excludes feed/order
latency; older frozen budgets retain their original pass/fail results.

## Preserved research and evidence

`research/decision_protocol.py` still combines the existing proposal contract and pinned
EUR/USD–FOMC v1 rules. A future live schema should be separated deliberately rather than
silently broadening this experiment. No execution approval is granted by schema checks.

Read [memory](memory.md), [reasoning](reasoning.md) and [Fed intake](fed-intake.md) for
existing operations; [research replay](research/research-replay.md),
[historical context](research/historical-context.md),
[series recovery](research/chronological-replay.md) and
[acceptance](research/fomc-series-acceptance.md) for offline behavior. The former README
is retained as a [historical snapshot](research/implementation-history.md).
