# Event → Thesis Memory → Position

This document describes the overall design. Durable SQLite thesis storage, memory-only DeepSeek reasoning and a first Fed RSS polling adapter are implemented; see [memory stage](memory.md), [reasoning stage](reasoning.md) and [Fed intake stage](fed-intake.md). Portfolio, risk and execution interfaces remain future obligations.

## Core objects

| Object | Meaning |
|---|---|
| Event | Immutable source evidence with stable identity, provenance and source/receipt timestamps |
| FilterResult | Versioned accept/ignore/quarantine result and explanation |
| Thesis | Versioned narrative, affected instruments, supporting/contradicting event IDs, confidence, invalidation conditions and review deadline |
| MemorySnapshot | One committed revision per thesis, at a known memory version and cutoff |
| PortfolioSnapshot | Observed quantities, account equity/currency and outstanding order references |
| MarketSnapshot | Identified snapshot of received bid/ask observations |
| AgentContext | Point-in-time evidence, thesis memory, portfolio and market state |
| AgentDecision | Identified/versioned reasoning result, thesis changes and optional target |
| TargetPortfolio | Complete desired signed exposure linked to theses and the observed portfolio snapshot |
| RiskAssessment | Expiring trusted approval/rejection, linked to decision, target and checked account snapshot |
| ExecutionReceipt | Shadow intention record; not an order fill or an updated position |
| AuditRecord | Structured lifecycle outcome with event and decision trace IDs |

Thesis confidence expresses the agent's belief, not a calibrated probability of profit. Link validation must confirm cited evidence exists and was available to the decision. Source authenticity is checked outside the model. Thesis IDs remain stable across revisions; source event IDs, decision IDs, target IDs, risk IDs and execution IDs must be traceable end to end.

## Minimal interfaces

| Module | Interface | Contract |
|---|---|---|
| events | EventSource.stream | Future general async stream abstraction |
| fed | FedMonetarySource.fetch_items / fetch_article | First poll-only adapter; HTTP retry, pacing, source URL checks and stable IDs |
| fed | FedInbox / FedWatcher.poll_once | Baseline, durable pending/quarantine states, deduplication and optional memory-only reasoning |
| events | EventFilter.evaluate | Future cross-source semantic deduplication and provenance checks |
| memory | ThesisMemory.record_event / snapshot / decision_for_event / commit | Durable evidence and atomic, version-checked thesis history |
| reasoning / deepseek | MemoryReasoner.process / JSONModelClient.complete_json | One event to validated memory-only proposal through an interchangeable model client |
| decision_protocol | ResearchPositionProposal / ResearchProposalAgent.propose | Typed target-direction proposal linked to supplied thesis revisions; future adapter only |
| decision_protocol | DecisionReplayProtocol / check_proposal | Pinned experiment controls and pure supplied-context checks; no execution authorization |
| historical_context | build_historical_context | Separate assumed replay clocks, commit-time memory selection and allowlisted historical model inputs |
| replay_inference | ReplayInferenceRunner.run | Two-stage run-local memory/proposal reasoning with frozen metadata and actual processing-budget checks; no execution |
| series_replay | SeriesRunner.run / SeriesStore | Frozen chronological catalog, transactional research memory/results and paid-call journal; safe event-boundary resume |
| latency | processing_latency | Observed receipt/start/completion trace; source age is not network latency |
| research_data | ResearchWindow / parse_probe_window | Offline provenance and bounded Demo snapshots; no historical live receipt claim |
| research_replay | replay_roundtrip / accounting_grid | Fixed-position cost/timing diagnostics, independent of agent and future execution |
| agent | TradingAgent.decide | Future full portfolio decision behind a provider-neutral boundary |
| portfolio | PortfolioState.snapshot / TargetValidator.validate | Authoritative account state; validate complete desired exposures |
| risk | HardRiskEngine.assess | Independent checks against account state, market data and operator policy |
| execution | ShadowExecutor.record | Idempotent recording of current risk-approved intent only |
| runtime | EventRuntime.handle / AuditSink.append | Future lifecycle coordination and append-only audit |

SQLite provides durable memory and the Fed collector inbox. The synthetic demonstration uses predetermined decisions; the separate `reason` CLI or Fed watcher can call DeepSeek and commit only valid thesis changes. There is no fake risk approval or fabricated fill.

## Beliefs and positions are separate

A thesis may exist without a position; one thesis may support several positions, including a hedge. A position may reflect several theses. Invalidation changes memory first. Any resulting exposure adjustment still passes independent portfolio and risk checks.

Targets use signed account-equity notional weights, not buy/sell commands or broker lot counts. Positive is long, negative is short, zero is an explicit exit. Every currently held instrument must be represented in a complete target. A missing held instrument is invalid, not an implicit liquidation. `target=None` means no portfolio change. A hedge is an additional instrument target linked to the relevant thesis. Broker units, contract multipliers, currency conversion, rounding and pending-order handling belong to future execution planning.

The foundation schema intentionally does not hard-code an instrument universe or a leverage limit. Instrument eligibility and exposure constraints must come from a deterministic configured risk policy. Merely constructing a `RiskAssessment(outcome="approve")` is not authorization: only the trusted runtime's own risk-engine result may be passed to execution. No executor is implemented in this release.

## Time, concurrency and persistence

All modeled timestamps reject naive datetimes and normalize aware inputs to UTC. The source clock and receipt clock remain distinct. Agent start/completion times and provider/model/prompt versions are stamped by the adapter, not supplied as authoritative model text. Runtime validation must enforce context cutoff <= reasoning start <= completion < expiry, and verify all event/target/snapshot references before accepting a proposal. Current schema checks cover local ordering; cross-object orchestration checks are deferred with the runtime.

Initial runtime design: serialize portfolio decisions in one process. Memory still uses compare-and-swap revisions to detect stale state. A commit atomically appends decision and changed thesis revisions. Identical repeated decision IDs are idempotent; conflicting reuse fails. Store both revision effective time and actual commit time so historical snapshots cannot reveal information before it was committed. Never overwrite earlier thesis revisions.

Persist cycle progress separately: received → filtered → reasoned → memory committed → risk approved/rejected → shadow recorded. Memory commit does not mean execution happened. A crash between stages must resume from durable progress and fresh state, not duplicate an order or reason against an obsolete snapshot. A future durable outbox/ledger can coordinate memory and execution without making external calls part of a database transaction.

## Future failure behavior

- Malformed or unsupported model output: record failure, retain evidence, create no target.
- Timeout: bounded retry with the same operation identity; never synthesize a trade.
- Memory conflict: reload and reason again; do not merge portfolio targets blindly.
- Risk rejection: retain valid beliefs and log the rejection; do not execute.
- Stale market/account data, database unavailable or kill switch: stop new execution.
- Restart: reload durable memory and reconcile observed/pending positions before new action.
- Expired thesis review time: schedule reassessment; expiry is not a fabricated fill.

Before any executor exists, deterministic risk implementation must test per-asset/gross/net exposure, losses, stale data, spread, order rate and emergency stop behavior. Shadow execution must model costs and latency explicitly. Live credentials, broker adapters, model training, RL and a full backtester are outside this rebuild.

## Current stage and next review

The SQLite implementation and synthetic CLI exercise thesis creation, reinforcement, invalidation, exact duplicate handling and restart recovery. The DeepSeek memory-only path can propose create/update/ignore, but its output is checked against the current event and existing thesis IDs before commit. Offline tests cover this path; real provider behavior must be checked separately with a user-owned key. The Fed adapter validates its official allowlist; general cross-source verification, semantic duplicate detection, portfolio decisions, risk and shadow execution remain later stages.

v0.7 adds a separate [offline accounting ledger](research-replay.md), not an implementation of `ShadowExecutor`. It tests fixed EUR/USD exposures with assumed timing/costs and observed market snapshots. It never reads a target portfolio, calls the model or submits orders. All eight predeclared Demo windows pass endpoint/accounting checks; the [quality report](fomc-2025-data-quality.md) preserves sample gaps and unresolved zero-spread cost concerns. Continuous-path quality, executable costs and fresh-event latency measurements remain acceptance gates before strategy claims.

v0.8 adds assumed spread floors, per-fill minimum commissions and typed Demo probe metadata/counters. These are [offline cost/path safeguards](cost-path-check.md), not verified execution costs, a live data adapter or a risk implementation. Raw quotes stay immutable. The one dense replay does not enlarge the independent event sample.

v0.9 defines [research position proposals and frozen rules](decision-protocol.md). This separate contract does not change SQLite's memory-only target prohibition or the existing model prompt. Offline checks may derive fixed signed research units but neither commit a target nor call a model, risk engine or executor. A future historical context adapter must preserve real capture times separately from simulated availability; the current `AgentContext` cannot silently turn a 2026 archive capture into a 2025 decision input.

v0.10 implements that separate [historical input adapter](historical-context.md). It keeps original archive clocks in an audit envelope and explicitly assumed availability in a model view. Quotes and run-local memory are selected at a pinned cutoff without future prices or uncommitted revisions. There is no conversion into a live `AgentContext`, model call, actual inference trace, target dispatch or fill. The v1 decision manifest remains unchanged.

v0.11 connects the JSON client to this historical view for [one-event research inference](replay-inference.md). It preserves actual API clocks, run-local hypothetical memory commits, a later portfolio cutoff and retained failures/abstentions. It neither writes live SQLite targets nor dispatches positions. A [real acceptance case](llm-replay-case.md) passed only the slow predeclared processing budget; it did not satisfy the primary budget or validate alpha. Catalog orchestration, complete hard risk, fill integration and live shadow remain later work.

v0.12 adds the [chronological research coordinator](chronological-replay.md) with a separate SQLite registry, immutable case results and atomic run-local revisions. Every paid request is reserved before sending; unknown in-flight requests block automatic retry. The [real eight-event series](fomc-series-acceptance.md) demonstrates clean pause/resume and seven memory revisions, but only three portfolio proposals pass evidence checks. Output reliability and the primary latency budget fail; no risk/strategy/execution gate is passed.
