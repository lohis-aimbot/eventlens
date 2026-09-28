# Event → Thesis Memory → Position

This document describes the overall design. Durable SQLite thesis storage and memory-only DeepSeek reasoning are implemented; see [memory stage](memory.md) and [reasoning stage](reasoning.md). Portfolio, risk and execution interfaces remain future obligations.

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
| events | EventSource.stream | Async event stream; each adapter owns rate limits, retry, logging and stable IDs |
| events | EventFilter.evaluate | Deduplicate and validate provenance; ambiguous sources quarantine |
| memory | ThesisMemory.record_event / snapshot / decision_for_event / commit | Durable evidence and atomic, version-checked thesis history |
| reasoning / deepseek | MemoryReasoner.process / JSONModelClient.complete_json | One event to validated memory-only proposal through an interchangeable model client |
| agent | TradingAgent.decide | Future full portfolio decision behind a provider-neutral boundary |
| portfolio | PortfolioState.snapshot / TargetValidator.validate | Authoritative account state; validate complete desired exposures |
| risk | HardRiskEngine.assess | Independent checks against account state, market data and operator policy |
| execution | ShadowExecutor.record | Idempotent recording of current risk-approved intent only |
| runtime | EventRuntime.handle / AuditSink.append | Future lifecycle coordination and append-only audit |

SQLite provides durable memory. The synthetic demonstration uses predetermined decisions; the separate `reason` CLI can call DeepSeek and commit only valid thesis changes. There is no fake risk approval or fabricated fill.

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

The SQLite implementation and synthetic CLI exercise thesis creation, reinforcement, invalidation, exact duplicate handling and restart recovery. The DeepSeek memory-only path can propose create/update/ignore, but its output is checked against the current event and existing thesis IDs before commit. Offline tests cover this path; real provider behavior must be checked separately with a user-owned key. Semantic duplicate detection, source verification, portfolio decisions, risk and shadow execution remain later stages.
