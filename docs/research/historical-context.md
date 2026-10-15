# Historical replay context — v0.10

This stage implements the **input boundary**, not a historical trading agent. It builds
an explicit hypothetical replay context without changing the live `AgentContext`, raw
events, SQLite memory, market observations or the v1 experiment manifest. No model,
network, risk, broker, fill or database calls occur.

## Separate clocks and separate views

| Clock | Meaning | Where it appears |
|---|---|---|
| Event `source_timestamp` | Supplied historical publication time | Model view and original evidence |
| Original event `received_timestamp` | Actual archive/local receipt, possibly in 2026 | Audit provenance only; unchanged |
| Window `captured_at` | Actual market backfill capture | Audit provenance only; unchanged |
| Event `assumed_available_at` | Source time + pinned scenario ingest delay | Replay clock and model view; explicitly assumed |
| Quote observation timestamp | Time on the historical sampled Bid/Ask observation | Model view |
| Quote `assumed_available_at` | Observation time + explicit assumed market-feed delay | Model view; not a measured delivery time |
| `portfolio_reasoning_start` | Input cutoff after any prior memory processing | Replay clock and model view |
| `reasoning_deadline` | Event availability + total pinned reasoning budget | Replay clock; not actual model completion |
| Revision `replay_committed_at` | Hypothetical time a run-local thesis becomes readable | Selected-memory audit records |
| Revision `recorded_at` | Actual local generation/recording time | Audit records; never compared to historical cutoff |

The envelope contains `model_input`, `clock`, `provenance` and
`execution_authorized=false`. A future adapter must send **only `model_input`** to its
model, not the envelope or original input file. Full market windows, future prices,
archive capture times, API clocks and return labels are absent from the allowlisted
view. Original evidence and selected revision provenance remain available for audit.

`model_input_id` hashes exactly the serialized model view. Changing a later market
observation can change the audit `market_data_id`, but cannot change an earlier model
input or its ID. This separates full-window provenance from decision inputs.

## Pure builder and selection rules

`HistoricalContextInput` supplies a run ID, current event ID, archived raw events, one
market window, a pinned scenario name, explicit portfolio reasoning start, explicit
assumed market delay, and optional run-local memory history.

`build_historical_context` revalidates all serialized inputs, including unchecked
Pydantic copies, then:

1. Rejects duplicate/unknown event IDs, unknown scenarios, non-macro events, nonmatching
   event/window metadata and cutoffs outside the pinned reasoning interval. Supplied
   official statement references must have the established URL/date/14:00 New York
   shape. This checks metadata, not source authenticity or text correctness. Full
   predeclared catalog enumeration remains a responsibility of the later runner.
2. Derives assumed event availability without rewriting original receipt timestamps.
3. Chooses the **latest** quote delivered by the cutoff under the explicit assumed
   market delay. It never takes the next quote, interpolates a gap or waits for future
   data to construct the current context. Observation age must be at most two seconds,
   including delivery delay; a newly delivered old observation is stale.
4. Validates complete run-local thesis revision chains, evidence references and
   clocks. Evidence must be available at the revision update time; actual recording
   cannot precede receipt of its referenced archive evidence.
5. Selects the latest revision **committed** by the cutoff for each thesis. Backdating
   an update cannot expose an uncommitted revision. Inactive or expired beliefs remain
   visible for later abstention/reasoning; visibility is not directional eligibility.
6. Creates the fixed initial flat USD research portfolio from the pinned protocol.
   No observed broker account or position-management implementation exists here.

Missing/stale market data or malformed histories raise a validation error. They never
become an invented flat decision, zero-return trade or forward-filled price. The later
runner must retain failures. Future valid revisions and uncited future events are
excluded, although all supplied history is checked for structural corruption.

## Replay memory is not live memory moved backwards

`ReplayThesisRevision` is a separate run-local wrapper with
`basis=generated_for_replay`. Thesis creation/update/review clocks describe the
hypothetical replay; actual recording is retained independently. No SQLite import or
live-to-replay conversion is provided. Different-run/scenario revisions, gaps/duplicates,
future evidence and inconsistent commit clocks are rejected.

An unchanged 2026 thesis is not visible at a 2025 cutoff. Moving just its commit time
backwards fails ordering validation. This does not prove a caller's claimed generation
history: the future trusted runner must create records from recorded, cutoff-bound
inference and must not manually relabel live theses. These are typed checks, not an
inference provenance system.

History may persist across chronological episodes within one run. Different
availability scenarios need separate histories when their inputs differ. This builder
checks matching run/scenario names but cannot prove arbitrary supplied history really
used the claimed model, prompt or latency budget.

## Offline example

```bash
eventlens-decision replay-context examples/synthetic-historical-context.json
```

The fixture uses invented text, prices and a thesis. Its official-shaped URL tests
metadata only; it does not reproduce the linked Fed statement.

| Example value | UTC time |
|---|---|
| Source publication | 2025-01-29 19:00:00 |
| Original event receipt | 2026-10-14 12:00:00 |
| Actual window capture | 2026-10-14 12:01:00 |
| Assumed event availability (60 s) | 2025-01-29 19:01:00 |
| Hypothetical thesis update / commit | 19:01:00.800 / 19:01:01 |
| Actual thesis recording | 2026-10-14 12:02:00 |
| Portfolio input cutoff | 2025-01-29 19:01:02 |
| Selected observation / assumed delivery (250 ms) | 19:01:01.500 / 19:01:01.750 |
| Pinned reasoning deadline | 2025-01-29 19:01:04.850 |

The 19:01:02 observation is excluded: assumed delivery occurs at 19:01:02.250.
The 19:06:00 future price is also excluded. The selected thesis is revision 1.
250 ms is an illustrative explicit input, not a measured broker delay.

## Acceptance and limitations

New tests cover archive-clock preservation, UTC normalization, deterministic input
IDs, future-price/event/revision exclusion, commit-time selection, cross-run rejection,
evidence causality, malformed chains, empty/inactive memory, missing quotes, freshness
boundaries, scenario budgets, extra label fields and unchecked-copy revalidation.
CLI smoke checks use the fixture only. Full suite: **142 tests**, Ruff formatting/lint,
mypy and package build.

These checks establish context construction, not historical delivery, authenticity,
unrevised publication text, live side-specific quote freshness, inference performance,
trading correctness or alpha. Archive articles may have been edited after release;
the future adapter must establish content vintage or mark that limitation. Pretrained
LLM knowledge of historical outcomes remains possible despite filtered inputs. No real
statement is evaluated by an LLM here.

## Next stage, separately accepted

Connect the JSON client to memory reasoning and position proposals through this model
view. Freeze model/prompt/decoding/retry metadata first. Record actual API clocks
separately, accounting for all memory/portfolio calls, retries, validation and waits.
The portfolio cutoff must reflect prior memory processing; total processing must fit
the scenario budget. Over-budget cases are unavailable/rejected, never shifted back.
Signals/fills must follow completed decisions. No inference trace, target dispatch,
risk engine or ledger integration is implemented by this release.

**Later-stage update:** v0.11 adds [single-event research inference](replay-inference.md)
using these explicit views and actual processing clocks. This v0.10 builder remains
pure and unchanged. The new runner still provides no risk approval, fills or broker
connection; see the [real API acceptance and retained failure](llm-replay-case.md).
