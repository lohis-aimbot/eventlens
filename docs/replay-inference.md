# One-event LLM replay inference — v0.11

This stage connects **Event → Thesis Memory → research direction proposal** through
the existing JSON model interface. It uses existing LLM APIs, not model training.
It does not create live targets, write SQLite memory, simulate fills, implement hard
risk or connect a broker. Every result has `execution_authorized=false`.

## Two calls, two context cutoffs

1. Validate archived inputs and construct the initial historical view at assumed
   event availability. Only the allowlisted context and output schema are sent.
2. Ask for a memory create/update/ignore proposal. Reuse the existing pure evidence
   and revision checks; only EUR/USD is permitted. Parser failures stop the cycle.
3. Stamp a run-local revision after content validation, preserving original archive
   clocks and actual recording separately from hypothetical update/commit clocks.
4. Build a new portfolio context at the mapped elapsed processing time. Select fresh
   quotes and only memory committed by that cutoff, including a valid new revision.
5. Ask for a `ResearchPositionProposal`. Validate exact current-event excerpts and
   eligible, exact thesis revisions. Quantities come from the fixed research protocol.
6. Record actual API/processing evidence and hypothetical decision completion. Check
   the pinned scenario budget; an over-budget proposal is never a successful result.

Ignore does not automatically mean flat. The portfolio call must independently return
a valid explicit proposal. A valid memory update may remain recorded when portfolio
reasoning fails; it does not authorize a position. Memory-only live CLI/SQLite behavior
is unchanged and continues to reject non-null targets.

## Frozen metadata and bounded failures

`configs/replay-inference-v1.json` publishes the default request configuration.
`freeze_manifest` binds the complete case hash, v1 decision protocol, provider/model,
endpoint, timeout, decoding settings, output schemas, prompt versions and exact hashes
before inference. Changed inputs/prompts/settings produce a new experiment ID.

The current client requests `deepseek-flash`, JSON output, thinking disabled,
1,200 maximum output tokens and non-streaming responses. Temperature is omitted and
recorded as provider default; responses are not claimed to be deterministic. A serving
alias is not proof of an underlying model release: returned model/fingerprint metadata
are kept per attempt when provided.

Hidden client retries are prohibited. The runner owns at most one retry per stage
for transport/timeout or explicitly transient provider errors, with a 500 ms wait.
Each attempt has actual UTC start/end, monotonic duration, exact user-input hash,
outcome and raw successful reply/usage. Authentication, malformed semantic output and
invalid evidence are not retried. No successful response is selected by profitability.
Failed provider attempts may have unknown billing; missing usage is not zero cost.

The CLI freezes the manifest and prompt snapshots before any model call, uses a
dedicated new output directory and refuses to overwrite/resume an existing one.
A crash can leave a manifest without a result: billing/completion is then unknown.
Automatic crash recovery or cross-directory exactly-once inference is not implemented.
Changing the run ID/directory deliberately permits a new billed experiment.

## Actual time versus hypothetical time

Processing begins before the initial context construction. A monotonic clock measures
context preparation, model calls, retry waits and validation. Elapsed time maps forward
from assumed event availability into the hypothetical timeline. Actual API timestamps
and archive receipt/capture times remain real UTC values in the audit envelope.
Pre-run source acquisition, case loading, manifest freezing and final audit-file writes
are outside this processing timer; this is not end-to-end live feed latency.

The portfolio cutoff is stamped after memory processing, not backdated to publication.
No decision completion is moved backwards to fit a scenario. Memory exceeding the
budget stops before portfolio inference. A portfolio response exceeding the budget is
retained as a non-actionable `budget_exceeded` case. Missing/stale quotes, clock
regressions and invalid outputs also prevent success. Clock anomalies keep observed
evidence but cannot pass any processing-budget diagnostic.

Budget diagnostics compare **processing duration only** with all predeclared budgets.
They do not generate alternative decisions or authorize reuse at different cutoffs.
Passing this gate does not replace the ledger's pinned stage delays with measured
processing time. Later fills must respect both scheduled readiness and completed
decisions; changing the fixed fill schedule requires a new protocol version.
`proposal_check` means schema and supplied-context validation, never hard risk, source
truth, calibrated probability, order readiness or permission to execute. Neither its
presence nor a `validated_proposal` status is sufficient to trade.
Quote freshness here applies at the input cutoff; later hard risk must obtain fresh
market/account state after inference, before any execution.

## Offline plan and opt-in inference

```bash
# Offline; no key, network, database or model needed.
eventlens-infer plan examples/synthetic-inference-case.json

# Explicitly paid research; use a new directory and a locally configured key.
eventlens-infer run path/to/case.json.gz --call-model --output data/new-identified-run
```

The example is invented, not an actual Fed statement or forecasting result. JSON and
gzip case files are accepted. An existing environment can use
`python -m eventlens.inference_cli` without reinstalling its entrypoint. The key stays
in the environment or OS credential store and never enters cases, prompts or Git.
Original market windows remain local; results are compact gzip JSON, not repeated
raw tick exports. The CLI defaults to offline `plan` usage unless `run --call-model`
is explicitly selected.

## Acceptance

**168 tests** pass, with Ruff formatting/lint, mypy and package build. New tests cover
two-stage context transitions, serving metadata, measured cutoffs, retry accounting,
hard budget boundaries, retained over-budget outputs, invalid/unknown references,
exact case-sensitive evidence, inactive/expired thesis eligibility, ignored events,
stale/missing quotes, future-price exclusion, oversized inputs, frozen manifest/client
consistency, clock anomalies, bounded timeouts, credential-free plans and opt-in/
overwrite guards. Existing live memory-only behavior remains covered.

A [real one-event API acceptance](llm-replay-case.md) records both a rejected initial
experiment and a later prompt revision. These are integration observations, not a
strategy backtest or evidence of alpha. There is no PnL, order or fill in either run.

## Remaining boundaries

The context builder validates clocks/references, not the truth of arbitrary narrative
claims. Prompt constraints reduce unsupported inference but cannot establish factual
grounding; source validation and independent evaluation remain necessary. Backfilled
article vintage and pretrained knowledge of historical outcomes remain limitations.

This is a single-event runner, not a catalog-wide experiment manager or persistent
real-time portfolio agent. Run-local revisions may be supplied to later chronological
events, but trusted multi-event persistence/recovery is not implemented here. The next
research step is a frozen chronological catalog runner retaining all failures and
abstentions. Hard risk and offline ledger integration require separate acceptance;
live shadow and cTrader Demo execution remain later stages.
