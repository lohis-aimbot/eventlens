# Research position proposal and fixed replay rules — v0.9

This stage defines the boundary between **Thesis Memory** and a future **target-position proposal**. It adds a typed model-response contract, pinned experiment manifest, pure context checks and offline CLI. It does not call an LLM, update memory, implement hard risk, generate fills or connect to cTrader. The existing memory-only reasoner still rejects portfolio targets.

## Model output

The exact response JSON schema is published in [position-proposal.schema.json](../examples/position-proposal.schema.json). The model may supply only:

| Field | Meaning |
|---|---|
| target_direction | `long`, `short` or `flat` for the one eligible research instrument |
| thesis_references | Specific thesis IDs and revisions from the supplied memory snapshot |
| evidence_quotes | One to five distinct, nonblank, verbatim excerpts from the current event |
| rationale | Explanation of the proposed direction or abstention |
| confidence | Finite numeric belief score in [0,1], not calibrated probability or an approval |
| invalidation_conditions | Conditions that would invalidate a directional belief |

Non-flat proposals require at least one thesis reference and one invalidation condition. References must match the supplied revision; cited directional theses must be active, unexpired and associated with EUR/USD. Flat may cite an invalidated/resolved thesis or no thesis, but still needs current-event evidence and a reason. Flat is intentional abstention in the initial flat-account experiment. A parser failure is a rejection, never a guessed flat prediction or successful decision.

The model cannot specify instruments, quantities, weights, latency, costs, horizons, clock stamps, model identity, order objects or risk approval. Extra fields are rejected. IDs, versions and future inference times must be stamped by application code outside the response. `ResearchProposalAgent.propose` is a provider-neutral interface only; no adapter is implemented yet.

## Fixed experiment manifest

[decision-protocol-v1.json](../configs/decision-protocol-v1.json) pins the following rules. These were fixed **before any target-position LLM run**, after the exploratory data/cost checks already documented in this repository. They are not a claim that the historical data or outcomes were untouched.

| Rule | v1 value |
|---|---|
| Mode | Offline research only |
| Catalog / instrument | All eight scheduled 2025 FOMC statements; EUR/USD |
| Initial USD collateral / position | 10,000 USD; flat, no pending orders |
| Directional size | Exactly +1,000 or −1,000 EUR; flat means zero |
| Horizon | 300 seconds from signal time, followed by order/execution delay |
| Input cutoff | At the start of portfolio reasoning; earlier committed memory and available quotes only |
| Entry quote | First observed quote after decision, signal, order and execution readiness; maximum two-second wait |
| Primary delay sensitivity | `poll_plus_reasoning_assumed`: 65 seconds total source-to-entry readiness in the current ledger scenario |
| Other delay sensitivities | Five seconds and 120 seconds; every stage retained in the manifest |
| Primary cost | `floor_1`: 1 pip minimum spread, 0.5 bps USD-notional commission and 0.25 bps adverse slippage per fill |
| Other cost sensitivities | All five predeclared spread floors: 0, 0.2, 0.5, 1, 2 pips; identical fees/slippage |
| Context quote age / fill quote wait | At most 2,000 ms each |
| Path-gap diagnostic limit | 3,000 ms; retained without interpolation or loosening |
| Within-position management | None: no adding, reducing, reversal, hedge, trailing stop or gap-triggered exit in v1 |
| Benchmarks | Flat, fixed long and fixed short; always report all |

These are research assumptions, not broker-confirmed prices or charges. Fixed size is an experiment control, not a live sizing recommendation. Five-minute exit is an operator rule: the model's invalidation text is recorded for future work but does not trigger intraperiod execution. The initial experiment starts each event flat; thesis memory may persist chronologically. It does not yet exercise continuous portfolio management across events.

Changing pinned amounts, cost profiles, delays or baselines is rejected under v1. A changed experiment requires a new version and manifest; the code does not silently tune a v1 run. `protocol_id` hashes the complete serialized manifest. Context and proposal IDs include the supplied context, complete proposal and protocol hash. The exported schema/manifest are tested against code and the existing cost configuration.

## Supplied-context checks

`check_proposal` revalidates serialized objects before checking references, including objects modified through Pydantic's unchecked `model_copy`. It rejects future or naive timestamps, missing/stale EUR/USD quotes, unsubstantiated evidence, obsolete thesis revisions, nonflat starting holdings, wrong research currency/equity and pending orders. Signed research quantity comes from the fixed protocol, never the model.

The result always says `validation_scope=schema_and_supplied_context_only` and `execution_authorized=false`. It is neither `RiskAssessment` nor a `TargetPortfolio` committed to memory. Passing these checks does not authenticate the source, prove snapshot truth, enforce all risk limits, schedule an order or create a position. Future integration must use trusted collector/store snapshots and independent hard risk. A manually constructed check record is never execution authority.

## Time and historical replay requirements

The current contract checks an `AgentContext`; the sample uses an invented clock. A genuine archived statement captured in 2026 cannot be placed in a 2025 `AgentContext` by ignoring its receipt time: this is rejected in tests. The future historical runner must preserve original source and capture timestamps and represent assumed historical availability in a separate, explicit replay context. It must not overwrite real receipt times, import a 2026 thesis revision as 2025 memory or present historical capture as measured live delivery. That adapter/clock is **not implemented in this stage**.

Future orchestration must separately record actual API start/completion times and hypothetical replay times. Total reasoning includes all required memory and portfolio calls, retries and validation. No simulated signal/fill may precede completion of the decision that produced it. If observed processing cannot fit a fixed scenario's budget, mark that scenario unavailable/rejected and retain the timing evidence; never move completion backwards or claim the same budget was measured historically.

Model inputs must exclude future prices, labels, post-event outcomes and future memory revisions. Even a correctly filtered context cannot prevent a pretrained model from already knowing a historical event's outcome. The eight cases are a small, previously inspected research sample. Historical replay can validate workflow and initial behavior, not establish predictive alpha or honest unseen-test performance. Future live shadow evidence remains required.

## Evaluation and failure rules for the later runner

Run the complete catalog chronologically, retaining every event and failed case. Freeze provider/model metadata, prompt version and decoding/retry settings before collecting decisions. A changed prompt/model/configuration starts a new identified experiment; never overwrite earlier outputs or select the most profitable response. Use the same input for cost sensitivities so cost comparisons do not select different model outputs. Inputs at different availability cutoffs may require distinct decisions, each with its own context ID.

Report primary and all sensitivity results, benchmark comparisons, failure/abstention counts, quote gaps and cost assumptions. Do not combine repeated scenarios into additional independent events. Invalid response/context means rejected proposal with a recorded reason, not a trade or manufactured zero-return closed case. Missing exits retain the ledger's open/unavailable state. All after-cost PnL remains hypothetical until the separate data/execution gates pass.

## Offline use and acceptance

```bash
eventlens-decision schema
eventlens-decision protocol
eventlens-decision check examples/synthetic-position-proposal.json
```

The CLI reads or prints JSON only; no database, API, model, broker or order calls occur. For an existing development environment, the commands are also available through `python -m eventlens.decision_cli`. The sample event, thesis, quotes, confidence and short direction are **invented fixtures**. No actual FOMC statement was evaluated by an LLM here, and the output is not a trading recommendation.

Local acceptance: 102 pytest tests pass; Ruff and mypy pass. New checks cover exact schema/manifest exports, fixed signed units, invalid model controls, finite confidence, evidence and revision errors, future/stale context, inactive theses, initial portfolio requirements, deterministic IDs, changed-context IDs and rejection of historical capture-time substitution. Existing memory-only behavior remains covered by its original tests.

Stop after this foundation. Next stage is to design the explicit historical context/clock adapter, then connect the existing JSON model client to proposals and record inference provenance. The complete hard-risk engine and fill integration require separate implementation and acceptance; no target is dispatched by this release.
