# Chronological FOMC research replay — v0.12

The single-event inference boundary now has a chronological coordinator and a
separate SQLite research registry. This stage tests memory continuity, durable
results, paid-call bookkeeping and safe restart. It does not implement positions,
risk, PnL, a strategy backtest or broker execution.

## Fixed inputs and chronology

`SeriesPlan` binds a run ID, all eight episodes in the exact predeclared 2025 catalog,
one pinned latency scenario, an explicit assumed market delay and a call cap (maximum
32). Catalog identity is checked against its canonical content hash, not just its
version name. Reordered, pruned, substituted or conflicting event/window metadata
are rejected. Missing event text or prices remain explicit `data_unavailable` rows.

The complete plan, config, schemas, exact prompts and versions are frozen before the
first call. The same run ID cannot bind changed inputs/configs on resume. Current
defaults use the existing slow scenario; the primary and faster budgets stay fixed.

Each episode receives only its current event, already committed run-local revisions,
and the historical quote available at its own cutoff. Archive evidence supplied to
the single-event builder is a chronological prefix, never later episodes. Full-window
gap/zero-spread counts are **audit metadata only**, never sent to the model. Every
episode starts flat; there is no holding a position across episodes.

## Registry and paid-call journal

The default `data/replay-series.sqlite` is separate from live memory/Fed inbox stores.

| Table | Purpose |
|---|---|
| `series` | Frozen manifest with run/experiment identity and exact prompts |
| `episodes` | Ordered pending/running/completed state, per-case manifest, result and audit quality |
| `calls` | Durably reserved request, exact input/hash, actual timestamps, reply or known error |
| `revisions` | Run-local thesis ID/revision, causal episode and original recording clocks |

Compressed JSON blobs keep evidence small. The result and generated memory revisions
commit in one SQLite transaction; a failed transaction rolls both back. Completed
episodes are immutable. Valid memory can survive a rejected portfolio response. An
ignored/invalid memory response never creates a default thesis or flat proposal.

A nonblocking OS file lock prevents simultaneous writers to the same registry. Each
request is committed to the journal **before** contacting the provider. The cap counts
reserved attempts, including errors and unknown calls, conservatively. Hidden client
retries remain prohibited; known transient errors follow the frozen bounded retry
policy. Unknown pending requests, including cancelled requests, cannot trigger another
automatic paid request for that episode.

This is not provider-side exactly-once execution. A process can crash between journal
reservation and network send, or after provider processing before recording a reply;
billing is then unknown. Different databases or deliberately new run IDs can permit
new paid experiments. Use the canonical registry and do not delete it to resume.

## Pause, resume and uncertain completion

- `--max-events N` stops cleanly between episodes. Re-running the same plan skips
  completed episodes without model calls and restores committed memory for the next.
- An interrupted `running` episode blocks automatic continuation. Even if replies
  were journaled, missing final local validation/commit is not guessed successful.
- `abandon-interrupted` is an explicit operator action. It records
  `interrupted_unknown`, retains all calls/evidence and does not retry. Only earlier
  committed memory is carried forward. Unknown charges remain unknown.
- Unexpected process/storage failures leave the episode pending review; the runner
  does not silently continue with fabricated outcomes or partial memory commits.

No automatic reconstruction of interrupted mid-event results is implemented. Clean
event-boundary recovery is implemented; uncertain recovery requires an explicit choice.

## Commands

```bash
# Offline, no registry creation, credentials or paid calls.
eventlens-series plan examples/synthetic-series-plan.json

# Public Fed archive HTTP requests plus LOCAL quote files; no model/broker requests.
python scripts/prepare_fomc_series.py --run-id new-research-id --output data/series.json.gz

# Explicitly paid research, optionally stop after two episodes.
eventlens-series run data/series.json.gz --call-model --max-events 2
eventlens-series run data/series.json.gz --call-model

# Read-only SQLite connection; no model key required.
eventlens-series report new-research-id

# Explicit abandonment of an uncertain episode; no model call.
eventlens-series abandon-interrupted new-research-id fed-2025-03-19 --acknowledge-unknown-billing
```

Use `python -m eventlens.series_cli` in an existing environment if its entrypoint has
not been reinstalled. The example contains invented event text/prices on the pinned
catalog dates; official-shaped URLs do not authenticate fixture contents. Preparation
records actual archive receipt, preserving supplied source time and market capture.
Recapturing data creates a different plan hash; never substitute it into an old run.

The report includes failures/abstentions, processing budgets, input revision counts,
new revisions, cutoff/quote times, audit gaps, actual reserved calls, known tokens and
unreported usage. `direction` is populated only for a validated proposal;
`proposed_direction` may expose a rejected model response for audit. Neither is an
executed trade. Reports never manufacture PnL for invalid or missing cases.

## Acceptance and next gate

**190 tests** pass, plus Ruff, formatting, mypy, CLI smoke and package build. New tests
cover the entire ordered memory chain, clean reopen/resume, zero-call repeats, unknown
and cancelled requests, crashes after known replies, explicit abandonment, call cap,
missing data, retained failures, config/plan immutability, concurrent-writer locks,
read-only inspection and transaction rollback. Tests use mock clients and synthetic
data; CI makes no paid calls.

The [real eight-event acceptance](fomc-series-acceptance.md) demonstrates successful
traversal/recovery and seven revisions, but only **3/8** portfolio outputs passed exact
evidence checks. This is not trading readiness. The next priority is distinguishing
current-event excerpts from thesis narrative evidence in the proposal interface,
then separately validating a frozen repair. Do not silently relax the checks or rerun
rejected cases until they pass. Independent narrative grounding, live distribution/
latency, data vintage, cost validation, hard risk and fill integration remain pending.
