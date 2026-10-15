# Real single-event inference acceptance — October 14, 2026

Scope: one archived [January 29, 2025 official FOMC statement](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250129a.htm),
the existing EUR/USD cTrader Demo sampled window, and the predeclared
`slow_reasoning_assumed` scenario. There is no account connection, PnL or order.
This validates integration, not unseen forecasting skill or trading profitability.

## Inputs and fixed assumptions

- Current event: `fed-2025-01-29`; actual official-page retrieval/receipt:
  `2026-10-14T14:04:48.703680Z`; extracted statement text: 2,115 characters.
- Source publication: `2025-01-29T19:00:00Z`; the 60 s ingest assumption makes replay
  availability `19:01:00Z`. Actual historical news delivery was not measured.
- Market evidence: the existing 2,017 sampled Demo observations. No data was fetched
  from a broker for this stage. Assumed market delivery delay: 250 ms, not measured.
- Initial memory/portfolio: empty run-local memory; flat 10,000 USD research account.
- Selected reasoning budget: 59,850 ms from the pre-existing slow scenario. The
  4,850 ms primary budget and 3,850 ms faster budget were not changed.
- Provider: DeepSeek; requested and returned alias: `deepseek-flash`; returned
  fingerprint in all three calls: `aeb56401ca74e127821c4f9126dcb669`. This does not
  independently identify an underlying foundation-model release.

Each experiment's case/config/schema/prompt hashes were frozen before its calls.
Exact prompt snapshots were reconstructed after these two development runs and
verified against their immutable pre-call hashes. The final CLI additionally saves
those snapshots before future calls. Original manifests/results were not overwritten.

## Retained experiments

| Experiment | Memory prompt | Calls | Result | Total processing |
|---|---|---:|---|---:|
| `fomc-2025-01-29-deepseek-smoke-v1` | `replay-memory-v1` | 1 | `invalid_output`; no revision or portfolio call | 5,011 ms |
| `fomc-2025-01-29-deepseek-smoke-v2` | `replay-memory-v2` | 2 | `validated_proposal`, direction `flat` | 7,951 ms |

Experiment IDs:

```text
v1 a50f1b6490e28393f1e309a23c2cef45500c35f77693cacf24ac299b0c04257e
v2 148cdbe9d9060b4fd8c35356047cdfd6835cf5aba5aedf01316f0b100817181d
```

The first response capitalized an excerpt that began lowercase in the supplied text.
Exact evidence validation rejected it; the system neither normalized the quote nor
invented a flat decision. Manual review also found an unsupported euro-area outlook
claim. That narrative issue was observed manually, not detected by a factuality model.

The revised memory prompt explicitly required case/punctuation-preserving excerpts
and unknown treatment of missing euro-area/ECB information. This change created a
separate experiment before another call. It was an output-grounding repair, not
profit optimization; neither experiment computed returns or selected a profitable reply.

The second run created one cautious thesis, distinguishing supplied Fed facts from
unknown ECB/relative-policy information. Portfolio inference explicitly abstained
because those inputs did not establish a directional EUR/USD catalyst. Flat remained
a validated model response, not an automatic fallback from a parser failure.

## Successful-run clocks and usage

| Item | Actual / hypothetical value |
|---|---|
| Actual processing start / completion | `2026-10-14T14:07:25.037654Z` / `14:07:32.988055Z` |
| Memory / portfolio API elapsed | 4,768 ms / 3,149 ms |
| Total processing, including local preparation/validation | 7,951 ms |
| Hypothetical memory commit | `2025-01-29T19:01:04.779823Z` |
| Portfolio input cutoff | `2025-01-29T19:01:04.779955Z` |
| Selected quote observation / assumed availability | `19:01:04.055Z` / `19:01:04.305Z` |
| Hypothetical validated decision completion | `2025-01-29T19:01:07.950362Z` |
| Successful-run input / output tokens | 3,491 / 540 |
| Both experiments, total input / output tokens | 5,083 / 826 |

The quote and committed thesis precede the portfolio cutoff; decision completion
follows both calls. There were three paid calls in total and no retries. Token usage
includes the rejected response. No exact RMB fee is inferred from total tokens:
provider cache billing and the account statement determine actual charges.

The successful pipeline **does not fit the 4.85 s primary processing budget**, but
fits the predeclared 59.85 s slow budget. Diagnostics for other budgets are processing
comparisons only, not alternative strategy runs. Do not relabel the result as a
successful primary-scenario decision or measured historical transport latency.

## Acceptance conclusion and limits

Pass: actual API integration, raw reply/serving metadata capture, exact evidence checks,
run-local thesis generation, context cutoff selection, explicit abstention and timing
audit. The v1 failure remains visible. Both results have `execution_authorized=false`.

Not established: end-to-end live latency, repeated-run latency distribution, article
content vintage at publication, narrative truth, model calibration, event alpha,
transaction-cost viability, catalog-wide reliability, hard risk or executable fills.
The two experiments are one historical event, not two independent research samples.
LLM pretraining can contain its outcome. Future live shadow evidence remains required.

Local artifacts live under ignored `data/inference-fed-2025-01-29-v1*` and `v2*` paths;
GitHub contains this aggregate acceptance record, code/configs and synthetic fixtures,
not credentials, raw broker windows or local response archives. Stop after this stage.
