# Eight-event chronological inference acceptance — October 14, 2026

**Coordinator/restart acceptance passed; model-output reliability is insufficient.**
All eight declared episodes completed, seven thesis revisions were carried forward,
and a clean two-event pause/resume made no duplicate requests. Only **3/8** portfolio
responses passed evidence validation; all three explicitly proposed flat. Five
rejected responses are not counted as valid abstentions, trades or zero-PnL cases.

This is an integration study, not a strategy backtest or an alpha claim. No orders,
fills, risk approvals, PnL or actual positions were created. Aggregate machine-readable
evidence is in [fomc-series-acceptance.json](fomc-series-acceptance.json).

## Frozen experiment

```text
run_id: fomc-2025-deepseek-series-v1
experiment_id: 6cf411e4e3683808347148bf616fb5025d83f0ff11a0412edb6c0c21bfcb7f3b
catalog: fomc-2025-v1 (exact eight-event manifest)
protocol: eurusd-fomc-decision-v1 (unchanged)
memory prompt: replay-memory-v2
portfolio prompt: replay-portfolio-v1
requested/returned model alias: deepseek-flash
returned fingerprint: aeb56401ca74e127821c4f9126dcb669
scenario: slow_reasoning_assumed (59,850 ms processing budget)
event ingest / market delivery assumptions: 60 s / 250 ms
maximum reserved API requests: 32
```

The dataset combines currently retrieved official archived statements with the eight
existing sampled cTrader Demo windows. Original archive receipt and market capture
times remain real 2026 values; historical availability is explicitly assumed. Statement
vintage at original publication and historical live delivery are not independently
established. No broker connection or new tick capture was made during this stage.

## All episodes retained

| Official statement | Memory action | Input revision count | New revision | Portfolio status | Valid direction | Processing |
|---|---|---:|---:|---|---|---:|
| [Jan 29](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250129a.htm) | ignore | 0 | — | invalid_output | — | 6,852 ms |
| [Mar 19](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250319a.htm) | create | 0 | 1 | invalid_output | — | 6,344 ms |
| [May 7](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250507a.htm) | update | 1 | 2 | invalid_output | — | 7,957 ms |
| [Jun 18](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a.htm) | update | 2 | 3 | validated_proposal | flat | 6,052 ms |
| [Jul 30](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250730a.htm) | update | 3 | 4 | invalid_output | — | 6,752 ms |
| [Sep 17](https://www.federalreserve.gov/newsevents/pressreleases/monetary20250917a.htm) | update | 4 | 5 | validated_proposal | flat | 7,839 ms |
| [Oct 29](https://www.federalreserve.gov/newsevents/pressreleases/monetary20251029a.htm) | update | 5 | 6 | invalid_output | — | 8,438 ms |
| [Dec 10](https://www.federalreserve.gov/newsevents/pressreleases/monetary20251210a.htm) | update | 6 | 7 | validated_proposal | flat | 9,312 ms |

One thesis ID persists through revisions 1–7. Each update references the accumulated
supporting/contradicting event IDs and its current event; the next episode reads only prior committed
revisions. All directional eligibility still expires according to the existing one-hour
review policy; old beliefs remain visible and are reassessed. This does not validate
intraday hold/add/reduce/close behavior: the events are months apart and accounts start flat.

## Failure diagnosis

All five rejected portfolio replies were structurally parseable but failed exact
current-event evidence checks:

- January capitalized a mid-sentence excerpt instead of preserving the source case.
- March, May and October copied a statement about missing euro-area/ECB information
  from the thesis narrative into `evidence_quotes`; it was absent from the current release.
- July similarly cited thesis narrative and a tentative transmission hypothesis as
  if they were source excerpts.

The guards worked; the model did not reliably obey the evidence boundary. Manual review
also found an unsupported anticipated-policy characterization in the January rationale.
The automatic checks do not verify every narrative claim. No prompt/schema/validation
relaxation or semantic retry was performed during the series. Valid memory was retained
even when a portfolio response was rejected. The failed outputs remain auditable.

## Restart, timing, calls and data quality

The first process stopped cleanly after January/March: four requests, one committed
revision. A second process skipped those episodes, restored that revision and processed
the remaining six with twelve requests. After completion, running the same plan with
a client that raises on any model call returned the identical report: **zero new calls**.
Crash/cancellation, uncertain billing, lock contention and transaction rollback are
covered by synthetic fault tests, not claims that a real paid call was deliberately interrupted.

Total: **16 paid requests**, no retries, no pending/unknown calls; **31,331 input + 5,956
output tokens**. Usage from rejected responses is included. Actual RMB charges were
not read from the provider account and are not inferred from these totals.

Processing ranged **6.052–9.312 s**, mean **7.443 s**. All eight fit the predeclared slow
processing budget; **0/8** fit the 4.85 s primary budget or the 3.85 s faster budget.
Budget compatibility is not output validity or end-to-end feed/order latency. No fixed
ledger entry schedule was accelerated based on observed processing.

All existing 16,298 observations remain in the dataset. The report preserves full-window
gap and zero-spread counts outside model inputs. Existing source gaps, side freshness
uncertainty and 60.79% zero-spread share are not repaired by this coordinator. See the
[data audit](fomc-2025-data-quality.md) and [cost/path limitations](cost-path-check.md).

## Gate conclusion

Pass: chronological enumeration, immutable metadata, causal memory continuity, atomic
recording, real clean restart, completed-run reuse and retention of failures/abstentions.

Not passed: reliable portfolio evidence output, primary latency budget, independent
narrative grounding, calibrated confidence or any after-cost trading-performance gate.
**37.5% output-validation acceptance is not forecast accuracy**, and eight events are
not multiplied into extra independent samples by model calls or delay diagnostics.
Historical pretrained knowledge and already inspected data also preclude an honest
unseen-alpha claim. Future live shadow evaluation remains necessary.

Next priority: repair the proposal evidence interface so current-event excerpts cannot
be confused with thesis commentary, under a separately frozen experiment that retains
this failure baseline. Do not proceed to execution or claim the strategy is viable
from these results. Hard risk and ledger integration remain separate later gates.
