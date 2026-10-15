# FOMC memory case — historical evidence replayed in research mode

This is a small acceptance case for `Event → Thesis Memory`. On September 28, 2026, EventLens supplied two summaries of official 2024 FOMC statements to DeepSeek using the `deepseek-flash` API name. The [DeepSeek model table](https://api-docs.deepseek.com/quick_start/pricing/) identified that name as DeepSeek-V4.1-Flash at the time of the run. The provider response returned the alias `deepseek-flash` and fingerprint `aeb56401ca74e127821c4f9126dcb669`; the new decision record stores both as `deepseek-flash@aeb56401ca74e127821c4f9126dcb669`.

## Verified source facts and timestamps

| Event | Official source | Source release time (UTC) | Actual research receipt time (UTC) | Event summary supplied to model |
|---|---|---|---|---|
| September cut | [Federal Reserve statement, September 18, 2024](https://www.federalreserve.gov/newsevents/pressreleases/monetary20240918a.htm) | 2024-09-18 18:00 | 2026-09-28 04:48:09 | FOMC lowered target range by 0.50 percentage points to 4.75%–5.00%; it cited inflation progress and more balanced risks. |
| November cut | [Federal Reserve statement, November 7, 2024](https://www.federalreserve.gov/newsevents/pressreleases/monetary20241107a.htm) | 2024-11-07 19:00 | 2026-09-28 04:48:12 | FOMC lowered target range by a further 0.25 percentage points to 4.50%–4.75%; it described continued economic expansion and roughly balanced risks. |

Both official pages state a 2:00 p.m. local release time. New York was on EDT in September and EST in November, giving the distinct UTC times above. The event text was a short human-prepared summary of each linked release, not a verbatim publication or a live collector output. `received_timestamp` is when this research replay actually handed the historical event to EventLens. It is **not** backdated to 2024. The two events arrived about three seconds apart in this test, so the run cannot measure a real 2024 reaction or historical agent latency.

## Observed memory transitions

| Received event | Memory version | Thesis revision | Evidence IDs | Model confidence | Portfolio target |
|---|---:|---:|---|---:|---|
| September cut | 1 | 1, active | September event supports new thesis | 0.7 | `None` |
| November cut | 2 | 2, active, same thesis ID | Both events support the thesis | 0.8 | `None` |

The second decision revised thesis `f3b1e6b6-c075-4fd0-a50e-21cb0173d370` rather than creating an unrelated one. The provider used 503 input / 269 output tokens for the first call and 816 input / 310 output tokens for the second. Confidence is a model judgment, not a calibrated probability or trade signal.

An earlier attempt with prompt `thesis-memory-v1` returned syntactically valid JSON but put `invalidation_conditions` in a string instead of an array. Pydantic rejected it and committed no thesis. We clarified the required JSON types in `thesis-memory-v2`, which produced the two valid transitions above. This one successful replay does not establish a low schema-error rate; the failure remains visible as a limitation.

The case shows that the code can preserve a thesis identity and append evidence across two related official events. It does **not** test source-collection latency, consensus surprise, market prices, abnormal returns, transaction costs, alpha, risk limits or execution. Those are separate research stages. The SQLite database for this one-off run was temporary and was deleted after inspection; no account credentials or model prompts containing secrets were added to the repository.
