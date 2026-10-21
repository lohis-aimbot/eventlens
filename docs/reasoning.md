# Memory-only model reasoning — stage 2

The optional `reason` command sends one manually supplied event and the current committed thesis snapshot to a JSON-capable model. The [Fed watcher](fed-intake.md) can now call the same memory-only reasoner for newly collected official statements when `--reason` is enabled. The adapter currently uses DeepSeek `deepseek-flash` with thinking disabled, JSON output mode, a 45-second timeout and at most two retries for transient failures. `JSONModelClient` is the provider-neutral interface. No model training is involved.

DeepSeek's [current model/pricing documentation](https://api-docs.deepseek.com/quick_start/pricing/) labels the `deepseek-flash` API name as **DeepSeek-V4.1-Flash**. The API response itself reports `model: deepseek-flash`, not that full marketing version. When present, EventLens appends the provider-reported `system_fingerprint` to the persisted `model_version`, for example `deepseek-flash@<fingerprint>`. This preserves a serving fingerprint alongside the alias; it does not independently prove model internals or ensure the alias never changes. Recheck the provider mapping for later runs.

The model may return `ignore`, `create` or `update`. A change must include a verbatim quote from the current event, the thesis state and rationale. Updates must name an existing thesis ID; the application assigns new IDs. The application validates structure, quotes, IDs and memory revisions, stamps all decision times, and commits through SQLite. It always sets `target=None`; the memory store rejects any non-null target. Model output cannot place an order or approve risk.

## Local setup and one-event run

1. Open the official [DeepSeek Open Platform](https://platform.deepseek.com/), create or sign in to your own account, review the current pricing and set a small spend limit or balance according to the controls offered there. Create an API key in the platform console. Do not share the key in chat or GitHub.
2. Install this package with `python -m pip install -e '.[dev]'` in a Python 3.11+ virtual environment.
3. Set `DEEPSEEK_API_KEY` in the same terminal session. For an interactive `zsh`/`bash` terminal, `read -rs DEEPSEEK_API_KEY; export DEEPSEEK_API_KEY` lets you paste the key without echoing it or saving it in shell history. Press Enter after pasting. Do not print the variable or commit a `.env` file.
4. Prepare a local JSON file conforming to `Event`. For example, use a unique event ID, a real source URL, its true publication time, the time you actually received it, and the text you want analyzed. Both timestamps need a timezone, such as `+00:00`. The file is a manually supplied research input; EventLens does not verify the source yet.
5. Run `eventlens-memory --database data/research.sqlite reason path/to/event.json`. Inspect the JSON response and then run `eventlens-memory --database data/research.sqlite snapshot` or `history THESIS_ID`.

An example of the required event shape (replace **all** example values with a real event and times before calling the model):

```json
{
  "event_id": "unique-source-event-id",
  "source": "manual-research",
  "source_type": "company",
  "source_reference": "https://example.com/actual-announcement",
  "source_timestamp": "2026-01-01T12:00:00+00:00",
  "received_timestamp": "2026-01-01T12:00:08+00:00",
  "raw_text": "Exact source excerpt to analyze",
  "author": null
}
```

The command requires a separate database path so live reasoning cannot accidentally alter the synthetic demo. Repeating the exact same event ID and payload returns the committed decision without a second model call. Reusing an event ID with changed content fails. A malformed response, unknown thesis ID, fabricated evidence quote, expired decision or concurrent memory update leaves the raw event recorded but commits no new belief. A retry of an uncommitted event can make another billable call. API errors show a sanitized status; provider response bodies and key are not logged.

## Acceptance and limits

v0.7 responses also include an application-stamped `latency` trace linking event, decision, provider and model version. It reports receipt-to-start, reasoning duration and receipt-to-completion in milliseconds. Exact duplicates return the original trace. `source_age_at_receipt_ms` measures source age, not feed transport delay; old historical text received today must not be described as live latency. Timing this memory-only operation does not measure the future signal, risk, order or fill path.

Offline tests use a fake model and an HTTP mock to verify create/update/ignore, evidence and ID rejection, persistence, deduplication, request format, token accounting, response model/fingerprint capture, transient retry, nonretryable rejection and truncated output. CI makes no paid API calls. A real call remains an explicit, user-controlled smoke test; its results should be inspected manually because structural validation cannot establish factual truth or reliable trading judgment. The [FOMC case study](research/fomc-memory-case.md) records one such manual run, including a rejected schema-violating response before the prompt was revised.

The model's confidence is a subjective belief, not a calibrated probability. A verbatim quote only proves that text was present in the supplied event; it does not prove the source is authentic or the interpretation is correct. The full event and active thesis context are sent to the API. Do not use private or licensed content without the right to send it. Manually supplied events remain unverified; the separate Fed collector validates official sources. Market-data tools and offline accounting are separate research components. General semantic deduplication, position reconciliation, risk engine and execution are still absent.

## Single-pass event and exposure reasoning (`v0.15.2`)

In addition to memory-only `reason`, `eventlens memory decide` (backed by `SinglePassTradingAgent` and `ObservationReasoner` in `src/eventlens/trading/agent.py`) evaluates an event, updates thesis memory and proposes a signed `EUR/USD` target weight in a single JSON call:

- **Explicit evidence separation**: `current_event_verbatim_text` is separated from `prior_memory_theses_do_not_quote`, and `flat` abstentions with `memory_action="ignore"` do not require a fabricated event quote.
- **Canonical verbatim quote recovery (`resolve_verbatim_quote`)**: Recovers the exact substring slice from `raw_text` when the model capitalizes the first letter of a mid-sentence excerpt or wraps across newlines, while still rejecting any words absent from the current event.
- **Offline context preview by default**: Running `eventlens memory --database data/research.sqlite decide path/to/event.json --quotes data/live-quotes.sqlite` previews the aligned `AgentContext` without calling DeepSeek; add `--call-model` to invoke the model.
- **Live single-pass smoke check**: On October 21, 2026, a single-pass live call (`deepseek-flash@e72d3c7f663d3e1383b00b3884cb4501`, prompt `single-pass-agent-v1`, 1,634 input + 173 output tokens) evaluated filtered WallStreetCN observation `wallstreetcn:news:3179089:r1`, validated verbatim evidence, committed thesis revision 1 with a `0.1` `EUR/USD` target weight in **3,854 ms**, and reused the committed decision on duplicate submission without a second API call. This confirms adapter/schema connectivity, not trading profitability.
