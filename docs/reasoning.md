# Memory-only model reasoning — stage 2

The optional `reason` command sends one manually supplied event and the current committed thesis snapshot to a JSON-capable model. The adapter currently uses DeepSeek `deepseek-flash` with thinking disabled, JSON output mode, a 45-second timeout and at most two retries for transient failures. `JSONModelClient` is the provider-neutral interface. No model training is involved.

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

Offline tests use a fake model and an HTTP mock to verify create/update/ignore, evidence and ID rejection, persistence, deduplication, request format, token accounting, transient retry, nonretryable rejection and truncated output. CI makes no paid API calls. A real call remains an explicit, user-controlled smoke test; its results should be inspected manually because structural validation cannot establish factual truth or reliable trading judgment.

The model's confidence is a subjective belief, not a calibrated probability. A verbatim quote only proves that text was present in the supplied event; it does not prove the source is authentic or the interpretation is correct. The full event and active thesis context are sent to the API. Do not use private or licensed content without the right to send it. There is no automatic collector, source validation, semantic deduplication, market data, position state, risk engine or execution in this stage.
