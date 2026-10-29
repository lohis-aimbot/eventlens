# Source observation — v0.15.6

The observer stage captures financial news and calendar snapshots across the Tier-1 English
FX/central-bank stack (`sources/english_tier1.py`, default) and the legacy WallStreetCN
adapter (`sources/wallstreetcn.py`). It does not call an LLM, fetch market quotes, infer
returns, connect a broker or create orders.

## Adapters and inputs

`sources/english_tier1.py` (`Tier1EnglishSource`) is the default live intake for
`eventlens observe capture` and `eventlens shadow start --poll-news`, polling:
- `investinglive` (`news/live_feed`, `news/central_banks_live`): live HTML Next.js RSC/DOM desk wire (`live_forward_acceptance_status = "pending"`—replacing the CloudFront-cached `news/rss` feed observed with `Age: 313s` on `2026-10-29`, while remaining pending live forward multi-poll acceptance; `X-Cache: Miss` alone is never treated as proof of zero-lag delivery)
- `ecb` (`news/press`, `news/stats`): official ECB press and statistical releases
- `fed` (`news/monetary`, `news/speeches`): official Federal Reserve monetary releases and speeches
- `us_bea` (`news/releases`): official US Bureau of Economic Analysis GDP/PCE releases
- `forexfactory` (`calendar/thisweek`): weekly EUR/USD macro release schedule JSON (preserving `reported_forecast = None` and explicit `Consensus Forecast: MISSING` whenever consensus estimates are absent—never substituting `previous`)

Each stream in `Tier1EnglishSource.poll_due_streams` executes in its own task, respects
per-stream `min_poll_seconds` rate limits, `timeout_seconds`, `max_attempts` retry caps, and
in-flight stream deduplication, and **persists (`store.record` / `store.failure`) and publishes
(`available_at`) independently as soon as that stream completes**—fast streams never wait for a
slow or hung stream, and cancellation stops any in-flight stream cleanly.

For newly observed live `ecb` and `fed` announcements (`enrich_live_articles=True`), full
article HTML is fetched once and persisted in `observer_enrichments` together with gzipped raw
HTTP HTML bytes in `observer_payloads`, `request_started_at`, `fetched_at`, `recorded_at`,
`available_at`, and `rss_fingerprint`:
- Subsequent RSS polls reuse the persisted body when `rss_fingerprint` is unchanged, so items
  never regress back to bare RSS summaries or emit bogus revisions when HTML is not re-fetched.
- Genuine upstream RSS/article updates re-fetch and append a new immutable revision (`r2`, …)
  while preserving prior revisions and raw HTML payloads.
- Historical replay (`history(as_of=…)`, `revisions(as_of=…)`, `enrichments(as_of=…)`) filters
  strictly by `available_at <= as_of` (where `available_at >= recorded_at >= received_at >= fetched_at`),
  preventing lookahead into later-fetched full text.

`sources/wallstreetcn.py` implements `CaptureSource.fetch` (`--source wallstreetcn`). Its initial website endpoints
are also used by RSSHub's [news](https://github.com/DIYgod/RSSHub/blob/master/lib/routes/wallstreetcn/live.tsx)
and [calendar](https://github.com/DIYgod/RSSHub/blob/master/lib/routes/wallstreetcn/calendar.ts)
adapters. They are not a documented public developer API with a known service guarantee.
No login or API key is used by this adapter. An upstream access change fails visibly;
there is no browser-login, anti-bot bypass or third-party paid proxy fallback.

The default news channel is `global` (要闻); `forex` and `commodity` are optional.
News records retain titles, text, expanded text, importance and canonical source links.
Importance is a publisher label, not model confidence or predicted price impact.
Calendar input includes both numerical releases and other scheduled events. It is not
restricted to CPI/NFP/FOMC. Reported actual/forecast/previous/revised values are stored
as strings with their original unit: zero and negative values are not missing values.
No numerical surprise or verified pre-release consensus is inferred.

Each calendar request selects one Asia/Shanghai date using explicit UTC epoch bounds.
Calendar identity is `indicator_id@scheduled_epoch_seconds`, so recurring occurrences
do not overwrite each other. A rescheduled occurrence gets a new key; automatic
rescheduling linkage and cross-source event grouping are future work.

## Commands

```bash
# No network: invented inputs, explicitly tagged synthetic_fixture
eventlens observe demo examples/synthetic-observer.json
eventlens observe --database data/observer-demo.sqlite report

# Public HTTP, one news/calendar cycle; writes only the dedicated observer database
eventlens observe capture
eventlens observe report

# Optional bounded sampling. It exits after three cycles and leaves no daemon.
eventlens observe capture --polls 3 --interval 10

# Inspect an ID returned by report.sample_source_ids, without network access
eventlens observe history news SOURCE_ID
eventlens observe history calendar 'INDICATOR_ID@SCHEDULED_EPOCH'

# Filter stored observations for EUR/USD macro/news eligibility without LLM calls
eventlens observe filter --limit 100
```

Put global `--database PATH` before the subcommand. Capture/report/history default to
`data/observer.sqlite`; demo defaults to `data/observer-demo.sqlite`. Reports never create
a missing database. `--as-of` on history requires a timezone-aware cutoff. Synthetic
history requires `--basis synthetic_fixture`; it is never returned by the default live
history query. Neither basis grants trading eligibility.

Capture bounds are 1–100 cycles, 1–100 news items per response, and at least 10 seconds
between completed cycles (default 30). The cycle interval is additional to request
time and retries, not a fixed release-to-receipt latency. `--kind news|calendar|both`,
`--channel` and `--date YYYY-MM-DD` control the requested inputs. Running a historical-date
calendar request today still records today's receipt; it does not reconstruct the past.

## Clock meanings & latency taxonomy

| Field | Meaning |
|---|---|
| `original_published_at` | Unknown (`None`) unless independently verified; never filled from display, modified, or scheduled time |
| `publisher_display_at` | Publisher's initial/displayed article time (`publishedUtc`, RSS `<pubDate>`, or `display_time`); unverified |
| `modified_at` | Publisher's explicit content-modification timestamp (`modifiedUtc` or RSS `<updated>/<modified>`) when provided |
| `scheduled_at` | Calendar release schedule (`public_date` / ISO schedule); not actual release confirmation (`schedule_to_receipt_seconds` is never called `publication_lag_seconds`) |
| `request_started_at` | Local request operation start, including retries/pacing |
| `received_at` | Actual local time after the successful response body is fully received |
| `recorded_at` | Start of the storage write; not a claim of completed commit |
| `available_at` | Conservative clock sampled after normalized data commits, then published to history |

Revisions (`revision >= 2`) never compute latency against the initial article's `publisher_display_at`:
when `modified_at` is present, `classify_timestamp_taxonomy` reports `latency_basis = "modified_to_receipt"`;
when `modified_at` is absent, `verifiable_lag_seconds` remains `None` (`latency_basis = "revision_without_modified_timestamp"`).
Upstream clocks slightly ahead of local `received_at` leave unverified display timestamps `None` rather than clamping lag to a misleading `0.0s`.

## Stream health evaluation (`evaluate_observer_stream_health`)

`ObserverStore.report()` and `eventlens shadow status|report` expose 4 orthogonal dimensions per stream:
1. `http_transport`: `last_poll_at`, `elapsed_ms`, `attempts`, `consecutive_failures`, `last_error`, and `last_parse_error` (including `investinglive_dom_or_rsc_structure_changed` if an HTML response returns 0 articles/cards).
2. `cache_observation`: `x_cache`, `cf_cache_status`, `age_seconds`, `cache_control`, `header_date_skew_seconds`, and `cache_miss_proves_zero_lag = False`.
3. `content_activity`: `total_polls`, `unchanged_polls_since_update`, `last_content_update_at`, and `quiet_duration_seconds`—cleanly separating `quiet_market` (valid HTTP/parse + fresh headers + 0 new items, `allows_new_risk = True`) from `fault` (timeouts, HTTP errors, DOM breakage) and `degraded_cache_or_lag` (`Age > 60s`, stale `Date` header, unaged CDN `Hit`, or excessive verified lag, `allows_new_risk = False`).
4. `verifiable_lag`: `last_verifiable_lag_seconds`, `max_verifiable_lag_seconds`, `lag_basis`, and `verifiability`.

Storage uses UTC; only calendar-date selection uses Shanghai time. A capture first
commits raw evidence, revisions and sightings atomically, then publishes an availability
marker. An interrupted publication leaves `unpublished_polls` and raw evidence visible
to audit, but those sightings cannot make a revision eligible in history. A later fresh
sighting may establish availability. History selects by this marker, never receipt or
scheduled time. A clock rollback stops publication rather than backdating it.

Source timestamp age is not measured transport latency. A forecast beside an already
reported actual is not proof of a pre-release expectation. Stored versions allow later
research to select an earlier observed forecast, but that also requires verifying the
applicable release time and excluding snapshots that already contain the actual.

## Storage and failure behavior

| Table | Content |
|---|---|
| `observer_payloads` | Exact successful-response bytes, hashed and gzip-deduplicated |
| `observer_polls` | Request/receipt/storage/availability clocks, safe response headers, parse outcome |
| `observer_revisions` | Immutable normalized versions; A → B → A retains all three revisions |
| `observer_sightings` | Baseline/new/revised/repeat state for every item seen in every capture |
| `observer_streams` | Last successful window IDs for overlap diagnostics |
| `observer_failures` | Bounded transport/status/size failures, without model or broker actions |
| `observer_enrichments` | Official full-text enrichment bodies, `rss_fingerprint`, `request_started_at` / `fetched_at` / `recorded_at` / `available_at` clocks, and FK to raw HTML bytes in `observer_payloads` |

A dedicated SQLite application ID prevents accidental writes into existing memory or
research stores. Transactions prevent half-written captures; a file lock permits one
collector per database. Reporting/history use read-only connections. Data stays local
and ignored by Git; no retention/deletion daemon is introduced.

First captures are baselines, not freshly released trading opportunities. Later news
windows with no ID overlap report `gap_possible`; overlap is also not proof of complete
coverage. Pagination/backfill, late insertions, source outages and long-running delivery
need further evaluation. Missing rows are not interpreted as deleted events.

Requests use fixed endpoints, a size cap, pacing, timeouts and bounded transient retries.
Redirects and nonretryable HTTP errors are rejected. A long `Retry-After` stops the request
instead of retrying early. Malformed successful-response payloads remain stored, with
no fabricated records; bad rows are individually quarantined with their indices/reasons.
The other source kind can still succeed when one fetch fails. Any rejected/failed cycle
returns nonzero status after retaining its evidence. No automatic trade or default flat
prediction is generated.

See [acceptance](observer-acceptance.md) for the bounded live-source check and its limits.
The separate [Demo quote path](live-quotes.md) adds optional sampled quotes and retrospective
news alignment; the news capture command itself still calls neither cTrader nor an LLM.
