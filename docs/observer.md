# Source observation — v0.14

The first observer stage captures financial news and calendar snapshots. It does not
call an LLM, fetch market quotes, infer returns, connect a broker or create orders.
This is Stage 1A, not completion of the realtime observer roadmap.

## Adapter and inputs

`sources/wallstreetcn.py` implements `CaptureSource.fetch`. Its initial website endpoints
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

## Clock meanings

| Field | Meaning |
|---|---|
| `original_published_at` | Unknown for this adapter; never filled from display/schedule time |
| `publisher_display_at` | News website's `display_time`, which may change after an edit |
| `scheduled_at` | Calendar `public_date`; may be in the future and is not release confirmation |
| `request_started_at` | Local request operation start, including retries/pacing |
| `received_at` | Actual local time after the successful response body is fully received |
| `recorded_at` | Start of the storage write; not a claim of completed commit |
| `available_at` | Conservative clock sampled after normalized data commits, then published to history |

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
