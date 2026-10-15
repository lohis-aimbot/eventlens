# Stage 1A acceptance — source capture

Checked on **2026-10-15**. Outcome: **source-capture foundation accepted**.
The full realtime observer, trading latency target and strategy are not accepted by this check.

## Bounded public-source observation

Two finite CLI invocations used public website endpoints without login, paid model calls,
market-data connections or broker access:

```bash
eventlens observe capture --polls 3 --interval 10 --date 2026-10-15
# New process, verifying continuity of stored source identity:
eventlens observe capture --date 2026-10-15
```

These commands completed and left no background collector. Local raw news and database
files are ignored by Git. The following is an aggregate record, not a published news corpus.

| Measure | Observed |
|---|---:|
| Successful response captures | 8: four news and four calendar |
| Distinct news IDs | 31 |
| Calendar occurrence IDs | 68 |
| Normalized revisions | 99 |
| Item sightings across captures | 392 |
| Distinct raw response bodies | 3 |
| Transport failures / rejected rows / unpublished captures | 0 / 0 / 0 |
| Average request operation duration | 2,448.43 ms |
| Local capture database size | 249,856 bytes |

Request duration includes local request work and response reception. It is **not**
publication-to-system latency or publication-to-order latency. Calendar records include
scheduled events as well as data releases; 68 records do not mean 68 macro releases.

The first snapshot established 30 news and 68 calendar baselines. The restarted process
observed one new news ID (`3179115`) and 29 repeated IDs; the calendar's 68 items repeated.
Overlapping windows suggest continuity only within this small sample. They do not prove
complete news coverage. No actual content revision or major economic release was sampled.

## UTC clock and evidence checks

| Check | Result |
|---|---|
| Request start ≤ receipt ≤ storage write start ≤ availability marker | Pass, all 8 captures |
| Every stored raw payload decompresses to its SHA-256 identity | Pass, all 3 bodies |
| History cutoff just before new item's availability | No revision returned |
| History cutoff exactly at its availability | One revision returned |
| Report/history inspection leaves database bytes unchanged | Pass |

For news ID `3179115`, actual receipt was `2026-10-15T04:37:00.646235Z`;
write start was `2026-10-15T04:37:00.650252Z`; the post-data-commit availability
marker was `2026-10-15T04:37:00.658144Z`. History at one microsecond before that
marker excluded the item; history at the marker included it. This tests observation
availability, not the truth of the publisher's display time. Original publication time
remains unknown. No label, simulated fill or historical consensus is inferred.

The check can be inspected locally without making another network request:

```bash
eventlens observe report
eventlens observe history news 3179115 --as-of 2026-10-15T04:37:00.658143Z
eventlens observe history news 3179115 --as-of 2026-10-15T04:37:00.658144Z
```

These example history commands require the original local capture database. Fresh clones
should use the offline fixture described in [observer.md](observer.md); an empty or absent
database cannot reproduce a past live observation.

## Automated verification

The local suite passes **247 tests** including existing historical research regressions.
Ruff and strict mypy checks also pass. CI runs offline fixtures only, without source,
model or broker credentials. New tests exercise:

- UTC and future-time handling; calendar schedules distinct from publication times.
- Zero/negative numerical values and recurrence-safe calendar IDs.
- Repeated sightings, A → B → A revisions, restart and point-in-time forecast history.
- Raw payload preservation, rollback, failed availability publication and later recovery.
- Read-only database access, foreign-database rejection and collector locking.
- Malformed responses, individual row quarantine and partial CLI failure reporting.
- Bounded retries, timeouts, size caps, redirects and `Retry-After` handling.
- Synthetic/live provenance separation and offline demo operation.

Fault/revision tests use invented fixtures. They support implementation correctness;
they are not evidence of real-world delivery reliability or strategy performance.

## Remaining acceptance work

Still unverified: important-release completeness, release-time provenance, verified
pre-release consensus, sustained source availability, commercial-use suitability,
pagination/backfill and 30–60 second event-to-decision delivery. The website endpoints
can change and have no known public developer SLA.

Still unimplemented in this path: live Bid/Ask collection, a validated bridge to LLM
inputs, realtime thesis/portfolio orchestration, executable hard risk controls and
broker execution. Existing research/replay code remains separate. No inference,
profitability, paper-order or live-order result is claimed.
