# Fed FOMC intake — stage 3

EventLens's first automatic source polls the [Federal Reserve monetary-policy RSS feed](https://www.federalreserve.gov/feeds/press_monetary.xml). It selects items titled “Federal Reserve issues FOMC statement,” validates the RSS GUID and exact HTTPS release URL, then fetches the statement body from the official host. Redirects are not followed. The adapter has bounded timeout/retry, request pacing, size limits and ID-based logs. It does not authenticate a cryptographic Fed signature or independently fact-check the statement.

The RSS `pubDate` is stored as `source_timestamp`. The actual time the statement body has been received and parsed by this process is stored as `received_timestamp`. Both are UTC. A URL filename date must match the release date in New York time. The feed's publication timestamp is a publisher claim, and RSS may arrive late; a source timestamp is never substituted for actual receipt time.

## Safe startup and operation

```bash
eventlens-fed --database data/fed.sqlite
eventlens-fed --database data/fed.sqlite
eventlens-fed --database data/fed.sqlite --watch --interval 60
```

The first command creates a baseline of FOMC statements already present in the feed. It fetches no articles and makes no model calls, even if `--reason` was supplied. The second command sees the same IDs and skips them. Later new URLs are fetched once and their extracted article text becomes immutable `Event` evidence. The third command polls continuously; its default interval is 60 seconds and the minimum accepted watch interval is 30 seconds.

Add `--reason` **only** to permit DeepSeek calls for new or pending events. The API key must be in the local `DEEPSEEK_API_KEY` environment variable. A separate `data/fed.sqlite` database is required; the synthetic memory demo database is rejected. The feed inbox and thesis memory share the file. The command prints one JSON result per poll, including baseline, collected, reasoned, skipped, quarantined and failed-fetch counts. All data under `data/` remains out of Git.

The inbox states are `baseline`, `pending`, `processed` and `quarantined`. A collected event is persisted before inference. If the process stops after evidence commit, a later poll recovers the stored event without changing its first receipt timestamp. Pending evidence can be reasoned about even after its URL falls out of the RSS window. If model output is malformed or a memory update fails, the raw event stays available but the item is quarantined and automatic re-polling does not generate repeated paid calls. Transient article-fetch failures remain unprocessed for the next poll. A local file lock prevents two Fed watchers from using the same database simultaneously.

The current implementation deliberately handles only one official title. Different URLs with identical meaning are not semantically deduplicated; revisions to a previously processed page are not re-ingested. Manual quarantine review, independent source verification and bounded total API-spend accounting are future work. Running this watcher does not create target positions or orders. RSS polling is a research-grade intake, not a claim of low-latency market access.

## Acceptance evidence

Offline tests cover first-run baseline, repeat and restart deduplication, pending-event recovery, parser failure quarantine with raw evidence preserved, spoofed URL rejection, duplicate RSS items, article-text extraction and the single-watcher lock. A manual read of the live official feed on September 29, 2026 found four FOMC statement items. In a temporary database the first CLI run reported `baseline=4, reasoned=0`; the second reported `skipped=4, reasoned=0`. The latest linked statement yielded 963 characters of article text. These checks used no paid model calls. No new Fed release arrived during this test, so actual live end-to-end release latency remains unmeasured.
