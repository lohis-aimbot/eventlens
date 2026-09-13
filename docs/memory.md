# Durable Thesis Memory — stage 1

## Scope

A local SQLite database implements the `ThesisMemory` async protocol. Operations run in worker threads with a separate connection per operation. Writes use `BEGIN IMMEDIATE` and a 10-second busy timeout. WAL supports concurrent readers; this is a single-host store, not a distributed service. No new runtime dependencies, LLM, prices, targets or orders are required.

## Database schema (user_version=1)

| Table | Key | Stored data |
|---|---|---|
| raw_events | event_id | Validated event JSON; actual recorded_at UTC |
| decisions | decision_id; unique event_id and memory_version | Complete decision JSON; actual committed_at UTC |
| thesis_revisions | thesis_id + revision | Thesis JSON, update reason, decision foreign key |

Decision and all thesis revisions are written in one transaction. Foreign keys protect trigger/decision linkage; evidence JSON references are validated by the repository. There are no update/delete methods. Previous revisions are never overwritten through this API. Direct external SQL changes are outside this guarantee.

## Time and evidence

`source_timestamp` and `received_timestamp` remain event facts. `recorded_at` and `committed_at` are storage-clock facts. Defaults use the real UTC clock; only the synthetic demo/tests inject a simulated clock. Never run historical imports under an unmarked simulated clock in a real database.

A decision may cite only events both received and recorded by its reasoning start. Its base memory must already exist at reasoning start, completion cannot be in the future, and the decision must not be expired at commit. A new thesis is created at its first update; later revisions preserve that creation time. Each changed thesis must cite the trigger among its evidence. Missing evidence, contradictory evidence membership, regressing clocks and invalid revision references reject the whole transaction.

Historical snapshots use actual commit time, not source time or a backdated thesis update time. A decision completed at 00:00 but committed at 00:00:10 is invisible at 00:00:09. `history()` is an explicit full audit query, not a point-in-time agent context; use `snapshot(as_of=...)` for context. Raw event lookup is also an audit query.

## Duplicate and concurrency policy

- Same event ID and identical normalized contract: no-op; differing payload: IdentityConflict.
- Same decision ID and identical contract: return original version, including retries after expiry.
- Different decision for an already committed trigger: EventAlreadyProcessed. This stage allows one successful decision per trigger, preventing duplicate memory reinforcement.
- Different event with the same text: retained as distinct evidence. Semantic deduplication, repost detection and provenance verification are not implemented.
- Global base-memory version and each expected thesis revision must match. Conflicts reload/reason again; the repository does not merge beliefs.
- A no-change decision can record an ignored event without changing theses. It still advances global memory version, since the decision history changed.
- Portfolio targets are rejected at this stage. Future scheduled reviews or re-evaluation need explicit new trigger identities, not silent reuse of an event.

## Inspect the demo

```bash
eventlens-memory --verbose demo
eventlens-memory history demo-supply
eventlens-memory event demo-event-2
eventlens-memory snapshot --as-of 2026-01-01T00:01:00Z
```

Expected example history (invented, not financial evidence):

| Revision | State | Confidence | Reason |
|---|---|---|---|
| 1 | active | 0.3 | Unconfirmed disruption; watch only |
| 2 | active | 0.7 | Confirmation strengthens supply thesis |
| 3 | invalidated | 0.1 | Verified restoration contradicts thesis |

No position is created. Confidence is preset, not a calibrated probability. Re-running the same demo retains exactly three revisions. Reopening the database recovers the same state. The historical query at 00:01 returns revision 2, while full history includes revision 3.

## Acceptance checks

Tests verify restart persistence, evidence lookup, idempotent event and decision delivery, conflicting ID rejection, simultaneous writers, stale thesis revisions, expired decisions, future/unrecorded evidence exclusion and actual commit-time visibility. An injected SQL trigger deliberately aborts after decision insertion to verify transaction rollback, followed by a successful retry. A multi-thesis invalid update must leave no partial state.

Remaining limitations: SQLite uses a single writer; process/disk failures propagate to callers and no automatic busy retry loop is implemented. Schema version 1 has no migration yet. Backups, operational monitoring, semantic duplication, authenticity checks and learned reasoning are future stages. Structured application logs complement the durable decision/revision audit; they contain IDs and outcomes, not secrets.
