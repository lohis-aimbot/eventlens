"""Single-host durable memory. Each operation owns a short-lived SQLite connection."""

import asyncio
import logging
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from .contracts import AgentDecision, Contract, Event, MemorySnapshot, Thesis, Timestamp
from .memory import MemoryConflict

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


def stamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timezone-aware timestamp required")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


class IdentityConflict(ValueError):
    """An immutable ID was reused with different content."""


class EventAlreadyProcessed(MemoryConflict):
    """This trigger event already has a committed decision."""


class RevisionRecord(Contract):
    thesis: Thesis
    decision_id: str
    trigger_event_id: str
    reason: str
    committed_at: Timestamp
    memory_version: int


class SQLiteThesisMemory:
    def __init__(self, path: Path, *, clock: Callable[[], datetime] = utc_now) -> None:
        if str(path) == ":memory:":
            raise ValueError("Use a filesystem database for durable memory")
        self.path = path
        self.clock = clock
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("Unsupported memory schema version")
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS raw_events (
                    event_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS decisions (
                    decision_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE REFERENCES raw_events(event_id),
                    payload TEXT NOT NULL, committed_at TEXT NOT NULL,
                    memory_version INTEGER NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS thesis_revisions (
                    thesis_id TEXT NOT NULL, revision INTEGER NOT NULL,
                    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
                    payload TEXT NOT NULL, reason TEXT NOT NULL,
                    PRIMARY KEY(thesis_id, revision)
                );
                CREATE INDEX IF NOT EXISTS decisions_time ON decisions(committed_at);
                PRAGMA user_version=1;
            """)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10)
        db.execute("PRAGMA foreign_keys=ON")
        return db

    async def record_event(self, event: Event) -> None:
        await asyncio.to_thread(self._record_event, event)

    def _record_event(self, event: Event) -> None:
        db = self._connect()
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                existing = db.execute(
                    "SELECT payload FROM raw_events WHERE event_id=?", (event.event_id,)
                ).fetchone()
                payload = event.model_dump_json()
                if existing:
                    if existing[0] != payload:
                        raise IdentityConflict("Event ID has different content")
                    logger.info("event_duplicate event_id=%s", event.event_id)
                    return
                now = stamp(self.clock())
                if stamp(event.received_timestamp) > now:
                    raise ValueError("Cannot persist an event received in the future")
                db.execute(
                    "INSERT INTO raw_events VALUES (?, ?, ?)", (event.event_id, payload, now)
                )
            logger.info("event_recorded event_id=%s", event.event_id)
        finally:
            db.close()

    async def snapshot(self, *, as_of: datetime) -> MemorySnapshot:
        return await asyncio.to_thread(self._snapshot, as_of)

    def _snapshot(self, as_of: datetime) -> MemorySnapshot:
        cutoff = stamp(as_of)
        db = self._connect()
        try:
            with db:
                db.execute("BEGIN")
                version = db.execute(
                    "SELECT COALESCE(MAX(memory_version), 0) FROM decisions WHERE committed_at<=?",
                    (cutoff,),
                ).fetchone()[0]
                rows = db.execute(
                    """
                    SELECT r.payload FROM thesis_revisions r
                    JOIN decisions d USING(decision_id)
                    WHERE d.memory_version<=? ORDER BY d.memory_version, r.thesis_id
                """,
                    (version,),
                ).fetchall()
            latest = {}
            for row in rows:
                thesis = Thesis.model_validate_json(row[0])
                latest[thesis.thesis_id] = thesis
            return MemorySnapshot(
                memory_version=version,
                as_of=as_of,
                theses=tuple(latest[key] for key in sorted(latest)),
            )
        finally:
            db.close()

    async def commit(self, decision: AgentDecision) -> int:
        try:
            return await asyncio.to_thread(self._commit, decision)
        except (ValueError, MemoryConflict, sqlite3.Error):
            logger.exception("memory_commit_rejected decision_id=%s", decision.decision_id)
            raise

    def _commit(self, decision: AgentDecision) -> int:
        db = self._connect()
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                payload = decision.model_dump_json()
                existing = db.execute(
                    "SELECT payload, memory_version FROM decisions WHERE decision_id=?",
                    (decision.decision_id,),
                ).fetchone()
                if existing:
                    if existing[0] != payload:
                        raise IdentityConflict("Decision ID has different content")
                    return int(existing[1])
                if db.execute(
                    "SELECT 1 FROM decisions WHERE event_id=?", (decision.event_id,)
                ).fetchone():
                    raise EventAlreadyProcessed("One committed decision per trigger event")
                current = db.execute(
                    "SELECT memory_version, committed_at FROM decisions ORDER BY memory_version DESC LIMIT 1"
                ).fetchone()
                version = current[0] if current else 0
                if decision.based_on_memory_version != version:
                    raise MemoryConflict("Memory changed: reload snapshot and re-evaluate")
                now = stamp(self.clock())
                if current and (now < current[1] or stamp(decision.started_at) < current[1]):
                    raise ValueError(
                        "Clock regression or reasoning started before its memory existed"
                    )
                if not stamp(decision.completed_at) <= now < stamp(decision.valid_until):
                    raise ValueError("Incomplete or expired decision")
                # This step stores beliefs only. Targets are deferred until portfolio/risk work.
                if decision.target is not None:
                    raise ValueError("Memory-only stage does not accept portfolio targets")
                self._evidence(db, decision.event_id, decision.started_at)
                for change in decision.thesis_changes:
                    thesis = change.thesis
                    previous = db.execute(
                        "SELECT payload FROM thesis_revisions WHERE thesis_id=? ORDER BY revision DESC LIMIT 1",
                        (thesis.thesis_id,),
                    ).fetchone()
                    old = Thesis.model_validate_json(previous[0]) if previous else None
                    if change.expected_revision != (old.revision if old else 0):
                        raise MemoryConflict("Stale thesis revision")
                    if old and (
                        thesis.created_at != old.created_at or thesis.updated_at < old.updated_at
                    ):
                        raise ValueError("Creation time is immutable; update time cannot regress")
                    if not decision.started_at <= thesis.updated_at <= decision.completed_at:
                        raise ValueError("Thesis update must belong to the reasoning interval")
                    if old is None and thesis.created_at != thesis.updated_at:
                        raise ValueError("New thesis must be created at its first update")
                    support = set(thesis.supporting_event_ids)
                    against = set(thesis.contradicting_event_ids)
                    if support & against:
                        raise ValueError(
                            "Evidence cannot both support and contradict the same revision"
                        )
                    if decision.event_id not in support | against:
                        raise ValueError("Every changed thesis must cite the trigger event")
                    for evidence_id in support | against:
                        self._evidence(db, evidence_id, decision.started_at)
                db.execute(
                    "INSERT INTO decisions VALUES (?, ?, ?, ?, ?)",
                    (decision.decision_id, decision.event_id, payload, now, version + 1),
                )
                for change in decision.thesis_changes:
                    db.execute(
                        "INSERT INTO thesis_revisions VALUES (?, ?, ?, ?, ?)",
                        (
                            change.thesis.thesis_id,
                            change.thesis.revision,
                            decision.decision_id,
                            change.thesis.model_dump_json(),
                            change.reason,
                        ),
                    )
            logger.info(
                "memory_committed decision_id=%s memory_version=%s",
                decision.decision_id,
                version + 1,
            )
            return int(version + 1)
        finally:
            db.close()

    def _evidence(self, db: sqlite3.Connection, event_id: str, started: datetime) -> None:
        row = db.execute(
            "SELECT payload, recorded_at FROM raw_events WHERE event_id=?", (event_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"Missing evidence: {event_id}")
        event = Event.model_validate_json(row[0])
        if row[1] > stamp(started) or event.received_timestamp > started:
            raise ValueError("Evidence was unavailable at reasoning start")

    async def history(self, thesis_id: str) -> tuple[RevisionRecord, ...]:
        return await asyncio.to_thread(self._history, thesis_id)

    def _history(self, thesis_id: str) -> tuple[RevisionRecord, ...]:
        db = self._connect()
        try:
            rows = db.execute(
                """SELECT r.payload, r.decision_id, d.event_id, r.reason,
                                d.committed_at, d.memory_version FROM thesis_revisions r
                                JOIN decisions d USING(decision_id)
                                WHERE r.thesis_id=? ORDER BY r.revision""",
                (thesis_id,),
            ).fetchall()
            return tuple(
                RevisionRecord(
                    thesis=Thesis.model_validate_json(row[0]),
                    decision_id=row[1],
                    trigger_event_id=row[2],
                    reason=row[3],
                    committed_at=row[4],
                    memory_version=row[5],
                )
                for row in rows
            )
        finally:
            db.close()

    async def get_event(self, event_id: str) -> Event | None:
        return await asyncio.to_thread(self._get_event, event_id)

    def _get_event(self, event_id: str) -> Event | None:
        db = self._connect()
        try:
            row = db.execute(
                "SELECT payload FROM raw_events WHERE event_id=?", (event_id,)
            ).fetchone()
            return Event.model_validate_json(row[0]) if row else None
        finally:
            db.close()
