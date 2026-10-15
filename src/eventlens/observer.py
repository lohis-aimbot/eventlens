"""Source observations and durable capture storage, without model or trading calls."""

import fcntl
import gzip
import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable, Literal, Protocol, Self
from uuid import uuid4

from pydantic import Field, model_validator

from .contracts import Contract, Identifier, Timestamp

Kind = Literal["news", "calendar"]
Basis = Literal["live_poll", "synthetic_fixture"]
APPLICATION_ID = 0x45564F42


class MacroValues(Contract):
    """Source-reported values, not normalized surprises or verified consensus."""

    actual: str | None = None
    reported_forecast: str | None = None
    previous: str | None = None
    revised_previous: str | None = None
    unit: str | None = None
    country: str | None = None
    period: str | None = None


class SourceRecord(Contract):
    source_id: Identifier
    kind: Kind
    source_url: Identifier
    title: str
    text: str = Field(min_length=1)
    publisher_display_at: Timestamp | None = None
    scheduled_at: Timestamp | None = None
    original_published_at: Timestamp | None = None
    importance: int | None = Field(default=None, ge=0)
    macro: MacroValues | None = None

    @model_validator(mode="after")
    def time_semantics(self) -> Self:
        if self.kind == "calendar" and (
            self.scheduled_at is None or self.macro is None or self.publisher_display_at is not None
        ):
            raise ValueError(
                "Calendar records require a schedule and values, not a publication time"
            )
        if self.kind == "news" and (self.scheduled_at is not None or self.macro is not None):
            raise ValueError("News records cannot manufacture a macro release")
        return self


class RejectedRecord(Contract):
    index: int = Field(ge=0)
    reason: str


class CaptureBatch(Contract):
    provider: Identifier
    stream: Identifier
    kind: Kind
    basis: Basis = "live_poll"
    request_url: Identifier
    request_started_at: Timestamp
    received_at: Timestamp
    elapsed_ms: float = Field(ge=0)
    attempts: int = Field(ge=1)
    payload: bytes = Field(max_length=1_000_000)
    response_headers: dict[str, str] = Field(default_factory=dict)
    records: tuple[SourceRecord, ...] = ()
    rejected: tuple[RejectedRecord, ...] = ()
    parse_error: str | None = None

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.received_at < self.request_started_at:
            raise ValueError("Capture clock moved backwards")
        if any(record.kind != self.kind for record in self.records):
            raise ValueError("Record and capture kinds disagree")
        if any(
            record.publisher_display_at is not None
            and record.publisher_display_at > self.received_at
            for record in self.records
        ):
            raise ValueError("News display time is after receipt")
        if self.parse_error and self.records:
            raise ValueError("Invalid envelopes must not emit normalized records")
        return self


class StoredRevision(Contract):
    provider: Identifier
    basis: Basis
    revision: int = Field(ge=1)
    received_at: Timestamp
    recorded_at: Timestamp
    available_at: Timestamp
    record: SourceRecord


class CaptureSource(Protocol):
    provider: str

    async def fetch(
        self, kind: Kind, *, channel: str = "global", limit: int = 30, day: date | None = None
    ) -> CaptureBatch:
        """Return observed source data, not a trade or an assumed original publication time."""
        ...


class ObserverStore:
    """Dedicated SQLite database; existing memory/research databases are rejected."""

    def __init__(
        self,
        path: Path,
        *,
        readonly: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.path, self.clock = path.resolve(), clock
        if readonly and not self.path.exists():
            raise FileNotFoundError(self.path)
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        uri = self.path.as_uri() + ("?mode=ro" if readonly else "?mode=rwc")
        self.db = sqlite3.connect(uri, uri=True, timeout=10)
        self.db.row_factory = sqlite3.Row
        try:
            app_id = self.db.execute("PRAGMA application_id").fetchone()[0]
            tables = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if app_id not in {0, APPLICATION_ID} or (app_id == 0 and (tables or readonly)):
                raise ValueError("Not an EventLens observer database")
            if (
                app_id == APPLICATION_ID
                and self.db.execute("PRAGMA user_version").fetchone()[0] != 1
            ):
                raise ValueError("Unsupported observer database version")
            self.db.execute("PRAGMA foreign_keys=ON")
            if not readonly:
                self.db.execute("PRAGMA synchronous=FULL")
                self.db.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS observer_payloads (
                        sha256 TEXT PRIMARY KEY, body_gzip BLOB NOT NULL);
                    CREATE TABLE IF NOT EXISTS observer_polls (
                        poll_id TEXT PRIMARY KEY, provider TEXT NOT NULL, stream TEXT NOT NULL,
                        kind TEXT NOT NULL, basis TEXT NOT NULL, started_at TEXT NOT NULL,
                        received_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
                        available_at TEXT,
                        payload_hash TEXT NOT NULL REFERENCES observer_payloads(sha256),
                        metadata TEXT NOT NULL, result TEXT NOT NULL);
                    CREATE TABLE IF NOT EXISTS observer_revisions (
                        provider TEXT NOT NULL, basis TEXT NOT NULL, kind TEXT NOT NULL,
                        source_id TEXT NOT NULL, revision INTEGER NOT NULL, content_hash TEXT NOT NULL,
                        received_at TEXT NOT NULL, recorded_at TEXT NOT NULL, record TEXT NOT NULL,
                        PRIMARY KEY (provider, basis, kind, source_id, revision));
                    CREATE TABLE IF NOT EXISTS observer_sightings (
                        poll_id TEXT NOT NULL REFERENCES observer_polls(poll_id),
                        source_id TEXT NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL,
                        PRIMARY KEY (poll_id, source_id));
                    CREATE TABLE IF NOT EXISTS observer_streams (
                        provider TEXT NOT NULL, basis TEXT NOT NULL, stream TEXT NOT NULL,
                        anchors TEXT NOT NULL, PRIMARY KEY (provider, basis, stream));
                    CREATE TABLE IF NOT EXISTS observer_failures (
                        failure_id TEXT PRIMARY KEY, provider TEXT NOT NULL,
                        stream TEXT NOT NULL, recorded_at TEXT NOT NULL, error TEXT NOT NULL);
                    """
                )
                self.db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                self.db.execute("PRAGMA user_version=1")
                self.db.commit()
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        with self.path.with_suffix(self.path.suffix + ".observer.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another observer owns this database") from exc
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def record(self, batch: CaptureBatch) -> dict[str, Any]:
        batch = CaptureBatch.model_validate(batch.model_dump())
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Observer storage requires timezone-aware clocks")
        now = now.astimezone(UTC)
        if now < batch.received_at:
            raise ValueError("Cannot persist a future receipt")
        poll_id = str(uuid4())
        payload_hash = hashlib.sha256(batch.payload).hexdigest()
        key = (batch.provider, batch.basis, batch.stream)
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            previous = self.db.execute(
                "SELECT anchors FROM observer_streams WHERE provider=? AND basis=? AND stream=?",
                key,
            ).fetchone()
            ids = [item.source_id for item in batch.records]
            if len(ids) != len(set(ids)):
                raise ValueError("Duplicate source IDs within a capture")
            coverage = "not_evaluated"
            if batch.kind == "news" and not batch.parse_error:
                coverage = (
                    "baseline"
                    if previous is None
                    else "empty"
                    if not ids
                    else "overlap"
                    if set(ids).intersection(json.loads(previous["anchors"]))
                    else "gap_possible"
                )
            counts = {"baseline": 0, "new": 0, "revised": 0, "repeat": 0}
            sightings = []
            for item in batch.records:
                encoded = item.model_dump_json()
                digest = hashlib.sha256(encoded.encode()).hexdigest()
                item_key = (batch.provider, batch.basis, item.kind, item.source_id)
                latest = self.db.execute(
                    """SELECT revision, content_hash FROM observer_revisions
                    WHERE provider=? AND basis=? AND kind=? AND source_id=?
                    ORDER BY revision DESC LIMIT 1""",
                    item_key,
                ).fetchone()
                if latest and latest["content_hash"] == digest:
                    revision, state = latest["revision"], "repeat"
                else:
                    revision = latest["revision"] + 1 if latest else 1
                    state = "revised" if latest else "baseline" if previous is None else "new"
                    self.db.execute(
                        "INSERT INTO observer_revisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            *item_key,
                            revision,
                            digest,
                            batch.received_at.isoformat(),
                            now.isoformat(),
                            encoded,
                        ),
                    )
                counts[state] += 1
                sightings.append((poll_id, item.source_id, revision, state))
            result = {
                "poll_id": poll_id,
                "provider": batch.provider,
                "stream": batch.stream,
                "kind": batch.kind,
                "basis": batch.basis,
                "status": "invalid_payload"
                if batch.parse_error
                else "partial"
                if batch.rejected
                else "ok",
                "received_at": batch.received_at.isoformat(),
                "recorded_at": now.isoformat(),
                "available_at": None,
                "counts": counts,
                "rejected": len(batch.rejected),
                "parse_error": batch.parse_error,
                "coverage": coverage,
                "sample_source_ids": ids[:3],
                "execution_authorized": False,
            }
            self.db.execute(
                "INSERT OR IGNORE INTO observer_payloads VALUES (?, ?)",
                (payload_hash, gzip.compress(batch.payload, mtime=0)),
            )
            metadata = batch.model_dump(mode="json", exclude={"payload", "records"})
            self.db.execute(
                "INSERT INTO observer_polls VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    poll_id,
                    batch.provider,
                    batch.stream,
                    batch.kind,
                    batch.basis,
                    batch.request_started_at.isoformat(),
                    batch.received_at.isoformat(),
                    now.isoformat(),
                    None,
                    payload_hash,
                    json.dumps(metadata),
                    json.dumps(result),
                ),
            )
            self.db.executemany("INSERT INTO observer_sightings VALUES (?, ?, ?, ?)", sightings)
            if not batch.parse_error:
                self.db.execute(
                    """INSERT INTO observer_streams VALUES (?, ?, ?, ?)
                    ON CONFLICT(provider, basis, stream) DO UPDATE SET anchors=excluded.anchors""",
                    (*key, json.dumps(ids)),
                )
        # The normalized records are durably committed before this clock is sampled.
        # A second, small transaction publishes their conservative availability marker.
        # A crash in this gap leaves auditable but ineligible observations, not backdated inputs.
        available = self.clock()
        if available.tzinfo is None or available.utcoffset() is None or available < now:
            raise ValueError("Storage completion clock is invalid; capture remains unpublished")
        result["available_at"] = available.astimezone(UTC).isoformat()
        with self.db:
            self.db.execute(
                "UPDATE observer_polls SET available_at=?, result=? WHERE poll_id=?",
                (result["available_at"], json.dumps(result), poll_id),
            )
        return result

    def failure(self, provider: str, stream: str, error: str) -> None:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Observer storage requires timezone-aware clocks")
        with self.db:
            self.db.execute(
                "INSERT INTO observer_failures VALUES (?, ?, ?, ?, ?)",
                (str(uuid4()), provider, stream, now.astimezone(UTC).isoformat(), error),
            )

    def payload(self, poll_id: str) -> bytes:
        row = self.db.execute(
            """SELECT body_gzip FROM observer_payloads JOIN observer_polls
            ON observer_payloads.sha256=observer_polls.payload_hash WHERE poll_id=?""",
            (poll_id,),
        ).fetchone()
        if row is None:
            raise KeyError(poll_id)
        return gzip.decompress(row["body_gzip"])

    def history(
        self,
        kind: Kind,
        source_id: str,
        *,
        provider: str = "wallstreetcn",
        basis: Basis = "live_poll",
        as_of: datetime | None = None,
    ) -> tuple[StoredRevision, ...]:
        if as_of is not None and (as_of.tzinfo is None or as_of.utcoffset() is None):
            raise ValueError("History cutoff requires a timezone")
        rows = self.db.execute(
            """SELECT r.*, MIN(p.available_at) AS available_at
            FROM observer_revisions r JOIN observer_sightings s
                ON s.source_id=r.source_id AND s.revision=r.revision
            JOIN observer_polls p ON p.poll_id=s.poll_id AND p.provider=r.provider
                AND p.basis=r.basis AND p.kind=r.kind
            WHERE r.provider=? AND r.basis=? AND r.kind=? AND r.source_id=?
                AND p.available_at IS NOT NULL
            GROUP BY r.provider, r.basis, r.kind, r.source_id, r.revision
            ORDER BY r.revision""",
            (provider, basis, kind, source_id),
        ).fetchall()
        records = tuple(
            StoredRevision(
                provider=provider,
                basis=basis,
                revision=row["revision"],
                received_at=row["received_at"],
                recorded_at=row["recorded_at"],
                available_at=row["available_at"],
                record=SourceRecord.model_validate_json(row["record"]),
            )
            for row in rows
        )
        return tuple(item for item in records if as_of is None or item.available_at <= as_of)

    def report(self) -> dict[str, Any]:
        counts = {
            name: self.db.execute(f"SELECT COUNT(*) FROM observer_{name}").fetchone()[0]
            for name in ("polls", "revisions", "sightings", "failures", "payloads")
        }
        recent = self.db.execute(
            "SELECT result FROM observer_polls ORDER BY rowid DESC LIMIT 10"
        ).fetchall()
        return {
            "scope": "source_observation_only",
            "execution_authorized": False,
            "counts": counts,
            "unpublished_polls": self.db.execute(
                "SELECT COUNT(*) FROM observer_polls WHERE available_at IS NULL"
            ).fetchone()[0],
            "recent_polls": [json.loads(row["result"]) for row in recent],
        }
