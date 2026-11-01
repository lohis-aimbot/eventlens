"""Point-in-time quote observations. No account positions, model or order capabilities."""

import fcntl
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Callable, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from .contracts import (
    Contract,
    Identifier,
    MarketSnapshot,
    PriceTrajectorySummary,
    Quote,
    Timestamp,
)
from .observer import Basis, ObserverStore


class BridgeStart(Contract):
    version: Literal[1]
    kind: Literal["start"]
    session_id: UUID
    account_mode: Literal["demo"]
    running_mode: Literal["realtime"]
    symbol: Literal["EURUSD"]
    started_at: Timestamp
    sample_interval_ms: int = Field(strict=True, ge=100, le=10000)
    duration_seconds: int = Field(strict=True, ge=10, le=86400)
    digits: int | None = Field(default=None, strict=True, ge=0, le=10)
    pip_size: Decimal | None = Field(default=None, gt=0)
    commission: Decimal | None = Field(default=None, ge=0)
    commission_type: Identifier | None = None


class BridgeQuote(Contract):
    version: Literal[1]
    kind: Literal["quote"]
    session_id: UUID
    sequence: int = Field(strict=True, ge=1)
    symbol: Literal["EURUSD"]
    source_at: Timestamp
    bridge_at: Timestamp
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.ask < self.bid or self.source_at > self.bridge_at:
            raise ValueError("Crossed quote or market clock ahead of bridge")
        return self


class BridgeStop(Contract):
    version: Literal[1]
    kind: Literal["stop"]
    session_id: UUID
    stopped_at: Timestamp
    quotes: int = Field(strict=True, ge=0)
    raw_ticks: int | None = Field(default=None, strict=True, ge=0)
    raw_zero_spread_ticks: int | None = Field(default=None, strict=True, ge=0)
    symbol_mismatch_ticks: int | None = Field(default=None, strict=True, ge=0)
    throttled_ticks: int | None = Field(default=None, strict=True, ge=0)

    @model_validator(mode="after")
    def valid_diagnostics(self) -> Self:
        if self.raw_ticks is not None and self.raw_ticks < self.quotes:
            raise ValueError("Raw tick count cannot be smaller than emitted quotes")
        if self.raw_zero_spread_ticks is not None and (
            self.raw_ticks is None or self.raw_zero_spread_ticks > self.raw_ticks
        ):
            raise ValueError("Raw zero-spread ticks require a bounding raw_ticks count")
        if self.symbol_mismatch_ticks is not None and (
            self.raw_ticks is None or self.symbol_mismatch_ticks > self.raw_ticks
        ):
            raise ValueError("Symbol mismatch ticks require a bounding raw_ticks count")
        if self.throttled_ticks is not None and (
            self.raw_ticks is None or self.throttled_ticks != self.raw_ticks - self.quotes
        ):
            raise ValueError("Throttled ticks must equal raw_ticks minus emitted quotes")
        return self


class QuoteObservation(Contract):
    provider: Identifier = "ctrader_algo"
    basis: Basis
    quote: BridgeQuote
    received_at: Timestamp
    recorded_at: Timestamp
    available_at: Timestamp
    commission: Decimal | None = Field(default=None, ge=0)
    commission_type: Identifier | None = None

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if not self.quote.bridge_at <= self.received_at <= self.recorded_at <= self.available_at:
            raise ValueError("Quote observation clocks are out of order")
        return self


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timezone-aware clocks are required")
    return value.astimezone(UTC)


class QuoteStore:
    """Dedicated append-only quote evidence; availability follows the data commit."""

    APPLICATION_ID = 0x45565154

    def __init__(
        self,
        path: Path,
        *,
        readonly: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.path, self.clock = path.resolve(), clock
        self._lock = threading.RLock()
        if readonly and not self.path.exists():
            raise FileNotFoundError(self.path)
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(
            self.path.as_uri() + ("?mode=ro" if readonly else "?mode=rwc"),
            uri=True,
            timeout=10,
            check_same_thread=False,
        )
        self.db.row_factory = sqlite3.Row
        try:
            app = self.db.execute("PRAGMA application_id").fetchone()[0]
            tables = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if app not in {0, self.APPLICATION_ID} or (app == 0 and (tables or readonly)):
                raise ValueError("Not an EventLens quote database")
            if (
                app == self.APPLICATION_ID
                and self.db.execute("PRAGMA user_version").fetchone()[0] != 1
            ):
                raise ValueError("Unsupported quote database version")
            self.db.execute("PRAGMA foreign_keys=ON")
            if not readonly:
                self.db.execute("PRAGMA synchronous=FULL")
                self.db.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS quote_sessions (
                        session_id TEXT PRIMARY KEY, basis TEXT NOT NULL, start TEXT NOT NULL,
                        received_at TEXT NOT NULL, stop TEXT);
                    CREATE TABLE IF NOT EXISTS quote_observations (
                        session_id TEXT NOT NULL REFERENCES quote_sessions(session_id),
                        sequence INTEGER NOT NULL, symbol TEXT NOT NULL, basis TEXT NOT NULL,
                        source_at TEXT NOT NULL, bridge_at TEXT NOT NULL, received_at TEXT NOT NULL,
                        recorded_at TEXT NOT NULL, available_at TEXT, raw_line TEXT NOT NULL,
                        quote TEXT NOT NULL, PRIMARY KEY(session_id, sequence));
                    CREATE INDEX IF NOT EXISTS quote_lookup ON quote_observations
                        (symbol, basis, available_at, received_at);
                    CREATE TABLE IF NOT EXISTS quote_failures (
                        received_at TEXT NOT NULL, reason TEXT NOT NULL, raw_line TEXT NOT NULL);
                    """
                )
                self.db.execute(f"PRAGMA application_id={self.APPLICATION_ID}")
                self.db.execute("PRAGMA user_version=1")
                self.db.commit()
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        with self._lock:
            self.db.close()

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        with self.path.with_suffix(self.path.suffix + ".observer.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another quote observer owns this database") from exc
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def start(self, item: BridgeStart, received: datetime, *, basis: Basis) -> None:
        with self._lock:
            item = BridgeStart.model_validate(item.model_dump())
            received = utc(received)
            if item.started_at > received:
                raise ValueError("Future bridge start")
            with self.db:
                self.db.execute(
                    "INSERT INTO quote_sessions VALUES (?, ?, ?, ?, NULL)",
                    (str(item.session_id), basis, item.model_dump_json(), received.isoformat()),
                )

    def record(self, item: BridgeQuote, received: datetime, raw_line: str) -> None:
        with self._lock:
            item = BridgeQuote.model_validate(item.model_dump())
            received, now = utc(received), utc(self.clock())
            if item.bridge_at > received or now < received:
                raise ValueError("Future bridge receipt or storage clock rollback")
            session = self.db.execute(
                "SELECT * FROM quote_sessions WHERE session_id=?", (str(item.session_id),)
            ).fetchone()
            if session is None or session["stop"] is not None:
                raise ValueError("Quote requires an open Demo session")
            start = BridgeStart.model_validate_json(session["start"])
            if item.symbol != start.symbol or not start.started_at <= item.bridge_at:
                raise ValueError("Quote does not match session identity/time")
            if item.bridge_at > start.started_at + timedelta(seconds=start.duration_seconds + 2):
                raise ValueError("Quote is outside the bounded session")
            previous = self.db.execute(
                "SELECT * FROM quote_observations WHERE session_id=? ORDER BY sequence DESC LIMIT 1",
                (str(item.session_id),),
            ).fetchone()
            if previous and (
                item.sequence <= previous["sequence"]
                or item.source_at < datetime.fromisoformat(previous["source_at"])
                or item.bridge_at < datetime.fromisoformat(previous["bridge_at"])
                or received < datetime.fromisoformat(previous["received_at"])
            ):
                raise ValueError("Duplicate, reordered or backward-clock quote")
            if len(raw_line.encode()) > 16384:
                raise ValueError("Quote evidence exceeds size limit")
            with self.db:
                self.db.execute(
                    "INSERT INTO quote_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                    (
                        str(item.session_id),
                        item.sequence,
                        item.symbol,
                        session["basis"],
                        item.source_at.isoformat(),
                        item.bridge_at.isoformat(),
                        received.isoformat(),
                        now.isoformat(),
                        raw_line,
                        item.model_dump_json(),
                    ),
                )
            available = utc(self.clock())
            if available < now:
                raise ValueError("Storage clock rollback; quote remains unpublished")
            with self.db:
                self.db.execute(
                    "UPDATE quote_observations SET available_at=? WHERE session_id=? AND sequence=?",
                    (available.isoformat(), str(item.session_id), item.sequence),
                )

    def stop(self, item: BridgeStop, received: datetime) -> None:
        with self._lock:
            item = BridgeStop.model_validate(item.model_dump())
            received = utc(received)
            session = self.db.execute(
                "SELECT * FROM quote_sessions WHERE session_id=?", (str(item.session_id),)
            ).fetchone()
            if session is None or session["stop"] is not None:
                raise ValueError("Stop requires an open session")
            start = BridgeStart.model_validate_json(session["start"])
            last = self.db.execute(
                "SELECT MAX(sequence), MAX(bridge_at) FROM quote_observations WHERE session_id=?",
                (str(item.session_id),),
            ).fetchone()
            if not start.started_at <= item.stopped_at <= received or (
                last[0] is not None
                and (item.quotes < last[0] or item.stopped_at < datetime.fromisoformat(last[1]))
            ):
                raise ValueError("Invalid bridge stop time/count")
            with self.db:
                self.db.execute(
                    "UPDATE quote_sessions SET stop=? WHERE session_id=?",
                    (item.model_dump_json(), str(item.session_id)),
                )

    def failure(self, received: datetime, reason: str, raw_line: str) -> None:
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO quote_failures VALUES (?, ?, ?)",
                (utc(received).isoformat(), reason[:200], raw_line[:16384]),
            )

    def latest(
        self, symbol: str, cutoff: datetime, *, basis: Basis = "live_poll"
    ) -> QuoteObservation | None:
        with self._lock:
            cutoff = utc(cutoff)
            row = self.db.execute(
                """SELECT o.*, s.start AS session_start FROM quote_observations o
                JOIN quote_sessions s ON s.session_id = o.session_id
                WHERE o.symbol=? AND o.basis=?
                AND o.available_at IS NOT NULL AND o.available_at<=? ORDER BY o.received_at DESC,
                o.source_at DESC, o.sequence DESC LIMIT 1""",
                (symbol, basis, cutoff.isoformat()),
            ).fetchone()
            if row is None:
                return None
            start = BridgeStart.model_validate_json(row["session_start"])
            return QuoteObservation(
                basis=row["basis"],
                quote=BridgeQuote.model_validate_json(row["quote"]),
                received_at=row["received_at"],
                recorded_at=row["recorded_at"],
                available_at=row["available_at"],
                commission=start.commission,
                commission_type=start.commission_type,
            )

    def recent_mids(
        self,
        symbol: str,
        cutoff: datetime,
        *,
        lookback: timedelta = timedelta(minutes=38),
        basis: Basis = "live_poll",
    ) -> tuple[tuple[datetime, Decimal], ...]:
        """Return causal (received_at, mid_price) samples strictly published at or before cutoff."""
        with self._lock:
            cutoff_utc = utc(cutoff)
            earliest_utc = cutoff_utc - max(timedelta(0), lookback)
            rows = self.db.execute(
                """SELECT received_at, quote FROM quote_observations
                WHERE symbol=? AND basis=?
                  AND available_at IS NOT NULL AND available_at <= ?
                  AND received_at >= ? AND received_at <= ?
                ORDER BY received_at ASC, sequence ASC""",
                (
                    symbol,
                    basis,
                    cutoff_utc.isoformat(),
                    earliest_utc.isoformat(),
                    cutoff_utc.isoformat(),
                ),
            ).fetchall()
            out: list[tuple[datetime, Decimal]] = []
            for row in rows:
                q = BridgeQuote.model_validate_json(row["quote"])
                mid = (q.bid + q.ask) / Decimal("2")
                out.append((utc(datetime.fromisoformat(row["received_at"])), mid))
            return tuple(out)

    def active_session_remaining_seconds(
        self, cutoff: datetime, *, symbol: str = "EURUSD", basis: Basis = "live_poll"
    ) -> float | None:
        """Return remaining duration in seconds of the latest open session, 0.0 if stopped, or None if no session exists."""
        with self._lock:
            cutoff_utc = utc(cutoff)
            rows = self.db.execute(
                "SELECT start, stop FROM quote_sessions WHERE basis=? ORDER BY rowid DESC",
                (basis,),
            ).fetchall()
            for row in rows:
                start = BridgeStart.model_validate_json(row["start"])
                if start.symbol != symbol:
                    continue
                if row["stop"] is not None:
                    return 0.0
                session_end = start.started_at + timedelta(seconds=start.duration_seconds)
                return max(0.0, (session_end - cutoff_utc).total_seconds())
            return None

    def report(self) -> dict[str, Any]:
        with self._lock:
            return self._report_locked()

    def _report_locked(self) -> dict[str, Any]:
        sessions = []
        quality: dict[str, dict[str, Any]] = {}
        for session in self.db.execute("SELECT * FROM quote_sessions ORDER BY rowid"):
            start = BridgeStart.model_validate_json(session["start"])
            rows = self.db.execute(
                "SELECT * FROM quote_observations WHERE session_id=? ORDER BY sequence",
                (session["session_id"],),
            )
            count, last_sequence = 0, 0
            first_source: datetime | None = None
            previous_source: datetime | None = None
            previous_received: datetime | None = None
            bucket = quality.setdefault(
                session["basis"],
                {
                    "quotes": 0,
                    "zero_spread": 0,
                    "positive_spread": 0,
                    "min_positive_spread": None,
                    "max_spread": None,
                    "max_sample_source_gap_seconds": 0.0,
                    "max_sample_receipt_gap_seconds": 0.0,
                    "max_source_to_bridge_seconds": 0.0,
                    "max_bridge_to_receipt_seconds": 0.0,
                },
            )
            for row in rows:
                count += 1
                last_sequence = row["sequence"]
                item = BridgeQuote.model_validate_json(row["quote"])
                received = datetime.fromisoformat(row["received_at"])
                spread = item.ask - item.bid
                if first_source is None:
                    first_source = item.source_at
                bucket["quotes"] += 1
                bucket["zero_spread"] += int(spread == 0)
                bucket["positive_spread"] += int(spread > 0)
                if spread > 0 and (
                    bucket["min_positive_spread"] is None
                    or spread < Decimal(bucket["min_positive_spread"])
                ):
                    bucket["min_positive_spread"] = str(spread)
                if bucket["max_spread"] is None or spread > Decimal(bucket["max_spread"]):
                    bucket["max_spread"] = str(spread)
                bucket["max_source_to_bridge_seconds"] = max(
                    bucket["max_source_to_bridge_seconds"],
                    (item.bridge_at - item.source_at).total_seconds(),
                )
                bucket["max_bridge_to_receipt_seconds"] = max(
                    bucket["max_bridge_to_receipt_seconds"],
                    (received - item.bridge_at).total_seconds(),
                )
                if previous_source is not None:
                    bucket["max_sample_source_gap_seconds"] = max(
                        bucket["max_sample_source_gap_seconds"],
                        (item.source_at - previous_source).total_seconds(),
                    )
                if previous_received is not None:
                    bucket["max_sample_receipt_gap_seconds"] = max(
                        bucket["max_sample_receipt_gap_seconds"],
                        (received - previous_received).total_seconds(),
                    )
                previous_source = item.source_at
                previous_received = received
            stop = BridgeStop.model_validate_json(session["stop"]) if session["stop"] else None
            sessions.append(
                {
                    "session_id": session["session_id"],
                    "basis": session["basis"],
                    "closed": stop is not None,
                    "quotes": count,
                    "missing_sequences": (stop.quotes if stop else last_sequence) - count,
                    "first_source_before_start": first_source < start.started_at
                    if first_source is not None
                    else False,
                    "symbol_metadata": {
                        "digits": start.digits,
                        "pip_size": str(start.pip_size) if start.pip_size is not None else None,
                        "commission": str(start.commission)
                        if start.commission is not None
                        else None,
                        "commission_type": start.commission_type,
                    }
                    if any(
                        v is not None
                        for v in (
                            start.digits,
                            start.pip_size,
                            start.commission,
                            start.commission_type,
                        )
                    )
                    else None,
                    "bridge_diagnostics": {
                        "raw_ticks": stop.raw_ticks,
                        "raw_zero_spread_ticks": stop.raw_zero_spread_ticks,
                        "symbol_mismatch_ticks": stop.symbol_mismatch_ticks,
                        "throttled_ticks": stop.throttled_ticks,
                    }
                    if stop is not None
                    and any(
                        v is not None
                        for v in (
                            stop.raw_ticks,
                            stop.raw_zero_spread_ticks,
                            stop.symbol_mismatch_ticks,
                            stop.throttled_ticks,
                        )
                    )
                    else None,
                }
            )
        return {
            "scope": "sampled_demo_quotes_only",
            "execution_authorized": False,
            "sessions": self.db.execute("SELECT COUNT(*) FROM quote_sessions").fetchone()[0],
            "quotes": self.db.execute("SELECT COUNT(*) FROM quote_observations").fetchone()[0],
            "failures": self.db.execute("SELECT COUNT(*) FROM quote_failures").fetchone()[0],
            "unpublished": self.db.execute(
                "SELECT COUNT(*) FROM quote_observations WHERE available_at IS NULL"
            ).fetchone()[0],
            "session_details": sessions,
            "quality_by_basis": quality,
            "by_basis": [
                dict(r)
                for r in self.db.execute(
                    "SELECT basis, COUNT(*) AS quotes FROM quote_observations GROUP BY basis"
                )
            ],
            "range": dict(
                self.db.execute(
                    "SELECT MIN(received_at) AS first_received_at, MAX(received_at) AS last_received_at FROM quote_observations"
                ).fetchone()
            ),
        }


def align_news(
    events: ObserverStore,
    quotes: QuoteStore,
    source_id: str,
    cutoff: datetime,
    *,
    symbol: str = "EURUSD",
    max_quote_age: float = 2,
    max_event_age: float = 60,
    basis: Basis = "live_poll",
    allow_commission_bearing_zero_spread: bool = False,
) -> dict[str, Any]:
    """Retrospective audit of observable inputs, never a signal or a simulated fill."""
    import math

    cutoff = utc(cutoff)
    if any(not math.isfinite(v) or v < 0 for v in (max_quote_age, max_event_age)):
        raise ValueError("Freshness limits must be finite and nonnegative")
    revisions = events.history("news", source_id, basis=basis, as_of=cutoff)
    quote = quotes.latest(symbol, cutoff, basis=basis)
    event = revisions[-1] if revisions else None
    event_age = (cutoff - event.available_at).total_seconds() if event else None
    quote_age = (cutoff - quote.quote.source_at).total_seconds() if quote else None
    quote_receipt_age = (cutoff - quote.received_at).total_seconds() if quote else None
    quote_source_to_bridge = (
        (quote.quote.bridge_at - quote.quote.source_at).total_seconds() if quote else None
    )
    quote_bridge_to_receipt = (
        (quote.received_at - quote.quote.bridge_at).total_seconds() if quote else None
    )
    has_verified_commission = (
        allow_commission_bearing_zero_spread
        and quote is not None
        and quote.basis == "live_poll"
        and quote.provider == "ctrader_algo"
        and quote.commission is not None
        and quote.commission > Decimal("0")
        and quote.commission_type == "UsdPerMillionUsdVolume"
    )
    status = (
        "missing_event"
        if event is None
        else "missing_quote"
        if quote is None
        else "stale_event"
        if event_age is not None and event_age > max_event_age
        else "stale_quote"
        if quote_age is not None and quote_age > max_quote_age
        else "zero_spread"
        if quote.quote.ask == quote.quote.bid and not has_verified_commission
        else "aligned_observation"
    )
    return {
        "scope": "retrospective_input_alignment",
        "status": status,
        "execution_authorized": False,
        "basis": basis,
        "as_of": cutoff.isoformat(),
        "event": event.model_dump(mode="json") if event else None,
        "quote": quote.model_dump(mode="json") if quote else None,
        "event_observation_age_seconds": event_age,
        "quote_source_age_seconds": quote_age,
        "quote_receipt_age_seconds": quote_receipt_age,
        "quote_source_to_bridge_seconds": quote_source_to_bridge,
        "quote_bridge_to_receipt_seconds": quote_bridge_to_receipt,
        "spread": str(quote.quote.ask - quote.quote.bid) if quote else None,
        "limits": {"max_quote_age_seconds": max_quote_age, "max_event_age_seconds": max_event_age},
    }


EURUSD_PIP = Decimal("0.0001")


def compute_price_trajectory(
    samples: Sequence[tuple[datetime, Decimal]],
    cutoff: datetime,
    *,
    instrument: str = "EUR/USD",
) -> PriceTrajectorySummary | None:
    """Compute causal pip changes and range bounds strictly from observations <= cutoff."""
    cutoff_utc = utc(cutoff)
    causal = sorted(
        ((utc(ts), mid) for ts, mid in samples if utc(ts) <= cutoff_utc and mid > Decimal("0")),
        key=lambda item: item[0],
    )
    if not causal:
        return None
    current_mid = causal[-1][1]

    def _horizon_delta_pips(target_sec: float, min_sec: float, max_sec: float) -> Decimal | None:
        best_mid: Decimal | None = None
        best_gap: float | None = None
        for ts, mid in causal:
            age_s = (cutoff_utc - ts).total_seconds()
            if min_sec <= age_s <= max_sec:
                gap = abs(age_s - target_sec)
                if best_gap is None or gap < best_gap:
                    best_gap = gap
                    best_mid = mid
        if best_mid is None:
            return None
        return ((current_mid - best_mid) / EURUSD_PIP).quantize(
            Decimal("0.1"), rounding=ROUND_HALF_UP
        )

    mids_15m = [mid for ts, mid in causal if (cutoff_utc - ts).total_seconds() <= 900.0]
    mids_30m = [mid for ts, mid in causal if (cutoff_utc - ts).total_seconds() <= 1800.0]
    return PriceTrajectorySummary(
        instrument=instrument,
        as_of=cutoff_utc,
        current_mid=current_mid,
        change_1m_pips=_horizon_delta_pips(60.0, 35.0, 120.0),
        change_5m_pips=_horizon_delta_pips(300.0, 210.0, 420.0),
        change_15m_pips=_horizon_delta_pips(900.0, 660.0, 1140.0),
        change_30m_pips=_horizon_delta_pips(1800.0, 1350.0, 2250.0),
        window_15m_high_mid=max(mids_15m) if mids_15m else None,
        window_15m_low_mid=min(mids_15m) if mids_15m else None,
        window_30m_high_mid=max(mids_30m) if mids_30m else None,
        window_30m_low_mid=min(mids_30m) if mids_30m else None,
        sample_count_30m=len(mids_30m),
    )


def build_market_snapshot(
    quotes: QuoteStore,
    cutoff: datetime,
    *,
    symbol: str = "EURUSD",
    instrument: str = "EUR/USD",
    basis: Basis = "live_poll",
) -> MarketSnapshot:
    """Build a point-in-time MarketSnapshot from published quote observations."""
    cutoff = utc(cutoff)
    obs = quotes.latest(symbol, cutoff, basis=basis)
    if obs is None:
        return MarketSnapshot(
            snapshot_id=f"market-empty:{symbol}:{cutoff.isoformat()}",
            as_of=cutoff,
            quotes=(),
        )
    samples = quotes.recent_mids(symbol, cutoff, basis=basis)
    trajectory = compute_price_trajectory(samples, cutoff, instrument=instrument)
    return MarketSnapshot(
        snapshot_id=f"quote:{obs.quote.session_id}:{obs.quote.sequence}",
        as_of=cutoff,
        quotes=(
            Quote(
                instrument=instrument,
                source_at=obs.quote.source_at,
                bridge_at=obs.quote.bridge_at,
                received_at=obs.received_at,
                available_at=obs.available_at,
                source_provider=obs.provider,
                basis=obs.basis,
                bid=obs.quote.bid,
                ask=obs.quote.ask,
                commission=obs.commission,
                commission_type=obs.commission_type,
            ),
        ),
        price_trajectory=trajectory,
    )


class QuoteQualityPolicy(Contract):
    """Unified quote quality and freshness rules across agent, risk, shadow fills, exits, and valuation.

    Zero-spread quotes (`Bid == Ask`) are rejected by default (`allow_zero_spread=False` and
    `allow_commission_bearing_zero_spread=False`). Commission-bearing zero-spread quotes are
    accepted ONLY when `allow_commission_bearing_zero_spread=True` is explicitly configured AND
    the quote comes from an explicitly allowlisted live source (`trusted_commission_providers`)
    with verified clock provenance and a verified commission type (`trusted_commission_types`).
    """

    instrument: Identifier = "EUR/USD"
    max_quote_age_ms: int = Field(default=2000, gt=0)
    max_spread_pips: Decimal = Field(default=Decimal("2.0"), gt=0)
    allow_zero_spread: bool = False
    allow_commission_bearing_zero_spread: bool = False
    trusted_commission_providers: tuple[Identifier, ...] = ("ctrader_algo",)
    trusted_commission_types: tuple[Identifier, ...] = ("UsdPerMillionUsdVolume",)
    require_verified_provenance: bool = False


class QuoteValidationResult(Contract):
    valid: bool
    reason: str
    quote: Quote | None = None
    source_age_ms: float | None = None
    receipt_age_ms: float | None = None
    receipt_lag_ms: float | None = None
    spread_pips: Decimal | None = None
    has_verified_provenance: bool = False


def validate_market_quote(
    market: MarketSnapshot | None,
    *,
    as_of: datetime,
    policy: QuoteQualityPolicy = QuoteQualityPolicy(),
    min_causal_time: datetime | None = None,
) -> QuoteValidationResult:
    """Validate quote presence, provenance, causal ordering, source-time freshness, and spread."""
    as_of_utc = utc(as_of)
    if market is None:
        return QuoteValidationResult(valid=False, reason="missing_market_snapshot")
    if market.as_of > as_of_utc:
        return QuoteValidationResult(valid=False, reason="future_market_snapshot")
    quote = next((q for q in market.quotes if q.instrument == policy.instrument), None)
    if quote is None:
        return QuoteValidationResult(valid=False, reason="missing_market_quote")

    source_ts = quote.effective_source_at
    if (
        quote.received_at > as_of_utc
        or source_ts > as_of_utc
        or (quote.bridge_at is not None and quote.bridge_at > as_of_utc)
        or (quote.available_at is not None and quote.available_at > as_of_utc)
    ):
        return QuoteValidationResult(
            valid=False,
            reason="future_market_quote",
            quote=quote,
            has_verified_provenance=quote.has_verified_provenance,
        )

    if min_causal_time is not None:
        causal_utc = utc(min_causal_time)
        if (
            source_ts < causal_utc
            or quote.received_at < causal_utc
            or (quote.available_at is not None and quote.available_at < causal_utc)
        ):
            return QuoteValidationResult(
                valid=False,
                reason="quote_precedes_causal_cutoff",
                quote=quote,
                has_verified_provenance=quote.has_verified_provenance,
            )

    if policy.require_verified_provenance and not quote.has_verified_provenance:
        return QuoteValidationResult(
            valid=False,
            reason="unverified_quote_provenance",
            quote=quote,
            has_verified_provenance=False,
        )

    source_age_ms = (as_of_utc - source_ts).total_seconds() * 1000.0
    receipt_age_ms = (as_of_utc - quote.received_at).total_seconds() * 1000.0
    snapshot_source_age_ms = (market.as_of - source_ts).total_seconds() * 1000.0
    receipt_lag_ms = quote.receipt_lag_ms
    spread = quote.ask - quote.bid
    spread_pips = spread / EURUSD_PIP

    if (
        source_age_ms > policy.max_quote_age_ms
        or receipt_age_ms > policy.max_quote_age_ms
        or snapshot_source_age_ms > policy.max_quote_age_ms
    ):
        return QuoteValidationResult(
            valid=False,
            reason="stale_market_quote",
            quote=quote,
            source_age_ms=source_age_ms,
            receipt_age_ms=receipt_age_ms,
            receipt_lag_ms=receipt_lag_ms,
            spread_pips=spread_pips,
            has_verified_provenance=quote.has_verified_provenance,
        )

    zero_spread_allowed = policy.allow_zero_spread or (
        policy.allow_commission_bearing_zero_spread
        and quote.has_verified_commission
        and quote.source_provider in policy.trusted_commission_providers
        and quote.commission_type in policy.trusted_commission_types
    )
    if spread <= Decimal("0") and not zero_spread_allowed:
        return QuoteValidationResult(
            valid=False,
            reason="zero_spread_rejected",
            quote=quote,
            source_age_ms=source_age_ms,
            receipt_age_ms=receipt_age_ms,
            receipt_lag_ms=receipt_lag_ms,
            spread_pips=spread_pips,
            has_verified_provenance=quote.has_verified_provenance,
        )

    if spread_pips > policy.max_spread_pips:
        return QuoteValidationResult(
            valid=False,
            reason="spread_limit_exceeded",
            quote=quote,
            source_age_ms=source_age_ms,
            receipt_age_ms=receipt_age_ms,
            receipt_lag_ms=receipt_lag_ms,
            spread_pips=spread_pips,
            has_verified_provenance=quote.has_verified_provenance,
        )

    return QuoteValidationResult(
        valid=True,
        reason="quote_valid",
        quote=quote,
        source_age_ms=source_age_ms,
        receipt_age_ms=receipt_age_ms,
        receipt_lag_ms=receipt_lag_ms,
        spread_pips=spread_pips,
        has_verified_provenance=quote.has_verified_provenance,
    )
