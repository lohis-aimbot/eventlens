"""Conservative data-availability and spread checks before any event study."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Sequence

from pydantic import Field, model_validator

from .contracts import Contract, Identifier, Timestamp
from .market_data import HistoricalTick, quote_at_or_before

HORIZONS = (1, 5, 10, 30, 60, 300, 1800)


class CoveragePoint(Contract):
    seconds_after_available: int = Field(ge=0)
    timestamp: Timestamp
    within_requested_window: bool
    bid: Decimal | None = None
    ask: Decimal | None = None
    spread_pips: Decimal | None = None


class TickQualityReport(Contract):
    instrument: Identifier
    window_start: Timestamp
    window_end: Timestamp
    event_source_time: Timestamp
    available_time: Timestamp
    availability_basis: Literal["measured", "assumed"]
    source_kind: Literal["historical_backfill"]
    complete: bool
    bid_count: int = Field(ge=0)
    ask_count: int = Field(ge=0)
    first_tick: Timestamp | None = None
    last_tick: Timestamp | None = None
    max_bid_gap_ms: int | None = None
    max_ask_gap_ms: int | None = None
    crossed_quote_count: int = Field(ge=0)
    coverage: tuple[CoveragePoint, ...]

    @model_validator(mode="after")
    def valid_window(self) -> TickQualityReport:
        if not self.event_source_time <= self.available_time:
            raise ValueError("Event source time cannot follow availability")
        if not self.window_start <= self.available_time <= self.window_end:
            raise ValueError("Availability time must lie within the data window")
        return self


def _max_gap_ms(ticks: Sequence[HistoricalTick]) -> int | None:
    if len(ticks) < 2:
        return None
    ordered = sorted(ticks, key=lambda tick: tick.timestamp)
    return max(
        int((right.timestamp - left.timestamp).total_seconds() * 1000)
        for left, right in zip(ordered, ordered[1:])
    )


def inspect_ticks(
    ticks: Sequence[HistoricalTick],
    *,
    instrument: str,
    window_start: datetime,
    window_end: datetime,
    event_source_time: datetime,
    available_time: datetime,
    availability_basis: Literal["measured", "assumed"],
    complete: bool,
    max_side_age: timedelta = timedelta(seconds=2),
    pip_size: Decimal = Decimal("0.0001"),
) -> TickQualityReport:
    if any(
        value.tzinfo is None or value.utcoffset() is None
        for value in (window_start, window_end, event_source_time, available_time)
    ):
        raise ValueError("All report timestamps must be timezone-aware")
    if not window_start <= available_time <= window_end:
        raise ValueError("Availability time must lie within the data window")
    if event_source_time > available_time:
        raise ValueError("Event source time cannot follow availability")
    if pip_size <= 0:
        raise ValueError("Pip size must be positive")
    if any(
        tick.instrument != instrument or not window_start <= tick.timestamp <= window_end
        for tick in ticks
    ):
        raise ValueError("Mixed instruments or out-of-window ticks")
    by_side = {side: [tick for tick in ticks if tick.side == side] for side in ("bid", "ask")}
    coverage: list[CoveragePoint] = []
    for seconds in (0, *HORIZONS):
        at = available_time + timedelta(seconds=seconds)
        within_window = at <= window_end
        quote = quote_at_or_before(ticks, at, max_side_age=max_side_age) if within_window else None
        coverage.append(
            CoveragePoint(
                seconds_after_available=seconds,
                timestamp=at,
                within_requested_window=within_window,
                bid=quote.bid if quote else None,
                ask=quote.ask if quote else None,
                spread_pips=quote.spread / pip_size if quote else None,
            )
        )
    ordered = sorted(ticks, key=lambda tick: tick.timestamp)
    crossed = 0
    latest: dict[str, HistoricalTick] = {}
    for tick in ordered:
        latest[tick.side] = tick
        bid, ask = latest.get("bid"), latest.get("ask")
        if (
            bid is not None
            and ask is not None
            and tick.timestamp - bid.timestamp <= max_side_age
            and tick.timestamp - ask.timestamp <= max_side_age
            and ask.price < bid.price
        ):
            crossed += 1
    return TickQualityReport(
        instrument=instrument,
        window_start=window_start,
        window_end=window_end,
        event_source_time=event_source_time,
        available_time=available_time,
        availability_basis=availability_basis,
        source_kind="historical_backfill",
        complete=complete,
        bid_count=len(by_side["bid"]),
        ask_count=len(by_side["ask"]),
        first_tick=ordered[0].timestamp if ordered else None,
        last_tick=ordered[-1].timestamp if ordered else None,
        max_bid_gap_ms=_max_gap_ms(by_side["bid"]),
        max_ask_gap_ms=_max_gap_ms(by_side["ask"]),
        crossed_quote_count=crossed,
        coverage=tuple(coverage),
    )
