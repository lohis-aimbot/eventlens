"""Historical quote evidence for research; never infer live receipt time from backfills."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Protocol, Sequence

from pydantic import Field, model_validator

from .contracts import Contract, Identifier, Timestamp

Side = Literal["bid", "ask"]


class HistoricalTick(Contract):
    instrument: Identifier
    provider: Identifier
    side: Side
    timestamp: Timestamp
    price: Decimal = Field(gt=0)


class HistoricalQuote(Contract):
    """Last observed sides at `as_of`; side timestamps preserve quote age."""

    instrument: Identifier
    as_of: Timestamp
    bid_timestamp: Timestamp
    ask_timestamp: Timestamp
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def valid_quote(self) -> HistoricalQuote:
        if self.bid_timestamp > self.as_of or self.ask_timestamp > self.as_of:
            raise ValueError("A historical quote cannot use future ticks")
        if self.ask < self.bid:
            raise ValueError("Crossed historical quote")
        return self

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid


class HistoricalTickSource(Protocol):
    """Future adapters must return side-specific timestamped ticks and completeness."""

    async def fetch(
        self, instrument: str, side: Side, start: datetime, end: datetime
    ) -> tuple[Sequence[HistoricalTick], bool]:
        """Return ticks in [start, end] and whether the whole window was retrieved."""
        ...


def quote_at_or_before(
    ticks: Sequence[HistoricalTick], at: datetime, *, max_side_age: timedelta
) -> HistoricalQuote | None:
    """Build a quote only from ticks known by `at`; do not zip Bid and Ask arrays."""
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError("Quote lookup time must be timezone-aware")
    if max_side_age <= timedelta(0):
        raise ValueError("Maximum side age must be positive")
    latest: dict[Side, HistoricalTick] = {}
    instruments = {tick.instrument for tick in ticks}
    if len(instruments) > 1:
        raise ValueError("Quote lookup requires a single instrument")
    for tick in ticks:
        if tick.timestamp <= at and (
            tick.side not in latest or latest[tick.side].timestamp <= tick.timestamp
        ):
            latest[tick.side] = tick
    if "bid" not in latest or "ask" not in latest:
        return None
    bid, ask = latest["bid"], latest["ask"]
    if at - bid.timestamp > max_side_age or at - ask.timestamp > max_side_age:
        return None
    if ask.price < bid.price:
        return None
    return HistoricalQuote(
        instrument=bid.instrument,
        as_of=at,
        bid_timestamp=bid.timestamp,
        ask_timestamp=ask.timestamp,
        bid=bid.price,
        ask=ask.price,
    )
