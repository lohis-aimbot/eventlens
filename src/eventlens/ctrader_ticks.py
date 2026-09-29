"""Pure cTrader Open API historical-tick decoder; no credentials or order access."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Self

from pydantic import Field, model_validator

from .contracts import Contract, Identifier, Timestamp
from .market_data import HistoricalTick, Side

MAX_WINDOW = timedelta(days=7)


class EncodedTick(Contract):
    timestamp: int = Field(ge=0)
    tick: int = Field(gt=0)


class CTraderTickPage(Contract):
    """One response per side; first timestamp is absolute, others are backward deltas."""

    instrument: Identifier
    side: Side
    digits: int = Field(ge=0, le=10)
    request_start: Timestamp
    request_end: Timestamp
    has_more: bool
    tick_data: tuple[EncodedTick, ...] = ()

    @model_validator(mode="after")
    def valid_window(self) -> Self:
        if not self.request_start < self.request_end <= self.request_start + MAX_WINDOW:
            raise ValueError("cTrader tick request window must be positive and at most 7 days")
        if self.has_more and not self.tick_data:
            raise ValueError("Truncated page with no ticks cannot be resumed")
        return self


def decode_page(page: CTraderTickPage) -> tuple[HistoricalTick, ...]:
    """Decode newest-first relative timestamps to ascending UTC ticks."""
    result: list[HistoricalTick] = []
    current_ms: int | None = None
    quantum = Decimal(1).scaleb(-page.digits)
    for encoded in page.tick_data:
        if current_ms is None:
            current_ms = encoded.timestamp
        else:
            if encoded.timestamp > current_ms:
                raise ValueError("cTrader timestamp delta exceeds previous absolute time")
            current_ms -= encoded.timestamp
        at = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=current_ms)
        if not page.request_start <= at <= page.request_end:
            raise ValueError("cTrader tick lies outside requested window")
        result.append(
            HistoricalTick(
                instrument=page.instrument,
                provider="ctrader",
                side=page.side,
                timestamp=at,
                price=(Decimal(encoded.tick) / 100_000).quantize(quantum),
            )
        )
    return tuple(reversed(result))
