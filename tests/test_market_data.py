"""Synthetic-only tests for historical tick decoding and time-safe quote lookup."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from eventlens.ctrader_ticks import CTraderTickPage, decode_page
from eventlens.market_data import HistoricalTick, quote_at_or_before
from eventlens.market_quality import inspect_ticks

T = datetime(2026, 1, 1, tzinfo=UTC)


def page(side: str, **changes: object) -> CTraderTickPage:
    base = {
        "instrument": "EUR/USD",
        "side": side,
        "digits": 5,
        "request_start": T,
        "request_end": T + timedelta(seconds=10),
        "has_more": False,
        "tick_data": [
            {"timestamp": 1_767_225_610_000, "tick": 110_020 if side == "bid" else 110_040},
            {"timestamp": 5_000, "tick": 110_010 if side == "bid" else 110_030},
            {"timestamp": 5_000, "tick": 110_000 if side == "bid" else 110_020},
        ],
    }
    return CTraderTickPage.model_validate(base | changes)


def test_ctrader_newest_first_delta_decode_and_price():
    ticks = decode_page(page("bid"))
    assert [tick.timestamp for tick in ticks] == [
        T,
        T + timedelta(seconds=5),
        T + timedelta(seconds=10),
    ]
    assert [tick.price for tick in ticks] == [
        Decimal("1.10000"),
        Decimal("1.10010"),
        Decimal("1.10020"),
    ]


def test_reject_invalid_window_timestamp_or_page():
    with pytest.raises(ValidationError):
        page("bid", request_end=T + timedelta(days=8))
    with pytest.raises(ValidationError):
        page("bid", request_start=datetime(2026, 1, 1))
    with pytest.raises(ValidationError):
        page("bid", has_more=True, tick_data=[])
    with pytest.raises(ValueError, match="outside requested window"):
        decode_page(page("bid", tick_data=[{"timestamp": 1_767_225_620_000, "tick": 110_020}]))


def test_quote_uses_last_bid_and_ask_as_of_query_time_only():
    ticks = (*decode_page(page("bid")), *decode_page(page("ask")))
    before_update = quote_at_or_before(
        ticks, T + timedelta(seconds=4), max_side_age=timedelta(seconds=5)
    )
    assert before_update is not None
    assert before_update.bid == Decimal("1.10000")
    assert before_update.ask == Decimal("1.10020")
    assert (
        quote_at_or_before(ticks, T - timedelta(milliseconds=1), max_side_age=timedelta(seconds=5))
        is None
    )
    assert (
        quote_at_or_before(ticks, T + timedelta(seconds=8), max_side_age=timedelta(seconds=2))
        is None
    )


def test_separate_side_updates_are_not_zipped_or_forward_filled_forever():
    bid = HistoricalTick(
        instrument="EUR/USD", provider="ctrader", side="bid", timestamp=T, price="1.10000"
    )
    ask = HistoricalTick(
        instrument="EUR/USD",
        provider="ctrader",
        side="ask",
        timestamp=T + timedelta(seconds=1),
        price="1.10020",
    )
    assert quote_at_or_before((bid, ask), T, max_side_age=timedelta(seconds=2)) is None
    assert (
        quote_at_or_before((bid, ask), T + timedelta(seconds=1), max_side_age=timedelta(seconds=2))
        is not None
    )
    assert (
        quote_at_or_before((bid, ask), T + timedelta(seconds=3), max_side_age=timedelta(seconds=1))
        is None
    )


def test_quality_report_marks_missing_and_unverified_horizons():
    ticks = (*decode_page(page("bid")), *decode_page(page("ask")))
    report = inspect_ticks(
        ticks,
        instrument="EUR/USD",
        window_start=T,
        window_end=T + timedelta(seconds=10),
        event_source_time=T,
        available_time=T + timedelta(seconds=2),
        availability_basis="assumed",
        complete=False,
    )
    assert report.complete is False
    assert report.bid_count == report.ask_count == 3
    assert report.max_bid_gap_ms == 5000
    assert report.available_time > report.event_source_time
    assert report.coverage[0].bid == Decimal("1.10000")
    assert report.coverage[1].bid is None  # At +1s, the last bid is now 3s old.
    assert report.coverage[-1].within_requested_window is False
    assert report.coverage[-1].bid is None


def test_quality_report_rejects_future_publication_and_naive_availability():
    with pytest.raises(ValueError, match="source time"):
        inspect_ticks(
            (),
            instrument="EUR/USD",
            window_start=T,
            window_end=T + timedelta(seconds=10),
            event_source_time=T + timedelta(seconds=3),
            available_time=T + timedelta(seconds=2),
            availability_basis="assumed",
            complete=True,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        inspect_ticks(
            (),
            instrument="EUR/USD",
            window_start=T,
            window_end=T + timedelta(seconds=10),
            event_source_time=T,
            available_time=datetime(2026, 1, 1),
            availability_basis="assumed",
            complete=True,
        )


def test_crossed_quote_is_flagged_and_unusable():
    bid = HistoricalTick(
        instrument="EUR/USD", provider="ctrader", side="bid", timestamp=T, price="1.10030"
    )
    ask = HistoricalTick(
        instrument="EUR/USD", provider="ctrader", side="ask", timestamp=T, price="1.10020"
    )
    assert quote_at_or_before((bid, ask), T, max_side_age=timedelta(seconds=1)) is None
    report = inspect_ticks(
        (bid, ask),
        instrument="EUR/USD",
        window_start=T,
        window_end=T + timedelta(seconds=1),
        event_source_time=T,
        available_time=T,
        availability_basis="assumed",
        complete=True,
    )
    assert report.crossed_quote_count >= 1
    assert report.coverage[0].bid is None
