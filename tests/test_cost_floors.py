"""Independent price/fee checks: stress assumptions never edit evidence."""

from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from eventlens.research_data import read_window
from eventlens.research_replay import (
    DEFAULT_LATENCIES,
    CostScenario,
    ReplayObservation,
    replay_roundtrip,
)


def test_zero_spread_floor_and_existing_wider_quotes():
    quote = ReplayObservation(timestamp="2025-01-29T19:00:00Z", bid="1.1", ask="1.1")
    floor = CostScenario(name="floor", commission_bps=0, slippage_bps=0, spread_floor_pips=1)
    assert floor.fill_price(quote, buy=True) == Decimal("1.10005")
    assert floor.fill_price(quote, buy=False) == Decimal("1.09995")
    assert quote.bid == quote.ask == Decimal("1.1")
    wider = ReplayObservation(timestamp=quote.timestamp, bid="1.10000", ask="1.10020")
    assert floor.fill_price(wider, buy=True) == wider.ask
    assert floor.fill_price(wider, buy=False) == wider.bid
    too_small = ReplayObservation(timestamp=quote.timestamp, bid="0.00001", ask="0.00001")
    with pytest.raises(ValueError, match="nonpositive bid"):
        floor.fill_price(too_small, buy=False)


def test_slippage_after_floor_and_minimum_commission_per_fill():
    quote = ReplayObservation(timestamp="2025-01-29T19:00:00Z", bid="1", ask="1")
    costs = CostScenario(
        name="floor_and_fee",
        commission_bps="0.1",
        slippage_bps=10,
        spread_floor_pips=2,
        minimum_commission_usd="0.03",
        fixed_fee_usd="0.01",
    )
    assert costs.fill_price(quote, buy=True) == Decimal("1.0011001")
    assert costs.fill_price(quote, buy=False) == Decimal("0.9989001")
    assert costs.fee(1000, Decimal("1")) == Decimal("0.04")
    assert costs.fee(100000, Decimal("1")) == Decimal("1.01")


def test_all_directions_cost_monotonicity_and_quote_identity():
    window = read_window(Path("examples/synthetic-research-window.json"))
    evidence_id = window.data_id
    for direction in ("long", "short"):
        results = [
            replay_roundtrip(
                window,
                direction=direction,
                horizon_seconds=300,
                latency=DEFAULT_LATENCIES[0],
                costs=CostScenario(
                    name=f"floor_{value}",
                    commission_bps="0.5",
                    slippage_bps="0.25",
                    spread_floor_pips=value,
                ),
            )
            for value in (0, 1, 2, 5)
        ]
        assert all(r.status == "closed" for r in results)
        assert all(
            a.closed_net_pnl_usd >= b.closed_net_pnl_usd for a, b in zip(results, results[1:])
        )
        assert len({r.case_id for r in results}) == 4
    assert window.data_id == evidence_id
    flat = replay_roundtrip(
        window,
        direction="flat",
        horizon_seconds=300,
        latency=DEFAULT_LATENCIES[0],
        costs=CostScenario(
            name="expensive",
            commission_bps=1,
            slippage_bps=1,
            spread_floor_pips=100,
            minimum_commission_usd=100,
        ),
    )
    assert flat.fills == () and flat.closed_net_pnl_usd == 0
    with pytest.raises(ValidationError):
        CostScenario(name="invalid", commission_bps=0, slippage_bps=0, spread_floor_pips=-1)
