"""Accounting invariants and independent hand-calculated cash/PnL examples."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from eventlens.research.research_data import (
    ResearchCatalog,
    StudyEvent,
    parse_probe_window,
    read_window,
    save_window,
)
from eventlens.research.research_replay import (
    CostScenario,
    LatencyScenario,
    ReplayObservation,
    ResearchPolicy,
    ResearchWindow,
    accounting_grid,
    replay_roundtrip,
)

T = datetime(2025, 6, 18, 18, tzinfo=UTC)
LATENCY = LatencyScenario(
    name="test", ingest_ms=1000, reasoning_ms=0, signal_ms=0, order_ms=0, execution_ms=0
)
ZERO = CostScenario(name="spread_only", commission_bps=0, slippage_bps=0)


def window(*rows, interval=None):
    return ResearchWindow(
        event_id="synthetic",
        source_reference="fixture",
        source_time=T,
        captured_at=T + timedelta(hours=1),
        provider="synthetic",
        data_kind="synthetic",
        sample_interval_ms=interval,
        observations=tuple(
            ReplayObservation(timestamp=T + timedelta(seconds=s), bid=bid, ask=ask)
            for s, bid, ask in rows
        ),
    )


def test_long_short_accounting_and_entry_notional_denominator():
    data = window((1, "1.10000", "1.10020"), (301, "1.10100", "1.10120"))
    long = replay_roundtrip(
        data, direction="long", horizon_seconds=300, latency=LATENCY, costs=ZERO
    )
    short = replay_roundtrip(
        data, direction="short", horizon_seconds=300, latency=LATENCY, costs=ZERO
    )
    assert long.status == short.status == "closed"
    assert long.closed_net_pnl_usd == Decimal("0.80")
    assert short.closed_net_pnl_usd == Decimal("-1.20")
    assert long.cash_usd == long.liquidation_equity_usd == Decimal("10000.80")
    assert short.cash_usd == Decimal("9998.80")
    assert long.ending_units_eur == short.ending_units_eur == 0
    assert short.closed_return_on_entry_notional == Decimal("-1.20") / Decimal("1100")
    assert short.closed_return_on_equity == Decimal("-0.00012")


def test_static_market_spread_loss_is_charged_once_both_directions():
    data = window((1, "1.10", "1.1002"), (301, "1.10", "1.1002"))
    for direction in ("long", "short"):
        result = replay_roundtrip(
            data, direction=direction, horizon_seconds=300, latency=LATENCY, costs=ZERO
        )
        assert result.closed_net_pnl_usd == Decimal("-0.20")
        assert result.commission_usd == 0


def test_commission_cash_and_slippage_are_not_double_counted():
    data = window((1, "1.00", "1.10"), (301, "1.30", "1.40"))
    policy = ResearchPolicy(units_eur=10)
    costs = CostScenario(name="fixed", commission_bps=0, slippage_bps=0, fixed_fee_usd=1)
    long = replay_roundtrip(
        data, direction="long", horizon_seconds=300, latency=LATENCY, costs=costs, policy=policy
    )
    short = replay_roundtrip(
        data, direction="short", horizon_seconds=300, latency=LATENCY, costs=costs, policy=policy
    )
    assert long.closed_net_pnl_usd == 0  # $2 gross gain minus two $1 fees.
    assert short.closed_net_pnl_usd == -6  # $4 gross loss plus two $1 fees.
    assert long.commission_usd == short.commission_usd == 2
    slipped = replay_roundtrip(
        data,
        direction="long",
        horizon_seconds=300,
        latency=LATENCY,
        costs=CostScenario(name="slip", commission_bps=0, slippage_bps=10),
        policy=policy,
    )
    assert slipped.fills[0].price == Decimal("1.1011")
    assert slipped.fills[1].price == Decimal("1.2987")
    assert slipped.closed_net_pnl_usd == Decimal("1.976")


def test_full_latency_timeline_never_fills_before_order_readiness():
    data = window((0, "1", "1.01"), (5.5, "2", "2.01"), (6.2, "3", "3.01"), (306.2, "4", "4.01"))
    latency = LatencyScenario(
        name="stages",
        ingest_ms=1000,
        reasoning_ms=2000,
        signal_ms=1000,
        order_ms=1000,
        execution_ms=1000,
    )
    result = replay_roundtrip(
        data, direction="long", horizon_seconds=300, latency=latency, costs=ZERO
    )
    assert result.timeline.ingest_time == T + timedelta(seconds=1)
    assert result.timeline.parsed_time == T + timedelta(seconds=3)
    assert result.timeline.signal_time == T + timedelta(seconds=4)
    assert result.timeline.entry_order_time == T + timedelta(seconds=5)
    assert result.timeline.earliest_entry_fill == T + timedelta(seconds=6)
    assert result.fills[0].timestamp == T + timedelta(seconds=6.2)
    assert result.fills[0].price == Decimal("3.01")
    assert result.fills[1].timestamp == T + timedelta(seconds=306.2)


def test_late_entry_is_unavailable_and_missing_exit_retains_position():
    late = replay_roundtrip(
        window((10, "1", "1.1")), direction="long", horizon_seconds=300, latency=LATENCY, costs=ZERO
    )
    assert late.status == "entry_unavailable"
    assert late.fills == () and late.ending_units_eur == 0
    assert late.closed_net_pnl_usd is None
    opened = replay_roundtrip(
        window((1, "1", "1.1"), (3, "1.2", "1.3")),
        direction="long",
        horizon_seconds=300,
        latency=LATENCY,
        costs=CostScenario(name="fee", commission_bps=0, slippage_bps=0, fixed_fee_usd=1),
        policy=ResearchPolicy(units_eur=10),
    )
    assert opened.status == "exit_unavailable"
    assert len(opened.fills) == 1 and opened.ending_units_eur == 10
    assert opened.cash_usd == 9999 and opened.commission_usd == 1
    assert opened.unrealized_pnl_usd == 1 and opened.estimated_exit_fee_usd == 1
    assert opened.liquidation_equity_usd == 9999
    assert opened.closed_net_pnl_usd is None
    assert opened.valued_at == T + timedelta(seconds=3)


def test_flat_exposure_guard_and_sparse_path_flags():
    data = window((1, "1", "1.1"), (301, "1.2", "1.3"), interval=1000)
    flat = replay_roundtrip(
        data,
        direction="flat",
        horizon_seconds=300,
        latency=LATENCY,
        costs=CostScenario(name="expensive", commission_bps=100, slippage_bps=100),
    )
    assert flat.closed_net_pnl_usd == 0 and flat.fills == ()
    reject = replay_roundtrip(
        data,
        direction="long",
        horizon_seconds=300,
        latency=LATENCY,
        costs=ZERO,
        policy=ResearchPolicy(units_eur=10000),
    )
    assert reject.status == "exposure_rejected" and reject.fills == ()
    result = replay_roundtrip(
        data, direction="long", horizon_seconds=300, latency=LATENCY, costs=ZERO
    )
    assert result.path_quality == "sampled_with_gaps"
    assert result.max_observed_gap_ms == 300000
    assert result.observed_drawdown_fraction > 0  # Entry spread is already a liquidation loss.


def test_no_entry_after_scheduled_exit_and_no_fee_exhausted_cash():
    too_late = replay_roundtrip(
        window((3, "1", "1.1")),
        direction="long",
        horizon_seconds=1,
        latency=LATENCY,
        costs=ZERO,
        policy=ResearchPolicy(max_quote_wait_ms=5000),
    )
    assert too_late.status == "entry_unavailable" and too_late.fills == ()
    expensive = replay_roundtrip(
        window((1, "1", "1.1")),
        direction="long",
        horizon_seconds=300,
        latency=LATENCY,
        costs=CostScenario(
            name="cash_exhausted", commission_bps=0, slippage_bps=0, fixed_fee_usd=10000
        ),
    )
    assert expensive.status == "exposure_rejected" and expensive.cash_usd == 10000


def test_reproducibility_and_window_validation(tmp_path):
    data = read_window(Path("examples/synthetic-research-window.json"))
    first, second = accounting_grid(data), accounting_grid(data)
    assert first == second and len({item.case_id for item in first}) == 54
    assert sum(item.status == "closed" for item in first) == 36
    path = tmp_path / "window.json.gz"
    save_window(data, path)
    assert read_window(path) == data
    assert (
        data.model_copy(update={"captured_at": data.captured_at + timedelta(days=1)}).data_id
        == data.data_id
    )
    with pytest.raises(ValidationError, match="increasing"):
        window((1, "1", "1.1"), (1, "1", "1.1"))
    with pytest.raises(ValidationError):
        ReplayObservation(timestamp=datetime(2025, 6, 18), bid="1", ask="1.1")


def test_catalog_dst_and_complete_sample_log_validation(tmp_path):
    catalog = ResearchCatalog.model_validate_json(Path("configs/fomc-2025.json").read_bytes())
    assert len(catalog.events) == 8
    assert catalog.events[0].source_time.hour == catalog.events[-1].source_time.hour == 19
    assert all(event.source_time.hour == 18 for event in catalog.events[1:-1])
    with pytest.raises(ValidationError, match="14:00"):
        StudyEvent(
            event_id="bad",
            source_time="2025-03-19T19:00:00Z",
            source_reference="https://www.federalreserve.gov/newsevents/pressreleases/monetary20250319a.htm",
        )
    event = catalog.events[3]
    log = tmp_path / "probe.log"
    log.write_text(
        "EVENTLENS_PROBE|start|symbol=EURUSD|mode=demo_backtest_only|focus_utc=2025-06-18T18:00:00Z|orders=none|sample_interval_ms=1000\n"
        "EVENTLENS_PROBE|quote|time_utc=2025-06-18T18:00:01Z|bid=1|ask=1.1\n"
        "EVENTLENS_PROBE|quote|time_utc=2025-06-18T18:00:02Z|bid=1|ask=1.1\n"
        "EVENTLENS_PROBE|summary|invalid_quotes=0|crossed_quotes=0|backward_times=0|window_samples=2\n"
        "EVENTLENS_PROBE|checkpoint|focus_utc=2025-06-18T18:00:00Z|offset_seconds=1|quote_utc=none|bid=none|ask=none\n",
        encoding="utf-8",
    )
    parsed = parse_probe_window(log, event)
    assert parsed.data_kind == "demo_sampled_snapshots" and len(parsed.observations) == 2
    log.write_text(log.read_text().replace("window_samples=2", "window_samples=3"))
    with pytest.raises(ValueError, match="Complete sampled window"):
        parse_probe_window(log, event)
    log.write_text(
        log.read_text()
        .replace("window_samples=3", "window_samples=2")
        .replace("focus_utc=2025-06-18T18:00:00Z|orders", "focus_utc=2025-06-18T19:00:00Z|orders")
    )
    with pytest.raises(ValueError, match="start focus"):
        parse_probe_window(log, event)
