"""Offline EUR/USD accounting diagnostics; no LLM strategy or broker execution.

One fixed-size linear position, USD cash collateral, and a scheduled exit. This
is an intentionally small research ledger, not a margin or liquidity simulator.
"""

from __future__ import annotations

import hashlib
from bisect import bisect_left
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, model_validator

from ..contracts import Contract, Identifier, Timestamp

BPS = Decimal("10000")
ENGINE_VERSION = "research-ledger-v2"
EURUSD_PIP = Decimal("0.0001")


class ReplayObservation(Contract):
    timestamp: Timestamp
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def valid_spread(self) -> Self:
        if self.ask < self.bid:
            raise ValueError("Crossed replay observation")
        return self


class ResearchWindow(Contract):
    event_id: Identifier
    source_reference: Identifier
    source_time: Timestamp
    captured_at: Timestamp
    instrument: Literal["EUR/USD"] = "EUR/USD"
    provider: Literal["synthetic", "ctrader_demo"]
    data_kind: Literal["synthetic", "demo_checkpoints", "demo_sampled_snapshots"]
    sample_interval_ms: int | None = Field(default=None, gt=0)
    observations: tuple[ReplayObservation, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def valid_window(self) -> Self:
        if (self.provider == "synthetic") != (self.data_kind == "synthetic"):
            raise ValueError("Synthetic provenance must be explicit")
        if self.data_kind == "demo_sampled_snapshots" and self.sample_interval_ms is None:
            raise ValueError("Sampled snapshots require the actual sampling interval")
        if self.data_kind == "demo_checkpoints" and self.sample_interval_ms is not None:
            raise ValueError("Sparse checkpoints cannot claim regular sampling")
        if any(
            left.timestamp >= right.timestamp
            for left, right in zip(self.observations, self.observations[1:])
        ):
            raise ValueError("Observations must have unique increasing UTC times")
        if self.provider != "synthetic" and self.captured_at < self.observations[-1].timestamp:
            raise ValueError("Backfill capture cannot precede its market observations")
        return self

    @property
    def data_id(self) -> str:
        # Re-import time cannot change the identity of identical market evidence.
        payload = self.model_dump_json(exclude={"captured_at"})
        return hashlib.sha256(payload.encode()).hexdigest()


class LatencyScenario(Contract):
    name: Identifier
    basis: Literal["assumed"] = "assumed"
    ingest_ms: int = Field(ge=0)
    reasoning_ms: int = Field(ge=0)
    signal_ms: int = Field(ge=0)
    order_ms: int = Field(ge=0)
    execution_ms: int = Field(ge=0)


class CostScenario(Contract):
    name: Identifier
    basis: Literal["assumed"] = "assumed"
    commission_bps: Decimal = Field(ge=0, le=100)
    slippage_bps: Decimal = Field(ge=0, le=100)
    fixed_fee_usd: Decimal = Field(default=Decimal(0), ge=0)
    minimum_commission_usd: Decimal = Field(default=Decimal(0), ge=0)
    spread_floor_pips: Decimal = Field(default=Decimal(0), ge=0, le=100)

    def fill_price(self, quote: ReplayObservation, *, buy: bool) -> Decimal:
        # Widen around the observed midpoint only if its spread is below the
        # explicit sensitivity floor. Keep raw quotes unchanged and do not
        # charge spread again as a cash fee. EUR/USD is the ledger's only asset.
        midpoint = (quote.bid + quote.ask) / 2
        half_floor = self.spread_floor_pips * EURUSD_PIP / 2
        ask = max(quote.ask, midpoint + half_floor)
        bid = min(quote.bid, midpoint - half_floor)
        if bid <= 0:
            raise ValueError("Assumed spread floor produces a nonpositive bid")
        return ask * (1 + self.slippage_bps / BPS) if buy else bid * (1 - self.slippage_bps / BPS)

    def fee(self, units: Decimal, price: Decimal) -> Decimal:
        commission = max(
            abs(units) * price * self.commission_bps / BPS, self.minimum_commission_usd
        )
        return commission + self.fixed_fee_usd


class ResearchPolicy(Contract):
    version: Literal["fixed-roundtrip-v1"] = "fixed-roundtrip-v1"
    initial_cash_usd: Decimal = Field(default=Decimal("10000"), gt=0)
    units_eur: Decimal = Field(default=Decimal("1000"), gt=0)
    max_notional_fraction: Decimal = Field(default=Decimal("1"), gt=0, le=1)
    max_quote_wait_ms: int = Field(default=2000, gt=0)
    max_path_gap_ms: int = Field(default=3000, gt=0)


class CostSensitivityConfig(Contract):
    version: Identifier
    scope: Identifier
    profiles: tuple[CostScenario, ...] = Field(min_length=2)

    @model_validator(mode="after")
    def isolate_spread_floor(self) -> Self:
        if len({p.name for p in self.profiles}) != len(self.profiles):
            raise ValueError("Cost profile names must be unique")
        floors = [p.spread_floor_pips for p in self.profiles]
        fees = {
            (p.commission_bps, p.slippage_bps, p.minimum_commission_usd, p.fixed_fee_usd)
            for p in self.profiles
        }
        if floors[0] != 0 or any(a >= b for a, b in zip(floors, floors[1:])) or len(fees) != 1:
            raise ValueError("Isolate increasing spread floors with identical fees and slippage")
        return self


class SimulatedTimeline(Contract):
    source_time: Timestamp
    ingest_time: Timestamp
    parsed_time: Timestamp
    signal_time: Timestamp
    entry_order_time: Timestamp
    earliest_entry_fill: Timestamp
    exit_order_time: Timestamp
    earliest_exit_fill: Timestamp

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if not (
            self.source_time
            <= self.ingest_time
            <= self.parsed_time
            <= self.signal_time
            <= self.entry_order_time
            <= self.earliest_entry_fill
            < self.exit_order_time
            <= self.earliest_exit_fill
        ):
            raise ValueError("Simulated timeline is noncausal or exit precedes entry readiness")
        return self


class SimulatedFill(Contract):
    side: Literal["buy", "sell"]
    timestamp: Timestamp
    units_eur: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    commission_usd: Decimal = Field(ge=0)


class AccountingResult(Contract):
    case_id: Identifier
    data_id: Identifier
    event_id: Identifier
    engine_version: str = ENGINE_VERSION
    policy: ResearchPolicy
    latency: LatencyScenario
    costs: CostScenario
    direction: Literal["flat", "long", "short"]
    horizon_seconds: int = Field(gt=0)
    timeline: SimulatedTimeline
    status: Literal["flat", "closed", "entry_unavailable", "exit_unavailable", "exposure_rejected"]
    fills: tuple[SimulatedFill, ...] = ()
    ending_units_eur: Decimal = Decimal(0)
    cash_usd: Decimal
    valued_at: Timestamp | None = None
    unrealized_pnl_usd: Decimal = Decimal(0)
    estimated_exit_fee_usd: Decimal = Decimal(0)
    liquidation_equity_usd: Decimal
    commission_usd: Decimal = Decimal(0)
    closed_net_pnl_usd: Decimal | None = None
    closed_return_on_equity: Decimal | None = None
    closed_return_on_entry_notional: Decimal | None = None
    observed_drawdown_fraction: Decimal | None = None
    path_quality: Literal["not_evaluated", "checkpoints", "sampled", "sampled_with_gaps"]
    max_observed_gap_ms: int | None = None


def _timeline(source: datetime, latency: LatencyScenario, horizon: int) -> SimulatedTimeline:
    ingest = source + timedelta(milliseconds=latency.ingest_ms)
    parsed = ingest + timedelta(milliseconds=latency.reasoning_ms)
    signal = parsed + timedelta(milliseconds=latency.signal_ms)
    order = signal + timedelta(milliseconds=latency.order_ms)
    exit_order = signal + timedelta(seconds=horizon, milliseconds=latency.order_ms)
    return SimulatedTimeline(
        source_time=source,
        ingest_time=ingest,
        parsed_time=parsed,
        signal_time=signal,
        entry_order_time=order,
        earliest_entry_fill=order + timedelta(milliseconds=latency.execution_ms),
        exit_order_time=exit_order,
        earliest_exit_fill=exit_order + timedelta(milliseconds=latency.execution_ms),
    )


def replay_roundtrip(
    window: ResearchWindow,
    *,
    direction: Literal["flat", "long", "short"],
    horizon_seconds: int,
    latency: LatencyScenario,
    costs: CostScenario,
    policy: ResearchPolicy = ResearchPolicy(),
) -> AccountingResult:
    """First observed quote after order readiness; never source-time fills.

    Scheduled exit is horizon seconds after the signal, not a hindsight-selected
    price. Missing exits retain the open position and its latest known valuation.
    """
    if horizon_seconds <= 0:
        raise ValueError("Horizon must be positive")
    if direction not in {"flat", "long", "short"}:
        raise ValueError("Unknown benchmark direction")
    timeline = _timeline(window.source_time, latency, horizon_seconds)
    identity = "|".join(
        (
            ENGINE_VERSION,
            window.data_id,
            direction,
            str(horizon_seconds),
            latency.model_dump_json(),
            costs.model_dump_json(),
            policy.model_dump_json(),
        )
    )
    base = dict(
        case_id=hashlib.sha256(identity.encode()).hexdigest()[:24],
        data_id=window.data_id,
        event_id=window.event_id,
        policy=policy,
        latency=latency,
        costs=costs,
        direction=direction,
        horizon_seconds=horizon_seconds,
        timeline=timeline,
    )
    unchanged = dict(
        cash_usd=policy.initial_cash_usd,
        liquidation_equity_usd=policy.initial_cash_usd,
        path_quality="not_evaluated",
    )
    if direction == "flat":
        return AccountingResult(
            **base, **unchanged, status="flat", closed_net_pnl_usd=0, closed_return_on_equity=0
        )

    observations = window.observations
    times = [quote.timestamp for quote in observations]

    def lookup(at: datetime) -> int | None:
        index = bisect_left(times, at)
        if index == len(times) or times[index] - at > timedelta(
            milliseconds=policy.max_quote_wait_ms
        ):
            return None
        return index

    entry_index = lookup(timeline.earliest_entry_fill)
    if entry_index is None:
        return AccountingResult(**base, **unchanged, status="entry_unavailable")
    entry_quote = observations[entry_index]
    if entry_quote.timestamp >= timeline.exit_order_time:
        return AccountingResult(**base, **unchanged, status="entry_unavailable")
    entry_price = costs.fill_price(entry_quote, buy=direction == "long")
    units = policy.units_eur if direction == "long" else -policy.units_eur
    entry_notional = abs(units) * entry_price
    if entry_notional > policy.initial_cash_usd * policy.max_notional_fraction:
        return AccountingResult(**base, **unchanged, status="exposure_rejected")
    entry_fee = costs.fee(units, entry_price)
    if entry_fee >= policy.initial_cash_usd:
        return AccountingResult(**base, **unchanged, status="exposure_rejected")
    entry_fill = SimulatedFill(
        side="buy" if units > 0 else "sell",
        timestamp=entry_quote.timestamp,
        units_eur=abs(units),
        price=entry_price,
        commission_usd=entry_fee,
    )
    cash = policy.initial_cash_usd - entry_fee
    exit_index = lookup(timeline.earliest_exit_fill)
    if exit_index is not None and observations[exit_index].timestamp <= entry_quote.timestamp:
        exit_index = None
    mark_index = exit_index if exit_index is not None else len(observations) - 1
    path = observations[entry_index : mark_index + 1]
    peak = policy.initial_cash_usd
    drawdown = Decimal(0)
    unrealized = exit_fee = equity = Decimal(0)
    for quote in path:
        close_price = costs.fill_price(quote, buy=units < 0)
        unrealized = units * (close_price - entry_price)
        exit_fee = costs.fee(units, close_price)
        equity = cash + unrealized - exit_fee
        peak = max(peak, equity)
        drawdown = max(drawdown, (peak - equity) / peak)
    gap = max(
        (
            int((right.timestamp - left.timestamp).total_seconds() * 1000)
            for left, right in zip(path, path[1:])
        ),
        default=0,
    )
    quality: Literal["checkpoints", "sampled", "sampled_with_gaps"] = (
        "checkpoints"
        if window.sample_interval_ms is None
        else "sampled_with_gaps"
        if gap > policy.max_path_gap_ms
        else "sampled"
    )
    diagnostics = dict(
        valued_at=path[-1].timestamp,
        observed_drawdown_fraction=drawdown,
        path_quality=quality,
        max_observed_gap_ms=gap,
    )
    if exit_index is None:
        return AccountingResult(
            **base,
            **diagnostics,
            status="exit_unavailable",
            fills=(entry_fill,),
            ending_units_eur=units,
            cash_usd=cash,
            unrealized_pnl_usd=unrealized,
            estimated_exit_fee_usd=exit_fee,
            liquidation_equity_usd=equity,
            commission_usd=entry_fee,
        )
    exit_fill = SimulatedFill(
        side="sell" if units > 0 else "buy",
        timestamp=path[-1].timestamp,
        units_eur=abs(units),
        price=costs.fill_price(path[-1], buy=units < 0),
        commission_usd=exit_fee,
    )
    net_pnl = equity - policy.initial_cash_usd
    return AccountingResult(
        **base,
        **diagnostics,
        status="closed",
        fills=(entry_fill, exit_fill),
        cash_usd=equity,
        liquidation_equity_usd=equity,
        commission_usd=entry_fee + exit_fee,
        closed_net_pnl_usd=net_pnl,
        closed_return_on_equity=net_pnl / policy.initial_cash_usd,
        closed_return_on_entry_notional=net_pnl / entry_notional,
    )


DEFAULT_LATENCIES = (
    LatencyScenario(
        name="five_seconds_assumed",
        ingest_ms=1000,
        reasoning_ms=3850,
        signal_ms=50,
        order_ms=50,
        execution_ms=50,
    ),
    LatencyScenario(
        name="poll_plus_reasoning_assumed",
        ingest_ms=60000,
        reasoning_ms=4850,
        signal_ms=50,
        order_ms=50,
        execution_ms=50,
    ),
    LatencyScenario(
        name="slow_reasoning_assumed",
        ingest_ms=60000,
        reasoning_ms=59850,
        signal_ms=50,
        order_ms=50,
        execution_ms=50,
    ),
)
DEFAULT_COSTS = (
    CostScenario(name="displayed_spread_only", commission_bps=0, slippage_bps=0),
    CostScenario(name="illustrative_base", commission_bps="0.5", slippage_bps="0.25"),
    CostScenario(name="illustrative_stress", commission_bps="1", slippage_bps="0.5"),
)


def accounting_grid(window: ResearchWindow) -> tuple[AccountingResult, ...]:
    """All predeclared directions; never select the best direction after the event."""
    directions: tuple[Literal["flat", "long", "short"], ...] = ("flat", "long", "short")
    return tuple(
        replay_roundtrip(
            window, direction=direction, horizon_seconds=horizon, latency=latency, costs=costs
        )
        for latency in DEFAULT_LATENCIES
        for costs in DEFAULT_COSTS
        for horizon in (300, 1800)
        for direction in directions
    )
