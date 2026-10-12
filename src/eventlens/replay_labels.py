"""Indicative FOMC labels from cTrader Demo backtest quote checkpoints.

Historical quote timestamps are not live feed receipt times or executable fills.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self

from pydantic import Field, model_validator

from .contracts import Contract, Identifier, Timestamp

DELAYS_SECONDS = (1, 5, 30, 60)
HORIZONS_SECONDS = (60, 300, 1800)


class ReplayCheckpoint(Contract):
    offset_seconds: int = Field(ge=0)
    quote_time: Timestamp
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def valid_quote(self) -> Self:
        if self.ask < self.bid:
            raise ValueError("Crossed replay quote")
        return self


class ReplayRun(Contract):
    event_id: Identifier
    instrument: Identifier
    source_time: Timestamp
    focus_time: Timestamp
    availability_basis: Literal["assumed"] = "assumed"
    source_kind: Literal["ctrader_demo_backtest_snapshot"] = "ctrader_demo_backtest_snapshot"
    checkpoints: tuple[ReplayCheckpoint, ...]

    @model_validator(mode="after")
    def valid_run(self) -> Self:
        if self.source_time != self.focus_time:
            raise ValueError("Focus must equal verified source publication time")
        offsets = [point.offset_seconds for point in self.checkpoints]
        if len(offsets) != len(set(offsets)):
            raise ValueError("Duplicate checkpoint offset")
        if any(
            point.quote_time < self.focus_time + timedelta(seconds=point.offset_seconds)
            for point in self.checkpoints
        ):
            raise ValueError("Checkpoint quote precedes its target time")
        return self


class ReplayLabel(Contract):
    event_id: Identifier
    instrument: Identifier
    event_source_time: Timestamp
    available_time: Timestamp
    availability_basis: Literal["assumed"] = "assumed"
    source_kind: Literal["ctrader_demo_backtest_snapshot"] = "ctrader_demo_backtest_snapshot"
    delay_seconds: int = Field(ge=0)
    horizon_seconds: int = Field(gt=0)
    status: Literal["available", "missing_checkpoint", "late_quote", "noncausal_sequence"]
    entry_quote_time: Timestamp | None = None
    exit_quote_time: Timestamp | None = None
    entry_bid: Decimal | None = None
    entry_ask: Decimal | None = None
    exit_bid: Decimal | None = None
    exit_ask: Decimal | None = None
    mid_return: Decimal | None = None
    spread_only_long_return: Decimal | None = None
    spread_only_short_return: Decimal | None = None

    @model_validator(mode="after")
    def valid_label(self) -> Self:
        values = (
            self.entry_quote_time,
            self.exit_quote_time,
            self.entry_bid,
            self.entry_ask,
            self.exit_bid,
            self.exit_ask,
            self.mid_return,
            self.spread_only_long_return,
            self.spread_only_short_return,
        )
        if self.status == "available":
            if any(value is None for value in values):
                raise ValueError("Available label requires both quotes and returns")
            entry_time, exit_time = self.entry_quote_time, self.exit_quote_time
            assert entry_time is not None and exit_time is not None
            if not self.available_time <= entry_time < exit_time:
                raise ValueError("Available label has noncausal quote times")
        elif any(value is not None for value in values):
            raise ValueError("Unavailable label cannot contain return data")
        return self


def label_replay(
    run: ReplayRun,
    *,
    delays_seconds: tuple[int, ...] = DELAYS_SECONDS,
    horizons_seconds: tuple[int, ...] = HORIZONS_SECONDS,
    max_checkpoint_lag: timedelta = timedelta(seconds=2),
) -> tuple[ReplayLabel, ...]:
    """Label later quotes; entry can never precede assumed availability.

    `spread_only_*` is a price diagnostic, excluding commission, slippage,
    liquidity, order latency and fills. It is not a strategy backtest.
    """
    if max_checkpoint_lag <= timedelta(0):
        raise ValueError("Maximum checkpoint lag must be positive")
    if any(delay < 0 for delay in delays_seconds) or any(horizon <= 0 for horizon in horizons_seconds):
        raise ValueError("Delays must be nonnegative and horizons positive")

    points = {point.offset_seconds: point for point in run.checkpoints}
    labels: list[ReplayLabel] = []
    for delay in delays_seconds:
        available_time = run.source_time + timedelta(seconds=delay)
        entry = points.get(delay)
        for horizon in horizons_seconds:
            exit_target = available_time + timedelta(seconds=horizon)
            exit_point = points.get(delay + horizon)
            base = dict(
                event_id=run.event_id,
                instrument=run.instrument,
                event_source_time=run.source_time,
                available_time=available_time,
                delay_seconds=delay,
                horizon_seconds=horizon,
            )
            if entry is None or exit_point is None:
                labels.append(ReplayLabel(**base, status="missing_checkpoint"))
                continue
            if (
                entry.quote_time - available_time > max_checkpoint_lag
                or exit_point.quote_time - exit_target > max_checkpoint_lag
            ):
                labels.append(ReplayLabel(**base, status="late_quote"))
                continue
            if exit_point.quote_time <= entry.quote_time:
                labels.append(ReplayLabel(**base, status="noncausal_sequence"))
                continue
            entry_mid = (entry.bid + entry.ask) / 2
            exit_mid = (exit_point.bid + exit_point.ask) / 2
            labels.append(
                ReplayLabel(
                    **base,
                    status="available",
                    entry_quote_time=entry.quote_time,
                    exit_quote_time=exit_point.quote_time,
                    entry_bid=entry.bid,
                    entry_ask=entry.ask,
                    exit_bid=exit_point.bid,
                    exit_ask=exit_point.ask,
                    mid_return=exit_mid / entry_mid - 1,
                    spread_only_long_return=exit_point.bid / entry.ask - 1,
                    spread_only_short_return=entry.bid / exit_point.ask - 1,
                )
            )
    return tuple(labels)


def parse_probe_log(
    path: Path, *, event_id: str, instrument: str, source_time: datetime
) -> ReplayRun:
    """Read only the probe's checkpoint lines; no broker identifiers are needed."""
    checkpoints: list[ReplayCheckpoint] = []
    focus: datetime | None = None
    started = False
    complete = False
    for line in path.read_text(encoding="utf-8").splitlines():
        marker = "EVENTLENS_PROBE|"
        if marker not in line:
            continue
        kind, *parts = line.split(marker, 1)[1].split("|")
        fields = dict(field.split("=", 1) for field in parts)
        if kind == "start":
            if started or fields.get("mode") != "demo_backtest_only" or fields.get("orders") != "none":
                raise ValueError("Log must contain one Demo backtest-only run")
            if fields.get("symbol", "").replace("/", "").upper() != instrument.replace("/", "").upper():
                raise ValueError("Probe symbol differs from requested instrument")
            started = True
            continue
        if kind == "summary":
            counters = ("invalid_quotes", "crossed_quotes", "backward_times")
            if complete or any(fields.get(name) != "0" for name in counters):
                raise ValueError("Incomplete or invalid probe summary")
            complete = True
            continue
        if kind != "checkpoint":
            continue
        row_focus = datetime.fromisoformat(fields["focus_utc"].replace("Z", "+00:00"))
        if focus is None:
            focus = row_focus
        elif focus != row_focus:
            raise ValueError("Log contains checkpoints from multiple focus times")
        if fields["quote_utc"] == "none":
            continue
        checkpoints.append(
            ReplayCheckpoint(
                offset_seconds=int(fields["offset_seconds"]),
                quote_time=datetime.fromisoformat(fields["quote_utc"].replace("Z", "+00:00")),
                bid=fields["bid"],
                ask=fields["ask"],
            )
        )
    if not started or not complete or focus is None:
        raise ValueError("Complete Demo probe run with checkpoints required")
    return ReplayRun(
        event_id=event_id,
        instrument=instrument,
        source_time=source_time,
        focus_time=focus,
        checkpoints=tuple(checkpoints),
    )
