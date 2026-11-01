"""Historical OHLC bar replay provenance, resolution boundaries, and arrival sensitivity audit.

Strictly separates:
- historical OHLC minute bars (`basis="historical_bar_replay"`, `bar_interval_seconds=60`),
- synthetic symmetric Bid/Ask spreads (`synthetic_spread_applied=True`),
- simulated arrival timestamps (`simulated_receipt=True`), and
- verified live Demo quote observations (`basis="live_poll"`).

Forbids refreshing stale 1-minute bar open timestamps into fake sub-second quotes to bypass
live freshness gates, marks sub-minute execution claims `not_evaluable_from_minute_bars`,
and flags post-hoc or manually trimmed articles without contemporaneous availability proofs.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from .contracts import Quote

EURUSD_PIP = Decimal("0.0001")


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("Timezone-aware timestamp required")
    return ts.astimezone(UTC)


def build_historical_bar_quote(
    *,
    bar_open_at: datetime,
    price: Decimal,
    assumed_spread_pips: Decimal = Decimal("0.2"),
    bar_interval_seconds: int = 60,
    observed_at: datetime | None = None,
    instrument: str = "EUR/USD",
    source_provider: str = "historical_ohlc_bar",
) -> Quote:
    """Build a provenance-honest Quote from a historical bar without refreshing `source_at`.

    `source_at` is anchored strictly to `bar_open_at` (never shifted to `observed_at - 40ms`),
    `bridge_at` is `None`, `basis` is `"historical_bar_replay"`, and `has_verified_provenance`
    is guaranteed `False`.
    """
    bar_dt = _utc(bar_open_at)
    obs_dt = _utc(observed_at) if observed_at is not None else bar_dt
    if obs_dt < bar_dt:
        raise ValueError("Cannot observe a historical bar before bar_open_at")
    if bar_interval_seconds <= 0:
        raise ValueError("bar_interval_seconds must be positive")
    if price <= Decimal("0") or assumed_spread_pips < Decimal("0"):
        raise ValueError("Bar price must be positive and assumed_spread_pips non-negative")

    half_spread = (assumed_spread_pips * EURUSD_PIP) / Decimal("2")
    return Quote(
        instrument=instrument,
        source_at=bar_dt,
        bridge_at=None,
        received_at=obs_dt,
        available_at=obs_dt,
        source_provider=source_provider,
        basis="historical_bar_replay",
        bid=price - half_spread,
        ask=price + half_spread,
        bar_interval_seconds=bar_interval_seconds,
        synthetic_spread_applied=True,
        simulated_receipt=True,
    )


def evaluate_bar_replay_assumptions(spec: dict[str, Any]) -> dict[str, Any]:
    """Audit a historical bar replay specification for resolution limits, lookahead, and arrival sensitivity."""
    bar_interval = int(spec.get("bar_interval_seconds", 60))
    assumed_spread_pips = Decimal(str(spec.get("assumed_spread_pips", "0.2")))
    offsets = [
        int(x) for x in spec.get("sensitivity_arrival_offsets_seconds", [0, 15, 60, 180, 300])
    ]

    raw_bars = spec.get("sample_bars", [])
    bars: list[tuple[datetime, Decimal, Decimal, Decimal, Decimal]] = []
    for item in raw_bars:
        b_dt = _utc(datetime.fromisoformat(str(item["bar_open_utc"])))
        bars.append(
            (
                b_dt,
                Decimal(str(item["open"])),
                Decimal(str(item["high"])),
                Decimal(str(item["low"])),
                Decimal(str(item["close"])),
            )
        )
    bars.sort(key=lambda b: b[0])
    bar_times = [b[0] for b in bars]

    events = spec.get("events", [])
    event_audits: list[dict[str, Any]] = []
    sensitivity_rows: list[dict[str, Any]] = []
    all_contemporaneous = bool(events)

    for ev in events:
        ev_id = str(ev["event_id"])
        assumed_rx = _utc(datetime.fromisoformat(str(ev["assumed_received_utc"])))
        contemporaneous = bool(ev.get("captured_contemporaneously", False))
        post_hoc_trim = bool(ev.get("post_hoc_manual_trim", False))
        if not contemporaneous or post_hoc_trim:
            all_contemporaneous = False

        lookahead_status = (
            "verified_contemporaneous_capture"
            if (contemporaneous and not post_hoc_trim)
            else "lookahead_not_excluded_post_hoc_text"
        )
        event_audits.append(
            {
                "event_id": ev_id,
                "source_url": str(ev.get("source_url", "")),
                "assumed_received_utc": assumed_rx.isoformat(),
                "arrival_timestamp_kind": str(
                    ev.get("arrival_timestamp_kind", "manual_investigator_assumption")
                ),
                "captured_contemporaneously": contemporaneous,
                "post_hoc_manual_trim": post_hoc_trim,
                "lookahead_status": lookahead_status,
            }
        )

        ev_offsets: list[dict[str, Any]] = []
        for off_sec in offsets:
            shifted_rx = assumed_rx + timedelta(seconds=off_sec)
            matched_bar: dict[str, Any] | None = None
            if bars:
                idx = bisect_right(bar_times, shifted_rx) - 1
                if idx >= 0:
                    b_open_dt, b_o, b_h, b_l, b_c = bars[idx]
                    age_sec = round((shifted_rx - b_open_dt).total_seconds(), 2)
                    intrabar_span_pips = round(float((b_h - b_l) / EURUSD_PIP), 2)
                    matched_bar = {
                        "bar_open_utc": b_open_dt.isoformat(),
                        "bar_age_at_event_seconds": age_sec,
                        "passes_live_2s_freshness_gate": age_sec <= 2.0,
                        "bar_open_mid": str(b_o),
                        "bar_low_mid": str(b_l),
                        "bar_high_mid": str(b_h),
                        "bar_close_mid": str(b_c),
                        "intrabar_uncertainty_pips": intrabar_span_pips,
                    }
            ev_offsets.append(
                {
                    "offset_seconds": off_sec,
                    "effective_received_utc": shifted_rx.isoformat(),
                    "matched_minute_bar": matched_bar,
                }
            )
        sensitivity_rows.append(
            {
                "event_id": ev_id,
                "base_assumed_received_utc": assumed_rx.isoformat(),
                "offsets": ev_offsets,
            }
        )

    sub_minute_status = (
        "not_evaluable_from_minute_bars"
        if bar_interval >= 60
        else "evaluable_within_bar_resolution"
    )
    return {
        "replay_id": str(spec.get("replay_id", "historical_bar_replay")),
        "data_provenance": {
            "market_data_kind": "historical_ohlc_bars",
            "market_source_provider": str(
                spec.get("market_source_provider", "yahoo_finance_1m_ohlc")
            ),
            "bar_interval_seconds": bar_interval,
            "quote_basis": "historical_bar_replay",
            "bid_ask_construction": "synthetic_symmetric_spread_around_bar_price",
            "assumed_spread_pips": str(assumed_spread_pips),
            "synthetic_assumption": True,
            "verified_live_quote_provenance": False,
        },
        "eligible_as_live_forward_evidence": False,
        "second_level_execution_conclusions": sub_minute_status,
        "intrabar_sl_tp_ordering": sub_minute_status,
        "lookahead_bias_excluded": all_contemporaneous,
        "manual_arrival_assumptions": event_audits,
        "arrival_time_sensitivity": sensitivity_rows,
    }
