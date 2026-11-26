"""Read-only Stage A research exporter, multi-horizon labeling, attribution & Net-R engine."""

from __future__ import annotations

import csv
import gzip
import json
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .dashboard import (
    _extract_headline_and_meta,
    _open_ro,
    _parse_iso,
    _safe_float,
    _sanitize_recursive,
    _table_exists,
    build_dashboard_snapshot,
    sanitize_and_verify_snapshot,
    sanitize_string,
)

RESEARCH_BUNDLE_SCHEMA_VERSION = "research-bundle-v1"
CHART_LAB_SCHEMA_VERSION = "chart-lab-v1"

PIP_SIZE = 0.0001
RAZOR_COMMISSION_ROUNDTRIP_PIPS = 0.60  # $3 per side per 100k lot ~= 0.60 pips round-trip

_LONDON_TZ = ZoneInfo("Europe/London")
_NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class OptimalTierSpec:
    tier: int
    key: str
    label: str
    horizon_min: int
    min_mfe_pips: float
    max_mae_pips: float
    nms_window_sec: int


@dataclass(frozen=True)
class ExitTemplateSpec:
    name: str
    sl_pips: float
    tp_pips: float
    trail_act_pips: float | None
    trail_dist_pips: float | None
    max_hold_min: int
    dynamic_atr: bool = False


ALGO_TIERS: tuple[OptimalTierSpec, ...] = (
    OptimalTierSpec(
        tier=1,
        key="tier1_scalp",
        label="Tier 1 Scalp",
        horizon_min=90,
        min_mfe_pips=10.0,
        max_mae_pips=8.0,
        nms_window_sec=2700,
    ),
    OptimalTierSpec(
        tier=2,
        key="tier2_swing",
        label="Tier 2 Mid Swing",
        horizon_min=240,
        min_mfe_pips=15.0,
        max_mae_pips=10.0,
        nms_window_sec=7200,
    ),
    OptimalTierSpec(
        tier=3,
        key="tier3_major",
        label="Tier 3 Large Swing",
        horizon_min=480,
        min_mfe_pips=25.0,
        max_mae_pips=15.0,
        nms_window_sec=14400,
    ),
)


STATIC_EXIT_TEMPLATES: tuple[ExitTemplateSpec, ...] = (
    ExitTemplateSpec(
        name="T1_scalp",
        sl_pips=10.0,
        tp_pips=15.0,
        trail_act_pips=None,
        trail_dist_pips=None,
        max_hold_min=90,
    ),
    ExitTemplateSpec(
        name="T2_mid_swing",
        sl_pips=14.0,
        tp_pips=24.0,
        trail_act_pips=10.0,
        trail_dist_pips=6.0,
        max_hold_min=240,
    ),
    ExitTemplateSpec(
        name="T3_large_swing",
        sl_pips=18.0,
        tp_pips=36.0,
        trail_act_pips=None,
        trail_dist_pips=None,
        max_hold_min=480,
    ),
)


def resolve_exit_templates(*, atr14_pips: float = 10.0) -> tuple[ExitTemplateSpec, ...]:
    """Return the 3 fixed exit templates plus T4 dynamic ATR volatility-scaled template."""
    atr = max(4.0, float(atr14_pips or 10.0))
    t4 = ExitTemplateSpec(
        name="T4_dynamic_atr",
        sl_pips=round(min(24.0, max(8.0, 1.5 * atr)), 1),
        tp_pips=round(min(48.0, max(14.0, 2.5 * atr)), 1),
        trail_act_pips=round(max(6.0, 1.2 * atr), 1),
        trail_dist_pips=round(max(4.0, 0.8 * atr), 1),
        max_hold_min=240,
        dynamic_atr=True,
    )
    return (*STATIC_EXIT_TEMPLATES, t4)


def classify_fx_session(dt: datetime) -> str:
    """Classify UTC/aware timestamp into Asian, London, London-NY Overlap, or Late NY."""
    utc_dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    ldn = utc_dt.astimezone(_LONDON_TZ)
    nyc = utc_dt.astimezone(_NY_TZ)

    ldn_mins = ldn.hour * 60 + ldn.minute
    nyc_mins = nyc.hour * 60 + nyc.minute

    is_london = (8 * 60) <= ldn_mins < (16 * 60 + 30)
    is_ny_am = (8 * 60) <= nyc_mins < (12 * 60)
    is_ny_pm = (12 * 60) <= nyc_mins < (17 * 60)

    if is_london and is_ny_am:
        return "overlap"
    if is_london:
        return "london"
    if is_ny_pm:
        return "late_ny"
    return "asian"


def _attach_atr14(bars: list[dict[str, Any]], *, step_seconds: int) -> None:
    """Attach ATR(14) in EUR/USD pips on timeframe bars without bridging weekend gaps."""
    trs: list[float] = []
    for idx, bar in enumerate(bars):
        tr = float(bar["h"]) - float(bar["l"])
        if idx > 0 and (int(bar["t"]) - int(bars[idx - 1]["t"])) <= step_seconds * 3:
            prev_c = float(bars[idx - 1]["c"])
            tr = max(tr, abs(float(bar["h"]) - prev_c), abs(float(bar["l"]) - prev_c))
        tr_pips = tr / PIP_SIZE
        trs.append(tr_pips)
        window = trs[max(0, len(trs) - 14) :]
        bar["atr14"] = round(sum(window) / len(window), 2)


def _rollup_bars(bars_m1: list[dict[str, Any]], *, step_minutes: int) -> list[dict[str, Any]]:
    """Deterministically roll sorted M1 bars up into M5, M15, or H1 bars."""
    if not bars_m1:
        return []
    step_sec = step_minutes * 60
    grouped: list[dict[str, Any]] = []
    cur_bucket: int | None = None
    cur: dict[str, Any] | None = None
    spread_num = 0.0
    spread_den = 0

    for b in bars_m1:
        bt = (int(b["t"]) // step_sec) * step_sec
        if bt != cur_bucket:
            if cur is not None:
                cur["sa"] = round(spread_num / max(1, spread_den), 2)
                grouped.append(cur)
            cur_bucket = bt
            dt_open = datetime.fromtimestamp(bt, tz=UTC)
            ticks = int(b["n"])
            spread_num = float(b["sa"]) * ticks
            spread_den = ticks
            cur = {
                "t": bt,
                "iso": dt_open.isoformat(),
                "o": b["o"],
                "h": b["h"],
                "l": b["l"],
                "c": b["c"],
                "bo": b["bo"],
                "bh": b["bh"],
                "bl": b["bl"],
                "bc": b["bc"],
                "ao": b["ao"],
                "ah": b["ah"],
                "al": b["al"],
                "ac": b["ac"],
                "sa": float(b["sa"]),
                "sm": float(b["sm"]),
                "n": ticks,
                "s": classify_fx_session(dt_open),
            }
        else:
            assert cur is not None
            cur["h"] = max(float(cur["h"]), float(b["h"]))
            cur["l"] = min(float(cur["l"]), float(b["l"]))
            cur["c"] = b["c"]
            cur["bh"] = max(float(cur["bh"]), float(b["bh"]))
            cur["bl"] = min(float(cur["bl"]), float(b["bl"]))
            cur["bc"] = b["bc"]
            cur["ah"] = max(float(cur["ah"]), float(b["ah"]))
            cur["al"] = min(float(cur["al"]), float(b["al"]))
            cur["ac"] = b["ac"]
            cur["sm"] = max(float(cur["sm"]), float(b["sm"]))
            ticks = int(b["n"])
            cur["n"] = int(cur["n"]) + ticks
            spread_num += float(b["sa"]) * ticks
            spread_den += ticks

    if cur is not None:
        cur["sa"] = round(spread_num / max(1, spread_den), 2)
        grouped.append(cur)

    _attach_atr14(grouped, step_seconds=step_sec)
    return grouped


def aggregate_quote_bars(
    quotes_db: Path | None,
    *,
    symbol: str = "EURUSD",
    basis: str = "live_poll",
    days: int | None = None,
) -> dict[str, Any]:
    """Stream quote_observations in read-only mode into M1/M5/M15/H1 bars and session stats."""
    empty_res: dict[str, Any] = {
        "bars_m1": [],
        "bars_m5": [],
        "bars_m15": [],
        "bars_h1": [],
        "total_ticks": 0,
        "first_iso": None,
        "last_iso": None,
        "session_stats": {},
    }
    conn = _open_ro(quotes_db)
    if conn is None or not _table_exists(conn, "quote_observations"):
        if conn is not None:
            conn.close()
        return empty_res

    try:
        min_received_iso: str | None = None
        if days is not None and days > 0:
            row_max = conn.execute(
                "SELECT MAX(received_at) FROM quote_observations WHERE symbol=? AND basis=?",
                (symbol, basis),
            ).fetchone()
            max_iso = row_max[0] if row_max else None
            max_dt = _parse_iso(str(max_iso)) if max_iso else None
            if max_dt is not None:
                min_received_iso = (max_dt - timedelta(days=int(days))).isoformat()

        if min_received_iso is not None:
            cursor = conn.execute(
                """
                SELECT received_at,
                       json_extract(quote, '$.bid') AS bid,
                       json_extract(quote, '$.ask') AS ask
                FROM quote_observations
                WHERE symbol=? AND basis=? AND received_at>=?
                ORDER BY received_at, source_at, sequence
                """,
                (symbol, basis, min_received_iso),
            )
        else:
            cursor = conn.execute(
                """
                SELECT received_at,
                       json_extract(quote, '$.bid') AS bid,
                       json_extract(quote, '$.ask') AS ask
                FROM quote_observations
                WHERE symbol=? AND basis=?
                ORDER BY received_at, source_at, sequence
                """,
                (symbol, basis),
            )

        # Accumulate M1 OHLC + bid/ask OHLC without buffering raw ticks in memory.
        m1_acc: dict[int, list[Any]] = {}
        total_ticks = 0
        first_iso: str | None = None
        last_iso: str | None = None

        while True:
            batch = cursor.fetchmany(4096)
            if not batch:
                break
            for rec_iso, raw_bid, raw_ask in batch:
                dt = _parse_iso(str(rec_iso))
                bid = _safe_float(raw_bid)
                ask = _safe_float(raw_ask)
                if dt is None or bid is None or ask is None:
                    continue
                if bid <= 0.0 or ask <= 0.0 or ask < bid:
                    continue
                spread_pips = (ask - bid) / PIP_SIZE
                if spread_pips > 50.0:
                    continue
                mid = (bid + ask) * 0.5
                iso_s = dt.isoformat()
                if first_iso is None:
                    first_iso = iso_s
                last_iso = iso_s
                total_ticks += 1

                bt = (int(dt.timestamp()) // 60) * 60
                acc = m1_acc.get(bt)
                if acc is None:
                    m1_acc[bt] = [
                        mid,
                        mid,
                        mid,
                        mid,  # 0..3 mid ohlc
                        bid,
                        bid,
                        bid,
                        bid,  # 4..7 bid ohlc
                        ask,
                        ask,
                        ask,
                        ask,  # 8..11 ask ohlc
                        spread_pips,
                        spread_pips,
                        1,  # 12..14 sum, max, count
                    ]
                else:
                    if mid > acc[1]:
                        acc[1] = mid
                    if mid < acc[2]:
                        acc[2] = mid
                    acc[3] = mid
                    if bid > acc[5]:
                        acc[5] = bid
                    if bid < acc[6]:
                        acc[6] = bid
                    acc[7] = bid
                    if ask > acc[9]:
                        acc[9] = ask
                    if ask < acc[10]:
                        acc[10] = ask
                    acc[11] = ask
                    acc[12] += spread_pips
                    if spread_pips > acc[13]:
                        acc[13] = spread_pips
                    acc[14] += 1

        bars_m1: list[dict[str, Any]] = []
        for bt in sorted(m1_acc.keys()):
            acc = m1_acc[bt]
            dt_open = datetime.fromtimestamp(bt, tz=UTC)
            bars_m1.append(
                {
                    "t": bt,
                    "iso": dt_open.isoformat(),
                    "o": round(float(acc[0]), 5),
                    "h": round(float(acc[1]), 5),
                    "l": round(float(acc[2]), 5),
                    "c": round(float(acc[3]), 5),
                    "bo": round(float(acc[4]), 5),
                    "bh": round(float(acc[5]), 5),
                    "bl": round(float(acc[6]), 5),
                    "bc": round(float(acc[7]), 5),
                    "ao": round(float(acc[8]), 5),
                    "ah": round(float(acc[9]), 5),
                    "al": round(float(acc[10]), 5),
                    "ac": round(float(acc[11]), 5),
                    "sa": round(float(acc[12]) / max(1, int(acc[14])), 2),
                    "sm": round(float(acc[13]), 2),
                    "n": int(acc[14]),
                    "s": classify_fx_session(dt_open),
                }
            )
        _attach_atr14(bars_m1, step_seconds=60)
        bars_m5 = _rollup_bars(bars_m1, step_minutes=5)
        bars_m15 = _rollup_bars(bars_m1, step_minutes=15)
        bars_h1 = _rollup_bars(bars_m1, step_minutes=60)

        session_stats: dict[str, dict[str, Any]] = {}
        for sess in ("asian", "london", "overlap", "late_ny"):
            s_m1 = [b for b in bars_m1 if b["s"] == sess]
            s_m5 = [b for b in bars_m5 if b["s"] == sess]
            avg_spread = (
                round(
                    sum(float(b["sa"]) * int(b["n"]) for b in s_m1)
                    / max(1, sum(int(b["n"]) for b in s_m1)),
                    2,
                )
                if s_m1
                else 0.0
            )
            avg_m5_range = (
                round(
                    sum((float(b["h"]) - float(b["l"])) / PIP_SIZE for b in s_m5)
                    / max(1, len(s_m5)),
                    2,
                )
                if s_m5
                else 0.0
            )
            session_stats[sess] = {
                "m1_bars": len(s_m1),
                "m5_bars": len(s_m5),
                "ticks": sum(int(b["n"]) for b in s_m1),
                "avg_spread_pips": avg_spread,
                "avg_m5_range_pips": avg_m5_range,
            }

        return {
            "bars_m1": bars_m1,
            "bars_m5": bars_m5,
            "bars_m15": bars_m15,
            "bars_h1": bars_h1,
            "total_ticks": total_ticks,
            "first_iso": first_iso,
            "last_iso": last_iso,
            "session_stats": session_stats,
        }
    finally:
        conn.close()


def detect_optimal_entries(
    bars_m1: list[dict[str, Any]],
    bars_m5: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detect causal 3-tier MFE/MAE optimal entries across M5 bars and deduplicate clusters."""
    if not bars_m1 or not bars_m5:
        return []

    m1_ts = [int(b["t"]) for b in bars_m1]
    raw_candidates: list[dict[str, Any]] = []
    max_horizon_sec = 480 * 60

    for bar in bars_m5:
        # Evaluate entry at M5 close (t + 300s) over strictly future M1 bars.
        bar_t = int(bar["t"])
        entry_t = bar_t + 300
        start_idx = bisect_left(m1_ts, entry_t)
        if start_idx >= len(bars_m1):
            continue
        # Ensure we actually have a fresh continuation sample within 15 minutes.
        if m1_ts[start_idx] - entry_t > 900:
            continue

        entry_ask = float(bar["ac"])
        entry_bid = float(bar["bc"])

        for side in ("long", "short"):
            entry_px = entry_ask if side == "long" else entry_bid
            running_mfe = 0.0
            running_mae = 0.0
            peak_min = 0

            # Per-tier causal trackers: target must trigger before MAE breaches limit.
            tier_ok: dict[int, bool] = {t.tier: False for t in ALGO_TIERS}
            tier_breached: dict[int, bool] = {t.tier: False for t in ALGO_TIERS}
            tier_mfe_at_hit: dict[int, float] = {}
            tier_mae_at_hit: dict[int, float] = {}
            tier_peak_min: dict[int, int] = {}

            end_t = entry_t + max_horizon_sec
            idx = start_idx
            while idx < len(bars_m1) and m1_ts[idx] <= end_t:
                fb = bars_m1[idx]
                elapsed_min = max(1, (m1_ts[idx] - entry_t) // 60 + 1)

                if side == "long":
                    adverse = (entry_px - float(fb["bl"])) / PIP_SIZE
                    favorable = (float(fb["bh"]) - entry_px) / PIP_SIZE
                else:
                    adverse = (float(fb["ah"]) - entry_px) / PIP_SIZE
                    favorable = (entry_px - float(fb["al"])) / PIP_SIZE

                if adverse > running_mae:
                    running_mae = adverse
                if favorable > running_mfe:
                    running_mfe = favorable
                    peak_min = elapsed_min

                # Conservative intra-bar ordering: check adverse stop breach BEFORE target touch.
                for spec in ALGO_TIERS:
                    if elapsed_min > spec.horizon_min or tier_ok[spec.tier]:
                        continue
                    if tier_breached[spec.tier]:
                        continue
                    if running_mae > spec.max_mae_pips:
                        tier_breached[spec.tier] = True
                        continue
                    if running_mfe >= spec.min_mfe_pips:
                        tier_ok[spec.tier] = True
                        tier_mfe_at_hit[spec.tier] = round(running_mfe, 1)
                        tier_mae_at_hit[spec.tier] = round(running_mae, 1)
                        tier_peak_min[spec.tier] = peak_min
                idx += 1

            matched = [spec for spec in ALGO_TIERS if tier_ok[spec.tier]]
            if not matched:
                continue
            best = max(matched, key=lambda s: s.tier)
            raw_candidates.append(
                {
                    "t": bar_t,
                    "entry_t": entry_t,
                    "iso": bar["iso"],
                    "side": side,
                    "tier": best.tier,
                    "tier_key": best.key,
                    "tier_label": best.label,
                    "mfe_pips": max(tier_mfe_at_hit[best.tier], round(running_mfe, 1)),
                    "mae_pips": tier_mae_at_hit[best.tier],
                    "horizon_min": best.horizon_min,
                    "time_to_peak_min": tier_peak_min[best.tier],
                    "price": round(float(bar["c"]), 5),
                    "entry_fill": round(entry_px, 5),
                    "session": bar["s"],
                    "nms_window_sec": best.nms_window_sec,
                    "_score": best.tier * 100.0
                    + (tier_mfe_at_hit[best.tier] - tier_mae_at_hit[best.tier]),
                }
            )

    # Non-maximum suppression: keep strongest/highest-tier anchor per direction window.
    selected: list[dict[str, Any]] = []
    for side in ("long", "short"):
        side_cands = [c for c in raw_candidates if c["side"] == side]
        side_cands.sort(key=lambda c: (-float(c["_score"]), int(c["t"])))
        kept_times: list[tuple[int, int]] = []
        for cand in side_cands:
            ct = int(cand["t"])
            win = int(cand["nms_window_sec"])
            if any(abs(ct - kt) < max(win, kwin) // 2 for kt, kwin in kept_times):
                continue
            kept_times.append((ct, win))
            clean = {k: v for k, v in cand.items() if k not in ("_score", "nms_window_sec")}
            selected.append(clean)

    selected.sort(key=lambda c: (int(c["t"]), str(c["side"])))
    return selected


def _price_at_timestamp(
    bars_m1: list[dict[str, Any]],
    m1_ts: list[int],
    target_ts: int,
    *,
    tolerance_sec: int = 900,
) -> dict[str, Any] | None:
    """Return M1 bar closest to target_ts (preferring <= target_ts) within tolerance."""
    if not bars_m1:
        return None
    pos = bisect_right(m1_ts, target_ts) - 1
    candidates: list[dict[str, Any]] = []
    if pos >= 0:
        candidates.append(bars_m1[pos])
    if pos + 1 < len(bars_m1):
        candidates.append(bars_m1[pos + 1])
    best: dict[str, Any] | None = None
    best_dist = tolerance_sec + 1
    for c in candidates:
        dist = abs(int(c["t"]) - target_ts)
        if dist <= tolerance_sec and dist < best_dist:
            best_dist = dist
            best = c
    return best


def compute_natural_drifts(
    bars_m1: list[dict[str, Any]],
    m1_ts: list[int],
    anchor_ts: int,
) -> dict[str, float | None]:
    """Compute signed EUR/USD pip changes across pre- and post-anchor windows."""
    anchor_bar = _price_at_timestamp(bars_m1, m1_ts, anchor_ts, tolerance_sec=900)
    if anchor_bar is None:
        return {
            "anchor_mid": None,
            "pre_60m_pips": None,
            "pre_15m_pips": None,
            "pre_5m_pips": None,
            "post_15m_pips": None,
            "post_30m_pips": None,
            "post_60m_pips": None,
            "post_15m_to_60m_pips": None,
            "post_4h_pips": None,
        }

    p0 = float(anchor_bar["c"])

    def _delta(offset_min: int, tol: int = 600) -> tuple[float | None, float | None]:
        b = _price_at_timestamp(bars_m1, m1_ts, anchor_ts + offset_min * 60, tolerance_sec=tol)
        if b is None:
            return None, None
        pc = float(b["c"])
        if offset_min < 0:
            return round((p0 - pc) / PIP_SIZE, 1), pc
        return round((pc - p0) / PIP_SIZE, 1), pc

    pre_60, _ = _delta(-60, tol=900)
    pre_15, _ = _delta(-15, tol=480)
    pre_5, _ = _delta(-5, tol=240)
    post_15, p15 = _delta(15, tol=480)
    post_30, _ = _delta(30, tol=600)
    post_60, p60 = _delta(60, tol=900)
    post_4h, _ = _delta(240, tol=1200)
    post_15_to_60 = (
        round((p60 - p15) / PIP_SIZE, 1) if p15 is not None and p60 is not None else None
    )

    return {
        "anchor_mid": round(p0, 5),
        "pre_60m_pips": pre_60,
        "pre_15m_pips": pre_15,
        "pre_5m_pips": pre_5,
        "post_15m_pips": post_15,
        "post_30m_pips": post_30,
        "post_60m_pips": post_60,
        "post_15m_to_60m_pips": post_15_to_60,
        "post_4h_pips": post_4h,
    }


def simulate_exit_templates(
    bars_m1: list[dict[str, Any]],
    entry_ts: int,
    side: str,
    *,
    atr14_pips: float = 10.0,
    commission_roundtrip_pips: float = RAZOR_COMMISSION_ROUNDTRIP_PIPS,
    m1_ts: list[int] | None = None,
) -> dict[str, Any]:
    """Simulate all 4 exit templates causally using Bid/Ask quotes and Razor commissions."""
    if side not in ("long", "short"):
        raise ValueError(f"Unsupported trade side: {side}")
    ts_idx = m1_ts if m1_ts is not None else [int(b["t"]) for b in bars_m1]
    entry_bar = _price_at_timestamp(bars_m1, ts_idx, entry_ts, tolerance_sec=600)
    templates = resolve_exit_templates(atr14_pips=atr14_pips)

    if entry_bar is None:
        return {
            "available": False,
            "templates": {},
            "drift_30m_pips": None,
            "drift_1h_pips": None,
            "drift_4h_pips": None,
        }

    entry_px = float(entry_bar["ac"]) if side == "long" else float(entry_bar["bc"])
    start_idx = bisect_left(ts_idx, int(entry_bar["t"]) + 60)

    drifts = compute_natural_drifts(bars_m1, ts_idx, int(entry_bar["t"]))
    sign = 1.0 if side == "long" else -1.0

    results: dict[str, dict[str, Any]] = {}
    for tmpl in templates:
        end_t = int(entry_bar["t"]) + tmpl.max_hold_min * 60
        sl_level = (
            entry_px - tmpl.sl_pips * PIP_SIZE
            if side == "long"
            else entry_px + tmpl.sl_pips * PIP_SIZE
        )
        tp_level = (
            entry_px + tmpl.tp_pips * PIP_SIZE
            if side == "long"
            else entry_px - tmpl.tp_pips * PIP_SIZE
        )
        trail_stop: float | None = None
        peak_fav_pips = 0.0
        mfe_pips = 0.0
        mae_pips = 0.0
        exit_px: float | None = None
        exit_reason = "timeout"
        hold_min = 0

        idx = start_idx
        last_seen_bar: dict[str, Any] | None = None
        while idx < len(bars_m1) and ts_idx[idx] <= end_t:
            fb = bars_m1[idx]
            last_seen_bar = fb
            hold_min = max(1, (ts_idx[idx] - int(entry_bar["t"])) // 60)

            if side == "long":
                adv_px = float(fb["bl"])
                fav_px = float(fb["bh"])
                mfe_pips = max(mfe_pips, (fav_px - entry_px) / PIP_SIZE)
                mae_pips = max(mae_pips, (entry_px - adv_px) / PIP_SIZE)

                active_stop = max(sl_level, trail_stop) if trail_stop is not None else sl_level
                # Conservative ordering: stop checks run before take-profit within the same bar.
                if adv_px <= active_stop:
                    exit_px = active_stop
                    exit_reason = (
                        "trailing_stop"
                        if trail_stop is not None and active_stop == trail_stop
                        else "stop_loss"
                    )
                    break
                if fav_px >= tp_level:
                    exit_px = tp_level
                    exit_reason = "take_profit"
                    break
                cur_fav = (fav_px - entry_px) / PIP_SIZE
                if cur_fav > peak_fav_pips:
                    peak_fav_pips = cur_fav
                if (
                    tmpl.trail_act_pips is not None
                    and tmpl.trail_dist_pips is not None
                    and peak_fav_pips >= tmpl.trail_act_pips
                ):
                    cand_trail = entry_px + (peak_fav_pips - tmpl.trail_dist_pips) * PIP_SIZE
                    trail_stop = cand_trail if trail_stop is None else max(trail_stop, cand_trail)
            else:
                adv_px = float(fb["ah"])
                fav_px = float(fb["al"])
                mfe_pips = max(mfe_pips, (entry_px - fav_px) / PIP_SIZE)
                mae_pips = max(mae_pips, (adv_px - entry_px) / PIP_SIZE)

                active_stop = min(sl_level, trail_stop) if trail_stop is not None else sl_level
                if adv_px >= active_stop:
                    exit_px = active_stop
                    exit_reason = (
                        "trailing_stop"
                        if trail_stop is not None and active_stop == trail_stop
                        else "stop_loss"
                    )
                    break
                if fav_px <= tp_level:
                    exit_px = tp_level
                    exit_reason = "take_profit"
                    break
                cur_fav = (entry_px - fav_px) / PIP_SIZE
                if cur_fav > peak_fav_pips:
                    peak_fav_pips = cur_fav
                if (
                    tmpl.trail_act_pips is not None
                    and tmpl.trail_dist_pips is not None
                    and peak_fav_pips >= tmpl.trail_act_pips
                ):
                    cand_trail = entry_px - (peak_fav_pips - tmpl.trail_dist_pips) * PIP_SIZE
                    trail_stop = cand_trail if trail_stop is None else min(trail_stop, cand_trail)

            idx += 1

        if exit_px is None:
            if last_seen_bar is None:
                results[tmpl.name] = {
                    "available": False,
                    "sl_pips": tmpl.sl_pips,
                    "tp_pips": tmpl.tp_pips,
                    "net_pips": None,
                    "r_net": None,
                    "mfe_pips": 0.0,
                    "mae_pips": 0.0,
                    "exit_reason": "insufficient_future_bars",
                    "holding_min": 0,
                }
                continue
            exit_px = float(last_seen_bar["bc"]) if side == "long" else float(last_seen_bar["ac"])
            exit_reason = "timeout"

        gross_pips = (exit_px - entry_px) / PIP_SIZE * sign
        net_pips = round(gross_pips - commission_roundtrip_pips, 2)
        r_net = round(net_pips / max(0.1, tmpl.sl_pips), 3)
        results[tmpl.name] = {
            "available": True,
            "sl_pips": tmpl.sl_pips,
            "tp_pips": tmpl.tp_pips,
            "net_pips": net_pips,
            "r_net": r_net,
            "mfe_pips": round(mfe_pips, 1),
            "mae_pips": round(mae_pips, 1),
            "exit_reason": exit_reason,
            "holding_min": hold_min,
        }

    return {
        "available": True,
        "entry_t": int(entry_bar["t"]),
        "entry_fill": round(entry_px, 5),
        "templates": results,
        "drift_30m_pips": (
            round(float(drifts["post_30m_pips"]) * sign, 1)
            if drifts["post_30m_pips"] is not None
            else None
        ),
        "drift_1h_pips": (
            round(float(drifts["post_60m_pips"]) * sign, 1)
            if drifts["post_60m_pips"] is not None
            else None
        ),
        "drift_4h_pips": (
            round(float(drifts["post_4h_pips"]) * sign, 1)
            if drifts["post_4h_pips"] is not None
            else None
        ),
    }


def score_decision_net_r(
    *,
    action: str,
    side: str | None,
    long_eval: dict[str, Any] | None,
    short_eval: dict[str, Any] | None,
    has_qualified_setup: bool,
    lambda_mae: float = 0.50,
    alpha_miss: float = 0.30,
    alpha_avoid: float = 0.25,
) -> dict[str, Any]:
    """Compute Blueprint v2 Stage 3 standardized Net-R reward with MAE & miss/avoid shaping."""
    norm_action = action.upper().strip()
    norm_side = side.lower().strip() if side else None

    if norm_action == "API_ERROR":
        return {"score": None, "r_net": None, "mae_penalty": 0.0, "regime": "api_error"}

    if norm_action in ("BUY", "SELL", "SET_TARGET", "LONG", "SHORT") and norm_side in (
        "long",
        "short",
    ):
        chosen = long_eval if norm_side == "long" else short_eval
        if not chosen or not chosen.get("available"):
            return {"score": None, "r_net": None, "mae_penalty": 0.0, "regime": "unavailable"}
        sl_pips = max(0.1, float(chosen.get("sl_pips") or 14.0))
        r_net = float(chosen["r_net"])
        mae_pips = abs(float(chosen.get("mae_pips") or 0.0))
        mae_excess = max(0.0, mae_pips / sl_pips - 0.70)
        mae_penalty = round(lambda_mae * mae_excess, 3)
        score = round(r_net - mae_penalty, 3)
        return {
            "score": score,
            "r_net": round(r_net, 3),
            "mae_penalty": mae_penalty,
            "regime": "active_entry",
        }

    # Flat decisions (HOLD / IGNORE_EVENT / AVOID)
    r_long = (
        float(long_eval["r_net"])
        if long_eval and long_eval.get("available") and long_eval.get("r_net") is not None
        else None
    )
    r_short = (
        float(short_eval["r_net"])
        if short_eval and short_eval.get("available") and short_eval.get("r_net") is not None
        else None
    )
    if has_qualified_setup:
        valid_rs = [r for r in (r_long, r_short) if r is not None and r > 0.0]
        if valid_rs:
            best_r = max(valid_rs)
            return {
                "score": round(-alpha_miss * best_r, 3),
                "r_net": 0.0,
                "mae_penalty": 0.0,
                "regime": "missed_qualified_setup",
            }
    if r_long is not None and r_short is not None and r_long < 0.0 and r_short < 0.0:
        return {
            "score": round(alpha_avoid, 3),
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "avoided_two_way_trap",
        }
    return {"score": 0.0, "r_net": 0.0, "mae_penalty": 0.0, "regime": "neutral_flat"}


def split_chronological_oos(
    items: list[dict[str, Any]],
    *,
    ts_key: str = "t",
    train_ratio: float = 0.70,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int | None]:
    """Chronologically partition sorted items into in-sample discovery and unseen OOS slices."""
    if not items:
        return [], [], None
    ordered = sorted(items, key=lambda x: int(x.get(ts_key) or 0))
    if len(ordered) == 1:
        return ordered, [], None
    cut = max(1, min(len(ordered) - 1, int(len(ordered) * train_ratio)))
    split_ts = int(ordered[cut].get(ts_key) or 0)
    return ordered[:cut], ordered[cut:], split_ts


def _clean_snippet(raw_title: str, raw_text: str, *, max_len: int = 240) -> str:
    body = sanitize_string(raw_text or "").strip()
    title = sanitize_string(raw_title or "").strip()
    if body.lower().startswith(title.lower()):
        body = body[len(title) :].lstrip(" :-\n")
    compact = " ".join(body.split())
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 1].rstrip() + "…"


def _extract_target_direction(dec_obj: dict[str, Any]) -> str | None:
    """Resolve target direction from either flat fields or nested AgentDecision target positions."""
    direct = dec_obj.get("target_direction")
    if isinstance(direct, str) and direct.lower().strip() in ("long", "short"):
        return direct.lower().strip()
    target = dec_obj.get("target")
    if isinstance(target, dict):
        positions = target.get("positions")
        if isinstance(positions, list):
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                weight = _safe_float(pos.get("target_weight"))
                if weight is not None and weight > 0:
                    return "long"
                if weight is not None and weight < 0:
                    return "short"
    return None


def extract_attributed_events(
    *,
    observer_db: Path | None,
    shadow_db: Path | None = None,
    bars_m1: list[dict[str, Any]],
    algo_markers: list[dict[str, Any]] | None = None,
    snippet_max_len: int = 240,
    include_counterfactuals: bool = False,
) -> list[dict[str, Any]]:
    """Extract deduplicated arrival-clocked events with bidirectional ±60m price attribution."""
    obs_conn = _open_ro(observer_db)
    if obs_conn is None or not _table_exists(obs_conn, "observer_revisions"):
        if obs_conn is not None:
            obs_conn.close()
        return []

    shadow_conn = _open_ro(shadow_db)
    try:
        # Build lookup maps from shadow-ledger runtime_events & decisions without locking.
        rt_by_ref: dict[str, dict[str, Any]] = {}
        rt_by_prefix: dict[str, dict[str, Any]] = {}
        if shadow_conn is not None and _table_exists(shadow_conn, "runtime_events"):
            rt_rows = shadow_conn.execute(
                """
                SELECT event_id, source_reference, state, reason, decision_id, decision_json
                FROM runtime_events
                ORDER BY updated_at DESC
                """
            ).fetchall()
            for r in rt_rows:
                dec_obj: dict[str, Any] = {}
                if r["decision_json"]:
                    try:
                        parsed = json.loads(str(r["decision_json"]))
                        if isinstance(parsed, dict):
                            dec_obj = parsed
                    except ValueError:
                        pass
                info = {
                    "runtime_state": str(r["state"]),
                    "filter_reason": sanitize_string(str(r["reason"] or "")),
                    "decision_id": str(r["decision_id"]) if r["decision_id"] else None,
                    "portfolio_action": dec_obj.get("portfolio_action"),
                    "target_direction": _extract_target_direction(dec_obj),
                }
                ref = str(r["source_reference"] or "")
                eid = str(r["event_id"] or "")
                if ref and ref not in rt_by_ref:
                    rt_by_ref[ref] = info
                # event_id pattern: provider:kind:source_id:rN
                parts = eid.split(":")
                if len(parts) >= 3:
                    key = ":".join(parts[:3])
                    if key not in rt_by_prefix:
                        rt_by_prefix[key] = info

        enrich_by_key: dict[tuple[str, str], str] = {}
        if _table_exists(obs_conn, "observer_enrichments"):
            erows = obs_conn.execute(
                """
                SELECT provider, source_id, extracted_text
                FROM observer_enrichments
                ORDER BY recorded_at ASC
                """
            ).fetchall()
            for r in erows:
                txt = str(r["extracted_text"] or "").strip()
                if txt:
                    enrich_by_key[(str(r["provider"]), str(r["source_id"]))] = txt

        # Query only lightweight metadata/revisions; NEVER scan the multi-GB observer_payloads blob table.
        rev_rows = obs_conn.execute(
            """
            SELECT provider, kind, source_id, revision, received_at, record
            FROM observer_revisions
            ORDER BY provider, kind, source_id, revision ASC
            """
        ).fetchall()

        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rev_rows:
            gkey = (str(row["provider"]), str(row["kind"]), str(row["source_id"]))
            try:
                rec = json.loads(str(row["record"]))
            except ValueError:
                rec = {}
            if not isinstance(rec, dict):
                rec = {}

            rec_iso = str(row["received_at"])
            entry = grouped.get(gkey)
            if entry is None:
                grouped[gkey] = {
                    "provider": gkey[0],
                    "kind": gkey[1],
                    "source_id": gkey[2],
                    "first_received_at": rec_iso,
                    "revisions": 1,
                    "record": rec,
                }
            else:
                if rec_iso < entry["first_received_at"]:
                    entry["first_received_at"] = rec_iso
                entry["revisions"] = int(entry["revisions"]) + 1
                entry["record"] = rec

        m1_ts = [int(b["t"]) for b in bars_m1]
        marker_ts = [int(m["t"]) for m in (algo_markers or [])]
        min_bar_ts = m1_ts[0] - 1800 if m1_ts else None
        max_bar_ts = m1_ts[-1] + 1800 if m1_ts else None

        events: list[dict[str, Any]] = []
        for gkey, item in grouped.items():
            dt = _parse_iso(item["first_received_at"])
            if dt is None:
                continue
            ev_ts = int(dt.timestamp())
            # Keep chart-lab events scoped to the bar range when bar timestamps are loaded.
            if (
                min_bar_ts is not None
                and max_bar_ts is not None
                and not (min_bar_ts <= ev_ts <= max_bar_ts)
            ):
                continue

            rec = item["record"]
            raw_title = str(rec.get("title") or "").strip()
            raw_text = str(rec.get("text") or "").strip()
            if not raw_title and raw_text:
                raw_title, _ = _extract_headline_and_meta(raw_text)
            enriched = enrich_by_key.get((gkey[0], gkey[2]), "")
            body_text = enriched if len(enriched) > len(raw_text) else raw_text

            source_url = str(rec.get("source_url") or "")
            rt_info = rt_by_ref.get(source_url) or rt_by_prefix.get(
                f"{gkey[0]}:{gkey[1]}:{gkey[2]}", {}
            )

            pub_iso = rec.get("original_published_at") or rec.get("scheduled_at")
            pub_dt = _parse_iso(str(pub_iso)) if pub_iso else None
            feed_lag_sec: int | None = None
            if pub_dt is not None:
                lag = int((dt - pub_dt).total_seconds())
                if -60 <= lag <= 86400 * 7:
                    feed_lag_sec = lag

            drifts = compute_natural_drifts(bars_m1, m1_ts, ev_ts)
            pre5 = drifts["pre_5m_pips"]
            pre15 = drifts["pre_15m_pips"]
            front_run_flag = bool(
                (pre5 is not None and abs(pre5) >= 4.0) or (pre15 is not None and abs(pre15) >= 7.0)
            )

            anchor_bar = _price_at_timestamp(bars_m1, m1_ts, ev_ts, tolerance_sec=900)
            atr14 = float(anchor_bar.get("atr14") or 10.0) if anchor_bar else 10.0

            ev_dict: dict[str, Any] = {
                "event_uid": f"{gkey[0]}:{gkey[1]}:{gkey[2]}",
                "t": ev_ts,
                "iso": dt.isoformat(),
                "session": classify_fx_session(dt),
                "provider": gkey[0],
                "kind": gkey[1],
                "importance": rec.get("importance"),
                "revisions": item["revisions"],
                "title": sanitize_string(raw_title)[:200],
                "snippet": _clean_snippet(raw_title, body_text, max_len=snippet_max_len),
                "original_published_at": str(pub_iso) if pub_iso else None,
                "feed_lag_sec": feed_lag_sec,
                "runtime_state": rt_info.get("runtime_state"),
                "filter_reason": rt_info.get("filter_reason"),
                "portfolio_action": rt_info.get("portfolio_action"),
                "target_direction": rt_info.get("target_direction"),
                "decision_id": rt_info.get("decision_id"),
                "pre_60m_pips": drifts["pre_60m_pips"],
                "pre_15m_pips": pre15,
                "pre_5m_pips": pre5,
                "post_15m_pips": drifts["post_15m_pips"],
                "post_30m_pips": drifts["post_30m_pips"],
                "post_60m_pips": drifts["post_60m_pips"],
                "post_15m_to_60m_pips": drifts["post_15m_to_60m_pips"],
                "post_4h_pips": drifts["post_4h_pips"],
                "front_run_flag": front_run_flag,
            }

            if include_counterfactuals and bars_m1:
                has_setup = False
                if algo_markers and marker_ts:
                    pos = bisect_left(marker_ts, ev_ts - 900)
                    while pos < len(marker_ts) and marker_ts[pos] <= ev_ts + 900:
                        has_setup = True
                        break
                long_cf = simulate_exit_templates(
                    bars_m1, ev_ts, "long", atr14_pips=atr14, m1_ts=m1_ts
                )
                short_cf = simulate_exit_templates(
                    bars_m1, ev_ts, "short", atr14_pips=atr14, m1_ts=m1_ts
                )
                t2_long = (long_cf.get("templates") or {}).get("T2_mid_swing")
                t2_short = (short_cf.get("templates") or {}).get("T2_mid_swing")
                ev_dict["counterfactual_long"] = long_cf.get("templates", {})
                ev_dict["counterfactual_short"] = short_cf.get("templates", {})
                ev_dict["reward_t2"] = score_decision_net_r(
                    action=str(rt_info.get("portfolio_action") or "IGNORE_EVENT"),
                    side=rt_info.get("target_direction"),
                    long_eval=t2_long,
                    short_eval=t2_short,
                    has_qualified_setup=has_setup,
                )

            events.append(ev_dict)

        events.sort(key=lambda e: int(e["t"]))
        return events
    finally:
        obs_conn.close()
        if shadow_conn is not None:
            shadow_conn.close()


def _slim_bar_for_web(bar: dict[str, Any]) -> dict[str, Any]:
    """Strip redundant bid/ask OHLC keys for compact browser K-line payloads (~60% smaller)."""
    return {
        "t": bar["t"],
        "iso": bar["iso"],
        "o": bar["o"],
        "h": bar["h"],
        "l": bar["l"],
        "c": bar["c"],
        "sa": bar["sa"],
        "sm": bar["sm"],
        "n": bar["n"],
        "s": bar["s"],
        "atr14": bar.get("atr14"),
    }


def _enrich_trades_for_chart_lab(
    trades: list[dict[str, Any]],
    bars_m1: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach unix timestamps, FX session, MFE/MAE, and post-entry drift to executed trades."""
    m1_ts = [int(b["t"]) for b in bars_m1]
    enriched: list[dict[str, Any]] = []
    for tr in trades:
        item = dict(tr)
        open_dt = _parse_iso(str(tr.get("opened_at") or ""))
        close_dt = _parse_iso(str(tr.get("closed_at") or ""))
        open_ts = int(open_dt.timestamp()) if open_dt else None
        close_ts = int(close_dt.timestamp()) if close_dt else None
        item["open_ts"] = open_ts
        item["close_ts"] = close_ts
        item["session"] = classify_fx_session(open_dt) if open_dt else None
        if open_ts is not None and bars_m1:
            end_ts = close_ts if close_ts is not None else open_ts + 4 * 3600
            entry_px = _safe_float(tr.get("entry_price"))
            side = str(tr.get("side") or "")
            if entry_px and side in ("long", "short"):
                mfe = 0.0
                mae = 0.0
                idx = bisect_left(m1_ts, open_ts)
                while idx < len(bars_m1) and m1_ts[idx] <= end_ts:
                    fb = bars_m1[idx]
                    if side == "long":
                        mfe = max(mfe, (float(fb["bh"]) - entry_px) / PIP_SIZE)
                        mae = max(mae, (entry_px - float(fb["bl"])) / PIP_SIZE)
                    else:
                        mfe = max(mfe, (entry_px - float(fb["al"])) / PIP_SIZE)
                        mae = max(mae, (float(fb["ah"]) - entry_px) / PIP_SIZE)
                    idx += 1
                item["mfe_pips"] = round(mfe, 1)
                item["mae_pips"] = round(mae, 1)
            drifts = compute_natural_drifts(bars_m1, m1_ts, open_ts)
            sign = 1.0 if side == "long" else -1.0
            item["drift_1h_pips"] = (
                round(float(drifts["post_60m_pips"]) * sign, 1)
                if drifts["post_60m_pips"] is not None
                else None
            )
            item["drift_4h_pips"] = (
                round(float(drifts["post_4h_pips"]) * sign, 1)
                if drifts["post_4h_pips"] is not None
                else None
            )
        enriched.append(item)
    return enriched


def build_chart_lab_payload(
    *,
    quotes_db: Path | None,
    observer_db: Path | None = None,
    shadow_db: Path | None = None,
    memory_db: Path | None = None,
    cli_demo_db: Path | None = None,
    days: int | None = None,
    basis: str = "live_poll",
    include_counterfactuals: bool = False,
) -> dict[str, Any]:
    """Build sanitized Stage A K-line, optimal marker, bidirectional news, and trade payload."""
    agg = aggregate_quote_bars(quotes_db, symbol="EURUSD", basis=basis, days=days)
    bars_m1 = agg["bars_m1"]
    bars_m5 = agg["bars_m5"]
    bars_m15 = agg["bars_m15"]
    bars_h1 = agg["bars_h1"]

    algo_markers = detect_optimal_entries(bars_m1, bars_m5)
    events = extract_attributed_events(
        observer_db=observer_db,
        shadow_db=shadow_db,
        bars_m1=bars_m1,
        algo_markers=algo_markers,
        snippet_max_len=240 if not include_counterfactuals else 600,
        include_counterfactuals=include_counterfactuals,
    )

    trades: list[dict[str, Any]] = []
    if shadow_db is not None and (
        shadow_db.is_file() or (cli_demo_db is not None and cli_demo_db.is_file())
    ):
        snap = build_dashboard_snapshot(
            shadow_db=shadow_db,
            memory_db=memory_db,
            quotes_db=quotes_db,
            cli_demo_db=cli_demo_db,
            max_decisions=120,
        )
        trades = _enrich_trades_for_chart_lab(list(snap.get("trades") or []), bars_m1)

    _, _, oos_split_ts = split_chronological_oos(bars_m5, ts_key="t", train_ratio=0.70)

    # Summarize provider-level bidirectional price reaction & front-running rate.
    provider_lag_summary: dict[str, dict[str, Any]] = {}
    for ev in events:
        prov = str(ev["provider"])
        st = provider_lag_summary.setdefault(
            prov,
            {
                "count": 0,
                "front_run_count": 0,
                "lags_sec": [],
                "abs_pre_5m": [],
                "abs_post_15m": [],
                "abs_post_60m": [],
            },
        )
        st["count"] += 1
        if ev.get("front_run_flag"):
            st["front_run_count"] += 1
        if ev.get("feed_lag_sec") is not None:
            st["lags_sec"].append(int(ev["feed_lag_sec"]))
        if ev.get("pre_5m_pips") is not None:
            st["abs_pre_5m"].append(abs(float(ev["pre_5m_pips"])))
        if ev.get("post_15m_pips") is not None:
            st["abs_post_15m"].append(abs(float(ev["post_15m_pips"])))
        if ev.get("post_60m_pips") is not None:
            st["abs_post_60m"].append(abs(float(ev["post_60m_pips"])))

    provider_stats_out: dict[str, dict[str, Any]] = {}
    for prov, st in sorted(provider_lag_summary.items()):
        lags = sorted(st["lags_sec"])
        med_lag = lags[len(lags) // 2] if lags else None
        provider_stats_out[prov] = {
            "count": st["count"],
            "front_run_count": st["front_run_count"],
            "front_run_rate": round(st["front_run_count"] / max(1, st["count"]), 3),
            "median_feed_lag_sec": med_lag,
            "mean_abs_pre_5m_pips": round(sum(st["abs_pre_5m"]) / max(1, len(st["abs_pre_5m"])), 2)
            if st["abs_pre_5m"]
            else None,
            "mean_abs_post_15m_pips": round(
                sum(st["abs_post_15m"]) / max(1, len(st["abs_post_15m"])), 2
            )
            if st["abs_post_15m"]
            else None,
            "mean_abs_post_60m_pips": round(
                sum(st["abs_post_60m"]) / max(1, len(st["abs_post_60m"])), 2
            )
            if st["abs_post_60m"]
            else None,
        }

    return {
        "schema_version": CHART_LAB_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "symbol": "EUR/USD",
        "basis": basis,
        "window": {
            "first_iso": agg["first_iso"],
            "last_iso": agg["last_iso"],
            "days_requested": days,
            "total_ticks": agg["total_ticks"],
            "oos_split_ts": oos_split_ts,
        },
        "algo_tiers": [
            {
                "tier": spec.tier,
                "key": spec.key,
                "label": spec.label,
                "horizon_min": spec.horizon_min,
                "min_mfe_pips": spec.min_mfe_pips,
                "max_mae_pips": spec.max_mae_pips,
            }
            for spec in ALGO_TIERS
        ],
        "session_stats": agg["session_stats"],
        "provider_stats": provider_stats_out,
        "bars": {
            "m5": [_slim_bar_for_web(b) for b in bars_m5],
            "m15": [_slim_bar_for_web(b) for b in bars_m15],
            "h1": [_slim_bar_for_web(b) for b in bars_h1],
        },
        "algo_markers": algo_markers,
        "events": events,
        "trades": trades,
        "_bars_m1": bars_m1,
        "_bars_m5_full": bars_m5,
        "_bars_m15_full": bars_m15,
        "_bars_h1_full": bars_h1,
    }


def _write_bars_csv(path: Path, bars: list[dict[str, Any]], *, gzipped: bool = False) -> None:
    fields = [
        "t",
        "iso",
        "s",
        "o",
        "h",
        "l",
        "c",
        "bo",
        "bh",
        "bl",
        "bc",
        "ao",
        "ah",
        "al",
        "ac",
        "sa",
        "sm",
        "n",
        "atr14",
    ]
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    if gzipped:
        with gzip.open(tmp_path, "wt", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(bars)
    else:
        with tmp_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(bars)
    tmp_path.replace(path)


def export_research_bundle(
    *,
    quotes_db: Path | None,
    observer_db: Path | None = None,
    shadow_db: Path | None = None,
    memory_db: Path | None = None,
    cli_demo_db: Path | None = None,
    output_dir: Path | None = None,
    chart_lab_output: Path | None = None,
    days: int | None = None,
    basis: str = "live_poll",
) -> dict[str, Any]:
    """Export sanitized Chart Lab JSON and optional Colab CSV/JSON research bundle."""
    full_payload = build_chart_lab_payload(
        quotes_db=quotes_db,
        observer_db=observer_db,
        shadow_db=shadow_db,
        memory_db=memory_db,
        cli_demo_db=cli_demo_db,
        days=days,
        basis=basis,
        include_counterfactuals=output_dir is not None,
    )

    bars_m1 = full_payload.pop("_bars_m1", [])
    bars_m5_full = full_payload.pop("_bars_m5_full", [])
    bars_m15_full = full_payload.pop("_bars_m15_full", [])
    bars_h1_full = full_payload.pop("_bars_h1_full", [])

    # Keep browser payload compact by stripping counterfactual dictionaries from chart-lab.json.
    web_events = [
        {
            k: v
            for k, v in ev.items()
            if k not in ("counterfactual_long", "counterfactual_short", "reward_t2")
        }
        for ev in full_payload["events"]
    ]
    web_payload = dict(full_payload)
    web_payload["events"] = web_events

    chart_lab_rendered = sanitize_and_verify_snapshot(web_payload, indent=None)
    written_paths: dict[str, str] = {}

    if chart_lab_output is not None:
        chart_lab_output.parent.mkdir(parents=True, exist_ok=True)
        tmp = chart_lab_output.with_suffix(chart_lab_output.suffix + ".tmp")
        tmp.write_text(chart_lab_rendered, encoding="utf-8")
        tmp.replace(chart_lab_output)
        written_paths["chart_lab_json"] = str(chart_lab_output)

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        cl_path = output_dir / "chart-lab.json"
        tmp_cl = cl_path.with_suffix(".json.tmp")
        tmp_cl.write_text(chart_lab_rendered, encoding="utf-8")
        tmp_cl.replace(cl_path)
        written_paths["bundle_chart_lab_json"] = str(cl_path)

        m1_csv = output_dir / "bars_m1.csv.gz"
        m5_csv = output_dir / "bars_m5.csv"
        m15_csv = output_dir / "bars_m15.csv"
        h1_csv = output_dir / "bars_h1.csv"
        _write_bars_csv(m1_csv, bars_m1, gzipped=True)
        _write_bars_csv(m5_csv, bars_m5_full, gzipped=False)
        _write_bars_csv(m15_csv, bars_m15_full, gzipped=False)
        _write_bars_csv(h1_csv, bars_h1_full, gzipped=False)

        for fname, obj in (
            ("events.json", {"events": _sanitize_recursive(full_payload["events"], ())}),
            ("algo_markers.json", {"algo_markers": full_payload["algo_markers"]}),
            ("trades.json", {"trades": _sanitize_recursive(full_payload["trades"], ())}),
        ):
            target = output_dir / fname
            tmp_j = target.with_suffix(".json.tmp")
            tmp_j.write_text(sanitize_and_verify_snapshot(obj, indent=2), encoding="utf-8")
            tmp_j.replace(target)

        manifest = {
            "schema_version": RESEARCH_BUNDLE_SCHEMA_VERSION,
            "generated_at": full_payload["generated_at"],
            "window": full_payload["window"],
            "counts": {
                "ticks": full_payload["window"]["total_ticks"],
                "m1_bars": len(bars_m1),
                "m5_bars": len(bars_m5_full),
                "m15_bars": len(bars_m15_full),
                "h1_bars": len(bars_h1_full),
                "algo_markers": len(full_payload["algo_markers"]),
                "events": len(full_payload["events"]),
                "trades": len(full_payload["trades"]),
            },
            "session_stats": full_payload["session_stats"],
            "provider_stats": full_payload["provider_stats"],
        }
        m_path = output_dir / "manifest.json"
        m_tmp = m_path.with_suffix(".json.tmp")
        m_tmp.write_text(sanitize_and_verify_snapshot(manifest, indent=2), encoding="utf-8")
        m_tmp.replace(m_path)
        written_paths["output_dir"] = str(output_dir)

    return {
        "schema_version": CHART_LAB_SCHEMA_VERSION,
        "chart_lab_bytes": len(chart_lab_rendered.encode("utf-8")),
        "ticks": full_payload["window"]["total_ticks"],
        "m1_bars": len(bars_m1),
        "m5_bars": len(bars_m5_full),
        "m15_bars": len(bars_m15_full),
        "h1_bars": len(bars_h1_full),
        "algo_markers": len(full_payload["algo_markers"]),
        "events": len(full_payload["events"]),
        "trades": len(full_payload["trades"]),
        "paths": written_paths,
    }
