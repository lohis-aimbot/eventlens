"""Shadow-only intraday runtime CLI: start, stop, status, report, and crash recovery."""

import argparse
import asyncio
import json
import logging
import math
import os
import signal
import sqlite3
from bisect import bisect_left, bisect_right
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from .contracts import (
    AgentContext,
    AgentDecision,
    MarketSnapshot,
    TargetPortfolio,
    TargetPosition,
    Thesis,
    ThesisChange,
)
from .deepseek import DeepSeekClient
from .observer import APPLICATION_ID as OBSERVER_APPLICATION_ID
from .observer import (
    ObserverStore,
    classify_timestamp_taxonomy,
    evaluate_observer_stream_health,
)
from .quotes import EURUSD_PIP, QuoteStore, build_market_snapshot, compute_price_trajectory
from .runtime import (
    IntradayEventRuntime,
    ReviewPolicy,
    ShadowRunner,
    SQLiteRuntimeStore,
    inspect_runtime_store_readonly,
)
from .sources.english_tier1 import EURUSDEventFilter, Tier1EnglishSource
from .sources.wallstreetcn import WallStreetCNSource
from .sqlite_memory import SQLiteThesisMemory
from .trading.agent import SinglePassTradingAgent
from .trading.execution import (
    APPLICATION_ID as SHADOW_APPLICATION_ID,
)
from .trading.execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    SQLiteShadowLedger,
    build_shadow_audit_metrics,
    inspect_shadow_ledger_readonly,
)
from .trading.portfolio import LOT_UNITS_EUR, DeterministicTargetValidator
from .trading.risk import DeterministicHardRiskEngine, RiskPolicy

_DEFAULT_SHADOW_DB = Path("data/shadow-ledger.sqlite")
_DEFAULT_MEMORY_DB = Path("data/shadow-memory.sqlite")
_DEFAULT_OBSERVER_DB = Path("data/observer.sqlite")
_DEFAULT_QUOTES_DB = Path("data/live-quotes.sqlite")


def _pips_str(delta: Decimal) -> str:
    return str((delta / EURUSD_PIP).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def _parse_utc(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        return None
    return dt.astimezone(UTC)


def _extract_headline(raw_text: str | None) -> str | None:
    if not raw_text:
        return None
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return None
    for ln in lines:
        if not ln.startswith("[REVISION ") and not ln.startswith("[FULL UPDATED ARTICLE TEXT]"):
            return ln[:160]
    return lines[0][:160]


def _resolve_companion_dbs(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    """Resolve companion DBs for read-only status/report without cross-directory contamination."""
    shadow_db: Path = getattr(args, "shadow_db", _DEFAULT_SHADOW_DB)
    memory_db: Path = getattr(args, "memory_db", _DEFAULT_MEMORY_DB)
    observer_db: Path = getattr(args, "observer_db", _DEFAULT_OBSERVER_DB)
    quotes_db: Path = getattr(args, "quotes_db", _DEFAULT_QUOTES_DB)
    if (
        shadow_db != _DEFAULT_SHADOW_DB
        and shadow_db.resolve().parent != _DEFAULT_SHADOW_DB.resolve().parent
    ):
        base_dir = shadow_db.parent
        if memory_db == _DEFAULT_MEMORY_DB:
            memory_db = base_dir / "shadow-memory.sqlite"
        if observer_db == _DEFAULT_OBSERVER_DB:
            observer_db = base_dir / "observer.sqlite"
        if quotes_db == _DEFAULT_QUOTES_DB:
            quotes_db = base_dir / "live-quotes.sqlite"
    return memory_db, observer_db, quotes_db


def _build_objective_diagnostics(
    *,
    state_store: SQLiteRuntimeStore | None = None,
    shadow_db: Path | None = None,
    memory_db: Path,
    observer_db: Path,
    quotes_db: Path,
    basis: Literal["live_poll", "synthetic_fixture"],
    now: datetime,
    max_anchor_offset_seconds: float = 10.0,
    max_window_gap_seconds: float = 300.0,
    stale_quote_threshold_seconds: float = 5.0,
) -> dict[str, Any]:
    """Compute strictly read-only, cutoff-consistent, session-isolated objective diagnostics."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Diagnostics cutoff requires a timezone-aware timestamp")
    cutoff = now.astimezone(UTC)

    ro_shadow_conn: sqlite3.Connection | None = None
    mem_conn: sqlite3.Connection | None = None
    obs_conn: sqlite3.Connection | None = None
    try:
        raw_event_rows: list[sqlite3.Row] = []
        if state_store is not None:
            raw_event_rows = state_store.db.execute(
                """SELECT event_id, state, reason, received_at, first_seen_at, context_json, decision_json
                FROM runtime_events
                WHERE reason NOT LIKE 'baseline_%'
                  AND state != 'bundled'
                ORDER BY received_at ASC, first_seen_at ASC"""
            ).fetchall()
        elif shadow_db is not None and shadow_db.is_file():
            try:
                cand_sh = sqlite3.connect(
                    shadow_db.resolve().as_uri() + "?mode=ro",
                    uri=True,
                    timeout=5,
                )
                cand_sh.row_factory = sqlite3.Row
                sh_app = cand_sh.execute("PRAGMA application_id").fetchone()[0]
                sh_tables = {
                    str(r["name"])
                    for r in cand_sh.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                if sh_app == SHADOW_APPLICATION_ID and "runtime_events" in sh_tables:
                    ro_shadow_conn = cand_sh
                    raw_event_rows = ro_shadow_conn.execute(
                        """SELECT event_id, state, reason, received_at, first_seen_at, context_json, decision_json
                        FROM runtime_events
                        WHERE reason NOT LIKE 'baseline_%'
                          AND state != 'bundled'
                        ORDER BY received_at ASC, first_seen_at ASC"""
                    ).fetchall()
                else:
                    cand_sh.close()
            except sqlite3.Error:
                ro_shadow_conn = None

        filtered_event_rows: list[sqlite3.Row] = []
        for row in raw_event_rows:
            rx_dt_chk = _parse_utc(row["received_at"])
            if rx_dt_chk is not None and rx_dt_chk > cutoff:
                continue
            filtered_event_rows.append(row)
        event_rows = filtered_event_rows[-10:]

        if memory_db.is_file():
            try:
                mem_conn = sqlite3.connect(
                    memory_db.resolve().as_uri() + "?mode=ro",
                    uri=True,
                    timeout=5,
                )
            except sqlite3.Error:
                mem_conn = None

        if observer_db.is_file():
            try:
                cand_obs = sqlite3.connect(
                    observer_db.resolve().as_uri() + "?mode=ro",
                    uri=True,
                    timeout=5,
                )
                cand_obs.row_factory = sqlite3.Row
                app_id = cand_obs.execute("PRAGMA application_id").fetchone()[0]
                if app_id == OBSERVER_APPLICATION_ID:
                    obs_conn = cand_obs
                else:
                    cand_obs.close()
            except sqlite3.Error:
                obs_conn = None

        parsed_events: list[dict[str, Any]] = []
        event_anchor_times: list[datetime] = []
        for row in event_rows:
            ev_id = str(row["event_id"])
            received_dt = _parse_utc(row["received_at"])
            if received_dt is not None:
                event_anchor_times.append(received_dt)
            source_ts_str: str | None = None
            raw_text_str: str | None = None
            ev_source_type: str | None = None
            ev_source_name: str | None = None
            if row["context_json"]:
                try:
                    ctx_ev = json.loads(row["context_json"]).get("event") or {}
                    source_ts_str = ctx_ev.get("source_timestamp")
                    raw_text_str = ctx_ev.get("raw_text")
                    ev_source_type = ctx_ev.get("source_type")
                    ev_source_name = ctx_ev.get("source")
                except (ValueError, TypeError, AttributeError):
                    pass

            if (
                source_ts_str is None or raw_text_str is None or ev_source_type is None
            ) and mem_conn is not None:
                try:
                    mem_row = mem_conn.execute(
                        "SELECT payload FROM raw_events WHERE event_id=?",
                        (ev_id,),
                    ).fetchone()
                    if mem_row is not None:
                        raw_ev = json.loads(mem_row[0])
                        if source_ts_str is None:
                            source_ts_str = raw_ev.get("source_timestamp")
                        if raw_text_str is None:
                            raw_text_str = raw_ev.get("raw_text")
                        if ev_source_type is None:
                            ev_source_type = raw_ev.get("source_type")
                        if ev_source_name is None:
                            ev_source_name = raw_ev.get("source")
                except (sqlite3.Error, ValueError, TypeError):
                    pass

            obs_rec: dict[str, Any] | None = None
            obs_avail_str: str | None = None
            rev_num = 1
            inferred_kind = "news"
            parts = ev_id.split(":")
            if len(parts) >= 4 and parts[-1].startswith("r") and parts[-1][1:].isdigit():
                provider, kind_part = parts[0], parts[1]
                source_id = ":".join(parts[2:-1])
                rev_num = int(parts[-1][1:])
                inferred_kind = kind_part
                if obs_conn is not None:
                    try:
                        obs_row = obs_conn.execute(
                            """SELECT r.record, MIN(p.available_at) AS available_at
                            FROM observer_revisions r
                            JOIN observer_sightings s
                              ON s.source_id = r.source_id AND s.revision = r.revision
                            JOIN observer_polls p
                              ON p.poll_id = s.poll_id
                             AND p.provider = r.provider
                             AND p.basis = r.basis
                             AND p.kind = r.kind
                            WHERE r.provider=? AND r.basis=? AND r.kind=?
                              AND r.source_id=? AND r.revision=?
                              AND p.available_at IS NOT NULL""",
                            (provider, basis, kind_part, source_id, rev_num),
                        ).fetchone()
                        if obs_row is not None and obs_row["record"] is not None:
                            avail_dt_chk = _parse_utc(obs_row["available_at"])
                            if avail_dt_chk is None or avail_dt_chk <= cutoff:
                                obs_rec = json.loads(obs_row["record"])
                                obs_avail_str = obs_row["available_at"]
                                if source_ts_str is None:
                                    source_ts_str = (
                                        obs_rec.get("modified_at")
                                        or obs_rec.get("publisher_display_at")
                                        or obs_rec.get("scheduled_at")
                                    )
                                if raw_text_str is None:
                                    raw_text_str = obs_rec.get("title") or obs_rec.get("text")
                    except (sqlite3.Error, ValueError, TypeError):
                        obs_rec = None
            elif "calendar" in ev_id or (ev_source_name and "calendar" in ev_source_name):
                inferred_kind = "calendar"

            source_dt = _parse_utc(source_ts_str)
            if source_dt is not None and source_dt <= cutoff:
                event_anchor_times.append(source_dt)

            if obs_rec is not None:
                tax = classify_timestamp_taxonomy(
                    kind=str(obs_rec.get("kind") or inferred_kind),
                    revision=rev_num,
                    original_published_at=obs_rec.get("original_published_at"),
                    publisher_display_at=obs_rec.get("publisher_display_at"),
                    scheduled_at=obs_rec.get("scheduled_at"),
                    modified_at=obs_rec.get("modified_at"),
                    received_at=received_dt,
                    available_at=obs_avail_str,
                )
            elif inferred_kind == "calendar":
                tax = classify_timestamp_taxonomy(
                    kind="calendar",
                    revision=rev_num,
                    scheduled_at=source_dt,
                    received_at=received_dt,
                )
            elif rev_num >= 2:
                tax = classify_timestamp_taxonomy(
                    kind="news",
                    revision=rev_num,
                    received_at=received_dt,
                )
            else:
                tax = classify_timestamp_taxonomy(
                    kind="news",
                    revision=1,
                    publisher_display_at=source_dt,
                    received_at=received_dt,
                )

            portfolio_action: str | None = None
            if row["decision_json"]:
                try:
                    portfolio_action = json.loads(row["decision_json"]).get("portfolio_action")
                except (ValueError, TypeError, AttributeError):
                    portfolio_action = None

            verifiable_lag = tax["verifiable_lag_seconds"]
            lag_sec: float | None = (
                round(float(verifiable_lag), 2) if verifiable_lag is not None else None
            )

            parsed_events.append(
                {
                    "event_id": ev_id,
                    "headline": _extract_headline(raw_text_str),
                    "state": str(row["state"]),
                    "reason": str(row["reason"]),
                    "portfolio_action": portfolio_action,
                    "source_timestamp": source_dt.isoformat() if source_dt is not None else None,
                    "received_at": received_dt.isoformat()
                    if received_dt is not None
                    else str(row["received_at"]),
                    "receipt_lag_seconds": lag_sec,
                    "timestamp_taxonomy": tax,
                    "_received_dt": received_dt,
                    "_source_dt": source_dt,
                }
            )

        market_window: dict[str, Any] | None = None
        session_range: dict[str, Any] | None = None
        # Strictly single-session samples: (received_dt, source_dt, bid, ask, mid)
        sess_full_samples: list[tuple[datetime, datetime, Decimal, Decimal, Decimal]] = []

        if quotes_db.is_file():
            try:
                qstore = QuoteStore(quotes_db, readonly=True)
            except (FileNotFoundError, ValueError, sqlite3.Error):
                qstore = None
            if qstore is not None:
                try:
                    obs = qstore.latest("EURUSD", cutoff, basis=basis)
                    if obs is not None and obs.received_at <= cutoff:
                        latest_sess_id = str(obs.quote.session_id)
                        sess_row = qstore.db.execute(
                            "SELECT start FROM quote_sessions WHERE session_id=?",
                            (latest_sess_id,),
                        ).fetchone()
                        sess_start_dt = obs.received_at - timedelta(minutes=38)
                        if sess_row is not None:
                            try:
                                parsed_start = _parse_utc(
                                    json.loads(sess_row["start"]).get("started_at")
                                )
                                if parsed_start is not None:
                                    sess_start_dt = parsed_start
                            except (ValueError, TypeError):
                                pass

                        earliest_needed = min(
                            [
                                sess_start_dt,
                                obs.received_at - timedelta(minutes=38),
                                *event_anchor_times,
                            ]
                        )
                        q_rows = qstore.db.execute(
                            """SELECT session_id, received_at, quote
                            FROM quote_observations
                            WHERE session_id=? AND symbol='EURUSD' AND basis=?
                              AND available_at IS NOT NULL
                              AND received_at >= ?
                            ORDER BY received_at ASC, sequence ASC""",
                            (
                                latest_sess_id,
                                basis,
                                earliest_needed.isoformat(),
                            ),
                        ).fetchall()

                        sess_mids: list[tuple[datetime, Decimal]] = []
                        for qr in q_rows:
                            q_dt = _parse_utc(qr["received_at"])
                            if q_dt is None or q_dt > cutoff:
                                continue
                            q_obj = json.loads(qr["quote"])
                            q_src_dt = _parse_utc(q_obj.get("source_at")) or q_dt
                            q_bid = Decimal(str(q_obj["bid"]))
                            q_ask = Decimal(str(q_obj["ask"]))
                            mid = (q_bid + q_ask) / Decimal("2")
                            sess_full_samples.append((q_dt, q_src_dt, q_bid, q_ask, mid))
                            sess_mids.append((q_dt, mid))

                        traj = compute_price_trajectory(
                            sess_mids,
                            obs.received_at,
                            instrument="EUR/USD",
                        )
                        if traj is not None:
                            span_15m = (
                                _pips_str(traj.window_15m_high_mid - traj.window_15m_low_mid)
                                if (
                                    traj.window_15m_high_mid is not None
                                    and traj.window_15m_low_mid is not None
                                )
                                else None
                            )
                            span_30m = (
                                _pips_str(traj.window_30m_high_mid - traj.window_30m_low_mid)
                                if (
                                    traj.window_30m_high_mid is not None
                                    and traj.window_30m_low_mid is not None
                                )
                                else None
                            )
                            quote_age_s = round((cutoff - obs.received_at).total_seconds(), 2)
                            source_age_s = round((cutoff - obs.quote.source_at).total_seconds(), 2)
                            is_stale = quote_age_s > stale_quote_threshold_seconds
                            samples_30m = [
                                ts
                                for ts, _ in sess_mids
                                if ts >= obs.received_at - timedelta(minutes=30)
                            ]
                            max_gap_30m_s = (
                                round(
                                    max(
                                        (
                                            (samples_30m[i] - samples_30m[i - 1]).total_seconds()
                                            for i in range(1, len(samples_30m))
                                        ),
                                        default=0.0,
                                    ),
                                    3,
                                )
                                if len(samples_30m) >= 2
                                else None
                            )
                            market_window = {
                                "window_reference": "latest_quote_at",
                                "query_cutoff_at": cutoff.isoformat(),
                                "latest_quote_at": obs.received_at.isoformat(),
                                "latest_source_at": obs.quote.source_at.isoformat(),
                                "quote_age_seconds": quote_age_s,
                                "source_age_seconds": source_age_s,
                                "is_stale": is_stale,
                                "market_status": "stale_quote" if is_stale else "fresh_quote",
                                "latest_bid": str(obs.quote.bid),
                                "latest_ask": str(obs.quote.ask),
                                "latest_mid": str(traj.current_mid),
                                "current_bid": None if is_stale else str(obs.quote.bid),
                                "current_ask": None if is_stale else str(obs.quote.ask),
                                "current_mid": None if is_stale else str(traj.current_mid),
                                "change_1m_pips": str(traj.change_1m_pips)
                                if traj.change_1m_pips is not None
                                else None,
                                "change_5m_pips": str(traj.change_5m_pips)
                                if traj.change_5m_pips is not None
                                else None,
                                "change_15m_pips": str(traj.change_15m_pips)
                                if traj.change_15m_pips is not None
                                else None,
                                "change_30m_pips": str(traj.change_30m_pips)
                                if traj.change_30m_pips is not None
                                else None,
                                "window_15m_low_mid": str(traj.window_15m_low_mid)
                                if traj.window_15m_low_mid is not None
                                else None,
                                "window_15m_high_mid": str(traj.window_15m_high_mid)
                                if traj.window_15m_high_mid is not None
                                else None,
                                "window_15m_span_pips": span_15m,
                                "window_30m_low_mid": str(traj.window_30m_low_mid)
                                if traj.window_30m_low_mid is not None
                                else None,
                                "window_30m_high_mid": str(traj.window_30m_high_mid)
                                if traj.window_30m_high_mid is not None
                                else None,
                                "window_30m_span_pips": span_30m,
                                "sample_count_30m": traj.sample_count_30m,
                                "max_gap_30m_seconds": max_gap_30m_s,
                                "realizable_pnl_claim": False,
                            }

                        if sess_mids:
                            first_ts, first_mid = sess_mids[0]
                            last_ts, last_mid = sess_mids[-1]
                            high_ts, high_mid = first_ts, first_mid
                            low_ts, low_mid = first_ts, first_mid
                            for s_ts, s_mid in sess_mids[1:]:
                                if s_mid > high_mid:
                                    high_ts, high_mid = s_ts, s_mid
                                if s_mid < low_mid:
                                    low_ts, low_mid = s_ts, s_mid
                            sess_max_gap_s = (
                                round(
                                    max(
                                        (
                                            (sess_mids[i][0] - sess_mids[i - 1][0]).total_seconds()
                                            for i in range(1, len(sess_mids))
                                        ),
                                        default=0.0,
                                    ),
                                    3,
                                )
                                if len(sess_mids) >= 2
                                else None
                            )
                            session_range = {
                                "session_id": latest_sess_id,
                                "sample_count": len(sess_mids),
                                "max_gap_seconds": sess_max_gap_s,
                                "first_at": first_ts.isoformat(),
                                "first_mid": str(first_mid),
                                "low_at": low_ts.isoformat(),
                                "low_mid": str(low_mid),
                                "high_at": high_ts.isoformat(),
                                "high_mid": str(high_mid),
                                "latest_at": last_ts.isoformat(),
                                "latest_mid": str(last_mid),
                                "span_pips": _pips_str(high_mid - low_mid),
                                "net_change_pips": _pips_str(last_mid - first_mid),
                                "realizable_pnl_claim": False,
                            }
                finally:
                    qstore.close()

        sample_times = [ts for ts, _, _, _, _ in sess_full_samples]
        recent_events_out: list[dict[str, Any]] = []
        for item in parsed_events:
            rx_dt: datetime | None = item.pop("_received_dt")
            src_dt: datetime | None = item.pop("_source_dt")
            move_before_receipt_pips: str | None = None
            quote_at_event: dict[str, Any] | None = None

            if rx_dt is None:
                post_receipt_move: dict[str, Any] = {
                    "status": "unknown",
                    "reason": "missing_event_receipt_timestamp",
                    "window_end_reference": "unknown",
                    "anchor_at": None,
                    "anchor_offset_seconds": None,
                    "anchor_quote_age_seconds": None,
                    "anchor_bid": None,
                    "anchor_ask": None,
                    "anchor_mid": None,
                    "sample_count": 0,
                    "max_gap_seconds": None,
                    "high_at": None,
                    "high_mid": None,
                    "low_at": None,
                    "low_mid": None,
                    "latest_at": None,
                    "latest_mid": None,
                    "max_up_pips": None,
                    "max_down_pips": None,
                    "span_pips": None,
                    "net_change_pips": None,
                    "interpolated": False,
                    "realizable_pnl_claim": False,
                }
            else:
                if sess_full_samples:
                    causal_idx = bisect_right(sample_times, rx_dt) - 1
                    if causal_idx >= 0:
                        c_rx, c_src, c_bid, c_ask, c_mid = sess_full_samples[causal_idx]
                        c_age_s = round((rx_dt - c_rx).total_seconds(), 3)
                        quote_at_event = {
                            "observed_at": c_rx.isoformat(),
                            "source_at": c_src.isoformat(),
                            "quote_age_at_event_seconds": c_age_s,
                            "is_fresh_at_event": c_age_s <= stale_quote_threshold_seconds,
                            "bid": str(c_bid),
                            "ask": str(c_ask),
                            "mid": str(c_mid),
                            "interpolated": False,
                        }

                rx_idx = bisect_left(sample_times, rx_dt)
                if not sess_full_samples or rx_idx >= len(sess_full_samples):
                    post_receipt_move = {
                        "status": "unknown",
                        "reason": "no_post_event_quote_before_cutoff",
                        "window_end_reference": "latest_quote_at" if sample_times else "no_quotes",
                        "anchor_at": None,
                        "anchor_offset_seconds": None,
                        "anchor_quote_age_seconds": None,
                        "anchor_bid": None,
                        "anchor_ask": None,
                        "anchor_mid": None,
                        "sample_count": 0,
                        "max_gap_seconds": None,
                        "high_at": None,
                        "high_mid": None,
                        "low_at": None,
                        "low_mid": None,
                        "latest_at": None,
                        "latest_mid": None,
                        "max_up_pips": None,
                        "max_down_pips": None,
                        "span_pips": None,
                        "net_change_pips": None,
                        "interpolated": False,
                        "realizable_pnl_claim": False,
                    }
                else:
                    sub = sess_full_samples[rx_idx:]
                    a_ts, a_src, a_bid, a_ask, a_mid = sub[0]
                    l_ts, _, _, _, l_mid = sub[-1]
                    anchor_offset_s = round((a_ts - rx_dt).total_seconds(), 3)
                    anchor_q_age_s = round((a_ts - a_src).total_seconds(), 3)
                    sample_count = len(sub)
                    max_gap_s = (
                        round(
                            max(
                                (
                                    (sub[i][0] - sub[i - 1][0]).total_seconds()
                                    for i in range(1, sample_count)
                                ),
                                default=0.0,
                            ),
                            3,
                        )
                        if sample_count >= 2
                        else None
                    )
                    if anchor_offset_s > max_anchor_offset_seconds:
                        insuff_reason = (
                            f"anchor_offset_{anchor_offset_s}s_exceeds_{max_anchor_offset_seconds}s"
                        )
                    elif sample_count < 2:
                        insuff_reason = "fewer_than_two_post_event_quotes"
                    elif max_gap_s is not None and max_gap_s > max_window_gap_seconds:
                        insuff_reason = (
                            f"post_event_gap_{max_gap_s}s_exceeds_{max_window_gap_seconds}s"
                        )
                    else:
                        insuff_reason = None

                    if insuff_reason is not None:
                        post_receipt_move = {
                            "status": "insufficient_coverage",
                            "reason": insuff_reason,
                            "window_end_reference": "latest_quote_at",
                            "anchor_at": a_ts.isoformat(),
                            "anchor_offset_seconds": anchor_offset_s,
                            "anchor_quote_age_seconds": anchor_q_age_s,
                            "anchor_bid": str(a_bid),
                            "anchor_ask": str(a_ask),
                            "anchor_mid": str(a_mid),
                            "sample_count": sample_count,
                            "max_gap_seconds": max_gap_s,
                            "high_at": None,
                            "high_mid": None,
                            "low_at": None,
                            "low_mid": None,
                            "latest_at": l_ts.isoformat(),
                            "latest_mid": str(l_mid),
                            "max_up_pips": None,
                            "max_down_pips": None,
                            "span_pips": None,
                            "net_change_pips": None,
                            "interpolated": False,
                            "realizable_pnl_claim": False,
                        }
                    else:
                        h_ts, h_mid = a_ts, a_mid
                        lo_ts, lo_mid = a_ts, a_mid
                        for s_ts, _, _, _, s_mid in sub[1:]:
                            if s_mid > h_mid:
                                h_ts, h_mid = s_ts, s_mid
                            if s_mid < lo_mid:
                                lo_ts, lo_mid = s_ts, s_mid
                        post_receipt_move = {
                            "status": "ok",
                            "reason": "bounded_anchor_and_gap_verified",
                            "window_end_reference": "latest_quote_at",
                            "anchor_at": a_ts.isoformat(),
                            "anchor_offset_seconds": anchor_offset_s,
                            "anchor_quote_age_seconds": anchor_q_age_s,
                            "anchor_bid": str(a_bid),
                            "anchor_ask": str(a_ask),
                            "anchor_mid": str(a_mid),
                            "sample_count": sample_count,
                            "max_gap_seconds": max_gap_s,
                            "high_at": h_ts.isoformat(),
                            "high_mid": str(h_mid),
                            "low_at": lo_ts.isoformat(),
                            "low_mid": str(lo_mid),
                            "latest_at": l_ts.isoformat(),
                            "latest_mid": str(l_mid),
                            "max_up_pips": _pips_str(h_mid - a_mid),
                            "max_down_pips": _pips_str(lo_mid - a_mid),
                            "span_pips": _pips_str(h_mid - lo_mid),
                            "net_change_pips": _pips_str(l_mid - a_mid),
                            "interpolated": False,
                            "realizable_pnl_claim": False,
                        }
                        if src_dt is not None and src_dt < rx_dt:
                            src_idx = bisect_left(sample_times, src_dt)
                            if (
                                src_idx < len(sess_full_samples)
                                and abs((sess_full_samples[src_idx][0] - src_dt).total_seconds())
                                <= max_anchor_offset_seconds
                            ):
                                move_before_receipt_pips = _pips_str(
                                    a_mid - sess_full_samples[src_idx][4]
                                )

            item["quote_at_event"] = quote_at_event
            item["move_before_receipt_pips"] = move_before_receipt_pips
            item["post_receipt_move"] = post_receipt_move
            recent_events_out.append(item)

        stream_cache_headers: list[dict[str, Any]] = []
        if obs_conn is not None:
            try:
                poll_rows = obs_conn.execute(
                    """SELECT p.provider, p.stream, p.received_at, p.available_at, p.metadata
                    FROM observer_polls p
                    WHERE p.basis = ? AND p.available_at IS NOT NULL
                    ORDER BY p.rowid ASC""",
                    (basis,),
                ).fetchall()
                latest_by_stream: dict[tuple[str, str], sqlite3.Row] = {}
                for pr in poll_rows:
                    p_rx = _parse_utc(pr["received_at"])
                    p_av = _parse_utc(pr["available_at"])
                    if (p_rx is not None and p_rx > cutoff) or (p_av is not None and p_av > cutoff):
                        continue
                    key = (str(pr["provider"]), str(pr["stream"]))
                    latest_by_stream[key] = pr
                for key in sorted(latest_by_stream):
                    pr = latest_by_stream[key]
                    meta = json.loads(pr["metadata"])
                    headers = {
                        str(k).lower(): str(v)
                        for k, v in (meta.get("response_headers") or {}).items()
                    }
                    stream_cache_headers.append(
                        {
                            "provider": key[0],
                            "stream": key[1],
                            "request_url": meta.get("request_url"),
                            "last_received_at": str(pr["received_at"]),
                            "x_cache": headers.get("x-cache"),
                            "cf_cache_status": headers.get("cf-cache-status"),
                            "age": headers.get("age"),
                        }
                    )
            except (sqlite3.Error, ValueError, TypeError):
                pass

        return {
            "market_window": market_window,
            "session_range": session_range,
            "recent_events": recent_events_out,
            "stream_cache_headers": stream_cache_headers,
        }
    finally:
        if ro_shadow_conn is not None:
            ro_shadow_conn.close()
        if mem_conn is not None:
            mem_conn.close()
        if obs_conn is not None:
            obs_conn.close()


class _AbstainWithoutModelAgent:
    """Safe offline agent used when --call-model is not passed; never invents trades."""

    async def decide(self, context: AgentContext) -> AgentDecision:
        raise RuntimeError(
            "Model calls are disabled; pass --call-model to enable DeepSeek reasoning"
        )


def _load_latest_market(
    quotes_path: Path,
    basis: str,
    *,
    now: datetime | None = None,
) -> MarketSnapshot | None:
    if not quotes_path.is_file():
        return None
    try:
        qstore = QuoteStore(quotes_path, readonly=True)
    except (FileNotFoundError, ValueError, sqlite3.Error):
        return None
    try:
        return build_market_snapshot(
            qstore,
            (now or datetime.now(UTC)).astimezone(UTC),
            basis="synthetic_fixture" if basis == "synthetic_fixture" else "live_poll",
        )
    finally:
        qstore.close()


def _load_source_health_readonly(
    observer_db: Path,
    *,
    basis: Literal["live_poll", "synthetic_fixture"],
    now: datetime,
) -> list[dict[str, Any]]:
    if not observer_db.is_file():
        return []
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(
            observer_db.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=5,
        )
        conn.row_factory = sqlite3.Row
        app_id = conn.execute("PRAGMA application_id").fetchone()[0]
        if app_id != OBSERVER_APPLICATION_ID:
            return []
        return evaluate_observer_stream_health(conn, as_of=now.astimezone(UTC), basis=basis)
    except (sqlite3.Error, ValueError, TypeError):
        return []
    finally:
        if conn is not None:
            conn.close()


def _build_policies(
    args: argparse.Namespace,
) -> tuple[Decimal, RiskPolicy, ProtectiveExitPolicy, ShadowCostModel]:
    profile = getattr(args, "account_profile", "pepperstone_razor_200")
    kill_switch = bool(getattr(args, "kill_switch", False))
    max_quote_age_ms = int(getattr(args, "max_quote_age", 2.0) * 1000)
    raw_holding = getattr(args, "max_holding_seconds", None)
    explicit_zero_spread = getattr(args, "allow_commission_zero_spread", None)

    if profile == "pepperstone_razor_200":
        initial_cash = Decimal("200.00")
        risk = RiskPolicy.pepperstone_razor_200(
            kill_switch_active=kill_switch,
            max_quote_age_ms=max_quote_age_ms,
        )
        exit_pol = ProtectiveExitPolicy.pepperstone_razor_200(
            max_holding_seconds=int(raw_holding) if raw_holding is not None else 900,
            max_quote_age_ms=max_quote_age_ms,
        )
        cost_model = ShadowCostModel.pepperstone_razor()
    else:
        initial_cash = Decimal("10000.00")
        risk = RiskPolicy(
            kill_switch_active=kill_switch,
            max_quote_age_ms=max_quote_age_ms,
        )
        exit_pol = ProtectiveExitPolicy(
            max_holding_seconds=int(raw_holding) if raw_holding is not None else 1800,
            max_quote_age_ms=max_quote_age_ms,
        )
        cost_model = ShadowCostModel()

    if getattr(args, "initial_cash", None) is not None:
        initial_cash = Decimal(str(args.initial_cash))
    risk_updates: dict[str, Any] = {}
    exit_updates: dict[str, Any] = {}
    if explicit_zero_spread is not None:
        allow_zs = bool(explicit_zero_spread)
        risk_updates["allow_commission_bearing_zero_spread"] = allow_zs
        exit_updates["allow_commission_bearing_zero_spread"] = allow_zs
        cost_model = ShadowCostModel(
            name=cost_model.name,
            slippage_bps=cost_model.slippage_bps,
            commission_bps=cost_model.commission_bps,
            allow_commission_bearing_zero_spread=allow_zs,
        )
    if getattr(args, "leverage", None) is not None:
        risk_updates["leverage"] = Decimal(str(args.leverage))
    if getattr(args, "min_lot", None) is not None:
        risk_updates["min_volume_eur"] = (Decimal(str(args.min_lot)) * LOT_UNITS_EUR).quantize(
            Decimal("1")
        )
    if getattr(args, "max_lot", None) is not None:
        risk_updates["max_volume_eur"] = (Decimal(str(args.max_lot)) * LOT_UNITS_EUR).quantize(
            Decimal("1")
        )
    if getattr(args, "lot_step", None) is not None:
        risk_updates["volume_step_eur"] = (Decimal(str(args.lot_step)) * LOT_UNITS_EUR).quantize(
            Decimal("1")
        )
    if getattr(args, "min_equity", None) is not None:
        risk_updates["min_equity_usd"] = Decimal(str(args.min_equity))
    if getattr(args, "max_daily_loss", None) is not None:
        daily_loss = Decimal(str(args.max_daily_loss))
        risk_updates["max_daily_loss_usd"] = daily_loss
        exit_updates["max_daily_loss_usd"] = daily_loss
    if getattr(args, "max_position_loss", None) is not None:
        exit_updates["max_position_loss_usd"] = Decimal(str(args.max_position_loss))
    if getattr(args, "take_profit", None) is not None:
        tp = Decimal(str(args.take_profit))
        exit_updates["take_profit_usd"] = tp if tp > Decimal("0") else None
    if getattr(args, "stop_loss_pips", None) is not None:
        sl_pips = Decimal(str(args.stop_loss_pips))
        exit_updates["stop_loss_pips"] = sl_pips if sl_pips > Decimal("0") else None
    if getattr(args, "take_profit_pips", None) is not None:
        tp_pips = Decimal(str(args.take_profit_pips))
        exit_updates["take_profit_pips"] = tp_pips if tp_pips > Decimal("0") else None
    raw_trail_act = getattr(args, "trailing_activation_pips", None)
    raw_trail_dist = getattr(args, "trailing_distance_pips", None)
    if raw_trail_act is not None or raw_trail_dist is not None:
        act_val = Decimal(str(raw_trail_act)) if raw_trail_act is not None else None
        dist_val = Decimal(str(raw_trail_dist)) if raw_trail_dist is not None else None
        if (act_val is not None and act_val <= Decimal("0")) or (
            dist_val is not None and dist_val <= Decimal("0")
        ):
            exit_updates["trailing_activation_pips"] = None
            exit_updates["trailing_distance_pips"] = None
        else:
            if act_val is not None:
                exit_updates["trailing_activation_pips"] = act_val
            if dist_val is not None:
                exit_updates["trailing_distance_pips"] = dist_val

    if risk_updates:
        risk = RiskPolicy.model_validate({**risk.model_dump(), **risk_updates})
    if exit_updates:
        exit_pol = ProtectiveExitPolicy.model_validate({**exit_pol.model_dump(), **exit_updates})
    raw_risk_pct = getattr(args, "max_risk_per_trade_pct", None)
    if raw_risk_pct is not None:
        risk_pct = Decimal(str(raw_risk_pct))
        if risk_pct > Decimal("0"):
            risk = RiskPolicy.model_validate(
                {
                    **risk.model_dump(),
                    "max_risk_per_trade_pct": risk_pct,
                    "ex_ante_stop_loss_pips": exit_pol.stop_loss_pips or Decimal("11.0"),
                    "cap_quantity_by_risk_budget": True,
                }
            )
    return initial_cash, risk, exit_pol, cost_model


def _parse_cli_expected_account_number(raw: str) -> int:
    from .trading.ctrader_cli_broker import parse_strict_int64_account_number

    parsed = parse_strict_int64_account_number(raw, allow_zero=True)
    if parsed is None:
        raise argparse.ArgumentTypeError(
            f"Invalid --expected-demo-account-number={raw!r}: must be strict 64-bit ASCII digits"
        )
    return parsed


def _resolve_expected_demo_account_number(
    args: argparse.Namespace,
    *,
    require_positive: bool = False,
) -> int | None:
    from .trading.ctrader_cli_broker import parse_strict_int64_account_number

    explicit = getattr(args, "expected_demo_account_number", None)
    if explicit is not None:
        parsed_explicit = parse_strict_int64_account_number(
            explicit, allow_zero=not require_positive
        )
        if parsed_explicit is None:
            raise ValueError(
                "--expected-demo-account-number must be a positive 64-bit integer (> 0) "
                "when Demo order execution is enabled"
            )
        return parsed_explicit if parsed_explicit > 0 else None

    raw_env = os.environ.get("CTRADER_DEMO_ACCOUNT_NUMBER", "").strip()
    if raw_env:
        parsed_env = parse_strict_int64_account_number(raw_env, allow_zero=not require_positive)
        if parsed_env is None:
            req_kind = "positive " if require_positive else "non-negative "
            raise ValueError(
                f"Invalid CTRADER_DEMO_ACCOUNT_NUMBER={raw_env!r}: "
                f"must be a strict 64-bit {req_kind}integer"
            )
        return parsed_env if parsed_env > 0 else None

    if require_positive:
        raise ValueError(
            "Demo execution requires an explicit positive integer Demo account number via "
            "--expected-demo-account-number or CTRADER_DEMO_ACCOUNT_NUMBER"
        )
    return None


def _resolve_cli_demo_state_db(args: argparse.Namespace) -> Path:
    from .trading.ctrader_cli_broker import CTraderCliDemoBrokerAdapter

    raw_cli_state_db: Path | None = getattr(args, "cli_demo_state_db", None)
    if raw_cli_state_db is not None:
        return Path(raw_cli_state_db)
    shadow_db_path = Path(args.shadow_db)
    candidate = shadow_db_path.parent / "cli-demo-broker.sqlite"
    if candidate.is_file() or not shadow_db_path.is_file():
        return candidate
    try:
        conn = sqlite3.connect(
            shadow_db_path.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=5,
        )
        try:
            app_id = int(conn.execute("PRAGMA application_id").fetchone()[0])
        finally:
            conn.close()
        if app_id == CTraderCliDemoBrokerAdapter.APPLICATION_ID:
            return shadow_db_path
    except sqlite3.Error:
        pass
    return candidate


async def _run_demo_smoke(
    args: argparse.Namespace,
    *,
    risk_policy: RiskPolicy,
    exit_policy: ProtectiveExitPolicy,
) -> dict[str, Any]:
    """Execute one bounded, hard-risk-gated Demo open -> SL-confirmed -> close -> flat smoke cycle."""
    from .sources.ctrader_demo import DemoQuoteCollector, DirectoryTail
    from .trading.agent import SINGLE_PASS_PROMPT_VERSION
    from .trading.ctrader_cli_broker import (
        CTraderCliDemoBrokerAdapter,
        CTraderCliDemoExecutionEngine,
        DemoBrokerSafetyError,
    )

    bridge_dir: Path = getattr(args, "quote_bridge_dir")
    state_db = _resolve_cli_demo_state_db(args)
    state_db.parent.mkdir(parents=True, exist_ok=True)
    args.quotes_db.parent.mkdir(parents=True, exist_ok=True)
    expected_acct = _resolve_expected_demo_account_number(args, require_positive=True)
    volume_units = int(getattr(args, "volume_units", 1000))
    side: Literal["buy", "sell"] = "sell" if getattr(args, "side", "buy") == "sell" else "buy"
    hold_seconds = float(getattr(args, "hold_seconds", 0.2))
    timeout_seconds = float(getattr(args, "timeout_seconds", 20.0))

    qstore = QuoteStore(args.quotes_db, readonly=False)
    now_attach = datetime.now(UTC)
    tail = DirectoryTail(bridge_dir)
    collector = DemoQuoteCollector(
        qstore,
        now_attach,
        allow_multiple_sessions=True,
        allow_unclean_session_rollover=True,
        max_start_handshake_age_seconds=3600.0,
    )
    from .quotes import BridgeQuote, BridgeStart

    candidates = sorted(
        bridge_dir.resolve().glob("quotes-*.jsonl"),
        key=lambda p: (p.stat().st_mtime_ns, p.name),
        reverse=True,
    )
    for cand in candidates:
        try:
            stat = cand.stat()
            raw_bytes = cand.read_bytes()
            lines = raw_bytes.split(b"\n")
            remainder = lines.pop()
            if not lines:
                continue
            first_obj = json.loads(lines[0].decode("utf-8"))
            if first_obj.get("kind") != "start":
                continue
            if any(json.loads(ln.decode("utf-8")).get("kind") == "stop" for ln in lines if ln):
                continue
            start_item = BridgeStart.model_validate(first_obj)
            if now_attach > start_item.started_at + timedelta(seconds=start_item.duration_seconds):
                continue
            tail.ignored.discard(cand)
            consumed_offset = len(raw_bytes) - len(remainder)
            tail.files[cand] = (stat.st_ino, consumed_offset, remainder)
            if not qstore.has_session(str(start_item.session_id)):
                qstore.start(start_item, now_attach, basis="live_poll")
            collector.session = start_item
            collector.stopped = False
            last_seq = qstore.last_sequence(str(start_item.session_id))
            for raw_ln_b in lines[1:]:
                if not raw_ln_b:
                    continue
                raw_ln = raw_ln_b.decode("utf-8")
                obj = json.loads(raw_ln)
                if obj.get("kind") != "quote":
                    continue
                seq_val = int(obj.get("sequence", 0))
                if seq_val <= last_seq:
                    continue
                q_item = BridgeQuote.model_validate(obj)
                if now_attach - q_item.source_at <= timedelta(seconds=5):
                    qstore.record(q_item, now_attach, raw_ln)
                    last_seq = seq_val
            break
        except Exception:
            continue
    adapter = CTraderCliDemoBrokerAdapter(
        state_db_path=state_db,
        bridge_dir=bridge_dir,
        allow_orders=True,
        confirm_demo_execution=bool(getattr(args, "confirm_demo_execution", False)),
        expected_demo_account_number=expected_acct,
        default_stop_loss_pips=(
            float(exit_policy.stop_loss_pips) if exit_policy.stop_loss_pips is not None else 11.0
        ),
        default_take_profit_pips=(
            float(exit_policy.take_profit_pips)
            if exit_policy.take_profit_pips is not None
            else 22.0
        ),
    )
    try:
        smoke_risk_policy = risk_policy.model_copy(
            update={
                "max_volume_eur": Decimal(volume_units),
                "block_unverified_decision_evidence": False,
                "require_verified_decision_evidence": False,
            }
        )
        engine = CTraderCliDemoExecutionEngine(
            adapter=adapter,
            risk_policy=smoke_risk_policy,
            exit_policy=exit_policy,
            market_provider=lambda ts: build_market_snapshot(qstore, ts, basis="live_poll"),
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds

        def _poll_quotes_once() -> MarketSnapshot:
            for raw in tail.poll():
                collector.accept(raw, datetime.now(UTC))
            return build_market_snapshot(qstore, datetime.now(UTC), basis="live_poll")

        market = _poll_quotes_once()
        init_rec = adapter.reconcile()
        while loop.time() < deadline:
            has_eur = any(q.instrument == "EUR/USD" for q in market.quotes)
            if has_eur and not init_rec.blocks_new_risk and init_rec.allow_demo_orders is True:
                break
            await asyncio.sleep(0.05)
            market = _poll_quotes_once()
            init_rec = adapter.reconcile()

        if init_rec.open_positions or adapter.open_positions():
            raise DemoBrokerSafetyError(
                "demo-smoke requires a flat Demo account before starting; open positions detected"
            )
        if init_rec.blocks_new_risk or init_rec.allow_demo_orders is not True:
            raise DemoBrokerSafetyError(
                f"demo-smoke preflight failed: blocks_new_risk={init_rec.blocks_new_risk} "
                f"allow_demo_orders={init_rec.allow_demo_orders} drift={init_rec.drift_reasons}"
            )

        now = datetime.now(UTC)
        portfolio = engine.snapshot_sync(market=market)
        smoke_id = uuid4().hex[:10]
        thesis = Thesis(
            thesis_id=f"smoke-thesis-{smoke_id}",
            revision=1,
            status="active",
            created_at=now,
            updated_at=now,
            review_at=now + timedelta(minutes=15),
            narrative="Supervised bounded cTrader CLI Demo smoke verification",
            instruments=("EUR/USD",),
            supporting_event_ids=(f"smoke-event-{smoke_id}",),
            invalidation_conditions=("EUR/USD <= 1.0000",),
            confidence=0.60,
        )
        signed_weight = Decimal("0.20") if side == "buy" else Decimal("-0.20")
        decision = AgentDecision(
            decision_id=f"smoke-dec-{smoke_id}",
            event_id=f"smoke-event-{smoke_id}",
            based_on_memory_version=0,
            based_on_snapshot_id=portfolio.snapshot_id,
            provider="deterministic_demo_smoke",
            model_version="demo_smoke_v1",
            prompt_version=SINGLE_PASS_PROMPT_VERSION,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=30),
            thesis_changes=(
                ThesisChange(
                    expected_revision=0,
                    thesis=thesis,
                    reason="Supervised bounded Demo smoke verification",
                ),
            ),
            portfolio_action="SET_TARGET",
            target=TargetPortfolio(
                target_id=f"smoke-tgt-{smoke_id}",
                based_on_snapshot_id=portfolio.snapshot_id,
                rationale="Bounded Demo execution & SL attachment smoke check",
                positions=(
                    TargetPosition(
                        instrument="EUR/USD",
                        weight=signed_weight,
                        thesis_ids=(thesis.thesis_id,),
                    ),
                ),
            ),
            rationale="Bounded Demo execution & SL attachment smoke check",
        )

        # Enforce full deterministic target validator + hard risk gate (never bypass risk.py!)
        assert decision.target is not None
        DeterministicTargetValidator().validate(
            decision.target,
            portfolio,
            theses={thesis.thesis_id: thesis},
            as_of=now,
            portfolio_action="SET_TARGET",
        )
        risk_engine = DeterministicHardRiskEngine(policy=smoke_risk_policy)
        assessment = await risk_engine.assess(
            decision,
            portfolio,
            market,
            prior_theses=(),
        )
        if assessment.outcome != "approve":
            raise DemoBrokerSafetyError(
                f"Hard risk engine rejected demo-smoke order: {assessment.reasons}"
            )

        receipt = await engine.record(decision, assessment)
        if receipt.status != "recorded":
            raise DemoBrokerSafetyError(f"demo-smoke submission rejected: {receipt.reason}")

        # Wait for broker-confirmed fill + SL confirmation
        confirmed_pos: dict[str, Any] | None = None
        while loop.time() < deadline:
            adapter.poll_events()
            open_pos = adapter.open_positions()
            if open_pos:
                confirmed_pos = open_pos[0]
                break
            intent_state = adapter.get_intent(receipt.execution_id)
            if intent_state is not None and intent_state["status"] in (
                "rejected",
                "expired",
                "cancelled",
                "unknown_execution",
            ):
                raise DemoBrokerSafetyError(
                    f"demo-smoke entry failed with status={intent_state['status']} "
                    f"error={intent_state.get('error_code')}"
                )
            await asyncio.sleep(0.05)

        if confirmed_pos is None:
            raise DemoBrokerSafetyError("Timed out waiting for broker-confirmed Demo entry fill")
        pos_id = int(confirmed_pos["broker_position_id"])
        if not bool(confirmed_pos["sl_confirmed"]):
            adapter.submit_close_position(
                broker_position_id=pos_id, protective_reason="demo_smoke_emergency_close_no_sl"
            )
            raise DemoBrokerSafetyError(f"Broker position {pos_id} filled without confirmed SL!")

        if hold_seconds > 0:
            await asyncio.sleep(min(hold_seconds, 30.0))

        close_cid = adapter.submit_close_position(
            broker_position_id=pos_id,
            decision_id=decision.decision_id,
            assessment_id=assessment.assessment_id,
            protective_reason="demo_smoke_bounded_exit",
        )

        close_deadline = loop.time() + timeout_seconds
        final_rec = adapter.reconcile()
        while loop.time() < close_deadline:
            final_rec = adapter.reconcile()
            if (
                not adapter.open_positions()
                and not final_rec.open_positions
                and not final_rec.unpriced_closed_position_ids
                and final_rec.clean
            ):
                break
            await asyncio.sleep(0.05)

        engine.step_broker_events_and_protection(market)
        close_intent = adapter.get_intent(close_cid)
        pos_deals = adapter.closed_deals(broker_position_id=pos_id)
        flat_after = not bool(adapter.open_positions()) and not bool(final_rec.open_positions)
        return {
            "scope": "ctrader_cli_demo_smoke",
            "execution_authorized": True,
            "passed": flat_after and final_rec.clean and bool(pos_deals),
            "decision_id": decision.decision_id,
            "assessment_id": assessment.assessment_id,
            "approved_quantity_eur": str(assessment.approved_quantity_eur),
            "entry_intent_id": receipt.execution_id,
            "close_intent_id": close_cid,
            "broker_position_id": pos_id,
            "entry_fill_price": confirmed_pos["entry_price"],
            "sl_confirmed": bool(confirmed_pos["sl_confirmed"]),
            "exit_fill_price": close_intent["fill_price"] if close_intent else None,
            "close_deal_ids": [int(d["closing_deal_id"]) for d in pos_deals],
            "flat_after_close": flat_after,
            "reconciliation_after_close": final_rec.model_dump(mode="json"),
        }
    finally:
        adapter.close()
        qstore.close()


async def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    now = datetime.now(UTC)
    initial_cash_usd, risk_policy, exit_policy, cost_model = _build_policies(args)
    raw_basis = getattr(args, "basis", "live_poll")
    basis: Literal["live_poll", "synthetic_fixture"] = (
        "synthetic_fixture" if raw_basis == "synthetic_fixture" else "live_poll"
    )

    if args.command == "demo-reconcile":
        from .trading.ctrader_cli_broker import CTraderCliDemoBrokerAdapter

        bridge_dir_arg: Path = getattr(args, "quote_bridge_dir")
        state_db_arg = _resolve_cli_demo_state_db(args)
        state_db_arg.parent.mkdir(parents=True, exist_ok=True)
        adapter = CTraderCliDemoBrokerAdapter(
            state_db_path=state_db_arg,
            bridge_dir=bridge_dir_arg,
            allow_orders=False,
            confirm_demo_execution=False,
            expected_demo_account_number=_resolve_expected_demo_account_number(args),
        )
        try:
            rec_res = adapter.reconcile()
            return {
                "scope": "ctrader_cli_demo_reconciliation",
                "execution_authorized": False,
                "halted_reason": adapter.halted_reason,
                "manual_unlock_required": adapter.manual_unlock_required,
                "reconciliation": rec_res.model_dump(mode="json"),
                "local_open_positions": adapter.open_positions(),
                "unpriced_closed_positions": adapter.unpriced_closed_positions(),
                "closed_deals": adapter.closed_deals(),
            }
        finally:
            adapter.close()

    if args.command == "demo-unlock":
        from .trading.ctrader_cli_broker import CTraderCliDemoBrokerAdapter

        bridge_dir_u: Path = getattr(args, "quote_bridge_dir")
        state_db_u = _resolve_cli_demo_state_db(args)
        state_db_u.parent.mkdir(parents=True, exist_ok=True)
        adapter = CTraderCliDemoBrokerAdapter(
            state_db_path=state_db_u,
            bridge_dir=bridge_dir_u,
            allow_orders=False,
            confirm_demo_execution=False,
            expected_demo_account_number=_resolve_expected_demo_account_number(args),
        )
        try:
            rec_u = adapter.acknowledge_manual_unlock(note=str(getattr(args, "note", "")))
            return {
                "scope": "ctrader_cli_demo_unlock",
                "execution_authorized": False,
                "halted_reason": adapter.halted_reason,
                "manual_unlock_required": adapter.manual_unlock_required,
                "reconciliation": rec_u.model_dump(mode="json"),
            }
        finally:
            adapter.close()

    if args.command == "demo-smoke":
        return await _run_demo_smoke(
            args,
            risk_policy=risk_policy,
            exit_policy=exit_policy,
        )

    if args.command in ("status", "report"):
        diag_mem_db, diag_obs_db, diag_quotes_db = _resolve_companion_dbs(args)
        market = _load_latest_market(diag_quotes_db, basis, now=now)
        ro_ledger = inspect_shadow_ledger_readonly(
            args.shadow_db,
            market=market,
            cost_model=cost_model,
            exit_policy=exit_policy,
            risk_policy=risk_policy,
            now=now,
        )
        ro_runtime = inspect_runtime_store_readonly(args.shadow_db)
        runner_info = ro_runtime.get("runner") if isinstance(ro_runtime, dict) else None
        stored_summary = (
            runner_info.get("summary")
            if isinstance(runner_info, dict) and isinstance(runner_info.get("summary"), dict)
            else None
        )
        if not ro_ledger.get("degraded") and stored_summary is not None:
            ro_ledger["audit_metrics"] = build_shadow_audit_metrics(
                fills=ro_ledger.get("fills") or (),
                faults=ro_ledger.get("faults") or (),
                receipts=ro_ledger.get("receipts") or (),
                realized_pnl_usd=Decimal(str(ro_ledger.get("realized_pnl_usd", "0.00"))),
                total_fees_usd=Decimal(str(ro_ledger.get("total_fees_usd", "0.00"))),
                unclosed_risk=bool(ro_ledger.get("unclosed_risk")),
                max_holding_seconds=exit_policy.max_holding_seconds,
                protection_coverage=stored_summary.get("protection_coverage"),
                started_at=stored_summary.get("started_at")
                or (runner_info.get("started_at") if isinstance(runner_info, dict) else None),
                stopped_at=stored_summary.get("stopped_at")
                or (runner_info.get("stopped_at") if isinstance(runner_info, dict) else None),
                configured_duration_seconds=stored_summary.get("duration_seconds"),
            )
        diagnostics = _build_objective_diagnostics(
            shadow_db=args.shadow_db,
            memory_db=diag_mem_db,
            observer_db=diag_obs_db,
            quotes_db=diag_quotes_db,
            basis=basis,
            now=now,
        )
        source_health = _load_source_health_readonly(
            diag_obs_db,
            basis=basis,
            now=now,
        )
        degraded = bool(ro_ledger.get("degraded") or ro_runtime.get("degraded"))
        alerts = list(ro_ledger.get("alerts") or [])
        if ro_ledger.get("degraded") and ro_ledger.get("error"):
            alerts.append(f"READONLY_LEDGER_DEGRADED: {ro_ledger['error']}")
        if ro_runtime.get("degraded") and ro_runtime.get("detail"):
            alerts.append(f"READONLY_RUNTIME_DEGRADED: {ro_runtime['detail']}")

        if args.command == "status":
            return {
                "scope": "shadow_only",
                "execution_authorized": False,
                "readonly": True,
                "degraded": degraded,
                "ledger_status": ro_ledger.get("status"),
                "runtime_status": ro_runtime.get("status"),
                "unclosed_risk": ro_ledger.get("unclosed_risk"),
                "alerts": alerts,
                "recovery_instructions": ro_ledger.get("recovery_instructions"),
                "source_health": source_health,
                "diagnostics": diagnostics,
                "runner": ro_runtime["runner"],
                "portfolio": ro_ledger.get("snapshot"),
            }

        return {
            "scope": "shadow_only",
            "execution_authorized": False,
            "readonly": True,
            "degraded": degraded,
            "ledger_status": ro_ledger.get("status"),
            "runtime_status": ro_runtime.get("status"),
            "unclosed_risk": ro_ledger.get("unclosed_risk"),
            "alerts": alerts,
            "recovery_instructions": ro_ledger.get("recovery_instructions"),
            "source_health": source_health,
            "diagnostics": diagnostics,
            "ledger": ro_ledger,
            "runtime": ro_runtime["summary_report"],
        }

    args.shadow_db.parent.mkdir(parents=True, exist_ok=True)
    state_store = SQLiteRuntimeStore(args.shadow_db)
    ledger = SQLiteShadowLedger(
        args.shadow_db,
        initial_cash_usd=initial_cash_usd,
        cost_model=cost_model,
        exit_policy=exit_policy,
        risk_policy=risk_policy,
    )
    try:
        if args.command == "stop":
            state_store.request_stop(now)
            return {
                "scope": "shadow_only",
                "execution_authorized": False,
                "action": "stop_requested",
                "requested_at": now.isoformat(),
                "runner": state_store.runner_status(),
            }

        args.memory_db.parent.mkdir(parents=True, exist_ok=True)
        memory = SQLiteThesisMemory(args.memory_db, allow_targets=True)
        quote_bridge_dir: Path | None = getattr(args, "quote_bridge_dir", None)
        use_ctrader_openapi = bool(getattr(args, "ctrader_openapi", False))
        writable_quotes = quote_bridge_dir is not None or use_ctrader_openapi
        if writable_quotes:
            args.quotes_db.parent.mkdir(parents=True, exist_ok=True)
        qstore = (
            QuoteStore(args.quotes_db, readonly=not writable_quotes)
            if (args.quotes_db.is_file() or writable_quotes)
            else None
        )
        if getattr(args, "poll_news", False):
            args.observer_db.parent.mkdir(parents=True, exist_ok=True)
        ostore = (
            ObserverStore(
                args.observer_db,
                readonly=not getattr(args, "poll_news", False),
            )
            if (args.observer_db.is_file() or getattr(args, "poll_news", False))
            else None
        )
        cli_demo_adapter: Any = None
        try:
            runner_ref: list[ShadowRunner] = []

            def market_provider(ts: datetime) -> MarketSnapshot:
                if runner_ref:
                    runner_ref[0]._poll_quote_bridge_sync()
                if qstore is not None:
                    return build_market_snapshot(qstore, ts, basis=basis)
                return MarketSnapshot(
                    snapshot_id=f"market-empty:EURUSD:{ts.isoformat()}",
                    as_of=ts,
                    quotes=(),
                )

            call_model = getattr(args, "call_model", False)
            review_horizon_s = float(getattr(args, "review_horizon_seconds", 3600.0))
            agent = (
                SinglePassTradingAgent(
                    DeepSeekClient.from_environment(),
                    max_quote_age=timedelta(seconds=getattr(args, "max_quote_age", 2.0)),
                    allow_commission_bearing_zero_spread=(
                        risk_policy.allow_commission_bearing_zero_spread
                    ),
                    max_position_weight=risk_policy.max_position_weight,
                    leverage=risk_policy.leverage,
                    max_risk_per_trade_pct=risk_policy.max_risk_per_trade_pct,
                    ex_ante_stop_loss_pips=(
                        risk_policy.ex_ante_stop_loss_pips or exit_policy.stop_loss_pips
                    ),
                    review_horizon=timedelta(seconds=review_horizon_s),
                )
                if call_model
                else _AbstainWithoutModelAgent()
            )
            ev_filter = EURUSDEventFilter()
            max_quote_wait_s = float(getattr(args, "max_quote_wait_seconds", 60.0))
            raw_max_lag = getattr(args, "max_actionable_event_lag_seconds", None)
            max_actionable_lag_s: float | None = (
                float(raw_max_lag) if raw_max_lag is not None and float(raw_max_lag) > 0 else None
            )
            raw_daily_llm = getattr(args, "max_daily_llm_calls", None)
            max_daily_llm: int | None = (
                int(raw_daily_llm) if raw_daily_llm is not None and int(raw_daily_llm) > 0 else None
            )
            runtime = IntradayEventRuntime(
                memory=memory,
                portfolio_state=ledger,
                market_provider=market_provider,
                agent=agent,
                target_validator=DeterministicTargetValidator(),
                risk_engine=DeterministicHardRiskEngine(policy=risk_policy),
                shadow_executor=ledger,
                state_store=state_store,
                event_filter=ev_filter,
                max_quote_wait_age=timedelta(seconds=max_quote_wait_s),
                max_actionable_event_lag_seconds=max_actionable_lag_s,
                max_daily_llm_calls=max_daily_llm,
            )

            if args.command == "recover":
                if quote_bridge_dir is not None and qstore is not None:
                    bridge_runner = ShadowRunner(
                        runtime=runtime,
                        ledger=ledger,
                        quote_store=qstore,
                        quote_bridge_dir=quote_bridge_dir,
                        basis=basis,
                        poll_news=False,
                    )
                    runner_ref.append(bridge_runner)
                expired = ledger.expire_pending_orders()
                recovered = await runtime.recover_incomplete()
                if getattr(args, "close_on_recover", False):
                    ledger.request_protective_exit_on_stop(
                        now=datetime.now(UTC),
                        reason="protective_exit_on_manual_recover",
                    )
                market = market_provider(datetime.now(UTC))
                exits = list(ledger.evaluate_protective_exits(market))
                wait_seconds = float(getattr(args, "wait_for_quote_seconds", 0.0) or 0.0)
                if wait_seconds > 0.0:
                    deadline = asyncio.get_running_loop().time() + wait_seconds
                    while (
                        Decimal(ledger.report(market=market)["quantity_eur"]) != Decimal("0")
                        and asyncio.get_running_loop().time() < deadline
                    ):
                        await asyncio.sleep(0.25)
                        market = market_provider(datetime.now(UTC))
                        new_exits = ledger.evaluate_protective_exits(market)
                        exits.extend(new_exits)
                rep = ledger.report(market=market)
                return {
                    "scope": "shadow_only",
                    "execution_authorized": False,
                    "unclosed_risk": rep["unclosed_risk"],
                    "alerts": rep["alerts"],
                    "recovery_instructions": rep["recovery_instructions"],
                    "expired_orders": expired,
                    "recovered_events": [r.model_dump(mode="json") for r in recovered],
                    "protective_exits": [f.model_dump(mode="json") for f in exits],
                    "portfolio": ledger.snapshot_sync(market=market).model_dump(mode="json"),
                }

            allow_baseline = getattr(args, "allow_baseline", None)
            news_source_mode = getattr(args, "news_source", "tier1_english")
            active_news_source: Any = None
            supplemental_cal: WallStreetCNSource | None = None
            if args.poll_news:
                if news_source_mode == "wallstreetcn":
                    active_news_source = WallStreetCNSource()
                else:
                    active_news_source = Tier1EnglishSource()
                    if getattr(args, "supplement_calendar_actuals", False):
                        supplemental_cal = WallStreetCNSource()
            raw_breakout_pips = getattr(args, "review_breakout_pips", None)
            raw_periodic_s = getattr(args, "periodic_review_seconds", None)
            review_policy = ReviewPolicy(
                enabled=bool(getattr(args, "enable_stateful_reviews", False)),
                periodic_review_seconds=(
                    float(raw_periodic_s)
                    if raw_periodic_s is not None and float(raw_periodic_s) > 0
                    else None
                ),
                breakout_excursion_pips=(
                    Decimal(str(raw_breakout_pips))
                    if raw_breakout_pips is not None
                    and Decimal(str(raw_breakout_pips)) > Decimal("0")
                    else None
                ),
                structured_price_rules=tuple(getattr(args, "structured_price_rule", None) or ()),
                cooldown_seconds=float(getattr(args, "review_cooldown_seconds", 120.0)),
                max_reviews_per_thesis=int(getattr(args, "max_reviews_per_thesis", 3)),
                max_total_review_calls=int(getattr(args, "max_total_review_calls", 10)),
            )
            openapi_streamer: Any = None
            if use_ctrader_openapi:
                from .ctrader_openapi import CTraderOpenApiConfig, CTraderOpenApiQuoteStreamer

                assert qstore is not None
                oa_cfg = CTraderOpenApiConfig.from_environment()
                openapi_streamer = CTraderOpenApiQuoteStreamer(
                    config=oa_cfg,
                    quote_store=qstore,
                    basis=basis,
                    sample_interval_ms=int(getattr(args, "ctrader_sample_interval_ms", 200)),
                )
            if getattr(args, "ctrader_cli_demo_orders", False):
                from .trading.ctrader_cli_broker import CTraderCliDemoBrokerAdapter

                assert quote_bridge_dir is not None
                cli_state_db: Path = getattr(args, "cli_demo_state_db", None) or (
                    args.shadow_db.parent / "cli-demo-broker.sqlite"
                )
                cli_state_db.parent.mkdir(parents=True, exist_ok=True)
                cli_demo_adapter = CTraderCliDemoBrokerAdapter(
                    state_db_path=cli_state_db,
                    bridge_dir=quote_bridge_dir,
                    allow_orders=True,
                    confirm_demo_execution=bool(getattr(args, "confirm_demo_execution", False)),
                    expected_demo_account_number=_resolve_expected_demo_account_number(
                        args, require_positive=True
                    ),
                    default_stop_loss_pips=(
                        float(exit_policy.stop_loss_pips)
                        if exit_policy.stop_loss_pips is not None
                        else 11.0
                    ),
                    default_take_profit_pips=(
                        float(exit_policy.take_profit_pips)
                        if exit_policy.take_profit_pips is not None
                        else 22.0
                    ),
                )
                cli_demo_adapter.reconcile()
            continuous_mode = bool(getattr(args, "continuous", False))
            runner = ShadowRunner(
                runtime=runtime,
                ledger=ledger,
                observer_store=ostore,
                quote_store=qstore,
                quote_bridge_dir=quote_bridge_dir,
                ctrader_openapi_streamer=openapi_streamer,
                ctrader_demo_mirror=bool(getattr(args, "ctrader_demo_mirror_orders", False)),
                ctrader_cli_demo_adapter=cli_demo_adapter,
                enforce_fx_weekend_pause=continuous_mode or use_ctrader_openapi,
                news_source=active_news_source,
                supplemental_calendar_source=supplemental_cal,
                basis=basis,
                poll_news=args.poll_news,
                allow_baseline_revisions=(
                    bool(allow_baseline) if allow_baseline is not None else not args.poll_news
                ),
                bundle_simultaneous=bool(getattr(args, "bundle_events", True)),
                news_bundle_window_seconds=float(getattr(args, "bundle_window_seconds", 60.0)),
                news_poll_interval_seconds=args.news_interval,
                event_scan_interval_seconds=args.event_interval,
                quote_interval_seconds=args.quote_interval,
                protection_interval_seconds=args.protection_interval,
                exit_buffer_seconds=args.exit_buffer_seconds,
                exit_drain_timeout_seconds=args.exit_drain_timeout,
                strict_window_coverage=not getattr(args, "no_strict_window_coverage", False),
                close_on_stop=getattr(args, "close_on_stop", False),
                confirm_handoff=getattr(args, "confirm_handoff", False),
                review_policy=review_policy,
            )
            runner_ref.append(runner)
            loop = asyncio.get_running_loop()
            installed_signals: list[signal.Signals] = []

            def _handle_stop_signal(reason_name: str) -> None:
                runner.stop(reason=reason_name)

            for sig, sig_name in (
                (signal.SIGTERM, "signal_sigterm"),
                (signal.SIGINT, "signal_sigint"),
            ):
                try:
                    loop.add_signal_handler(sig, _handle_stop_signal, sig_name)
                    installed_signals.append(sig)
                except (NotImplementedError, RuntimeError):
                    pass
            try:
                return await runner.run(
                    duration_seconds=None if continuous_mode else float(args.duration)
                )
            finally:
                for sig in installed_signals:
                    try:
                        loop.remove_signal_handler(sig)
                    except Exception:
                        pass
        finally:
            if cli_demo_adapter is not None:
                cli_demo_adapter.close()
            if qstore is not None:
                qstore.close()
            if ostore is not None:
                ostore.close()
    finally:
        ledger.close()
        state_store.close()


def _add_common_db_args(p: argparse.ArgumentParser, *, suppress_defaults: bool = False) -> None:
    default_flag = argparse.SUPPRESS if suppress_defaults else None
    p.add_argument(
        "--shadow-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/shadow-ledger.sqlite"),
        help="SQLite database for shadow ledger, runtime state, and audit log",
    )
    p.add_argument(
        "--memory-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/shadow-memory.sqlite"),
        help="SQLite database for durable event evidence and thesis memory",
    )
    p.add_argument(
        "--observer-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/observer.sqlite"),
        help="SQLite database for captured WallStreetCN revisions",
    )
    p.add_argument(
        "--quotes-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/live-quotes.sqlite"),
        help="SQLite database for Demo EUR/USD quote observations",
    )
    p.add_argument(
        "--account-profile",
        choices=("pepperstone_razor_200", "institutional_10k"),
        default=default_flag if suppress_defaults else "pepperstone_razor_200",
        help="Account sizing and risk profile (default: pepperstone_razor_200 = $200 USD, 200x leverage, 0.01..100 lots)",
    )
    p.add_argument(
        "--initial-cash",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override initial cash in USD",
    )
    p.add_argument(
        "--leverage",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override account leverage multiplier (e.g. 200)",
    )
    p.add_argument(
        "--min-lot",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override minimum trade size in standard lots (e.g. 0.01)",
    )
    p.add_argument(
        "--max-lot",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override maximum trade size in standard lots (e.g. 100)",
    )
    p.add_argument(
        "--lot-step",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override lot step increment in standard lots (e.g. 0.01)",
    )
    p.add_argument(
        "--max-position-loss",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override single-position protective stop-loss in USD",
    )
    p.add_argument(
        "--take-profit",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override single-position protective take-profit in USD (0 to disable)",
    )
    p.add_argument(
        "--stop-loss-pips",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override position-scaled protective stop-loss in EUR/USD pips (0 to disable)",
    )
    p.add_argument(
        "--take-profit-pips",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override position-scaled protective take-profit in EUR/USD pips (0 to disable)",
    )
    p.add_argument(
        "--trailing-activation-pips",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override trailing profit lock activation threshold in EUR/USD pips (0 to disable trailing)",
    )
    p.add_argument(
        "--trailing-distance-pips",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override trailing profit lock pullback distance in EUR/USD pips (0 to disable trailing)",
    )
    p.add_argument(
        "--max-daily-loss",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override daily loss limit in USD",
    )
    p.add_argument(
        "--min-equity",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override hard minimum equity floor in USD",
    )
    p.add_argument(
        "--allow-commission-zero-spread",
        action=argparse.BooleanOptionalAction,
        default=default_flag if suppress_defaults else None,
        help="Explicitly allow or forbid commission-bearing zero-spread quotes from verified ctrader_algo sessions",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        default=default_flag if suppress_defaults else False,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _add_common_db_args(parser, suppress_defaults=False)
    sub = parser.add_subparsers(dest="command", required=True)

    start_p = sub.add_parser("start", help="Run bounded automated Shadow loop in a single process")
    _add_common_db_args(start_p, suppress_defaults=True)
    start_p.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Bounded runtime duration in seconds (0 < duration <= 604800; ignored when --continuous is set)",
    )
    start_p.add_argument(
        "--continuous",
        action="store_true",
        help="Run unattended 24x5 loop continuously until `eventlens shadow stop` or SIGTERM/SIGINT",
    )
    start_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )
    start_p.add_argument("--call-model", action="store_true", help="Opt in to paid DeepSeek calls")
    start_p.add_argument(
        "--poll-news",
        action="store_true",
        help="Poll live Tier-1 English FX & official central bank feeds (investingLive, ECB, Fed, US BEA, ForexFactory)",
    )
    start_p.add_argument(
        "--news-source",
        choices=("tier1_english", "wallstreetcn"),
        default="tier1_english",
        help="Live news/macro source stack when --poll-news is enabled (default: tier1_english)",
    )
    start_p.add_argument(
        "--supplement-calendar-actuals",
        action="store_true",
        help="Optionally poll structured numeric calendar actuals alongside tier1_english (without Chinese news)",
    )
    start_p.add_argument(
        "--bundle-events",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Bundle simultaneous macro calendar indicators and same-provider burst headlines into single LLM calls",
    )
    start_p.add_argument(
        "--bundle-window-seconds",
        type=float,
        default=60.0,
        help="Time bucket in seconds for grouping simultaneous same-provider news headlines (0 disables news bundling)",
    )
    start_p.add_argument(
        "--allow-baseline",
        action="store_true",
        default=None,
        help="Allow initial baseline snapshot revisions to trigger runtime cycles",
    )
    start_p.add_argument(
        "--quote-bridge-dir",
        type=Path,
        default=None,
        help="Optional EventLensQuoteBridge sandbox directory to tail live Demo EUR/USD quotes",
    )
    start_p.add_argument(
        "--ctrader-openapi",
        action="store_true",
        help="Stream live Demo EUR/USD quotes directly from headless cTrader Open API (demo.ctraderapi.com:5036)",
    )
    start_p.add_argument(
        "--ctrader-demo-mirror-orders",
        action="store_true",
        help="Unavailable: Open API Demo broker mirroring is blocked; use --ctrader-cli-demo-orders with EventLensCliBridge",
    )
    start_p.add_argument(
        "--ctrader-cli-demo-orders",
        action="store_true",
        help="Opt in to position-ID-aware Demo order intents & reconciliation via EventLensCliBridge (--quote-bridge-dir required)",
    )
    start_p.add_argument(
        "--confirm-demo-execution",
        action="store_true",
        help="Explicitly confirm non-Live Demo order execution when --ctrader-cli-demo-orders is enabled",
    )
    start_p.add_argument(
        "--cli-demo-state-db",
        type=Path,
        default=None,
        help="SQLite state DB path for CTraderCliDemoBrokerAdapter (defaults to <shadow-db-dir>/cli-demo-broker.sqlite)",
    )
    start_p.add_argument(
        "--expected-demo-account-number",
        type=_parse_cli_expected_account_number,
        default=None,
        help="Expected cTrader Demo Account.Number (positive 64-bit integer required when --ctrader-cli-demo-orders is enabled)",
    )
    start_p.add_argument(
        "--max-risk-per-trade-pct",
        type=str,
        default=None,
        help="Optional ex-ante risk budget fraction of equity per trade (e.g. '0.02' for 2%% ROUND_DOWN lot cap)",
    )
    start_p.add_argument(
        "--ctrader-sample-interval-ms",
        type=int,
        default=200,
        help="cTrader Open API tick sampling throttle in ms (default: 200; positive-spread ticks always flush immediately)",
    )
    start_p.add_argument(
        "--max-daily-llm-calls",
        type=int,
        default=None,
        help="Optional daily UTC cap on paid LLM invocations as a cloud cost circuit breaker (e.g. 80)",
    )
    start_p.add_argument("--news-interval", type=float, default=20.0)
    start_p.add_argument("--event-interval", type=float, default=1.0)
    start_p.add_argument("--quote-interval", type=float, default=0.5)
    start_p.add_argument("--protection-interval", type=float, default=0.5)
    start_p.add_argument("--max-quote-age", type=float, default=2.0)
    start_p.add_argument(
        "--max-quote-wait-seconds",
        type=float,
        default=60.0,
        help="Maximum event validity window in seconds to wait for a valid quote before marking expired",
    )
    start_p.add_argument(
        "--max-actionable-event-lag-seconds",
        type=float,
        default=None,
        help="Optional verifiable source-to-receipt lag threshold above which new/increased risk is blocked",
    )
    start_p.add_argument(
        "--enable-stateful-reviews",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable bounded, deduplicated stateful thesis re-evaluation on review_at or structured price rules",
    )
    start_p.add_argument(
        "--review-horizon-seconds",
        type=float,
        default=3600.0,
        help="Default thesis review_at horizon in seconds from decision completion (default: 3600)",
    )
    start_p.add_argument(
        "--periodic-review-seconds",
        type=float,
        default=None,
        help="Optional periodic thesis review interval in seconds when stateful reviews are enabled",
    )
    start_p.add_argument(
        "--review-breakout-pips",
        type=str,
        default=None,
        help="Optional EUR/USD excursion threshold in pips from thesis reference mid to trigger one review",
    )
    start_p.add_argument(
        "--structured-price-rule",
        action="append",
        default=None,
        help="Repeatable structured price trigger rule, e.g. 'EUR/USD >= 1.1145'",
    )
    start_p.add_argument(
        "--review-cooldown-seconds",
        type=float,
        default=120.0,
        help="Minimum cooldown in seconds between stateful review calls (default: 120)",
    )
    start_p.add_argument(
        "--max-reviews-per-thesis",
        type=int,
        default=3,
        help="Maximum stateful review calls per thesis ID (default: 3)",
    )
    start_p.add_argument(
        "--max-total-review-calls",
        type=int,
        default=10,
        help="Maximum total stateful review calls per ShadowRunner session (default: 10)",
    )
    start_p.add_argument("--max-holding-seconds", type=int, default=None)
    start_p.add_argument(
        "--exit-buffer-seconds",
        type=float,
        default=60.0,
        help="Minimum remaining run/quote-bridge buffer after max_holding_seconds required for new risk entries",
    )
    start_p.add_argument(
        "--exit-drain-timeout",
        type=float,
        default=60.0,
        help="Seconds to keep quote and protection tasks running during shutdown to drain open positions",
    )
    start_p.add_argument(
        "--no-strict-window-coverage",
        action="store_true",
        help="Disable strict remaining run/quote-bridge window check for new entries",
    )
    start_p.add_argument(
        "--close-on-stop",
        action="store_true",
        help="Request protective exit on open shadow position when stop or duration cutoff occurs",
    )
    start_p.add_argument(
        "--confirm-handoff",
        action="store_true",
        help="Explicitly confirm handoff of an open position at shutdown without draining to flat",
    )
    start_p.add_argument("--kill-switch", action="store_true")

    stop_p = sub.add_parser("stop", help="Signal a running Shadow loop to stop cleanly")
    _add_common_db_args(stop_p, suppress_defaults=True)

    status_p = sub.add_parser("status", help="Inspect current Shadow account and runner status")
    _add_common_db_args(status_p, suppress_defaults=True)
    status_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )

    report_p = sub.add_parser("report", help="Read full Shadow ledger, lifecycle, and audit report")
    _add_common_db_args(report_p, suppress_defaults=True)
    report_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )

    reconcile_p = sub.add_parser(
        "demo-reconcile",
        help="Read-only reconciliation check of EventLensCliBridge Demo positions vs local state DB",
    )
    _add_common_db_args(reconcile_p, suppress_defaults=True)
    reconcile_p.add_argument(
        "--quote-bridge-dir",
        type=Path,
        required=True,
        help="Shared EventLensCliBridge directory containing execution-events.jsonl",
    )
    reconcile_p.add_argument(
        "--cli-demo-state-db",
        type=Path,
        default=None,
        help="Optional separate SQLite state DB path for CTraderCliDemoBrokerAdapter",
    )
    reconcile_p.add_argument(
        "--expected-demo-account-number",
        type=_parse_cli_expected_account_number,
        default=None,
        help="Optional expected cTrader Demo Account.Number to enforce during reconciliation",
    )

    unlock_p = sub.add_parser(
        "demo-unlock",
        help="Explicit operator unlock after verifying ambiguous/unknown Demo execution state is clean",
    )
    _add_common_db_args(unlock_p, suppress_defaults=True)
    unlock_p.add_argument(
        "--quote-bridge-dir",
        type=Path,
        required=True,
        help="Shared EventLensCliBridge directory containing execution-events.jsonl",
    )
    unlock_p.add_argument(
        "--cli-demo-state-db",
        type=Path,
        default=None,
        help="Optional separate SQLite state DB path for CTraderCliDemoBrokerAdapter",
    )
    unlock_p.add_argument(
        "--expected-demo-account-number",
        type=_parse_cli_expected_account_number,
        default=None,
        help="Optional expected cTrader Demo Account.Number to enforce before unlocking",
    )
    unlock_p.add_argument(
        "--note",
        type=str,
        required=True,
        help="Mandatory operator audit note explaining broker verification (>= 5 chars)",
    )

    smoke_p = sub.add_parser(
        "demo-smoke",
        help="Supervised bounded cTrader CLI Demo smoke test (1,000-2,000 EUR open -> SL confirmed -> close -> flat)",
    )
    _add_common_db_args(smoke_p, suppress_defaults=True)
    smoke_p.add_argument(
        "--quote-bridge-dir",
        type=Path,
        required=True,
        help="Shared EventLensCliBridge directory containing quotes-*.jsonl & execution-events.jsonl",
    )
    smoke_p.add_argument(
        "--cli-demo-state-db",
        type=Path,
        default=None,
        help="Optional SQLite state DB path for CTraderCliDemoBrokerAdapter",
    )
    smoke_p.add_argument(
        "--confirm-demo-execution",
        action="store_true",
        help="Explicitly confirm placing a bounded Demo smoke trade on the connected non-Live account",
    )
    smoke_p.add_argument(
        "--expected-demo-account-number",
        type=_parse_cli_expected_account_number,
        default=None,
        help="Positive 64-bit cTrader Demo Account.Number enforced on preflight & dispatch (or set CTRADER_DEMO_ACCOUNT_NUMBER)",
    )
    smoke_p.add_argument(
        "--side",
        choices=("buy", "sell"),
        default="buy",
        help="Bounded Demo smoke entry side (default: buy)",
    )
    smoke_p.add_argument(
        "--volume-units",
        type=int,
        default=1000,
        help="Bounded Demo smoke EUR units (must be 1000 or 2000; default: 1000)",
    )
    smoke_p.add_argument(
        "--hold-seconds",
        type=float,
        default=0.2,
        help="Seconds to hold after broker fill before closing by Position.Id (default: 0.2, max: 30)",
    )
    smoke_p.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="Max seconds to wait for quote/reconcile/fill/close confirmation (default: 20, max: 120)",
    )

    recover_p = sub.add_parser(
        "recover", help="Reconcile incomplete pre-crash decisions and expired orders"
    )
    _add_common_db_args(recover_p, suppress_defaults=True)
    recover_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )
    recover_p.add_argument(
        "--quote-bridge-dir",
        type=Path,
        default=None,
        help="Optional EventLensQuoteBridge sandbox directory to poll fresh live quotes during recovery",
    )
    recover_p.add_argument(
        "--wait-for-quote-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait for a fresh live quote to execute pending protective exits during recovery",
    )
    recover_p.add_argument(
        "--close-on-recover",
        action="store_true",
        help="Request immediate protective exit on any open shadow position during recovery using a fresh quote",
    )
    recover_p.add_argument("--max-quote-age", type=float, default=2.0)
    recover_p.add_argument(
        "--max-quote-wait-seconds",
        type=float,
        default=60.0,
        help="Maximum event validity window in seconds for events awaiting market quotes during recovery",
    )
    recover_p.add_argument("--kill-switch", action="store_true")

    args = parser.parse_args(argv)
    if hasattr(args, "max_quote_wait_seconds") and (
        not math.isfinite(args.max_quote_wait_seconds) or args.max_quote_wait_seconds <= 0
    ):
        parser.error("--max-quote-wait-seconds must be a positive finite number")
    if args.command == "start":
        if not getattr(args, "continuous", False) and (
            not math.isfinite(args.duration) or not 0 < args.duration <= 604800
        ):
            parser.error("--duration must be a finite number in (0, 604800] seconds")
        if getattr(args, "ctrader_demo_mirror_orders", False):
            from .ctrader_openapi import DEMO_EXECUTION_UNAVAILABLE

            parser.error(DEMO_EXECUTION_UNAVAILABLE)
        if (
            getattr(args, "ctrader_openapi", False)
            and getattr(args, "quote_bridge_dir", None) is not None
        ):
            parser.error("--ctrader-openapi and --quote-bridge-dir are mutually exclusive")
        if getattr(args, "ctrader_cli_demo_orders", False):
            if getattr(args, "quote_bridge_dir", None) is None:
                parser.error("--ctrader-cli-demo-orders requires --quote-bridge-dir")
            if not getattr(args, "confirm_demo_execution", False):
                parser.error("--ctrader-cli-demo-orders requires --confirm-demo-execution")
            if not getattr(args, "call_model", False):
                parser.error(
                    "--ctrader-cli-demo-orders on `start` requires --call-model "
                    "(for model-free bounded Demo verification, use `eventlens-shadow demo-smoke`)"
                )
            try:
                _resolve_expected_demo_account_number(args, require_positive=True)
            except ValueError as exc:
                parser.error(str(exc))
        if getattr(args, "max_daily_llm_calls", None) is not None and args.max_daily_llm_calls < 1:
            parser.error("--max-daily-llm-calls must be >= 1")
        if getattr(args, "ctrader_sample_interval_ms", 200) < 0:
            parser.error("--ctrader-sample-interval-ms must be >= 0")
        if args.poll_news and args.news_interval < 10.0:
            parser.error("--news-interval must be at least 10 seconds when polling live news")
    elif args.command == "demo-smoke":
        if not getattr(args, "confirm_demo_execution", False):
            parser.error("demo-smoke requires --confirm-demo-execution")
        try:
            _resolve_expected_demo_account_number(args, require_positive=True)
        except ValueError as exc:
            parser.error(str(exc))
        if int(getattr(args, "volume_units", 1000)) not in (1000, 2000):
            parser.error("demo-smoke --volume-units is strictly bounded to 1000 or 2000 EUR")
        if not (0.0 <= float(getattr(args, "hold_seconds", 0.2)) <= 30.0):
            parser.error("demo-smoke --hold-seconds must be in [0, 30]")
        if not (1.0 <= float(getattr(args, "timeout_seconds", 20.0)) <= 120.0):
            parser.error("demo-smoke --timeout-seconds must be in [1, 120]")
    elif args.command in ("demo-reconcile", "demo-unlock"):
        try:
            _resolve_expected_demo_account_number(args, require_positive=False)
        except ValueError as exc:
            parser.error(str(exc))
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    result = asyncio.run(_run_command(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
