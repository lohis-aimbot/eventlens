"""Tests for Strategy v5: causal 1h/4h quotes, hard risk gates, T2 exits, and daily watch review reset."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from eventlens.contracts import (
    AgentDecision,
    Event,
    MarketSnapshot,
    PortfolioSnapshot,
    PriceTrajectorySummary,
    Quote,
    TargetPortfolio,
    TargetPosition,
)
from eventlens.deepseek import ModelResponse
from eventlens.quotes import (
    BridgeQuote,
    BridgeStart,
    QuoteStore,
    build_market_snapshot,
    classify_fx_session,
)
from eventlens.runtime import (
    IntradayEventRuntime,
    ReviewPolicy,
    ShadowRunner,
    SQLiteRuntimeStore,
)
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import SinglePassTradingAgent
from eventlens.trading.execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    SQLiteShadowLedger,
)
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy


def _record_quote(
    store: QuoteStore,
    *,
    session_id: Any,
    seq: int,
    ts: datetime,
    mid: Decimal,
) -> None:
    half = Decimal("0.00001")
    store.record(
        BridgeQuote(
            version=1,
            kind="quote",
            session_id=session_id,
            sequence=seq,
            symbol="EURUSD",
            source_at=ts - timedelta(milliseconds=50),
            bridge_at=ts - timedelta(milliseconds=20),
            bid=mid - half,
            ask=mid + half,
        ),
        ts,
        f"seq={seq}",
    )


def _live_market(
    ts: datetime,
    *,
    mid: Decimal,
    c5m: Decimal = Decimal("0.0"),
    c60m: Decimal | None = Decimal("0.0"),
    c240m: Decimal | None = Decimal("0.0"),
) -> MarketSnapshot:
    half = Decimal("0.00001")
    return MarketSnapshot(
        snapshot_id=f"mkt-{int(ts.timestamp() * 1000)}",
        as_of=ts,
        quotes=(
            Quote(
                instrument="EUR/USD",
                source_at=ts - timedelta(milliseconds=100),
                bridge_at=ts - timedelta(milliseconds=50),
                received_at=ts - timedelta(milliseconds=10),
                available_at=ts - timedelta(milliseconds=5),
                source_provider="ctrader_algo",
                basis="live_poll",
                bid=mid - half,
                ask=mid + half,
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
        ),
        price_trajectory=PriceTrajectorySummary(
            instrument="EUR/USD",
            as_of=ts,
            current_mid=mid,
            change_1m_pips=Decimal("0.5"),
            change_5m_pips=c5m,
            change_15m_pips=Decimal("1.0"),
            change_30m_pips=Decimal("1.5"),
            change_60m_pips=c60m,
            change_240m_pips=c240m,
            window_15m_high_mid=mid + Decimal("0.00050"),
            window_15m_low_mid=mid - Decimal("0.00050"),
            window_30m_high_mid=mid + Decimal("0.00080"),
            window_30m_low_mid=mid - Decimal("0.00080"),
            sample_count_30m=120,
            max_gap_30m_seconds=5.0,
            fx_session=classify_fx_session(ts),
        ),
    )


def test_causal_htf_1h_4h_lookup_and_cold_start_missing(tmp_path: Path) -> None:
    sid = uuid4()
    start_ts = datetime(2026, 11, 25, 10, 0, 0, tzinfo=UTC)
    cutoff = datetime(2026, 11, 25, 15, 0, 0, tzinfo=UTC)  # overlap session (10:00 EST / 15:00 GMT)
    now_box = [cutoff + timedelta(minutes=10)]
    qstore = QuoteStore(tmp_path / "quotes.sqlite", clock=lambda: now_box[0])
    try:
        qstore.start(
            BridgeStart(
                version=1,
                kind="start",
                session_id=sid,
                account_mode="demo",
                running_mode="realtime",
                symbol="EURUSD",
                started_at=start_ts,
                sample_interval_ms=500,
                duration_seconds=36000,
                digits=5,
                pip_size=Decimal("0.0001"),
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
            start_ts,
            basis="live_poll",
        )
        points = [
            (
                datetime(2026, 11, 25, 11, 0, 0, tzinfo=UTC),
                Decimal("1.10250"),
            ),  # 4h ago -> -25.0p at 1.10000
            (
                datetime(2026, 11, 25, 14, 0, 0, tzinfo=UTC),
                Decimal("1.10100"),
            ),  # 1h ago -> -10.0p at 1.10000
            (datetime(2026, 11, 25, 14, 30, 0, tzinfo=UTC), Decimal("1.10050")),
            (datetime(2026, 11, 25, 14, 45, 0, tzinfo=UTC), Decimal("1.10030")),
            (datetime(2026, 11, 25, 14, 55, 0, tzinfo=UTC), Decimal("1.10010")),
            (datetime(2026, 11, 25, 14, 59, 0, tzinfo=UTC), Decimal("1.10005")),
            (cutoff, Decimal("1.10000")),
            # Future observation past cutoff MUST never leak into 15:00 snapshot
            (cutoff + timedelta(minutes=5), Decimal("1.15000")),
        ]
        for idx, (ts, mid) in enumerate(points, start=1):
            now_box[0] = ts
            _record_quote(qstore, session_id=sid, seq=idx, ts=ts, mid=mid)

        snap = build_market_snapshot(qstore, cutoff)
        assert snap.price_trajectory is not None
        traj = snap.price_trajectory
        assert traj.current_mid == Decimal("1.10000")
        assert traj.change_60m_pips == Decimal("-10.0")
        assert traj.direction_60m == "down"
        assert traj.change_240m_pips == Decimal("-25.0")
        assert traj.direction_240m == "down"
        assert traj.fx_session == "overlap"
        assert traj.short_window_coverage_status == "complete"

        # Cold-start: only 20m of history before an early cutoff
        cold_cutoff = datetime(2026, 11, 25, 14, 46, 0, tzinfo=UTC)
        assert (
            qstore.mid_near(
                "EURUSD",
                datetime(2026, 11, 25, 10, 46, 0, tzinfo=UTC),
                cutoff=cold_cutoff,
                tolerance_seconds=600.0,
            )
            is None
        )

        # Separate cold store with 20m window keeps short_window_coverage_status == complete while HTF is None
        c_end = datetime(2026, 11, 25, 9, 20, 0, tzinfo=UTC)
        cold_now = [c_end - timedelta(minutes=25)]
        cold_store = QuoteStore(tmp_path / "cold.sqlite", clock=lambda: cold_now[0])
        try:
            cold_sid = uuid4()
            cold_store.start(
                BridgeStart(
                    version=1,
                    kind="start",
                    session_id=cold_sid,
                    account_mode="demo",
                    running_mode="realtime",
                    symbol="EURUSD",
                    started_at=c_end - timedelta(minutes=25),
                    sample_interval_ms=500,
                    duration_seconds=3600,
                    digits=5,
                    pip_size=Decimal("0.0001"),
                    commission=Decimal("3"),
                    commission_type="UsdPerMillionUsdVolume",
                ),
                c_end - timedelta(minutes=25),
                basis="live_poll",
            )
            for idx, m_ago in enumerate((16, 15, 5, 1, 0), start=1):
                qts = c_end - timedelta(minutes=m_ago)
                cold_now[0] = qts
                _record_quote(
                    cold_store,
                    session_id=cold_sid,
                    seq=idx,
                    ts=qts,
                    mid=Decimal("1.10000"),
                )
            cold_snap = build_market_snapshot(cold_store, c_end)
            assert cold_snap.price_trajectory is not None
            assert cold_snap.price_trajectory.change_60m_pips is None
            assert cold_snap.price_trajectory.change_240m_pips is None
            assert cold_snap.price_trajectory.direction_240m == "missing"
            assert cold_snap.price_trajectory.short_window_coverage_status == "complete"
            assert cold_snap.price_trajectory.fx_session == "london"
        finally:
            cold_store.close()
    finally:
        qstore.close()


def test_risk_policy_v5_session_chase_and_4h_guardrails() -> None:
    policy = RiskPolicy.pepperstone_razor_200_guarded()

    def _assess_at(
        ts: datetime,
        *,
        weight: Decimal,
        c5m: Decimal = Decimal("0.0"),
        c240m: Decimal | None = Decimal("0.0"),
        held_qty: Decimal = Decimal("0"),
    ):
        mkt = _live_market(ts, mid=Decimal("1.10000"), c5m=c5m, c240m=c240m)
        positions = () if held_qty == 0 else ()
        if held_qty != 0:
            from eventlens.contracts import Position

            positions = (Position(instrument="EUR/USD", quantity=held_qty),)
        port = PortfolioSnapshot(
            snapshot_id="port-test",
            as_of=ts,
            base_currency="USD",
            equity=Decimal("200"),
            positions=positions,
        )
        action = "CLOSE" if weight == 0 else "SET_TARGET"
        dec = AgentDecision(
            decision_id=str(uuid4()),
            event_id="ev-test",
            based_on_memory_version=0,
            based_on_snapshot_id="port-test",
            provider="gemini",
            model_version="gemini-4-hydrogen",
            prompt_version="single-pass-agent-v5.0",
            started_at=ts - timedelta(milliseconds=10),
            completed_at=ts,
            valid_until=ts + timedelta(seconds=60),
            portfolio_action=action,  # type: ignore[arg-type]
            target=TargetPortfolio(
                target_id=str(uuid4()),
                based_on_snapshot_id="port-test",
                positions=(TargetPosition(instrument="EUR/USD", weight=weight),),
                rationale="v5 check",
            ),
            rationale="v5 check",
        )
        engine = DeterministicHardRiskEngine(policy=policy, clock=lambda: ts)
        return asyncio.run(engine.assess(dec, port, mkt))

    london_ts = datetime(2026, 11, 25, 9, 0, 0, tzinfo=UTC)
    overlap_ts = datetime(2026, 11, 25, 14, 30, 0, tzinfo=UTC)
    asian_ts = datetime(2026, 11, 25, 2, 0, 0, tzinfo=UTC)
    late_ny_ts = datetime(2026, 11, 25, 18, 0, 0, tzinfo=UTC)

    # 1. Session gating for new entries vs 24x5 CLOSE protection
    assert _assess_at(london_ts, weight=Decimal("0.15")).outcome == "approve"
    assert _assess_at(overlap_ts, weight=Decimal("-0.15")).outcome == "approve"
    res_asian = _assess_at(asian_ts, weight=Decimal("0.15"))
    assert res_asian.outcome == "reject"
    assert "entry_blocked_outside_allowed_session:asian" in res_asian.reasons
    res_lny = _assess_at(late_ny_ts, weight=Decimal("-0.15"))
    assert res_lny.outcome == "reject"
    assert "entry_blocked_outside_allowed_session:late_ny" in res_lny.reasons
    # CLOSE MUST ALWAYS pass even in Asian and Late NY
    assert _assess_at(asian_ts, weight=Decimal("0"), held_qty=Decimal("2000")).outcome == "approve"
    assert (
        _assess_at(late_ny_ts, weight=Decimal("0"), held_qty=Decimal("2000")).outcome == "approve"
    )

    # 2. Same-direction 5m chase > 4.0p blocked, opposite-direction pullback allowed
    res_chase_long = _assess_at(overlap_ts, weight=Decimal("0.15"), c5m=Decimal("4.5"))
    assert res_chase_long.outcome == "reject"
    assert "chase_5m_move_exceeds_limit" in res_chase_long.reasons
    res_chase_short = _assess_at(overlap_ts, weight=Decimal("-0.15"), c5m=Decimal("-4.5"))
    assert res_chase_short.outcome == "reject"
    assert "chase_5m_move_exceeds_limit" in res_chase_short.reasons
    # Opposite-direction +4.5p rebound before short is a BETTER fill -> approved!
    assert _assess_at(overlap_ts, weight=Decimal("-0.15"), c5m=Decimal("4.5")).outcome == "approve"

    # 3. Extreme 4h trend >= 30p counter-trend blocked; moderate 25.9p, with-trend, and cold-start allowed
    res_ext_long = _assess_at(overlap_ts, weight=Decimal("0.15"), c240m=Decimal("-34.6"))
    assert res_ext_long.outcome == "reject"
    assert "extreme_htf_4h_countertrend_blocked" in res_ext_long.reasons
    res_ext_short = _assess_at(overlap_ts, weight=Decimal("-0.15"), c240m=Decimal("31.0"))
    assert res_ext_short.outcome == "reject"
    assert "extreme_htf_4h_countertrend_blocked" in res_ext_short.reasons
    assert (
        _assess_at(overlap_ts, weight=Decimal("-0.15"), c240m=Decimal("25.9")).outcome == "approve"
    )
    assert (
        _assess_at(overlap_ts, weight=Decimal("-0.15"), c240m=Decimal("-34.6")).outcome == "approve"
    )
    assert _assess_at(overlap_ts, weight=Decimal("0.15"), c240m=None).outcome == "approve"


def test_runtime_pre_llm_session_short_circuit_flat_vs_positioned(tmp_path: Path) -> None:
    class _SpyClient:
        provider = "gemini"
        model = "gemini-4-hydrogen"

        def __init__(self) -> None:
            self.calls = 0

        async def complete_json(self, *, system: str, user: str) -> ModelResponse:
            del system, user
            self.calls += 1
            return ModelResponse(
                content=json.dumps(
                    {
                        "portfolio_action": "CLOSE",
                        "target_direction": "flat",
                        "target_weight": "0",
                        "strategy_hypothesis": "fundamental_breakout",
                        "market_bias": "neutral",
                        "market_state_note": "Reversal confirmed; flatten open EUR/USD position.",
                        "watch_condition": None,
                        "rationale": "基本面反转，立即平仓保护净值。",
                    },
                    ensure_ascii=False,
                ),
                input_tokens=100,
                output_tokens=40,
                provider="gemini",
                model_id="gemini-4-hydrogen",
            )

    asian_ts = datetime(2026, 11, 25, 2, 0, 0, tzinfo=UTC)
    london_ts = datetime(2026, 11, 25, 9, 0, 0, tzinfo=UTC)
    now_box = [asian_ts]

    def clock() -> datetime:
        now_box[0] = now_box[0] + timedelta(milliseconds=1)
        return now_box[0]

    risk_pol = RiskPolicy.pepperstone_razor_200_guarded()
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200()
    mem = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=clock, allow_targets=True)
    shd = SQLiteShadowLedger(
        tmp_path / "shd.sqlite",
        initial_cash_usd=Decimal("200.00"),
        risk_policy=risk_pol,
        exit_policy=exit_pol,
        cost_model=ShadowCostModel.pepperstone_razor(),
        clock=clock,
    )
    spy = _SpyClient()
    try:
        agent = SinglePassTradingAgent(
            spy,
            allow_commission_bearing_zero_spread=True,
            max_position_weight=risk_pol.max_position_weight,
            leverage=risk_pol.leverage,
            max_risk_per_trade_pct=risk_pol.max_risk_per_trade_pct,
            ex_ante_stop_loss_pips=risk_pol.ex_ante_stop_loss_pips,
            clock=clock,
        )
        rt = IntradayEventRuntime(
            memory=mem,
            portfolio_state=shd,
            market_provider=lambda ts: _live_market(ts, mid=Decimal("1.10000")),
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(policy=risk_pol, clock=clock),
            shadow_executor=shd,
            clock=clock,
        )

        # 1. Flat in Asian -> pre-LLM short-circuit skips LLM cleanly!
        ev_asian = Event(
            event_id="ev-asian-flat",
            source="investinglive_fx",
            source_type="news",
            source_reference="https://example.com/1",
            source_timestamp=asian_ts - timedelta(seconds=2),
            received_timestamp=asian_ts,
            raw_text="US flash news arriving during Asian session while flat.",
        )
        res_flat = asyncio.run(rt.run_cycle(ev_asian))
        assert res_flat.status == "filtered_out"
        assert res_flat.reason == "outside_allowed_entry_session:asian"
        assert spy.calls == 0

        # 2. Open a London position via ledger, then enter Asian -> LLM IS called and CLOSE succeeds!
        now_box[0] = london_ts
        snap_lon = shd.snapshot_sync(market=_live_market(london_ts, mid=Decimal("1.10000")))
        open_dec = AgentDecision(
            decision_id="dec-open-lon",
            event_id="ev-open-lon",
            based_on_memory_version=0,
            based_on_snapshot_id=snap_lon.snapshot_id,
            provider="gemini",
            model_version="gemini-4-hydrogen",
            prompt_version="single-pass-agent-v5.0",
            started_at=london_ts,
            completed_at=london_ts,
            valid_until=london_ts + timedelta(seconds=60),
            portfolio_action="SET_TARGET",
            target=TargetPortfolio(
                target_id="tgt-open-lon",
                based_on_snapshot_id=snap_lon.snapshot_id,
                positions=(TargetPosition(instrument="EUR/USD", weight=Decimal("0.15")),),
                rationale="London entry",
            ),
            rationale="London entry",
        )
        assess_open = asyncio.run(
            rt.risk_engine.assess(
                open_dec, snap_lon, _live_market(london_ts, mid=Decimal("1.10000"))
            )
        )
        assert assess_open.outcome == "approve"
        shd.record_sync(open_dec, assess_open)
        now_box[0] = london_ts + timedelta(seconds=1)
        assert len(shd.fill_pending(_live_market(now_box[0], mid=Decimal("1.10000")))) == 1

        now_box[0] = datetime(2026, 11, 26, 2, 0, 0, tzinfo=UTC)  # Asian session while positioned
        ev_close = Event(
            event_id="ev-asian-close",
            source="fed_news",
            source_type="news",
            source_reference="https://example.com/2",
            source_timestamp=now_box[0] - timedelta(seconds=2),
            received_timestamp=now_box[0],
            raw_text="Emergency Fed statement reverses EUR/USD thesis while positioned.",
        )
        res_close = asyncio.run(rt.run_cycle(ev_close))
        assert spy.calls == 1
        assert res_close.status == "shadow_recorded"
        assert res_close.decision is not None
        assert res_close.decision.portfolio_action == "CLOSE"
    finally:
        shd.close()


def test_t2_mid_swing_protective_exit_policy_defaults(tmp_path: Path) -> None:
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200()
    assert exit_pol.stop_loss_pips == Decimal("14.0")
    assert exit_pol.take_profit_pips == Decimal("24.0")
    assert exit_pol.trailing_activation_pips == Decimal("10.0")
    assert exit_pol.trailing_distance_pips == Decimal("6.0")
    assert exit_pol.max_holding_seconds == 14400

    ts = datetime(2026, 11, 25, 13, 30, 0, tzinfo=UTC)
    now_box = [ts]
    risk_pol = RiskPolicy.pepperstone_razor_200_guarded()
    shd = SQLiteShadowLedger(
        tmp_path / "t2.sqlite",
        initial_cash_usd=Decimal("200.00"),
        risk_policy=risk_pol,
        exit_policy=exit_pol,
        cost_model=ShadowCostModel.pepperstone_razor(),
        clock=lambda: now_box[0],
    )
    try:
        mkt = _live_market(ts, mid=Decimal("1.10000"))
        snap = shd.snapshot_sync(market=mkt)
        dec = AgentDecision(
            decision_id="dec-t2",
            event_id="ev-t2",
            based_on_memory_version=0,
            based_on_snapshot_id=snap.snapshot_id,
            provider="gemini",
            model_version="gemini-4-hydrogen",
            prompt_version="single-pass-agent-v5.0",
            started_at=ts,
            completed_at=ts,
            valid_until=ts + timedelta(seconds=60),
            portfolio_action="SET_TARGET",
            target=TargetPortfolio(
                target_id="tgt-t2",
                based_on_snapshot_id=snap.snapshot_id,
                positions=(TargetPosition(instrument="EUR/USD", weight=Decimal("0.20")),),
                rationale="Long T2 test",
            ),
            rationale="Long T2 test",
        )
        assess = asyncio.run(
            DeterministicHardRiskEngine(policy=risk_pol, clock=lambda: ts).assess(dec, snap, mkt)
        )
        assert assess.outcome == "approve"
        shd.record_sync(dec, assess)
        now_box[0] = ts + timedelta(seconds=1)
        fills = shd.fill_pending(_live_market(now_box[0], mid=Decimal("1.10000")))
        assert len(fills) == 1

        # +9.0p (< 10.0p activation) followed by 6.5p pullback MUST NOT trigger trailing profit lock
        now_box[0] += timedelta(minutes=5)
        assert shd.evaluate_protective_exits(_live_market(now_box[0], mid=Decimal("1.10090"))) == ()
        now_box[0] += timedelta(minutes=1)
        assert shd.evaluate_protective_exits(_live_market(now_box[0], mid=Decimal("1.10025"))) == ()

        # +11.0p (>= 10.0p) activates trailing lock (exit bid = 1.10109); 6.2p pullback locks >= +4.0p profit
        now_box[0] += timedelta(minutes=5)
        assert shd.evaluate_protective_exits(_live_market(now_box[0], mid=Decimal("1.10110"))) == ()
        assert shd.report()["peak_favorable_mark"] == "1.10109"
        now_box[0] += timedelta(minutes=2)
        exits = shd.evaluate_protective_exits(_live_market(now_box[0], mid=Decimal("1.10048")))
        assert len(exits) == 1
        assert exits[0].reason == "protective_trailing_stop"
        assert exits[0].realized_pnl_usd > Decimal("0")
    finally:
        shd.close()


def test_market_state_watch_daily_reset_and_flat_breakout_suppression(tmp_path: Path) -> None:
    class _WatchClient:
        provider = "gemini"
        model = "gemini-4-hydrogen"

        def __init__(self) -> None:
            self.calls = 0

        async def complete_json(self, *, system: str, user: str) -> ModelResponse:
            del system, user
            self.calls += 1
            if self.calls == 1:
                payload = {
                    "portfolio_action": "HOLD",
                    "target_direction": "flat",
                    "target_weight": "0",
                    "strategy_hypothesis": "fundamental_breakout",
                    "market_bias": "bearish_eur",
                    "market_state_note": "Wait pullback >= 1.10100 to short.",
                    "watch_condition": "EUR/USD >= 1.10100",
                    "rationale": "等待反抽 1.10100 后做空。",
                }
            elif self.calls == 2:
                # Re-arm watch condition at 1.10200 after first Day 1 watch review
                payload = {
                    "portfolio_action": "HOLD",
                    "target_direction": "flat",
                    "target_weight": "0",
                    "strategy_hypothesis": "fundamental_breakout",
                    "market_bias": "bearish_eur",
                    "market_state_note": "Wait pullback >= 1.10200 to short.",
                    "watch_condition": "EUR/USD >= 1.10200",
                    "rationale": "继续等待 1.10200 反抽做空。",
                }
            elif self.calls == 3:
                payload = {
                    "portfolio_action": "SET_TARGET",
                    "target_direction": "short",
                    "target_weight": "-0.15",
                    "strategy_hypothesis": "fundamental_breakout",
                    "market_bias": "bearish_eur",
                    "market_state_note": "Triggered 1.10200 short entry on Day 2.",
                    "watch_condition": None,
                    "rationale": "次日欧盘触及 1.10200 顺势空单入场。",
                }
            else:
                payload = {
                    "portfolio_action": "HOLD",
                    "target_direction": "flat",
                    "target_weight": "0",
                    "strategy_hypothesis": "fundamental_breakout",
                    "market_bias": "bearish_eur",
                    "market_state_note": "Monitor open short for 1.12000 invalidation.",
                    "watch_condition": "EUR/USD >= 1.12000",
                    "rationale": "持仓观察 1.12000 止损参考。",
                }
            return ModelResponse(
                content=json.dumps(payload, ensure_ascii=False),
                input_tokens=150,
                output_tokens=60,
                provider="gemini",
                model_id="gemini-4-hydrogen",
            )

    day1_london = datetime(
        2026, 11, 24, 15, 0, 0, tzinfo=UTC
    )  # overlap before 22:00 UTC NY rollover
    now_box = [day1_london]
    mid_box = [Decimal("1.10000")]

    def clock() -> datetime:
        now_box[0] += timedelta(milliseconds=1)
        return now_box[0]

    risk_pol = RiskPolicy.pepperstone_razor_200_guarded()
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200()
    mem = SQLiteThesisMemory(tmp_path / "m.sqlite", clock=clock, allow_targets=True)
    store = SQLiteRuntimeStore(tmp_path / "s.sqlite")
    shd = SQLiteShadowLedger(
        tmp_path / "s.sqlite",
        initial_cash_usd=Decimal("200.00"),
        risk_policy=risk_pol,
        exit_policy=exit_pol,
        cost_model=ShadowCostModel.pepperstone_razor(),
        clock=clock,
    )
    watch_client = _WatchClient()
    try:
        agent = SinglePassTradingAgent(
            watch_client,
            allow_commission_bearing_zero_spread=True,
            max_position_weight=risk_pol.max_position_weight,
            leverage=risk_pol.leverage,
            max_risk_per_trade_pct=risk_pol.max_risk_per_trade_pct,
            ex_ante_stop_loss_pips=risk_pol.ex_ante_stop_loss_pips,
            clock=clock,
        )
        rt = IntradayEventRuntime(
            memory=mem,
            portfolio_state=shd,
            market_provider=lambda ts: _live_market(ts, mid=mid_box[0]),
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(policy=risk_pol, clock=clock),
            shadow_executor=shd,
            state_store=store,
            clock=clock,
        )
        runner = ShadowRunner(
            runtime=rt,
            ledger=shd,
            poll_news=False,
            strict_window_coverage=False,
            exit_buffer_seconds=0.0,
            review_policy=ReviewPolicy(
                enabled=True,
                enable_review_at=False,
                breakout_excursion_pips=Decimal("8"),
                cooldown_seconds=60.0,
                max_reviews_per_thesis=1,
                max_total_review_calls=20,
                breakout_only_when_positioned=True,
            ),
            clock=clock,
        )
        runner._run_duration_seconds = None  # continuous 24x5 mode

        # Event 1 arms watch_condition "EUR/USD >= 1.10100" on market-state-watch
        ev1 = Event(
            event_id="ev-watch-1",
            source="investinglive_fx",
            source_type="news",
            source_reference="https://example.com/watch1",
            source_timestamp=day1_london - timedelta(seconds=2),
            received_timestamp=day1_london,
            raw_text="US data beat consensus; wait pullback to short EUR/USD.",
        )
        assert asyncio.run(rt.run_cycle(ev1)).status == "no_target"

        # When FLAT, +9.0p excursion (>= 8p) without hitting 1.10100 MUST NOT trigger breakout review!
        now_box[0] = day1_london + timedelta(minutes=5)
        mid_box[0] = Decimal("1.10090")
        assert asyncio.run(runner.step_reviews()) == ()

        # Hit 1.10100 on Day 1 -> fires 1st review (re-arms 1.10200), hitting Day 1 max_reviews_per_thesis=1
        now_box[0] = day1_london + timedelta(minutes=10)
        mid_box[0] = Decimal("1.10110")
        revs_d1 = asyncio.run(runner.step_reviews())
        assert len(revs_d1) == 1
        assert watch_client.calls == 2

        # Same Day 1 later when 1.10200 hits -> capped at max_reviews_per_thesis=1 for Day 1
        now_box[0] = day1_london + timedelta(minutes=15)
        mid_box[0] = Decimal("1.10210")
        assert asyncio.run(runner.step_reviews()) == ()

        # Even on Day 2 Asian session, flat watch_condition MUST NOT fire outside london/overlap!
        now_box[0] = datetime(2026, 11, 25, 2, 0, 0, tzinfo=UTC)
        assert asyncio.run(runner.step_reviews()) == ()

        # Advance to Day 2 London (09:00 UTC) -> daily thesis cap resets across NY rollover & fires!
        now_box[0] = datetime(2026, 11, 25, 9, 0, 0, tzinfo=UTC)
        revs_d2 = asyncio.run(runner.step_reviews())
        assert len(revs_d2) == 1
        assert revs_d2[0].status == "shadow_recorded"
        assert watch_client.calls == 3

        # Fill short position, arm an active card via minor news, then move +8.5p from ref_mid -> breakout DOES fire while positioned!
        now_box[0] += timedelta(seconds=1)
        _, open_fills = asyncio.run(runner.step_quotes_and_fills())
        assert len(open_fills) == 1

        now_box[0] += timedelta(minutes=2)
        ev_pos = Event(
            event_id="ev-while-pos",
            source="investinglive_fx",
            source_type="news",
            source_reference="https://example.com/pos",
            source_timestamp=now_box[0] - timedelta(seconds=2),
            received_timestamp=now_box[0],
            raw_text="Minor follow-up while positioned.",
        )
        assert asyncio.run(rt.run_cycle(ev_pos)).status == "no_target"

        # Establish ref_mid while positioned at 1.10210 without crossing 8p or 1.12000
        runner.review_policy = ReviewPolicy(
            enabled=True,
            enable_review_at=False,
            breakout_excursion_pips=Decimal("8"),
            cooldown_seconds=60.0,
            max_reviews_per_thesis=5,
            max_total_review_calls=20,
            breakout_only_when_positioned=True,
        )
        assert asyncio.run(runner.step_reviews()) == ()

        # Move -8.5p (to 1.10125) while holding open short -> breakout review FIRES because positioned!
        now_box[0] += timedelta(minutes=2)
        mid_box[0] = Decimal("1.10125")
        revs_pos = asyncio.run(runner.step_reviews())
        assert len(revs_pos) == 1
        assert store.count_reviews(thesis_id="market-state-watch") == 3
    finally:
        shd.close()
        store.close()
