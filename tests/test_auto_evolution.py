"""Tests for Auto-Evolution Engine, anti-turtle reward penalties, hot-reload, and dual rollback."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from eventlens.contracts import Event
from eventlens.dashboard import export_dashboard_snapshot
from eventlens.deepseek import ModelResponse
from eventlens.evolution import (
    LiveStrategyReloader,
    SQLiteEvolutionStore,
    StrategyProfile,
    run_evolution_tick,
    save_active_strategy_atomic,
)
from eventlens.quotes import QuoteStore
from eventlens.runtime import IntradayEventRuntime, SQLiteRuntimeStore
from eventlens.shadow_cli import main as shadow_main
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import SinglePassTradingAgent
from eventlens.trading.execution import ProtectiveExitPolicy, ShadowCostModel, SQLiteShadowLedger
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy


def _make_quotes_db_with_move(
    path: Path,
    *,
    start: datetime,
    bars_count: int = 360,
    move_start_min: int = 60,
    move_pips_total: float = 28.0,
) -> Path:
    from uuid import uuid4

    from eventlens.sources.ctrader_demo import BridgeQuote, BridgeStart

    store = QuoteStore(path, clock=lambda: start)
    sid = uuid4()
    try:
        store.start(
            BridgeStart(
                version=1,
                kind="start",
                session_id=sid,
                account_mode="demo",
                running_mode="realtime",
                symbol="EURUSD",
                started_at=start,
                sample_interval_ms=500,
                duration_seconds=max(36000, bars_count * 60 + 300),
                digits=5,
                pip_size=Decimal("0.0001"),
                commission=Decimal("3"),
                commission_type="UsdPerOneLot",
            ),
            start,
            basis="live_poll",
        )
        mid = 1.10000
        for m in range(bars_count):
            ts = start + timedelta(minutes=m)
            store.clock = lambda t=ts: t
            if move_start_min <= m < move_start_min + 40:
                mid += (move_pips_total / 40.0) * 0.0001
            elif m >= move_start_min + 40:
                mid += 0.00001
            bid = Decimal(f"{mid - 0.00001:.5f}")
            ask = Decimal(f"{mid + 0.00001:.5f}")
            bq = BridgeQuote(
                version=1,
                kind="quote",
                session_id=sid,
                sequence=m + 1,
                symbol="EURUSD",
                source_at=ts,
                bridge_at=ts,
                bid=bid,
                ask=ask,
            )
            store.record(bq, received=ts, raw_line=bq.model_dump_json())
    finally:
        store.close()
    return path


def _seed_runtime_event(
    shadow_db: Path,
    *,
    event_id: str,
    ts: datetime,
    headline: str,
    action: str = "HOLD",
    direction: str = "flat",
    state: str = "no_target",
    reason: str = "no_target",
    prompt_version: str = "single-pass-agent-v5.0",
    assessment_json: str | None = None,
) -> None:
    store = SQLiteRuntimeStore(shadow_db)
    try:
        dec_obj = {
            "decision_id": f"dec-{event_id}",
            "event_id": event_id,
            "based_on_memory_version": 0,
            "based_on_snapshot_id": "snap-1",
            "provider": "gemini",
            "model_version": "gemini-4-hydrogen",
            "prompt_version": prompt_version,
            "started_at": ts.isoformat(),
            "completed_at": ts.isoformat(),
            "valid_until": (ts + timedelta(minutes=2)).isoformat(),
            "portfolio_action": action,
            "target": (
                {
                    "target_id": f"tgt-{event_id}",
                    "based_on_snapshot_id": "snap-1",
                    "positions": [
                        {
                            "instrument": "EUR/USD",
                            "weight": "0.2" if direction == "long" else "-0.2",
                            "thesis_ids": [],
                        }
                    ],
                    "rationale": "Test entry",
                }
                if action == "SET_TARGET" and direction in ("long", "short")
                else None
            ),
            "rationale": f"Evaluated {headline}",
        }
        store.upsert_event_state(
            event_id=event_id,
            source_reference=f"https://example.com/{event_id}",
            received_at=ts,
            now=ts,
            state=state,
            reason=reason,
            context_json=json.dumps(
                {
                    "event": {
                        "event_id": event_id,
                        "source": "investinglive_fx",
                        "source_type": "news",
                        "source_reference": f"https://example.com/{event_id}",
                        "source_timestamp": ts.isoformat(),
                        "received_timestamp": ts.isoformat(),
                        "raw_text": headline,
                    },
                    "memory": {"memory_version": 0, "as_of": ts.isoformat(), "theses": []},
                    "portfolio": {
                        "snapshot_id": "pf-1",
                        "as_of": ts.isoformat(),
                        "base_currency": "USD",
                        "equity": "165.00",
                    },
                    "market": {
                        "snapshot_id": "mk-1",
                        "as_of": ts.isoformat(),
                        "quotes": [
                            {
                                "instrument": "EUR/USD",
                                "source_at": ts.isoformat(),
                                "bridge_at": ts.isoformat(),
                                "received_at": ts.isoformat(),
                                "available_at": ts.isoformat(),
                                "source_provider": "ctrader_algo",
                                "basis": "live_poll",
                                "bid": "1.10000",
                                "ask": "1.10002",
                                "commission": "3",
                                "commission_type": "UsdPerOneLot",
                            }
                        ],
                    },
                    "as_of": ts.isoformat(),
                }
            ),
            decision_id=f"dec-{event_id}",
            decision_json=json.dumps(dec_obj),
            assessment_json=assessment_json,
        )
    finally:
        store.close()


class _FakeArenaLLM:
    provider = "gemini"
    model = "gemini-4-hydrogen"

    def __init__(self, candidates_payload: dict[str, Any] | None = None) -> None:
        self.candidates_payload = candidates_payload or {"candidates": []}
        self.calls: list[dict[str, str]] = []

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls.append({"system": system, "user": user})
        if "Meta-Optimizer" in system:
            return ModelResponse(
                provider="gemini",
                model_id="gemini-4-hydrogen",
                content=json.dumps(self.candidates_payload, ensure_ascii=False),
                input_tokens=100,
                output_tokens=80,
            )
        # Head-to-head replay agent mock:
        # Incumbent v5.0 misses if no dynamic rule or chase threshold <= 4.0,
        # while promoted candidate with chase >= 5.0 OR rule takes long on EUR CPI surprise.
        is_improved = ">= 5p" in system or "严禁缩头漏单" in system
        act = "SET_TARGET" if (is_improved and "ECB" in user) else "HOLD"
        payload = {
            "portfolio_action": act,
            "target_direction": "long" if act == "SET_TARGET" else "flat",
            "target_weight": "0.2" if act == "SET_TARGET" else "0",
            "strategy_hypothesis": "fundamental_breakout",
            "market_bias": "bullish_eur" if act == "SET_TARGET" else "neutral",
            "market_state_note": "Replay test",
            "watch_condition": None,
            "rationale": "Replay evaluation decision.",
        }
        return ModelResponse(
            provider="gemini",
            model_id="gemini-4-hydrogen",
            content=json.dumps(payload, ensure_ascii=False),
            input_tokens=80,
            output_tokens=40,
        )


def test_strategy_profile_bounds_and_prompt_rendering() -> None:
    now = datetime(2026, 11, 27, 12, 0, tzinfo=UTC)
    base = StrategyProfile.baseline_v5(now=now)
    rendered = base.render_system_prompt()
    assert "structured_price_condition" in rendered
    assert "price_breakout_excursion" in rendered
    assert "change_240m_pips <= -30.0" in rendered

    # Invalid extreme SL / chase / >6 rules / bad R:R rejected
    with pytest.raises(ValueError):
        StrategyProfile(
            version_id="v5.9",
            created_at=now,
            summary_zh="Bad SL",
            stop_loss_pips=Decimal("25.0"),
        )
    with pytest.raises(ValueError):
        StrategyProfile(
            version_id="v5.9",
            created_at=now,
            summary_zh="Bad chase",
            max_same_direction_chase_5m_pips=Decimal("10.0"),
        )
    with pytest.raises(ValueError):
        StrategyProfile(
            version_id="v5.9",
            created_at=now,
            summary_zh="Bad R:R",
            stop_loss_pips=Decimal("18.0"),
            take_profit_pips=Decimal("18.0"),
        )
    with pytest.raises(ValueError):
        StrategyProfile(
            version_id="v5.9",
            created_at=now,
            summary_zh="Too many rules",
            dynamic_prompt_rules=(
                "Rule 1 valid",
                "Rule 2 valid",
                "Rule 3 valid",
                "Rule 4 valid",
                "Rule 5 valid",
                "Rule 6 valid",
                "Rule 7 extra",
            ),
        )

    tuned = StrategyProfile(
        version_id="v5.1",
        parent_version_id="v5.0",
        created_at=now,
        status="promoted",
        summary_zh="适度放宽 5m 追单门禁避免真突破踏空",
        dynamic_prompt_rules=("欧盘一级 CPI 超预期果断顺势做多",),
        max_same_direction_chase_5m_pips=Decimal("5.5"),
        prompt_chase_5m_pips=Decimal("5.0"),
        max_countertrend_htf_4h_pips=Decimal("35.0"),
        stop_loss_pips=Decimal("12.0"),
        take_profit_pips=Decimal("24.0"),
        trailing_activation_pips=Decimal("10.0"),
        trailing_distance_pips=Decimal("6.0"),
        max_holding_seconds=10800,
    )
    tuned_prompt = tuned.render_system_prompt()
    assert "structured_price_condition" in tuned_prompt
    assert "price_breakout_excursion" in tuned_prompt
    assert "change_5m_pips >= 5p" in tuned_prompt
    assert "35.0" in tuned_prompt
    assert "欧盘一级 CPI 超预期果断顺势做多" in tuned_prompt


def test_dual_trigger_rollback_on_consecutive_losses_and_anti_turtle_zero_trade_floor(
    tmp_path: Path,
) -> None:
    base_time = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)  # london session
    evo_db = tmp_path / "evolution.sqlite"
    active_json = tmp_path / "active_strategy.json"
    shd_db = tmp_path / "shadow-ledger.sqlite"
    q_db = _make_quotes_db_with_move(
        tmp_path / "live-quotes.sqlite",
        start=base_time - timedelta(hours=1),
        bars_count=420,
        move_start_min=60,
        move_pips_total=28.0,
    )

    store = SQLiteEvolutionStore(evo_db)
    base, _ = store.ensure_baseline(base_time)
    cand = StrategyProfile(
        version_id="cand-1",
        parent_version_id="v5.0",
        created_at=base_time + timedelta(minutes=1),
        status="promoted",
        summary_zh="Candidate prone to turtle/loss",
        dynamic_prompt_rules=("仅最高级别事件才开仓",),
    )
    v51 = store.promote_candidate(
        cand,
        now=base_time + timedelta(minutes=1),
        incumbent_score=0.5,
        candidate_score=1.2,
        capture_rate=0.8,
    )
    save_active_strategy_atomic(active_json, v51)
    assert v51.version_id == "v5.1"

    # Case A: 0 real trades, but matured missed T2/T3 move after v5.1 promotion gives <= -1.5 reward -> auto rollback!
    _seed_runtime_event(
        shd_db,
        event_id="ev-miss-t3",
        ts=base_time + timedelta(minutes=2),
        headline="ECB hawkish surprise triggers rally",
        action="HOLD",
        direction="flat",
    )
    store.close()
    res_a = asyncio.run(
        run_evolution_tick(
            evolution_db=evo_db,
            active_strategy_path=active_json,
            shadow_db=shd_db,
            quotes_db=q_db,
            maturity_seconds=14400,
            clock=lambda: base_time + timedelta(hours=6),
        )
    )
    assert res_a["outcome"] == "rolled_back"
    assert res_a["active_version_before"] == "v5.1"
    assert res_a["active_version_after"] == "v5.0"
    assert res_a["consecutive_losses"] == 0
    assert res_a["version_reward_score"] <= -1.5
    assert json.loads(active_json.read_text(encoding="utf-8"))["version_id"] == "v5.0"

    # Verify v5.1 hash is blacklisted
    store = SQLiteEvolutionStore(evo_db)
    assert v51.profile_hash() in store.blacklisted_hashes()

    # Case B: Promote v5.2, record 2 consecutive losing closed Demo trades -> rollback!
    cand2 = StrategyProfile(
        version_id="cand-2",
        parent_version_id="v5.0",
        created_at=base_time + timedelta(hours=7),
        status="promoted",
        summary_zh="Over-aggressive candidate",
        stop_loss_pips=Decimal("14.0"),
        take_profit_pips=Decimal("28.0"),
        dynamic_prompt_rules=("积极测试入场",),
    )
    v52 = store.promote_candidate(
        cand2,
        now=base_time + timedelta(hours=7),
        incumbent_score=0.2,
        candidate_score=1.0,
        capture_rate=0.8,
    )
    save_active_strategy_atomic(active_json, v52)
    store.close()

    broker_db = tmp_path / "cli-demo-broker.sqlite"
    bconn = sqlite3.connect(broker_db)
    bconn.executescript(
        """
        CREATE TABLE demo_broker_events (event_rowid INTEGER PRIMARY KEY, bridge_session_id TEXT, event_seq INTEGER, kind TEXT, raw_json TEXT NOT NULL DEFAULT '{}');
        INSERT INTO demo_broker_events VALUES (1, 's1', 1, 'snapshot', '{}');
        CREATE TABLE demo_broker_positions (
            broker_position_id INTEGER PRIMARY KEY, client_intent_id TEXT, side TEXT, status TEXT,
            volume_units INTEGER, entry_price TEXT, stop_loss TEXT, take_profit TEXT,
            opened_at TEXT, open_commission_usd TEXT
        );
        CREATE TABLE demo_broker_close_deals (
            closing_deal_id INTEGER PRIMARY KEY, broker_position_id INTEGER, client_intent_id TEXT,
            closing_price TEXT, closing_time TEXT, gross_profit_usd TEXT, net_profit_usd TEXT,
            commissions_usd TEXT, swap_usd TEXT
        );
        """
    )
    t_loss1 = (base_time + timedelta(hours=8)).isoformat()
    t_loss2 = (base_time + timedelta(hours=9)).isoformat()
    bconn.execute(
        "INSERT INTO demo_broker_positions VALUES (101, NULL, 'sell', 'closed', 2000, '1.10000', '1.10140', '1.09720', ?, '-0.06')",
        (t_loss1,),
    )
    bconn.execute(
        "INSERT INTO demo_broker_close_deals VALUES (201, 101, NULL, '1.10140', ?, '-2.80', '-2.92', '-0.12', '0')",
        (t_loss1,),
    )
    bconn.execute(
        "INSERT INTO demo_broker_positions VALUES (102, NULL, 'sell', 'closed', 2000, '1.10050', '1.10190', '1.09770', ?, '-0.06')",
        (t_loss2,),
    )
    bconn.execute(
        "INSERT INTO demo_broker_close_deals VALUES (202, 102, NULL, '1.10190', ?, '-2.80', '-2.92', '-0.12', '0')",
        (t_loss2,),
    )
    bconn.commit()
    bconn.close()

    res_b = asyncio.run(
        run_evolution_tick(
            evolution_db=evo_db,
            active_strategy_path=active_json,
            shadow_db=shd_db,
            quotes_db=q_db,
            cli_demo_db=broker_db,
            maturity_seconds=14400,
            clock=lambda: base_time + timedelta(hours=10),
        )
    )
    assert res_b["outcome"] == "rolled_back"
    assert res_b["active_version_before"] == "v5.2"
    assert res_b["active_version_after"] == "v5.0"
    assert res_b["consecutive_losses"] == 2
    assert json.loads(active_json.read_text(encoding="utf-8"))["version_id"] == "v5.0"


def test_six_cluster_arena_vetoes_turtle_and_promotes_winner(tmp_path: Path) -> None:
    start = datetime(2026, 11, 18, 9, 0, tzinfo=UTC)  # london
    # Build 20 hours of M1 quotes with bullish climbs after ECB headline clusters
    q_db = _make_quotes_db_with_move(
        tmp_path / "live-quotes.sqlite",
        start=start,
        bars_count=1200,
        move_start_min=380,  # ~15:20 overlap
        move_pips_total=30.0,
    )
    shd_db = tmp_path / "shadow-ledger.sqlite"
    # Seed 6 separate matured clusters (>300s apart) inside london/overlap;
    # cluster #6 sits right before the +30p climb so incumbent HOLD loses heavily
    for idx in range(5):
        _seed_runtime_event(
            shd_db,
            event_id=f"ev-c-{idx}",
            ts=start + timedelta(minutes=20 * idx + 15),
            headline=f"Minor macro wire #{idx}",
            action="HOLD",
            direction="flat",
        )
    _seed_runtime_event(
        shd_db,
        event_id="ev-c-ecb-miss",
        ts=start + timedelta(minutes=380),
        headline="ECB surprise hawkish pivot lifts Eurozone yields",
        action="HOLD",
        direction="flat",
    )

    fake_llm = _FakeArenaLLM(
        candidates_payload={
            "candidates": [
                {
                    "summary_zh": "缩头乌龟候选（会被捕获率门禁淘汰）",
                    "dynamic_prompt_rules": ["观望为主不要出手"],
                    "max_same_direction_chase_5m_pips": 3.5,
                    "prompt_chase_5m_pips": 3.0,
                    "max_countertrend_htf_4h_pips": 25.0,
                    "stop_loss_pips": 14.0,
                    "take_profit_pips": 24.0,
                    "trailing_activation_pips": 10.0,
                    "trailing_distance_pips": 6.0,
                    "max_holding_seconds": 14400,
                },
                {
                    "summary_zh": "欧盘央行鹰派转向果断做多并适当放宽追高门槛",
                    "dynamic_prompt_rules": ["遇到 ECB 鹰派转向果断顺势做多，严禁缩头漏单"],
                    "max_same_direction_chase_5m_pips": 5.5,
                    "prompt_chase_5m_pips": 5.0,
                    "max_countertrend_htf_4h_pips": 35.0,
                    "stop_loss_pips": 14.0,
                    "take_profit_pips": 24.0,
                    "trailing_activation_pips": 10.0,
                    "trailing_distance_pips": 6.0,
                    "max_holding_seconds": 14400,
                },
            ]
        }
    )

    evo_db = tmp_path / "evolution.sqlite"
    active_json = tmp_path / "active_strategy.json"
    res = asyncio.run(
        run_evolution_tick(
            evolution_db=evo_db,
            active_strategy_path=active_json,
            shadow_db=shd_db,
            quotes_db=q_db,
            llm_client=fake_llm,
            maturity_seconds=14400,
            clock=lambda: start + timedelta(hours=18),
        )
    )
    assert res["matured_clusters_total"] == 6
    assert res["outcome"] == "promoted"
    assert res["active_version_before"] == "v5.0"
    assert res["active_version_after"] == "v5.1"
    assert res["capture_rate"] >= 0.40
    assert res["candidate_arena_score"] > res["incumbent_arena_score"]
    promoted_file = StrategyProfile.model_validate_json(active_json.read_text(encoding="utf-8"))
    assert promoted_file.version_id == "v5.1"
    assert promoted_file.max_same_direction_chase_5m_pips == Decimal("5.5")


def test_live_strategy_reloader_hot_reloads_and_preserves_safety_floors(tmp_path: Path) -> None:
    t0 = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    active_json = tmp_path / "active_strategy.json"
    save_active_strategy_atomic(active_json, StrategyProfile.baseline_v5(now=t0))

    risk_pol = RiskPolicy.pepperstone_razor_200_guarded()
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200()
    mem = SQLiteThesisMemory(tmp_path / "mem.sqlite", allow_targets=True)
    shd = SQLiteShadowLedger(
        tmp_path / "shd.sqlite",
        initial_cash_usd=Decimal("165.00"),
        risk_policy=risk_pol,
        exit_policy=exit_pol,
        cost_model=ShadowCostModel.pepperstone_razor(),
    )
    agent = SinglePassTradingAgent(
        _FakeArenaLLM(),
        allow_commission_bearing_zero_spread=True,
        max_risk_per_trade_pct=risk_pol.max_risk_per_trade_pct,
        ex_ante_stop_loss_pips=risk_pol.ex_ante_stop_loss_pips,
    )
    risk_engine = DeterministicHardRiskEngine(policy=risk_pol)
    reloader = LiveStrategyReloader(
        active_json,
        agent=agent,
        risk_engine=risk_engine,
        ledger=shd,
    )
    try:
        reloader.reload_if_changed(has_open_exposure=False)
        assert risk_engine.policy.max_same_direction_chase_5m_pips == Decimal("4.0")

        # Promote updated profile with different knobs; reload while position IS OPEN
        v51 = StrategyProfile(
            version_id="v5.1",
            parent_version_id="v5.0",
            created_at=t0 + timedelta(minutes=5),
            status="promoted",
            summary_zh="Hot reload test",
            dynamic_prompt_rules=("测试热更新动态规则不破坏硬底线",),
            max_same_direction_chase_5m_pips=Decimal("5.5"),
            prompt_chase_5m_pips=Decimal("5.0"),
            max_countertrend_htf_4h_pips=Decimal("40.0"),
            stop_loss_pips=Decimal("12.0"),
            take_profit_pips=Decimal("26.0"),
        )
        save_active_strategy_atomic(active_json, v51)
        reloader.reload_if_changed(has_open_exposure=True)

        # Prompt & entry risk updated immediately; hard capital floors UNTOUCHED; exit_policy deferred while open
        assert agent.prompt_version_override == "single-pass-agent-v5.0+v5.1"
        assert "测试热更新动态规则不破坏硬底线" in (agent.system_prompt_override or "")
        assert risk_engine.policy.max_same_direction_chase_5m_pips == Decimal("5.5")
        assert risk_engine.policy.max_countertrend_htf_4h_pips == Decimal("40.0")
        assert risk_engine.policy.max_risk_per_trade_pct == Decimal("0.02")
        assert risk_engine.policy.max_daily_loss_usd == Decimal("8.00")
        assert risk_engine.policy.min_equity_usd == Decimal("140.00")
        assert shd.exit_policy.stop_loss_pips == Decimal("14.0")

        # Once flat, deferred exit policy takes effect without needing another file write
        reloader.reload_if_changed(has_open_exposure=False)
        assert shd.exit_policy.stop_loss_pips == Decimal("12.0")
        assert shd.exit_policy.take_profit_pips == Decimal("26.0")

        # Verify IntradayEventRuntime accepts strategy_reloader cleanly
        from eventlens.contracts import MarketSnapshot

        rt = IntradayEventRuntime(
            memory=mem,
            portfolio_state=shd,
            market_provider=lambda ts: MarketSnapshot(snapshot_id="m", as_of=ts, quotes=()),
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=risk_engine,
            shadow_executor=shd,
            strategy_reloader=reloader.reload_if_changed,
            clock=lambda: t0 + timedelta(hours=8),
        )
        # Trigger inside non-trading asian/late_ny window just confirms reloader hook runs pre-LLM
        assert rt.strategy_reloader is not None
        _ = Event(
            event_id="e1",
            source="investinglive_fx",
            source_type="news",
            source_reference="https://example.com/e1",
            source_timestamp=t0,
            received_timestamp=t0,
            raw_text="Test",
        )
    finally:
        shd.close()


def test_cli_evolve_tick_and_dashboard_evolution_snapshot_sanitization(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    t0 = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
    shd_db = tmp_path / "shadow-ledger.sqlite"
    mem_db = tmp_path / "shadow-memory.sqlite"
    obs_db = tmp_path / "observer.sqlite"
    q_db = _make_quotes_db_with_move(
        tmp_path / "live-quotes.sqlite", start=t0 - timedelta(hours=1), bars_count=360
    )
    evo_db = tmp_path / "evolution.sqlite"
    active_json = tmp_path / "active_strategy.json"
    _seed_runtime_event(
        shd_db,
        event_id="ev-cli-1",
        ts=t0,
        headline="ECB policy shock with secret sk-antigravitysecret12345",
        action="HOLD",
        direction="flat",
    )
    SQLiteThesisMemory(mem_db, allow_targets=True)

    monkeypatch.setattr(
        "sys.argv",
        [
            "eventlens-shadow",
            "evolve-tick",
            "--shadow-db",
            str(shd_db),
            "--memory-db",
            str(mem_db),
            "--observer-db",
            str(obs_db),
            "--quotes-db",
            str(q_db),
            "--evolution-db",
            str(evo_db),
            "--active-strategy",
            str(active_json),
            "--no-call-model",
            "--maturity-seconds",
            "60",
        ],
    )
    shadow_main()
    out = json.loads(capsys.readouterr().out)
    assert out["active_version_after"] == "v5.0"
    assert active_json.is_file()

    dash_out = tmp_path / "snapshot.json"
    export_dashboard_snapshot(
        output_path=dash_out,
        shadow_db=shd_db,
        memory_db=mem_db,
        quotes_db=q_db,
        evolution_db=evo_db,
        active_strategy_path=active_json,
    )
    raw_dash = dash_out.read_text(encoding="utf-8")
    assert "sk-antigravitysecret12345" not in raw_dash
    snap = json.loads(raw_dash)
    assert "evolution" in snap
    assert snap["evolution"]["active_version"] == "v5.0"
    assert snap["evolution"]["rollback_loss_limit"] == 2
    assert snap["evolution"]["rollback_reward_floor"] == -1.5


def test_legacy_pre_v5_and_kill_switch_events_excluded_from_live_v5_scorecard(
    tmp_path: Path,
) -> None:
    start = datetime(2026, 11, 20, 11, 0, tzinfo=UTC)
    q_db = _make_quotes_db_with_move(
        tmp_path / "live-quotes.sqlite",
        start=start,
        bars_count=420,
        move_start_min=60,
        move_pips_total=28.0,
    )
    shd_db = tmp_path / "shadow-ledger.sqlite"
    evo_db = tmp_path / "evolution.sqlite"
    active_json = tmp_path / "active_strategy.json"

    # Legacy v4.1 event -> settles only into legacy_benchmark_clusters, leaving live v5 scorecard clean
    _seed_runtime_event(
        shd_db,
        event_id="ev-legacy-v41",
        ts=start + timedelta(minutes=60),
        headline="ECB hawkish surprise triggers rally",
        action="HOLD",
        direction="flat",
        prompt_version="single-pass-agent-v4.1",
    )
    # Operator kill-switch halts (one in reason, one inside assessment_json) -> completely skipped
    _seed_runtime_event(
        shd_db,
        event_id="ev-ks-reason",
        ts=start + timedelta(minutes=70),
        headline="Major US payrolls print",
        action="SET_TARGET",
        direction="long",
        state="risk_rejected",
        reason="kill_switch_active",
        prompt_version="single-pass-agent-v5.0",
    )
    _seed_runtime_event(
        shd_db,
        event_id="ev-ks-assess",
        ts=start + timedelta(minutes=80),
        headline="Major US CPI print",
        action="SET_TARGET",
        direction="long",
        state="risk_rejected",
        reason="risk_rejected",
        prompt_version="single-pass-agent-v5.0",
        assessment_json=json.dumps({"reasons": ["kill_switch_active"]}),
    )

    fake_llm = _FakeArenaLLM()
    res = asyncio.run(
        run_evolution_tick(
            evolution_db=evo_db,
            active_strategy_path=active_json,
            shadow_db=shd_db,
            quotes_db=q_db,
            llm_client=fake_llm,
            maturity_seconds=14400,
            clock=lambda: start + timedelta(hours=6),
        )
    )
    assert res["outcome"] == "insufficient_data"
    assert res["legacy_benchmark_clusters"] == 1
    assert res["matured_clusters_total"] == 0
    assert res["clusters_since_promotion"] == 0
    assert res["cumulative_reward_score"] == 0.0
    assert res["llm_calls_used"] == 0
    assert fake_llm.calls == []

    # Verify DB contains only ev-legacy-v41 (kill_switch rows skipped)
    with sqlite3.connect(evo_db) as conn:
        ids = [r[0] for r in conn.execute("SELECT event_id FROM scored_events").fetchall()]
        assert ids == ["ev-legacy-v41"]
