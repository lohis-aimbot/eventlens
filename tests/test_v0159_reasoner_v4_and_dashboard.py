"""Verification suite for EventLens v4.0:
1. DeepSeekClient `deepseek-reasoner` defaults, CoT (`reasoning_content`) capture, fenced JSON unwrap, & 400 fallback.
2. V4 SinglePassTradingAgent 8-field Chinese proposals without thesis_ids/verbatim_quote gating + auto-truncation.
3. Intraday swing Pepperstone Razor $200 profile (5h, 14p SL, 42p TP, 22p/10p trail, $140 min equity floor).
4. Read-only dashboard snapshot builder, daily FX reports, CoT linkage, HTTP /api/snapshot, & secret sanitization.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import threading
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import ValidationError

from eventlens.contracts import (
    Event,
    MarketSnapshot,
    PriceTrajectorySummary,
    Quote,
)
from eventlens.dashboard import (
    build_dashboard_snapshot,
    export_dashboard_snapshot,
    sanitize_and_verify_snapshot,
    serve_dashboard,
)
from eventlens.deepseek import DeepSeekClient, ModelResponse
from eventlens.quotes import BridgeQuote, BridgeStart, QuoteStore, is_fx_weekend_closed
from eventlens.runtime import (
    DeterministicTargetValidator,
    IntradayEventRuntime,
    ReviewPolicy,
    ShadowRunner,
    SQLiteRuntimeStore,
)
from eventlens.sources.ctrader_demo import prune_old_bridge_quote_files
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import (
    MARKET_STATE_WATCH_THESIS_ID,
    MAX_INPUT_CHARACTERS,
    SINGLE_PASS_PROMPT_VERSION,
    SinglePassProposal,
    SinglePassTradingAgent,
)
from eventlens.trading.ctrader_cli_broker import (
    BridgeWatchdogVerdict,
    evaluate_bridge_watchdog,
)
from eventlens.trading.execution import ProtectiveExitPolicy, ShadowCostModel, SQLiteShadowLedger
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy


class _V4ReasonerFakeClient:
    provider = "deepseek"
    model = "deepseek-reasoner"

    def __init__(self, handler: Any) -> None:
        self._handler = handler
        self.calls = 0
        self.payloads: list[dict[str, Any]] = []

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls += 1
        assert len(user) <= MAX_INPUT_CHARACTERS
        assert "简体中文" in system
        parsed = json.loads(user)
        self.payloads.append(parsed)
        out, cot = self._handler(parsed, self.calls)
        return ModelResponse(
            content=json.dumps(out, ensure_ascii=False),
            input_tokens=320,
            output_tokens=140,
            model_id="deepseek-reasoner",
            system_fingerprint="fp-v4",
            reasoning_content=cot,
            reasoning_tokens=480,
        )


def _live_market(as_of: datetime, *, mid: Decimal = Decimal("1.08500")) -> MarketSnapshot:
    half = Decimal("0.00001")
    return MarketSnapshot(
        snapshot_id=f"mkt-{int(as_of.timestamp() * 1000)}",
        as_of=as_of,
        quotes=(
            Quote(
                instrument="EUR/USD",
                source_at=as_of - timedelta(milliseconds=300),
                bridge_at=as_of - timedelta(milliseconds=200),
                received_at=as_of - timedelta(milliseconds=100),
                available_at=as_of - timedelta(milliseconds=90),
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
            evidence_id="traj-v4-1",
            as_of=as_of,
            current_mid=mid,
            change_1m_pips=Decimal("-3.2"),
            change_5m_pips=Decimal("-6.0"),
            change_15m_pips=Decimal("-9.5"),
            change_30m_pips=Decimal("-11.0"),
            window_15m_high_mid=mid + Decimal("0.00110"),
            window_15m_low_mid=mid - Decimal("0.00010"),
            window_30m_high_mid=mid + Decimal("0.00140"),
            window_30m_low_mid=mid - Decimal("0.00010"),
            sample_count_30m=360,
            max_gap_30m_seconds=3.0,
        ),
    )


def test_deepseek_reasoner_cot_extraction_fence_strip_and_400_fallback(monkeypatch: Any) -> None:
    seen_bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen_bodies.append(body)
        if len(seen_bodies) == 1:
            assert body["model"] == "deepseek-reasoner"
            assert "thinking" not in body
            assert body["max_tokens"] == 8192
            assert body["response_format"] == {"type": "json_object"}
            return httpx.Response(400, text="response_format unsupported")
        assert "response_format" not in body
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "reasoning_content": "第一步：核对美联储讲话基调与1m/5m汇率动量一致。",
                            "content": '```json\n{"portfolio_action":"HOLD","rationale":"先观察破位"}\n```',
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 210,
                    "completion_tokens": 95,
                    "completion_tokens_details": {"reasoning_tokens": 64},
                },
                "model": "deepseek-reasoner",
                "system_fingerprint": "fp-r1",
            },
        )

    client = DeepSeekClient.reasoner("sk-test-secret", transport=httpx.MockTransport(handler))
    assert client.request_settings.thinking == "enabled"
    assert client.request_settings.max_tokens == 8192
    assert client.request_settings.timeout_seconds == 120.0

    res = asyncio.run(client.complete_json(system="json", user="hello"))
    assert len(seen_bodies) == 2
    assert res.reasoning_content == "第一步：核对美联储讲话基调与1m/5m汇率动量一致。"
    assert res.reasoning_tokens == 64
    assert json.loads(res.content)["portfolio_action"] == "HOLD"

    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-env-secret")
    monkeypatch.delenv("DEEPSEEK_MODEL", raising=False)
    env_client = DeepSeekClient.from_environment()
    assert env_client.model == "deepseek-reasoner"
    assert env_client.request_settings.thinking == "enabled"


def test_v4_swing_agent_runtime_dashboard_export_and_sanitization(tmp_path: Path) -> None:
    assert SINGLE_PASS_PROMPT_VERSION == "single-pass-agent-v4.0"

    risk_pol = RiskPolicy.pepperstone_razor_200_guarded()
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200()
    assert risk_pol.min_equity_usd == Decimal("140.00")
    assert risk_pol.ex_ante_stop_loss_pips == Decimal("14.0")
    assert exit_pol.max_holding_seconds == 18000
    assert exit_pol.stop_loss_pips == Decimal("14.0")
    assert exit_pol.take_profit_pips == Decimal("42.0")
    assert exit_pol.trailing_activation_pips == Decimal("22.0")
    assert exit_pol.trailing_distance_pips == Decimal("10.0")

    t0 = datetime(2026, 11, 16, 13, 0, 0, tzinfo=UTC)
    now_box = [t0]
    mid_box = [Decimal("1.08500")]
    tick = [0]

    def clock() -> datetime:
        tick[0] += 1
        return now_box[0] + timedelta(milliseconds=tick[0])

    def handle_v4(payload: dict[str, Any], call_no: int) -> tuple[dict[str, Any], str]:
        if call_no == 1:
            assert payload["current_market_state_card"] is None
            return (
                {
                    "portfolio_action": "HOLD",
                    "target_direction": "flat",
                    "target_weight": "0",
                    "strategy_hypothesis": "fundamental_breakout",
                    "market_bias": "bearish_eur",
                    "market_state_note": "美联储官员释放偏鹰表态，等待 EUR/USD 有效跌破 1.08420。",
                    "watch_condition": "EUR/USD <= 1.08420",
                    "rationale": "美联储偏鹰信号确立，先盯盘等待跌破 1.08420 确认再顺势做空。",
                },
                "思考1：检查新闻语义与点差（0.2pips），设立 1.08420 下破观察位。",
            )
        if call_no == 2:
            assert payload["current_market_state_card"] is not None
            assert "1.08420" in payload["current_market_state_card"]["market_state_note"]
            return (
                {
                    "portfolio_action": "SET_TARGET",
                    "target_direction": "short",
                    "target_weight": "-0.25",
                    "strategy_hypothesis": "fundamental_breakout",
                    "market_bias": "bearish_eur",
                    "market_state_note": "价格跌破 1.08420 确认偏空波段，顺势持空。",
                    "watch_condition": None,
                    "rationale": "价格已跌破 1.08420 且短线空头动量一致，果断顺势做空。",
                },
                "思考2：触发结构化破位条件 EUR/USD <= 1.08420，且当前未过分透支，提出 -0.25 做空目标。",
            )
        return (
            {
                "portfolio_action": "CLOSE",
                "target_direction": "flat",
                "target_weight": "0",
                "strategy_hypothesis": "fundamental_breakout",
                "market_bias": "neutral",
                "market_state_note": "欧央行超预期偏鹰反转利差预期，主动平仓落袋。",
                "watch_condition": None,
                "rationale": "欧央行最新表态扭转欧美利差方向，主动平掉空单。",
            },
            "思考3：欧央行官员意外强硬，原做空前提发生实质改变，选择主动 CLOSE 平仓。",
        )

    client = _V4ReasonerFakeClient(handle_v4)
    shd_path = tmp_path / "shadow.sqlite"
    mem_path = tmp_path / "memory.sqlite"
    mem = SQLiteThesisMemory(mem_path, clock=clock, allow_targets=True)
    store = SQLiteRuntimeStore(shd_path)
    # Start with $176.10 USD equity (below legacy $180 floor, allowed by new $140 floor!)
    ledger = SQLiteShadowLedger(
        shd_path,
        initial_cash_usd=Decimal("176.10"),
        risk_policy=risk_pol,
        exit_policy=exit_pol,
        cost_model=ShadowCostModel.pepperstone_razor(),
        clock=clock,
    )
    try:
        agent = SinglePassTradingAgent(
            client,
            max_quote_age=timedelta(seconds=8),
            allow_commission_bearing_zero_spread=True,
            max_position_weight=risk_pol.max_position_weight,
            leverage=risk_pol.leverage,
            max_risk_per_trade_pct=risk_pol.max_risk_per_trade_pct,
            ex_ante_stop_loss_pips=risk_pol.ex_ante_stop_loss_pips,
            clock=clock,
        )
        rt = IntradayEventRuntime(
            memory=mem,
            portfolio_state=ledger,
            market_provider=lambda ts: _live_market(ts, mid=mid_box[0]),
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(policy=risk_pol, clock=clock),
            shadow_executor=ledger,
            state_store=store,
            clock=clock,
        )
        runner = ShadowRunner(
            runtime=rt,
            ledger=ledger,
            poll_news=False,
            strict_window_coverage=False,
            exit_buffer_seconds=0.0,
            review_policy=ReviewPolicy(
                enabled=True, cooldown_seconds=10.0, max_total_review_calls=5
            ),
            clock=clock,
        )

        # Call 1: Oversized raw_text auto-truncates without crashing, emits active watch thesis
        huge_padding = " Fed remarks detail." * 4000
        ev1 = Event(
            event_id="ev-v4-1",
            source="investinglive_fx",
            source_type="news",
            source_reference="https://example.com/1",
            source_timestamp=t0 - timedelta(seconds=2),
            received_timestamp=t0,
            raw_text="Fed official warns inflation persistence keeps rates higher." + huge_padding,
        )
        c1 = asyncio.run(rt.run_cycle(ev1))
        assert c1.status == "no_target"
        assert c1.decision is not None
        assert c1.decision.reasoning_tokens == 480
        assert c1.decision.thesis_changes[0].thesis.thesis_id == MARKET_STATE_WATCH_THESIS_ID
        assert c1.decision.thesis_changes[0].thesis.status == "active"

        # Call 2: Price breaks below 1.08420 -> stateful review fires -> short approved on $176.10 account!
        now_box[0] = t0 + timedelta(seconds=30)
        mid_box[0] = Decimal("1.08410")
        rev_res = asyncio.run(runner.step_reviews())
        assert len(rev_res) == 1
        assert rev_res[0].status == "shadow_recorded"
        assert rev_res[0].decision is not None
        assert rev_res[0].decision.target is not None
        assert rev_res[0].decision.target.positions[0].thesis_ids == ()
        open_dec_id = rev_res[0].decision.decision_id

        now_box[0] += timedelta(seconds=1)
        _, open_fills = asyncio.run(runner.step_quotes_and_fills())
        assert len(open_fills) == 1
        assert open_fills[0].new_quantity == Decimal("-2000")

        # Call 3: Reversal catalyst -> AI CLOSE in favor at 1.08210 (+20 pips profit!)
        now_box[0] += timedelta(minutes=10)
        mid_box[0] = Decimal("1.08210")
        ev3 = Event(
            event_id="ev-v4-3",
            source="ecb_news",
            source_type="news",
            source_reference="https://ecb.europa.eu/press/1",
            source_timestamp=now_box[0] - timedelta(seconds=1),
            received_timestamp=now_box[0],
            raw_text="ECB Governing Council signals zero rate cuts this year. Contact: ops@example.com account_number: 12345678",
        )
        c3 = asyncio.run(rt.run_cycle(ev3))
        assert c3.status == "shadow_recorded"
        now_box[0] += timedelta(seconds=1)
        _, close_fills = asyncio.run(runner.step_quotes_and_fills())
        assert len(close_fills) == 1
        assert close_fills[0].new_quantity == Decimal("0")

        # Add a pre-filtered noise row into runtime_events; verify it increments total_events/filtered_out_events
        # while staying excluded from decisions[], AND verify max_decisions=1 still retains pinned trade open_dec_id!
        store.upsert_event_state(
            event_id="noise-filtered-1",
            source_reference="https://example.com/noise",
            received_at=now_box[0] + timedelta(minutes=2),
            now=now_box[0] + timedelta(minutes=2),
            state="filtered_out",
            reason="below_news_importance_threshold",
        )

        # Build & sanitize dashboard snapshot
        snap_out = tmp_path / "snapshot.json"
        meta = export_dashboard_snapshot(
            output_path=snap_out,
            shadow_db=shd_path,
            memory_db=mem_path,
            vm_status="running",
            max_decisions=1,
        )
        assert meta["trades_exported"] == 1
        raw_json = snap_out.read_text(encoding="utf-8")
        assert "12345678" not in raw_json
        assert "ops@example.com" not in raw_json
        assert str(tmp_path) not in raw_json

        parsed = json.loads(raw_json)
        assert parsed["account"]["mode"] == "shadow"
        assert parsed["kpis"]["total_trades"] == 1
        assert parsed["kpis"]["win_count"] == 1
        assert parsed["kpis"]["net_pnl_usd"] > 3.0
        assert parsed["kpis"]["funnel"]["total_events"] == 4
        assert parsed["kpis"]["funnel"]["filtered_out_events"] == 1
        assert all(d["state"] != "filtered_out" for d in parsed["decisions"])
        dec_ids_exported = {d["decision_id"] for d in parsed["decisions"]}
        assert open_dec_id in dec_ids_exported
        assert parsed["trades"][0]["decision_id"] == open_dec_id
        assert parsed["trades"][0]["side_en"] == "SHORT"
        assert parsed["trades"][0]["exit_reason_en"]
        assert parsed["equity_curve"][0]["label_en"] == "Initial Baseline"
        assert any(d["review_meta"] is not None for d in parsed["decisions"])
        assert all("[INTERNAL STATEFUL REVIEW]" not in d["headline"] for d in parsed["decisions"])
        assert parsed["market_state_card"]["market_bias"] == "neutral"
        assert "deepseek-reasoner" in parsed["model_config"]["model"]

        # Verify local read-only HTTP server serves index.html and /api/snapshot cleanly
        docs_dash = Path("docs/dashboard")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])

        srv_thread = threading.Thread(
            target=serve_dashboard,
            kwargs={
                "host": "127.0.0.1",
                "port": port,
                "shadow_db": shd_path,
                "memory_db": mem_path,
                "static_dir": docs_dash,
            },
            daemon=True,
        )
        srv_thread.start()
        for _ in range(25):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/api/snapshot", timeout=2
                ) as r:
                    assert r.status == 200
                    api_payload = json.loads(r.read().decode("utf-8"))
                    assert api_payload["kpis"]["total_trades"] == 1
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2) as r_html:
                    assert r_html.status == 200
                    html_txt = r_html.read().decode("utf-8")
                    assert "每日量化报表" in html_txt
                    assert "Live Intelligence" in html_txt
                    assert "Performance & Ledger" in html_txt
                    assert 'data-theme="light"' in html_txt
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise AssertionError("Dashboard HTTP server did not become ready")
    finally:
        ledger.close()
        store.close()


def test_dashboard_sanitizer_strips_sensitive_identifiers(monkeypatch: Any) -> None:
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-abcdef1234567890abcdef")
    monkeypatch.setenv("CTRADER_DEMO_ACCOUNT_NUMBER", "99887766")
    raw = build_dashboard_snapshot(shadow_db=Path("/nonexistent/shadow.sqlite"))
    raw["market_state_card"]["market_state_note"] = (
        "Path /var/lib/eventlens/data/x.sqlite projects/project-abc123-xyz/zones/us-central1-a "
        "owner@test.com"
    )
    rendered = sanitize_and_verify_snapshot(raw)
    assert "/var/lib/eventlens" not in rendered
    assert "project-abc123-xyz" not in rendered
    assert "owner@test.com" not in rendered
    assert "99887766" not in rendered


def test_single_pass_proposal_normalizes_short_and_long_target_weight_sign() -> None:
    short_unsigned = SinglePassProposal(
        portfolio_action="SET_TARGET",
        target_direction="short",
        target_weight="0.15",
        strategy_hypothesis="fundamental_breakout",
        market_bias="bearish_eur",
        market_state_note="跌破关键支撑位，短线偏空。",
        watch_condition=None,
        rationale="下破后顺势做空。",
    )
    assert short_unsigned.target_weight == Decimal("-0.15")

    short_signed = SinglePassProposal(
        portfolio_action="SET_TARGET",
        target_direction="short",
        target_weight="-0.25",
        strategy_hypothesis="trend_following",
        market_bias="bearish_eur",
        rationale="维持负数做空权重。",
    )
    assert short_signed.target_weight == Decimal("-0.25")

    long_negative = SinglePassProposal(
        portfolio_action="SET_TARGET",
        target_direction="long",
        target_weight="-0.12",
        strategy_hypothesis="trend_following",
        market_bias="bullish_eur",
        rationale="误填负号应自动归一为正数做多权重。",
    )
    assert long_negative.target_weight == Decimal("0.12")

    with pytest.raises(ValidationError):
        SinglePassProposal(
            portfolio_action="SET_TARGET",
            target_direction="short",
            target_weight="1.5",
            rationale="超出 [-1, 0) 上下限必须仍报错。",
        )

    with pytest.raises(ValidationError):
        SinglePassProposal(
            portfolio_action="SET_TARGET",
            target_direction="short",
            target_weight="0",
            rationale="SET_TARGET 零权重必须仍报错。",
        )

    with pytest.raises(ValidationError):
        SinglePassProposal(
            portfolio_action="HOLD",
            target_direction="flat",
            target_weight="0.15",
            rationale="HOLD 非零权重必须仍报错。",
        )


def test_evaluate_bridge_watchdog_handles_zombie_cbot_stalls_weekend_and_safety_refusal(
    tmp_path: Path,
) -> None:
    t_open = datetime(2026, 11, 17, 14, 0, 0, tzinfo=UTC)  # Tuesday 09:00 NY (open market)
    t_active = t_open - timedelta(minutes=10)

    def _set_age(p: Path, age_s: float) -> None:
        ts = (t_open - timedelta(seconds=age_s)).timestamp()
        os.utime(p, (ts, ts))

    def _make_healthy_dir(sub: str) -> Path:
        bdir = tmp_path / sub
        bdir.mkdir(parents=True, exist_ok=True)
        exec_p = bdir / "execution-events.jsonl"
        exec_p.write_text(
            json.dumps(
                {
                    "version": 1,
                    "kind": "reconcile_snapshot",
                    "emitted_at": (t_open - timedelta(seconds=10)).isoformat(),
                }
            )
            + "\n",
            encoding="utf-8",
        )
        _set_age(exec_p, 10)
        q_p = bdir / "quotes-1111.jsonl"
        q_p.write_text(json.dumps({"kind": "quote", "bid": "1.1000"}) + "\n", encoding="utf-8")
        _set_age(q_p, 5)
        return bdir

    # 1. Healthy active run
    b_healthy = _make_healthy_dir("healthy")
    verdict = evaluate_bridge_watchdog(
        b_healthy, now_utc=t_open, service_active_since=t_active, cbot_terminated_in_logs=False
    )
    assert isinstance(verdict, BridgeWatchdogVerdict)
    assert verdict.should_restart is False
    assert verdict.reason == "healthy"

    # 2. Zombie cBot child termination detected in journalctl logs
    verdict_zombie = evaluate_bridge_watchdog(
        b_healthy, now_utc=t_open, service_active_since=t_active, cbot_terminated_in_logs=True
    )
    assert verdict_zombie.should_restart is True
    assert verdict_zombie.reason == "cbot_child_process_terminated"

    # 3. Stale reconcile heartbeat (> 180s) after startup grace during open market
    b_stale_rec = _make_healthy_dir("stale_rec")
    _set_age(b_stale_rec / "execution-events.jsonl", 200)
    verdict_rec = evaluate_bridge_watchdog(
        b_stale_rec, now_utc=t_open, service_active_since=t_active
    )
    assert verdict_rec.should_restart is True
    assert verdict_rec.reason == "stale_reconcile_heartbeat"

    # 4. Fresh reconcile heartbeat but stale quotes (> 300s) during open market
    b_stale_q = _make_healthy_dir("stale_quote")
    _set_age(b_stale_q / "quotes-1111.jsonl", 350)
    verdict_q = evaluate_bridge_watchdog(b_stale_q, now_utc=t_open, service_active_since=t_active)
    assert verdict_q.should_restart is True
    assert verdict_q.reason == "stale_open_market_quotes"

    # 5. Saturday FX weekend suppresses stale restart
    t_sat = datetime(2026, 11, 14, 15, 0, 0, tzinfo=UTC)
    verdict_wknd = evaluate_bridge_watchdog(
        b_stale_q, now_utc=t_sat, service_active_since=t_sat - timedelta(hours=10)
    )
    assert verdict_wknd.should_restart is False
    assert verdict_wknd.reason == "fx_weekend_or_reopen_grace"

    # 6. Startup grace suppresses restart before 180s elapsed
    verdict_grace = evaluate_bridge_watchdog(
        b_stale_q,
        now_utc=t_open,
        service_active_since=t_open - timedelta(seconds=60),
    )
    assert verdict_grace.should_restart is False
    assert verdict_grace.reason == "startup_grace_period"

    # 7. Safety refusal in current service run halts restarts unconditionally
    b_refusal = _make_healthy_dir("safety_refusal")
    (b_refusal / "execution-events.jsonl").write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "safety_refusal",
                "emitted_at": (t_active + timedelta(seconds=5)).isoformat(),
                "reason": "unexpected_demo_account_number",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    verdict_refusal = evaluate_bridge_watchdog(
        b_refusal,
        now_utc=t_open,
        service_active_since=t_active,
        cbot_terminated_in_logs=True,
    )
    assert verdict_refusal.should_restart is False
    assert verdict_refusal.reason == "safety_refusal_halt"


def test_is_fx_weekend_closed_and_48h_bridge_and_quote_failure_pruning(tmp_path: Path) -> None:
    ny = ZoneInfo("America/New_York")
    assert is_fx_weekend_closed(datetime(2026, 11, 13, 16, 59, tzinfo=ny).astimezone(UTC)) is False
    assert is_fx_weekend_closed(datetime(2026, 11, 13, 17, 1, tzinfo=ny).astimezone(UTC)) is True
    assert is_fx_weekend_closed(datetime(2026, 11, 14, 12, 0, tzinfo=ny).astimezone(UTC)) is True
    assert is_fx_weekend_closed(datetime(2026, 11, 15, 16, 59, tzinfo=ny).astimezone(UTC)) is True
    assert is_fx_weekend_closed(datetime(2026, 11, 15, 17, 1, tzinfo=ny).astimezone(UTC)) is False

    now = datetime(2026, 11, 17, 14, 0, 0, tzinfo=UTC)
    bdir = tmp_path / "bridge"
    bdir.mkdir()

    old_q = bdir / "quotes-00000000-0000-0000-0000-000000000001.jsonl"
    recent_stopped_q = bdir / "quotes-00000000-0000-0000-0000-000000000002.jsonl"
    active_q = bdir / "quotes-00000000-0000-0000-0000-000000000003.jsonl"
    exec_ev = bdir / "execution-events.jsonl"

    old_session = UUID("00000000-0000-0000-0000-000000000001")
    stopped_session = UUID("00000000-0000-0000-0000-000000000002")
    active_session = UUID("00000000-0000-0000-0000-000000000003")

    old_q.write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "start",
                "session_id": str(old_session),
                "account_mode": "demo",
                "running_mode": "realtime",
                "symbol": "EURUSD",
                "started_at": (now - timedelta(hours=72)).isoformat(),
                "sample_interval_ms": 200,
                "duration_seconds": 43200,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    os.utime(old_q, ((now - timedelta(hours=60)).timestamp(),) * 2)

    recent_stopped_q.write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "start",
                "session_id": str(stopped_session),
                "account_mode": "demo",
                "running_mode": "realtime",
                "symbol": "EURUSD",
                "started_at": (now - timedelta(hours=6)).isoformat(),
                "sample_interval_ms": 200,
                "duration_seconds": 43200,
            }
        )
        + "\n"
        + json.dumps(
            {
                "version": 1,
                "kind": "stop",
                "session_id": str(stopped_session),
                "stopped_at": (now - timedelta(hours=5)).isoformat(),
                "quotes": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    os.utime(recent_stopped_q, ((now - timedelta(hours=5)).timestamp(),) * 2)

    active_start = BridgeStart(
        version=1,
        kind="start",
        session_id=active_session,
        account_mode="demo",
        running_mode="realtime",
        symbol="EURUSD",
        started_at=now - timedelta(seconds=30),
        sample_interval_ms=200,
        duration_seconds=43200,
    )
    active_quote = BridgeQuote(
        version=1,
        kind="quote",
        session_id=active_session,
        sequence=1,
        symbol="EURUSD",
        source_at=now - timedelta(seconds=1),
        bridge_at=now - timedelta(milliseconds=500),
        bid=Decimal("1.10000"),
        ask=Decimal("1.10002"),
    )
    active_q.write_text(
        active_start.model_dump_json() + "\n" + active_quote.model_dump_json() + "\n",
        encoding="utf-8",
    )
    os.utime(active_q, ((now - timedelta(seconds=1)).timestamp(),) * 2)

    exec_ev.write_text('{"kind":"reconcile_snapshot"}\n', encoding="utf-8")
    os.utime(exec_ev, ((now - timedelta(hours=72)).timestamp(),) * 2)

    # 1. Prune files >48h directly, preserving recent, newest, and execution-events.jsonl
    deleted = prune_old_bridge_quote_files(bdir, now_utc=now, retention_hours=48.0)
    assert deleted == 1
    assert not old_q.exists()
    assert recent_stopped_q.exists()
    assert active_q.exists()
    assert exec_ev.exists()

    # 2. QuoteStore.prune_old_failures deletes >48h rows and preserves recent rows
    qstore = QuoteStore(tmp_path / "quotes.sqlite", clock=lambda: now)
    mem = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=lambda: now, allow_targets=True)
    ledger = SQLiteShadowLedger(tmp_path / "shd.sqlite", clock=lambda: now)
    try:
        qstore.failure(now - timedelta(hours=72), "stale_failure", "{}")
        qstore.failure(now - timedelta(hours=2), "recent_failure", "{}")
        removed_failures = qstore.prune_old_failures(before=now - timedelta(hours=48))
        assert removed_failures == 1
        rem_rows = qstore.db.execute("SELECT reason FROM quote_failures").fetchall()
        assert [r["reason"] for r in rem_rows] == ["recent_failure"]

        # 3. ShadowRunner.__init__ attaches cleanly to active_q via bounded read
        runtime = IntradayEventRuntime(
            memory=mem,
            portfolio_state=ledger,
            market_provider=lambda ts: MarketSnapshot(snapshot_id="m0", as_of=ts, quotes=()),
            agent=SinglePassTradingAgent(
                DeepSeekClient(api_key="test-key", model="deepseek-reasoner"), clock=lambda: now
            ),
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
            shadow_executor=ledger,
            clock=lambda: now,
        )
        runner = ShadowRunner(
            runtime=runtime,
            ledger=ledger,
            quote_store=qstore,
            quote_bridge_dir=bdir,
            clock=lambda: now,
        )
        assert runner._quote_collector is not None
        assert runner._quote_collector.session is not None
        assert runner._quote_collector.session.session_id == active_session
        assert runner._quote_tail is not None
        assert active_q.resolve() in runner._quote_tail.files
        assert qstore.latest("EURUSD", now) is not None
    finally:
        ledger.close()
        qstore.close()
