"""Verification suite for EventLens v4.0:
1. DeepSeekClient `deepseek-reasoner` defaults, CoT (`reasoning_content`) capture, fenced JSON unwrap, & 400 fallback.
2. V4 SinglePassTradingAgent 8-field Chinese proposals without thesis_ids/verbatim_quote gating + auto-truncation.
3. Intraday swing Pepperstone Razor $200 profile (5h, 14p SL, 42p TP, 22p/10p trail, $140 min equity floor).
4. Read-only dashboard snapshot builder, daily FX reports, CoT linkage, HTTP /api/snapshot, & secret sanitization.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx

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
from eventlens.reasoning import MAX_INPUT_CHARACTERS
from eventlens.runtime import (
    DeterministicTargetValidator,
    IntradayEventRuntime,
    ReviewPolicy,
    ShadowRunner,
    SQLiteRuntimeStore,
)
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import (
    MARKET_STATE_WATCH_THESIS_ID,
    SINGLE_PASS_PROMPT_VERSION,
    SinglePassTradingAgent,
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
