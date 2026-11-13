"""Regression suite for Nov 12-13 2026 live cloud 26.5h audit fixes:
1. Bounded active-thesis context pruning (no overflow across 25+ verbose theses; preserves pinned review thesis).
2. 12h quote session rotation when new UUID sorts lexicographically BEFORE old UUID + delayed poll + self-healing attach.
3. Stateful breakout review conversion (HOLD monitoring thesis -> SET_TARGET on structured_price_condition) + daily review budget in continuous mode.
4. EventLensCliBridge disconnect/tick-stall non-zero watchdog exit contract vs exit-0 safety refusal halt.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from eventlens.contracts import (
    AgentContext,
    Event,
    MarketSnapshot,
    MemorySnapshot,
    PortfolioSnapshot,
    PriceTrajectorySummary,
    Quote,
    ReviewContext,
    Thesis,
)
from eventlens.deepseek import ModelResponse
from eventlens.quotes import BridgeQuote, BridgeStart, BridgeStop, QuoteStore
from eventlens.reasoning import MAX_INPUT_CHARACTERS
from eventlens.runtime import (
    DeterministicTargetValidator,
    IntradayEventRuntime,
    ReviewPolicy,
    ShadowRunner,
    SQLiteRuntimeStore,
)
from eventlens.sources.ctrader_demo import DirectoryTail
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import (
    SINGLE_PASS_PROMPT_VERSION,
    SINGLE_PASS_SYSTEM_PROMPT,
    SinglePassTradingAgent,
)
from eventlens.trading.execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    SQLiteShadowLedger,
)
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy


class _RecordingClient:
    provider = "deepseek"
    model = "deepseek-chat"

    def __init__(self, handler: Any) -> None:
        self._handler = handler
        self.calls = 0
        self.payloads: list[dict[str, Any]] = []

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls += 1
        assert len(user) <= MAX_INPUT_CHARACTERS
        parsed = json.loads(user)
        self.payloads.append(parsed)
        out = self._handler(parsed, self.calls)
        return ModelResponse(
            content=json.dumps(out, ensure_ascii=False),
            input_tokens=200,
            output_tokens=100,
            model_id="deepseek-chat",
            system_fingerprint="fp-v0158",
        )


def _live_market(
    as_of: datetime,
    *,
    mid: Decimal = Decimal("1.15550"),
    c1m: Decimal = Decimal("-2.5"),
    c5m: Decimal = Decimal("-5.0"),
    c15m: Decimal = Decimal("-8.5"),
    c30m: Decimal = Decimal("-12.0"),
) -> MarketSnapshot:
    half = Decimal("0.00001")
    return MarketSnapshot(
        snapshot_id=f"mkt-{int(as_of.timestamp() * 1000)}",
        as_of=as_of,
        quotes=(
            Quote(
                instrument="EUR/USD",
                source_at=as_of - timedelta(milliseconds=300),
                bridge_at=as_of - timedelta(milliseconds=280),
                received_at=as_of - timedelta(milliseconds=250),
                available_at=as_of - timedelta(milliseconds=240),
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
            evidence_id="traj-live-1",
            as_of=as_of,
            current_mid=mid,
            change_1m_pips=c1m,
            change_5m_pips=c5m,
            change_15m_pips=c15m,
            change_30m_pips=c30m,
            window_15m_high_mid=mid + Decimal("0.00120"),
            window_15m_low_mid=mid - Decimal("0.00010"),
            window_30m_high_mid=mid + Decimal("0.00160"),
            window_30m_low_mid=mid - Decimal("0.00010"),
            sample_count_30m=400,
            max_gap_30m_seconds=2.5,
        ),
    )


def test_context_pruning_prevents_overflow_across_many_verbose_theses_and_keeps_pinned_review() -> (
    None
):
    t0 = datetime(2026, 11, 12, 16, 0, 0, tzinfo=UTC)
    theses: list[Thesis] = []
    for idx in range(28):
        status = "invalidated" if idx % 5 == 0 and idx > 0 else "active"
        ts = t0 - timedelta(minutes=(30 - idx) * 10)
        theses.append(
            Thesis(
                thesis_id=f"thesis-{idx:02d}",
                revision=3,
                status=status,  # type: ignore[arg-type]
                narrative=(
                    f"Verbose monitoring thesis #{idx}: ECB/Fed rate differential narrative "
                    + ("detailed macro commentary " * 45)
                ),
                instruments=("EUR/USD",),
                supporting_event_ids=tuple(f"ev-{idx}-{j}" for j in range(12)),
                contradicting_event_ids=tuple(f"cev-{idx}-{j}" for j in range(6)),
                invalidation_conditions=(
                    "EUR/USD <= 1.15580",
                    "Qualitative shift in Fed guidance " + ("detail " * 20),
                ),
                confidence=0.60,
                created_at=ts - timedelta(minutes=5),
                updated_at=ts,
                review_at=ts + timedelta(hours=1),
            )
        )

    # Pin the very oldest active thesis (`thesis-00`) via review_context while also invalidating `thesis-01`
    review_ctx = ReviewContext(
        review_id="rev-pin-00",
        trigger_kind="structured_price_condition",
        trigger_detail="EUR/USD <= 1.15580 (observed=1.15550)",
        triggered_at=t0,
        thesis_id="thesis-00",
        thesis_revision=3,
        origin_event_id="ev-origin-00",
        origin_source="investinglive_fx",
        origin_source_timestamp=t0 - timedelta(minutes=25),
        origin_received_timestamp=t0 - timedelta(minutes=25),
        origin_event_age_seconds=1500.0,
    )
    ev = Event(
        event_id="internal_review:rev-pin-00",
        source="internal_stateful_review",
        source_type="internal_review",
        source_reference="review://thesis-00/r3/structured_price_condition",
        source_timestamp=t0 - timedelta(minutes=25),
        received_timestamp=t0 - timedelta(minutes=25),
        raw_text=(
            "[INTERNAL STATEFUL REVIEW] trigger=structured_price_condition\n\n"
            "Fed's Hammack says inflation remains too high and rates may need to stay restrictive."
        ),
    )

    def reply_fn(payload: dict[str, Any], _call: int) -> dict[str, Any]:
        prior = payload["prior_memory_theses_do_not_quote"]
        prior_ids = [item["thesis_id"] for item in prior]
        assert "thesis-00" in prior_ids
        assert len(prior) <= 8
        assert all(item["status"] == "active" or item["thesis_id"] == "thesis-00" for item in prior)
        assert all(len(item["supporting_event_ids"]) <= 5 for item in prior)
        return {
            "memory_action": "update",
            "thesis_id": "thesis-00",
            "status": "active",
            "narrative": "Downside breakout confirmed below 1.15580 on hawkish Fed stance.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["EUR/USD >= 1.15710", "Dovish Fed pivot"],
            "confidence": 0.78,
            "evidence_role": "support",
            "evidence_quote": "inflation remains too high and rates may need to stay restrictive",
            "invalidate_thesis_ids": ["thesis-01"],
            "portfolio_action": "SET_TARGET",
            "target_direction": "short",
            "target_weight": "-0.15",
            "target_thesis_ids": ["__current__"],
            "strategy_hypothesis": "fundamental_breakout",
            "cited_evidence_ids": [ev.event_id, payload["market_snapshot"]["snapshot_id"]],
            "rationale": (
                "Structured downside breakout confirmed by 1m -2.5 pips, 5m -5.0 pips, and 15m -8.5 pips."
            ),
        }

    client = _RecordingClient(reply_fn)
    agent = SinglePassTradingAgent(
        client,
        allow_commission_bearing_zero_spread=True,
        clock=lambda: t0 + timedelta(milliseconds=100),
    )
    ctx = AgentContext(
        event=ev,
        memory=MemorySnapshot(memory_version=28, as_of=t0, theses=tuple(theses)),
        portfolio=PortfolioSnapshot(
            snapshot_id="port-flat",
            as_of=t0,
            base_currency="USD",
            equity=Decimal("176.10"),
        ),
        market=_live_market(t0),
        as_of=t0,
        review_context=review_ctx,
    )
    res = asyncio.run(agent.decide_with_usage(ctx))
    assert client.calls == 1
    assert res.decision.portfolio_action == "SET_TARGET"
    assert res.decision.target is not None
    assert res.decision.target.positions[0].weight == Decimal("-0.15")
    assert res.decision.evidence_verification_status == "verified"
    # Even though thesis-01 was pruned from the prompt view, full memory lookup still invalidates it cleanly
    assert len(res.decision.thesis_changes) == 2
    assert res.decision.thesis_changes[1].thesis.thesis_id == "thesis-01"
    assert res.decision.thesis_changes[1].thesis.status == "invalidated"


def test_12h_quote_rotation_with_smaller_uuid_and_delayed_poll_streams_seamlessly(
    tmp_path: Path,
) -> None:
    """Reproduces exact Nov 12 21:41 UTC rotation bug:
    - Old session UUID `9651eb64-...` rotates to new session UUID `12345678-...` (`'1234' < '9651'`).
    - Both trailing `quote`+`stop` in old file and `start`+`quote` in new file exist when `poll()` runs 95s after rotation.
    - `DirectoryTail` + `ShadowRunner` must drain old session's `stop` BEFORE new session's `start`,
      accept the 95s-old rotated `start` handshake, and record zero `Unknown or closed bridge session` failures."""
    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    t0 = datetime(2026, 11, 12, 9, 41, 36, tzinfo=UTC)
    now_box = [t0]

    qstore = QuoteStore(tmp_path / "quotes.sqlite", clock=lambda: now_box[0])
    mem = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=lambda: now_box[0], allow_targets=True)
    store = SQLiteRuntimeStore(tmp_path / "shd.sqlite")
    ledger = SQLiteShadowLedger(tmp_path / "shd.sqlite", clock=lambda: now_box[0])
    client = _RecordingClient(lambda _p, _c: {})
    rt = IntradayEventRuntime(
        memory=mem,
        portfolio_state=ledger,
        market_provider=lambda ts: _live_market(ts),
        agent=SinglePassTradingAgent(client, clock=lambda: now_box[0]),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now_box[0]),
        shadow_executor=ledger,
        state_store=store,
        clock=lambda: now_box[0],
    )
    runner = ShadowRunner(
        runtime=rt,
        ledger=ledger,
        quote_store=qstore,
        quote_bridge_dir=bridge_dir,
        poll_news=False,
        clock=lambda: now_box[0],
    )
    try:
        old_sid = UUID("9651eb64-0000-4000-8000-000000000001")
        new_sid = UUID("12345678-0000-4000-8000-000000000002")
        old_file = bridge_dir / f"quotes-{old_sid}.jsonl"
        new_file = bridge_dir / f"quotes-{new_sid}.jsonl"

        s_old = BridgeStart(
            version=1,
            kind="start",
            session_id=old_sid,
            account_mode="demo",
            running_mode="realtime",
            symbol="EURUSD",
            started_at=t0 + timedelta(seconds=1),
            sample_interval_ms=200,
            duration_seconds=43200,
            digits=5,
            pip_size="0.0001",
            commission="3",
            commission_type="UsdPerMillionUsdVolume",
        )
        q_old_1 = BridgeQuote(
            version=1,
            kind="quote",
            session_id=old_sid,
            sequence=1,
            symbol="EURUSD",
            source_at=t0 + timedelta(seconds=2),
            bridge_at=t0 + timedelta(seconds=2, milliseconds=10),
            bid=Decimal("1.15800"),
            ask=Decimal("1.15802"),
        )
        old_file.write_text(
            s_old.model_dump_json() + "\n" + q_old_1.model_dump_json() + "\n",
            encoding="utf-8",
        )
        now_box[0] = t0 + timedelta(seconds=3)
        runner._poll_quote_bridge_sync()
        assert qstore.report()["quotes"] == 1
        assert qstore.report()["failures"] == 0

        # 12h rotation occurs at t_rot; old file gets final stop, new file (with smaller UUID!) opens,
        # and Python polls 95s after rotation with a fresh quote in the new session.
        t_rot = t0 + timedelta(seconds=43201)
        stop_old = BridgeStop(
            version=1,
            kind="stop",
            session_id=old_sid,
            stopped_at=t_rot,
            quotes=1,
        )
        with old_file.open("a", encoding="utf-8") as fh:
            fh.write(stop_old.model_dump_json() + "\n")

        s_new = BridgeStart(
            version=1,
            kind="start",
            session_id=new_sid,
            account_mode="demo",
            running_mode="realtime",
            symbol="EURUSD",
            started_at=t_rot,
            sample_interval_ms=200,
            duration_seconds=43200,
            digits=5,
            pip_size="0.0001",
            commission="3",
            commission_type="UsdPerMillionUsdVolume",
        )
        t_poll = t_rot + timedelta(seconds=95)
        q_new_1 = BridgeQuote(
            version=1,
            kind="quote",
            session_id=new_sid,
            sequence=1,
            symbol="EURUSD",
            source_at=t_poll - timedelta(seconds=1),
            bridge_at=t_poll - timedelta(milliseconds=900),
            bid=Decimal("1.15540"),
            ask=Decimal("1.15542"),
        )
        new_file.write_text(
            s_new.model_dump_json() + "\n" + q_new_1.model_dump_json() + "\n",
            encoding="utf-8",
        )

        now_box[0] = t_poll
        runner._poll_quote_bridge_sync()
        rep = qstore.report()
        assert rep["sessions"] == 2
        assert rep["quotes"] == 2
        assert rep["failures"] == 0
        latest = qstore.latest("EURUSD", t_poll + timedelta(milliseconds=10))
        assert latest is not None
        assert latest.quote.bid == Decimal("1.15540")
    finally:
        qstore.close()
        ledger.close()
        store.close()


def test_directory_tail_drains_tracked_file_before_smaller_uuid_new_file(tmp_path: Path) -> None:
    tail = DirectoryTail(tmp_path)
    f_old = tmp_path / "quotes-ffff.jsonl"
    f_old.write_text("old-start\n", encoding="utf-8")
    assert list(tail.poll()) == ["old-start"]

    with f_old.open("a", encoding="utf-8") as fh:
        fh.write("old-stop\n")
    f_new = tmp_path / "quotes-0001.jsonl"
    f_new.write_text("new-start\nnew-quote\n", encoding="utf-8")

    assert list(tail.poll()) == ["old-stop", "new-start", "new-quote"]


def test_stateful_breakout_review_converts_hold_to_entry_and_daily_review_cap_resets_next_day(
    tmp_path: Path,
) -> None:
    assert SINGLE_PASS_PROMPT_VERSION == "single-pass-agent-v3.2"
    assert '"EUR/USD <= <level>"' in SINGLE_PASS_SYSTEM_PROMPT
    assert '"EUR/USD >= <level>"' in SINGLE_PASS_SYSTEM_PROMPT
    assert '"structured_price_condition"' in SINGLE_PASS_SYSTEM_PROMPT
    assert '"price_breakout_excursion"' in SINGLE_PASS_SYSTEM_PROMPT

    cs_src = Path("ctrader/EventLensCliBridge/EventLensCliBridge.cs").read_text(encoding="utf-8")
    svc_src = Path("deploy/gcp/eventlens-shadow-cli.service").read_text(encoding="utf-8")
    assert "public int MaxDisconnectSeconds" in cs_src
    assert "public int MaxIdleTickSeconds" in cs_src
    assert "Server.IsConnected" in cs_src
    assert "Symbol.MarketHours.IsOpened()" in cs_src
    assert "Environment.Exit(75);" in cs_src
    assert "--max-quote-age 8.0" in svc_src
    assert "--max-quote-wait-seconds 120" in svc_src
    assert "--review-breakout-pips 8" in svc_src

    t_day1 = datetime(2026, 11, 12, 23, 50, 0, tzinfo=UTC)
    now_box = [t_day1]
    mid_box = [Decimal("1.15700")]
    tick = [0]

    def clock() -> datetime:
        tick[0] += 1
        return now_box[0] + timedelta(milliseconds=tick[0])

    def handle_llm(payload: dict[str, Any], call_no: int) -> dict[str, Any]:
        ev_id = payload["event_metadata"]["event_id"]
        mkt_id = payload["market_snapshot"]["snapshot_id"]
        if call_no == 1:
            return {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Hawkish Fed speech watches 1.15580 downside breakout.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.15580", "Dovish Fed reversal"],
                "confidence": 0.68,
                "evidence_role": "support",
                "evidence_quote": "rates may need to stay restrictive for longer",
                "portfolio_action": "HOLD",
                "target_direction": "flat",
                "target_weight": "0",
                "target_thesis_ids": [],
                "strategy_hypothesis": "fundamental_breakout",
                "cited_evidence_ids": [ev_id, mkt_id],
                "rationale": "Monitor 15m low 1.15580 for downside breakout confirmation.",
            }
        prior_id = payload["prior_memory_theses_do_not_quote"][0]["thesis_id"]
        return {
            "memory_action": "update",
            "thesis_id": prior_id,
            "status": "active",
            "narrative": "Downside breakout below 1.15580 confirmed with short-window momentum.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["EUR/USD >= 1.15720", "Dovish Fed reversal"],
            "confidence": 0.80,
            "evidence_role": "support",
            "evidence_quote": "rates may need to stay restrictive for longer",
            "portfolio_action": "SET_TARGET",
            "target_direction": "short",
            "target_weight": "-0.15",
            "target_thesis_ids": ["__current__"],
            "strategy_hypothesis": "fundamental_breakout",
            "cited_evidence_ids": [ev_id, mkt_id],
            "rationale": "Breakout below 1.15580 confirmed by 1m -2.5 pips and 5m -5.0 pips.",
        }

    client = _RecordingClient(handle_llm)
    risk_pol = RiskPolicy.pepperstone_razor_200_guarded(max_quote_age_ms=8000)
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200(max_quote_age_ms=8000)
    mem = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=clock, allow_targets=True)
    store = SQLiteRuntimeStore(tmp_path / "shd.sqlite")
    ledger = SQLiteShadowLedger(
        tmp_path / "shd.sqlite",
        risk_policy=risk_pol,
        exit_policy=exit_pol,
        cost_model=ShadowCostModel.pepperstone_razor(),
        initial_cash_usd=Decimal("200.00"),
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
                enabled=True,
                breakout_excursion_pips=Decimal("8"),
                cooldown_seconds=60.0,
                max_reviews_per_thesis=3,
                max_total_review_calls=1,
            ),
            clock=clock,
        )
        # Simulate continuous mode (`_run_duration_seconds is None`) with 1 review already used on Day 1!
        runner._run_duration_seconds = None
        dummy_ctx = ReviewContext(
            review_id="prior-day-rev",
            trigger_kind="periodic_review",
            trigger_detail="prior day review",
            triggered_at=t_day1 - timedelta(minutes=30),
            thesis_id="older-thesis",
            thesis_revision=1,
            origin_event_id="older-ev",
            origin_source="investinglive_fx",
            origin_source_timestamp=t_day1 - timedelta(hours=1),
            origin_received_timestamp=t_day1 - timedelta(hours=1),
            origin_event_age_seconds=1800.0,
        )
        store.record_review_started(
            review_id="prior-day-rev",
            dedup_key="older-thesis:r1:periodic:1",
            review_context=dummy_ctx,
            market_snapshot_id="mkt-old",
            market_mid=Decimal("1.15800"),
            now=t_day1 - timedelta(minutes=30),
        )

        # Call 1 on Day 1: hawkish Fed news creates active HOLD thesis with "EUR/USD <= 1.15580"
        ev1 = Event(
            event_id="investinglive:fed:hammack:r1",
            source="investinglive_fx",
            source_type="news",
            source_reference="https://investinglive.com/forex/fed-hammack",
            source_timestamp=t_day1 - timedelta(seconds=2),
            received_timestamp=t_day1,
            raw_text="Fed speaker says inflation is sticky and rates may need to stay restrictive for longer.",
        )
        c1 = asyncio.run(rt.run_cycle(ev1))
        assert c1.status == "no_target"

        # On Day 1 at 23:55 UTC, daily review cap (1) is exhausted -> step_reviews waits
        now_box[0] = t_day1 + timedelta(minutes=5)
        mid_box[0] = Decimal("1.15550")
        assert asyncio.run(runner.step_reviews()) == ()

        # Roll past UTC midnight into Day 2 -> daily review budget resets automatically,
        # "EUR/USD <= 1.15580" triggers structured_price_condition, converts to short SET_TARGET, and fills!
        now_box[0] = datetime(2026, 11, 13, 0, 5, 0, tzinfo=UTC)
        rev_results = asyncio.run(runner.step_reviews())
        assert len(rev_results) == 1
        assert rev_results[0].status == "shadow_recorded"
        now_box[0] += timedelta(seconds=1)
        exits, fills = asyncio.run(runner.step_quotes_and_fills())
        assert len(exits) == 0
        assert len(fills) == 1
        snap = ledger.snapshot_sync(market=_live_market(clock(), mid=mid_box[0]))
        assert len(snap.positions) == 1
        assert snap.positions[0].quantity < Decimal("0")
    finally:
        ledger.close()
        store.close()
