"""Failing-first reproduction and verification suite for the 6 post-v0.15.5 audit findings (baseline 9282f44).

Covers:
1. Stateful re-evaluation (review_at + structured price triggers vs. non-executable natural language,
   origin timestamp/age preservation, dedup, cooldown, budget, stop control, non-blocking protection).
2. Historical bar replay & synthetic provenance isolation (no fake live_poll/40ms freshness on 1m bars,
   synthetic_assumption=True on non-verified quotes, minute-bar resolution limits, arrival sensitivity).
3. Objective diagnostic time & coverage semantics (bounded anchor offset, 30m outage -> insufficient_coverage
   instead of 0.0 pips, stale quote labeling, session isolation, causal vs. post-event quote, consistent cutoff).
4. Publication timestamp & latency taxonomy (original_published_at unknown stays None, schedule_to_receipt vs.
   publication lag, modified_at for revisions, missing Consensus never substituted by Previous).
5. Strictly read-only shadow status/report (no missing-DB creation, no schema migration, no shadow_daily_state
   writes, no pending-order cancellations, byte-identical SQLite before/after).
6. Four-dimensional source health evaluation (HTTP speed, cache headers, content updates, verifiable lag;
   X-Cache: Miss != zero-lag proof; quiet_market vs. fault vs. degraded_cache_or_lag; DOM breakage detection;
   stale source blocks new risk while preserving CLOSE and protective exits).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from email.utils import format_datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from eventlens.contracts import (
    AgentDecision,
    Event,
    ExecutionReceipt,
    MarketSnapshot,
    Quote,
    RiskAssessment,
    TargetPortfolio,
    TargetPosition,
    Thesis,
    ThesisChange,
)
from eventlens.deepseek import ModelResponse
from eventlens.observer import CaptureBatch, MacroValues, ObserverStore, SourceRecord
from eventlens.quotes import (
    BridgeQuote,
    BridgeStart,
    QuoteQualityPolicy,
    QuoteStore,
    validate_market_quote,
)
from eventlens.runtime import IntradayEventRuntime, ShadowRunner, SQLiteRuntimeStore
from eventlens.shadow_cli import _build_objective_diagnostics
from eventlens.shadow_cli import main as shadow_main
from eventlens.sources.english_tier1 import (
    INVESTINGLIVE_LIVE_FEED_URL,
    EURUSDEventFilter,
    parse_forexfactory_calendar,
    parse_investinglive_live_html,
)
from eventlens.sources.wallstreetcn import parse_capture
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import SinglePassTradingAgent
from eventlens.trading.execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    SQLiteShadowLedger,
)
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy

SESSION_A = UUID("11111111-1111-4111-8111-111111111111")
SESSION_B = UUID("22222222-2222-4222-8222-222222222222")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fresh_live_quote(mid: str, now: datetime, *, spread_pips: str = "0.2") -> MarketSnapshot:
    half = (Decimal(spread_pips) * Decimal("0.0001")) / Decimal("2")
    mid_d = Decimal(mid)
    return MarketSnapshot(
        snapshot_id=f"mkt-{int(now.timestamp() * 1000)}",
        as_of=now,
        quotes=(
            Quote(
                instrument="EUR/USD",
                source_at=now - timedelta(milliseconds=50),
                bridge_at=now - timedelta(milliseconds=20),
                received_at=now - timedelta(milliseconds=5),
                available_at=now,
                source_provider="ctrader_algo",
                basis="live_poll",
                bid=mid_d - half,
                ask=mid_d + half,
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
        ),
    )


class _ScriptedModelClient:
    provider = "mock-deepseek"
    model = "deepseek-reasoner"

    def __init__(self, handler: Any) -> None:
        self._handler = handler
        self.calls: list[dict[str, Any]] = []

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        parsed_user = json.loads(user)
        self.calls.append(parsed_user)
        payload = await self._handler(parsed_user, len(self.calls))
        return ModelResponse(
            content=json.dumps(payload),
            model_id="deepseek-reasoner",
            system_fingerprint="fp-v0156",
            input_tokens=400,
            output_tokens=90,
        )


# ==============================================================================
# Issue 1: Stateful Re-evaluation Mechanism
# ==============================================================================


def test_issue1_condition_parser_separates_structured_price_rules_from_natural_language() -> None:
    import eventlens.runtime as rt

    parse_cond = getattr(rt, "parse_invalidation_condition", None)
    assert parse_cond is not None, "Missing parse_invalidation_condition in runtime"

    ok_ge = parse_cond("EUR/USD >= 1.1145")
    assert ok_ge.executable is True
    assert ok_ge.status == "executable_price_rule"
    assert ok_ge.condition is not None
    assert ok_ge.condition.instrument == "EUR/USD"
    assert ok_ge.condition.operator == ">="
    assert ok_ge.condition.level == Decimal("1.1145")

    ok_le = parse_cond("EUR/USD bid <= 1.1105")
    assert ok_le.executable is True
    assert ok_le.condition is not None
    assert ok_le.condition.operator == "<="
    assert ok_le.condition.reference == "bid"
    assert ok_le.condition.level == Decimal("1.1105")

    for prose in (
        "ECB rhetoric turns dovish in Q&A",
        "EUR/USD loses breakout support",
        "EUR/USD >= 1.1145 on daily close",
        "DXY rallies above 104.50",
    ):
        nl_res = parse_cond(prose)
        assert nl_res.executable is False
        assert nl_res.status == "unexecutable_natural_language"
        assert nl_res.condition is None


def test_issue1_breakout_and_review_at_trigger_once_preserve_origin_age_dedup_budget_and_no_auto_trade(
    tmp_path: Path,
) -> None:
    import eventlens.runtime as rt

    review_policy_cls = getattr(rt, "ReviewPolicy", None)
    assert review_policy_cls is not None, "Missing ReviewPolicy for stateful reviews"

    t0 = datetime(2026, 10, 29, 14, 0, 0, tzinfo=UTC)
    clock_now = t0
    current_mid = "1.1130"

    def clock() -> datetime:
        return clock_now

    def market_provider(as_of: datetime) -> MarketSnapshot:
        return _fresh_live_quote(current_mid, as_of)

    async def scripted_llm(user_ctx: dict[str, Any], call_no: int) -> dict[str, Any]:
        if call_no == 1:
            return {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "ECB hawkish debate watches 1.1145 upside breakout.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": [
                    "EUR/USD >= 1.1145",
                    "ECB rhetoric turns dovish in Q&A",
                ],
                "confidence": 0.65,
                "evidence_role": "support",
                "evidence_quote": "There was a debate about a hike",
                "portfolio_action": "HOLD",
                "target_direction": "flat",
                "target_weight": "0",
                "target_thesis_ids": [],
                "rationale": "Hold initially while monitoring 1.1145 level.",
            }
        # Subsequent stateful review calls: return HOLD (breakout != automatic BUY/SELL!)
        prior_id = user_ctx["prior_memory_theses_do_not_quote"][0]["thesis_id"]
        return {
            "memory_action": "update",
            "thesis_id": prior_id,
            "status": "active",
            "narrative": "Re-evaluated after trigger; still abstaining without fresh catalyst.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": [
                "EUR/USD >= 1.1165",
                "ECB rhetoric turns dovish in Q&A",
            ],
            "confidence": 0.55,
            "evidence_role": "support",
            "evidence_quote": "There was a debate about a hike",
            "portfolio_action": "HOLD",
            "target_direction": "flat",
            "target_weight": "0",
            "target_thesis_ids": [],
            "rationale": "Breakout reviewed; spread/follow-through unconvincing, stay flat.",
        }

    client = _ScriptedModelClient(scripted_llm)
    mem = SQLiteThesisMemory(tmp_path / "memory.sqlite", clock=clock, allow_targets=True)
    state_store = SQLiteRuntimeStore(tmp_path / "shadow.sqlite")
    ledger = SQLiteShadowLedger(
        tmp_path / "shadow.sqlite",
        risk_policy=RiskPolicy.pepperstone_razor_200(),
        exit_policy=ProtectiveExitPolicy.pepperstone_razor_200(),
        cost_model=ShadowCostModel(),
        initial_cash_usd=Decimal("200"),
        clock=clock,
    )
    ostore = ObserverStore(tmp_path / "observer.sqlite", clock=clock)
    try:
        agent = SinglePassTradingAgent(
            client,
            max_position_weight=Decimal("0.25"),
            allow_commission_bearing_zero_spread=True,
            review_horizon=timedelta(minutes=10),
            clock=clock,
        )
        runtime = IntradayEventRuntime(
            memory=mem,
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(
                RiskPolicy.pepperstone_razor_200(), clock=clock
            ),
            shadow_executor=ledger,
            market_provider=market_provider,
            event_filter=EURUSDEventFilter(),
            state_store=state_store,
            clock=clock,
        )
        runner = ShadowRunner(
            runtime=runtime,
            ledger=ledger,
            observer_store=ostore,
            clock=clock,
            strict_window_coverage=False,
            review_policy=review_policy_cls(
                enabled=True,
                enable_review_at=True,
                enable_price_conditions=True,
                breakout_excursion_pips=Decimal("30.0"),
                cooldown_seconds=120.0,
                max_reviews_per_thesis=5,
                max_total_review_calls=2,
            ),
        )

        # 1. Seed initial news event at t0 (source_timestamp = t0 - 5s, received_timestamp = t0)
        ev1 = Event(
            event_id="investinglive:news:il:lagarde-1:r1",
            source="investinglive_news",
            source_type="news",
            source_reference="https://investinglive.com/central-banks/lagarde/",
            source_timestamp=t0 - timedelta(seconds=5),
            received_timestamp=t0,
            raw_text="[INVESTINGLIVE] Lagarde Q&A: There was a debate about a hike.",
        )
        res1 = asyncio.run(runtime.run_cycle(ev1))
        assert res1.status == "no_target"
        assert len(client.calls) == 1

        # 2. Ticks below 1.1145 with ZERO new news -> 0 review calls
        clock_now = t0 + timedelta(minutes=1)
        current_mid = "1.1140"
        rev_none = asyncio.run(runner.step_reviews())
        assert rev_none == ()
        assert len(client.calls) == 1

        # 3. Price crosses >= 1.1145 at t0 + 3m (still ZERO new news!) -> triggers ONE stateful review
        clock_now = t0 + timedelta(minutes=3)
        current_mid = "1.1148"
        rev_hit = asyncio.run(runner.step_reviews())
        assert len(rev_hit) == 1
        assert len(client.calls) == 2

        # Verify review context sent to LLM: NOT disguised as fresh news; original timestamps & age preserved
        review_llm_ctx = client.calls[1]
        assert review_llm_ctx["event_metadata"]["source"] == "internal_stateful_review"
        assert review_llm_ctx["event_metadata"]["source_type"] == "internal_review"
        assert (
            datetime.fromisoformat(review_llm_ctx["event_metadata"]["source_timestamp"])
            == ev1.source_timestamp
        )
        assert (
            datetime.fromisoformat(review_llm_ctx["event_metadata"]["received_timestamp"])
            == ev1.received_timestamp
        )
        r_ctx = review_llm_ctx["review_context"]
        assert r_ctx["is_internal_review"] is True
        assert r_ctx["trigger_kind"] == "structured_price_condition"
        assert r_ctx["origin_event_id"] == ev1.event_id
        assert r_ctx["origin_event_age_seconds"] == pytest.approx(180.0)
        cond_statuses = {c["raw_condition"]: c["status"] for c in r_ctx["condition_evaluations"]}
        assert cond_statuses["EUR/USD >= 1.1145"] == "executable_price_rule"
        assert cond_statuses["ECB rhetoric turns dovish in Q&A"] == "unexecutable_natural_language"

        # Original event in SQLiteThesisMemory must remain 100% unmodified
        stored_ev1 = asyncio.run(mem.get_event(ev1.event_id))
        assert stored_ev1 is not None
        assert stored_ev1.source_timestamp == ev1.source_timestamp
        assert stored_ev1.received_timestamp == ev1.received_timestamp

        # Breakout returned HOLD -> NEVER auto-trades
        snap_after = ledger.snapshot_sync(market=market_provider(clock_now))
        assert all(p.quantity == 0 for p in snap_after.positions)

        # Persisted runtime_reviews audit record has trigger, origin event, snapshot, model_version, outcome
        persisted_reviews = state_store.list_reviews()
        assert len(persisted_reviews) == 1
        assert persisted_reviews[0]["trigger_kind"] == "structured_price_condition"
        assert persisted_reviews[0]["origin_event_id"] == ev1.event_id
        assert persisted_reviews[0]["model_version"] == "deepseek-reasoner@fp-v0156"
        assert persisted_reviews[0]["portfolio_action"] == "HOLD"

        # 4. Repeated ticks during dedup / cooldown do NOT call the model again
        for extra_sec, px in ((5, "1.1149"), (30, "1.1151"), (90, "1.1168")):
            clock_now = t0 + timedelta(minutes=3, seconds=extra_sec)
            current_mid = px
            assert asyncio.run(runner.step_reviews()) == ()
            assert len(client.calls) == 2

        # 5. Scheduled review_at fires once after thesis.review_at (updated at t0+3m -> review_at = t0+13m)
        clock_now = t0 + timedelta(minutes=13, seconds=5)
        current_mid = "1.1150"
        rev_sched = asyncio.run(runner.step_reviews())
        assert len(rev_sched) == 1
        assert len(client.calls) == 3  # 1 initial news + 2 reviews == max_total_review_calls=2

        # 6. Call budget (max_total_review_calls=2) strictly blocks any 3rd review even when >= 1.1165 crosses
        clock_now = t0 + timedelta(minutes=20)
        current_mid = "1.1190"
        assert asyncio.run(runner.step_reviews()) == ()
        assert len(client.calls) == 3
    finally:
        ostore.close()
        state_store.close()
        ledger.close()


def test_issue1_stop_blocks_review_risk_and_hung_review_model_never_blocks_protective_exit(
    tmp_path: Path,
) -> None:
    import eventlens.runtime as rt

    review_policy_cls = getattr(rt, "ReviewPolicy", None)
    assert review_policy_cls is not None, "Missing ReviewPolicy"

    t0 = datetime(2026, 10, 29, 15, 0, 0, tzinfo=UTC)
    clock_now = t0
    current_mid = "1.1130"
    hang_gate = asyncio.Event()
    review_entered = asyncio.Event()

    def clock() -> datetime:
        return clock_now

    def market_provider(as_of: datetime) -> MarketSnapshot:
        return _fresh_live_quote(current_mid, as_of)

    async def hung_on_review_llm(user_ctx: dict[str, Any], call_no: int) -> dict[str, Any]:
        if call_no == 1:
            return {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Long EUR/USD with structured downside review at 1.1125.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1125"],
                "confidence": 0.75,
                "evidence_role": "support",
                "evidence_quote": "ECB hawkish signal",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.10",
                "target_thesis_ids": ["__current__"],
                "rationale": "Open bounded long.",
            }
        review_entered.set()
        await hang_gate.wait()
        prior_id = user_ctx["prior_memory_theses_do_not_quote"][0]["thesis_id"]
        return {
            "memory_action": "update",
            "thesis_id": prior_id,
            "status": "active",
            "narrative": "Attempted add after stop.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["EUR/USD <= 1.1100"],
            "confidence": 0.70,
            "evidence_role": "support",
            "evidence_quote": "ECB hawkish signal",
            "portfolio_action": "SET_TARGET",
            "target_direction": "long",
            "target_weight": "0.20",
            "target_thesis_ids": ["__current__"],
            "rationale": "Attempt risk increase.",
        }

    client = _ScriptedModelClient(hung_on_review_llm)
    mem = SQLiteThesisMemory(tmp_path / "memory.sqlite", clock=clock, allow_targets=True)
    state_store = SQLiteRuntimeStore(tmp_path / "shadow.sqlite")
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200().model_copy(
        update={"stop_loss_pips": Decimal("10.0")}
    )
    ledger = SQLiteShadowLedger(
        tmp_path / "shadow.sqlite",
        risk_policy=RiskPolicy.pepperstone_razor_200(),
        exit_policy=exit_pol,
        cost_model=ShadowCostModel(),
        initial_cash_usd=Decimal("200"),
        clock=clock,
    )
    try:
        agent = SinglePassTradingAgent(
            client,
            max_position_weight=Decimal("0.25"),
            allow_commission_bearing_zero_spread=True,
            clock=clock,
        )
        runtime = IntradayEventRuntime(
            memory=mem,
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(
                RiskPolicy.pepperstone_razor_200(), clock=clock
            ),
            shadow_executor=ledger,
            market_provider=market_provider,
            state_store=state_store,
            clock=clock,
        )
        runner = ShadowRunner(
            runtime=runtime,
            ledger=ledger,
            clock=clock,
            llm_timeout_seconds=5.0,
            strict_window_coverage=False,
            review_policy=review_policy_cls(
                enabled=True,
                enable_price_conditions=True,
                cooldown_seconds=60.0,
                max_total_review_calls=3,
            ),
        )

        # Open initial long position (+4,000 EUR around 1.1130)
        ev1 = Event(
            event_id="ecb:press:1:r1",
            source="ecb_press",
            source_type="macro",
            source_reference="https://www.ecb.europa.eu/press/1",
            source_timestamp=t0 - timedelta(seconds=2),
            received_timestamp=t0,
            raw_text="ECB hawkish signal lifts rates outlook.",
        )
        r1 = asyncio.run(runtime.run_cycle(ev1))
        assert r1.status == "shadow_recorded"
        clock_now = t0 + timedelta(seconds=1)
        fills = ledger.fill_pending(market_provider(clock_now))
        assert len(fills) == 1
        assert fills[0].delta_quantity > 0

        async def _concurrent_hung_review_and_protection() -> None:
            nonlocal clock_now, current_mid
            clock_now = t0 + timedelta(seconds=30)
            # Drops > 10 pips below 1.1130 -> triggers BOTH review (<= 1.1125) AND stop_loss_pips (10p)
            current_mid = "1.1116"
            review_task = asyncio.create_task(runner.step_reviews())
            await asyncio.wait_for(review_entered.wait(), timeout=1.0)
            # While review LLM is completely hung, step_protection MUST succeed immediately!
            _expired, exits = await asyncio.wait_for(runner.step_protection(), timeout=0.5)
            assert len(exits) == 1
            assert exits[0].reason == "protective_stop_loss"
            snap_mid = ledger.snapshot_sync(market=market_provider(clock_now))
            assert all(p.quantity == 0 for p in snap_mid.positions)

            # Request stop while review is still hung, then release hang_gate -> must NOT add risk!
            runner.stop()
            state_store.request_stop(clock_now)
            hang_gate.set()
            await review_task

        asyncio.run(_concurrent_hung_review_and_protection())
        snap_final = ledger.snapshot_sync(market=market_provider(clock_now))
        assert all(p.quantity == 0 for p in snap_final.positions)
        assert snap_final.pending_order_ids == ()

        # Calling step_reviews after stop does nothing and never invokes the model again
        calls_before = len(client.calls)
        assert asyncio.run(runner.step_reviews()) == ()
        assert len(client.calls) == calls_before
    finally:
        state_store.close()
        ledger.close()


# ==============================================================================
# Issue 2: Historical Replay & Synthetic Data Provenance
# ==============================================================================


def test_issue2_minute_bar_and_synthetic_quotes_cannot_fake_live_provenance_or_freshness(
    tmp_path: Path,
) -> None:
    # 1. Synthetic / bar-derived quotes must NEVER have verified provenance or produce synthetic_assumption=False fills
    now = datetime(2026, 10, 28, 18, 0, 45, tzinfo=UTC)
    synth_quote = Quote(
        instrument="EUR/USD",
        source_at=now - timedelta(milliseconds=40),
        bridge_at=now - timedelta(milliseconds=20),
        received_at=now,
        available_at=now,
        source_provider="yahoo_finance_1m_ohlc",
        basis="synthetic_fixture",
        bid=Decimal("1.08100"),
        ask=Decimal("1.08120"),
    )
    assert synth_quote.has_verified_provenance is False
    live_check = validate_market_quote(
        MarketSnapshot(snapshot_id="m-synth", as_of=now, quotes=(synth_quote,)),
        as_of=now,
        policy=QuoteQualityPolicy(
            instrument="EUR/USD",
            max_quote_age_ms=2000,
            require_verified_provenance=True,
        ),
    )
    assert live_check.valid is False

    # Execute a shadow fill on a non-verified / synthetic quote -> synthetic_assumption MUST be True!
    ledger_now = now
    ledger = SQLiteShadowLedger(
        tmp_path / "synth-ledger.sqlite",
        risk_policy=RiskPolicy.pepperstone_razor_200(),
        initial_cash_usd=Decimal("200"),
        clock=lambda: ledger_now,
    )
    try:
        mkt = MarketSnapshot(snapshot_id="m-synth", as_of=now, quotes=(synth_quote,))
        port = ledger.snapshot_sync(market=mkt)
        th = Thesis(
            thesis_id="th-1",
            revision=1,
            status="active",
            narrative="Test",
            instruments=("EUR/USD",),
            supporting_event_ids=("ev-1",),
            invalidation_conditions=("EUR/USD <= 1.0790",),
            confidence=0.7,
            created_at=now - timedelta(seconds=2),
            updated_at=now - timedelta(seconds=2),
            review_at=now + timedelta(hours=1),
        )
        dec = AgentDecision(
            decision_id="dec-synth-1",
            event_id="ev-1",
            based_on_memory_version=0,
            based_on_snapshot_id=port.snapshot_id,
            provider="mock",
            model_version="mock-v1",
            prompt_version="single-pass-agent-v2",
            started_at=now - timedelta(seconds=2),
            completed_at=now - timedelta(seconds=1),
            valid_until=now + timedelta(minutes=2),
            thesis_changes=(ThesisChange(expected_revision=0, thesis=th, reason="init"),),
            target=TargetPortfolio(
                target_id="tgt-synth-1",
                based_on_snapshot_id=port.snapshot_id,
                positions=(
                    TargetPosition(
                        instrument="EUR/USD",
                        weight=Decimal("0.10"),
                        thesis_ids=("th-1",),
                    ),
                ),
                rationale="Test fill provenance",
            ),
            portfolio_action="SET_TARGET",
            rationale="Test fill provenance",
        )
        assess = RiskAssessment(
            assessment_id="ass-synth-1",
            decision_id=dec.decision_id,
            target_id="tgt-synth-1",
            checked_snapshot_id=port.snapshot_id,
            policy_version="pepperstone-razor-200-v1",
            checked_at=now - timedelta(milliseconds=500),
            valid_until=now + timedelta(seconds=30),
            outcome="approve",
            reasons=("all_hard_risk_checks_passed",),
        )
        rcpt: ExecutionReceipt = asyncio.run(ledger.record(dec, assess))
        assert rcpt.status == "recorded"
        ledger_now = now + timedelta(milliseconds=100)
        fills = ledger.fill_pending(
            MarketSnapshot(
                snapshot_id="m-synth-2",
                as_of=ledger_now,
                quotes=(
                    synth_quote.model_copy(
                        update={
                            "source_at": ledger_now - timedelta(milliseconds=20),
                            "bridge_at": ledger_now - timedelta(milliseconds=10),
                            "received_at": ledger_now,
                            "available_at": ledger_now,
                        }
                    ),
                ),
            )
        )
        assert len(fills) == 1
        assert fills[0].synthetic_assumption is True
    finally:
        ledger.close()

    # 2. Historical 1-minute bar replay helper & repo artifacts must enforce bar-timestamp honesty
    from eventlens import replay_provenance as rp

    bar_open = datetime(2026, 10, 28, 18, 0, 0, tzinfo=UTC)
    bar_q = rp.build_historical_bar_quote(
        bar_open_at=bar_open,
        price=Decimal("1.08150"),
        assumed_spread_pips=Decimal("0.2"),
        bar_interval_seconds=60,
        observed_at=bar_open + timedelta(seconds=45),
    )
    assert bar_q.basis == "historical_bar_replay"
    assert bar_q.source_at == bar_open  # NEVER refreshed to observed_at - 40ms!
    assert bar_q.has_verified_provenance is False
    # Under a strict 2-second live freshness gate at +45s into the minute bar, it MUST be rejected as stale!
    fresh_2s = validate_market_quote(
        MarketSnapshot(
            snapshot_id="m-bar", as_of=bar_open + timedelta(seconds=45), quotes=(bar_q,)
        ),
        as_of=bar_open + timedelta(seconds=45),
        policy=QuoteQualityPolicy(instrument="EUR/USD", max_quote_age_ms=2000),
    )
    assert fresh_2s.valid is False
    assert fresh_2s.reason == "stale_market_quote"

    repo_root = Path(__file__).resolve().parents[1]
    assumptions_path = repo_root / "configs" / "fomc_2026_10_28_replay_assumptions.json"
    errata_path = repo_root / "docs" / "replay-errata-2026-10-28.md"
    replay_script = repo_root / "scripts" / "run_historical_bar_replay.py"
    assert assumptions_path.is_file()
    assert errata_path.is_file()
    assert replay_script.is_file()

    audit_summary = rp.evaluate_bar_replay_assumptions(
        json.loads(assumptions_path.read_text(encoding="utf-8"))
    )
    assert audit_summary["eligible_as_live_forward_evidence"] is False
    assert audit_summary["second_level_execution_conclusions"] == "not_evaluable_from_minute_bars"
    assert audit_summary["lookahead_bias_excluded"] is False
    assert len(audit_summary["arrival_time_sensitivity"]) >= 3


# ==============================================================================
# Issue 3: Objective Diagnostic Report Time & Coverage Semantics
# ==============================================================================


def test_issue3_diagnostics_30m_outage_stale_quotes_cross_session_missing_anchor_and_future_cutoff(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "diag_audit"
    run_dir.mkdir(parents=True)
    t0 = datetime(2026, 10, 29, 13, 0, 0, tzinfo=UTC)
    q_clock = t0 - timedelta(hours=2)

    qstore = QuoteStore(run_dir / "live-quotes.sqlite", clock=lambda: q_clock)
    ostore = ObserverStore(run_dir / "observer.sqlite", clock=lambda: t0 + timedelta(hours=2))
    mem = SQLiteThesisMemory(
        run_dir / "shadow-memory.sqlite",
        clock=lambda: t0 + timedelta(hours=2),
        allow_targets=True,
    )
    state_store = SQLiteRuntimeStore(run_dir / "shadow-ledger.sqlite")
    try:
        # Session A (older session at 1.10000 - MUST NOT be stitched into Session B range!)
        q_clock = t0 - timedelta(hours=2)
        qstore.start(
            BridgeStart(
                version=1,
                kind="start",
                session_id=SESSION_A,
                account_mode="demo",
                running_mode="realtime",
                symbol="EURUSD",
                started_at=q_clock,
                sample_interval_ms=200,
                duration_seconds=600,
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
            q_clock,
            basis="live_poll",
        )
        qstore.record(
            BridgeQuote(
                version=1,
                kind="quote",
                session_id=SESSION_A,
                sequence=1,
                symbol="EURUSD",
                source_at=q_clock,
                bridge_at=q_clock,
                bid=Decimal("1.10000"),
                ask=Decimal("1.10010"),
            ),
            q_clock,
            '{"kind":"quote"}',
        )

        # Session B starts at t0 - 1m
        q_clock = t0 - timedelta(minutes=1)
        qstore.start(
            BridgeStart(
                version=1,
                kind="start",
                session_id=SESSION_B,
                account_mode="demo",
                running_mode="realtime",
                symbol="EURUSD",
                started_at=q_clock,
                sample_interval_ms=200,
                duration_seconds=7200,
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
            q_clock,
            basis="live_poll",
        )
        # Quote 1 right before Event 1 (t0 - 1s at 1.11200), then 30-MINUTE OUTAGE until t0 + 30m!
        session_b_quotes = [
            (t0 - timedelta(seconds=1), "1.11200"),
            (t0 + timedelta(minutes=30), "1.11500"),
            (t0 + timedelta(minutes=30, seconds=2), "1.11500"),
            # Future quote beyond cutoff (cutoff will be t0 + 70m; quote at t0 + 90m must be excluded!)
            (t0 + timedelta(minutes=90), "1.12500"),
        ]
        for seq, (ts, px) in enumerate(session_b_quotes, start=1):
            q_clock = ts
            qstore.record(
                BridgeQuote(
                    version=1,
                    kind="quote",
                    session_id=SESSION_B,
                    sequence=seq,
                    symbol="EURUSD",
                    source_at=ts - timedelta(milliseconds=30),
                    bridge_at=ts - timedelta(milliseconds=10),
                    bid=Decimal(px),
                    ask=Decimal(px),
                ),
                ts,
                '{"kind":"quote"}',
            )

        # Event 1 received at t0 (followed by a 30m quote outage!)
        ev_outage = Event(
            event_id="ecb:press:outage:r1",
            source="ecb_press",
            source_type="macro",
            source_reference="https://www.ecb.europa.eu/press/outage",
            source_timestamp=t0 - timedelta(seconds=3),
            received_timestamp=t0,
            raw_text="ECB statement during 30m quote outage.",
        )
        asyncio.run(mem.record_event(ev_outage))
        state_store.upsert_event_state(
            event_id=ev_outage.event_id,
            source_reference=ev_outage.source_reference,
            received_at=t0,
            now=t0 + timedelta(seconds=1),
            state="no_target",
            reason="portfolio_action_hold",
            decision_json=json.dumps({"portfolio_action": "HOLD"}),
        )

        # Event 2 received at t0 + 35m (NO quotes after t0 + 30m02s within cutoff -> missing anchor!)
        ev_no_anchor = Event(
            event_id="ecb:press:noanchor:r1",
            source="ecb_press",
            source_type="macro",
            source_reference="https://www.ecb.europa.eu/press/noanchor",
            source_timestamp=t0 + timedelta(minutes=34, seconds=55),
            received_timestamp=t0 + timedelta(minutes=35),
            raw_text="ECB Q&A after quotes stopped.",
        )
        asyncio.run(mem.record_event(ev_no_anchor))
        state_store.upsert_event_state(
            event_id=ev_no_anchor.event_id,
            source_reference=ev_no_anchor.source_reference,
            received_at=ev_no_anchor.received_timestamp,
            now=ev_no_anchor.received_timestamp + timedelta(seconds=1),
            state="expired",
            reason="event_expired_awaiting_quote:stale_quote",
        )

        # Future Event 3 at t0 + 85m (beyond cutoff = t0 + 70m -> MUST be excluded!)
        ev_future = Event(
            event_id="ecb:press:future:r1",
            source="ecb_press",
            source_type="macro",
            source_reference="https://www.ecb.europa.eu/press/future",
            source_timestamp=t0 + timedelta(minutes=84),
            received_timestamp=t0 + timedelta(minutes=85),
            raw_text="Future event after cutoff.",
        )
        asyncio.run(mem.record_event(ev_future))
        state_store.upsert_event_state(
            event_id=ev_future.event_id,
            source_reference=ev_future.source_reference,
            received_at=ev_future.received_timestamp,
            now=ev_future.received_timestamp + timedelta(seconds=1),
            state="no_target",
            reason="portfolio_action_hold",
        )
    finally:
        qstore.close()
        ostore.close()
        state_store.close()

    cutoff = t0 + timedelta(minutes=70)  # ~40 minutes after latest pre-cutoff quote (t0 + 30m02s)
    diag_store = SQLiteRuntimeStore(run_dir / "shadow-ledger.sqlite")
    try:
        diag = _build_objective_diagnostics(
            state_store=diag_store,
            memory_db=run_dir / "shadow-memory.sqlite",
            observer_db=run_dir / "observer.sqlite",
            quotes_db=run_dir / "live-quotes.sqlite",
            basis="live_poll",
            now=cutoff,
        )
    finally:
        diag_store.close()

    # 1. Stale quote & cutoff vs. latest_quote_at separation (future quote 1.12500 at t0+90m excluded!)
    mw = diag["market_window"]
    assert mw is not None
    assert mw["query_cutoff_at"] == cutoff.isoformat()
    assert mw["latest_quote_at"] == (t0 + timedelta(minutes=30, seconds=2)).isoformat()
    assert mw["is_stale"] is True
    assert mw["market_status"] == "stale_quote"
    assert mw["realizable_pnl_claim"] is False
    assert mw["quote_age_seconds"] == pytest.approx(2398.0)

    # 2. Cross-session isolation: Session A (1.10000) MUST NOT pollute Session B range!
    sr = diag["session_range"]
    assert sr is not None
    assert sr["session_id"] == str(SESSION_B)
    assert sr["low_mid"] == "1.11200"
    assert sr["high_mid"] == "1.11500"
    assert sr["realizable_pnl_claim"] is False

    # 3. Future event excluded by consistent cutoff
    event_ids = [item["event_id"] for item in diag["recent_events"]]
    assert "ecb:press:future:r1" not in event_ids
    by_ev = {item["event_id"]: item for item in diag["recent_events"]}

    # 4. 30-minute outage after Event 1: MUST NOT claim 0.0 pips from an anchor 1800s later!
    outage_ev = by_ev["ecb:press:outage:r1"]
    assert outage_ev["quote_at_event"] is not None
    assert outage_ev["quote_at_event"]["mid"] == "1.11200"
    post_outage = outage_ev["post_receipt_move"]
    assert post_outage["status"] == "insufficient_coverage"
    assert post_outage["anchor_offset_seconds"] == pytest.approx(1800.0)
    assert post_outage["max_up_pips"] is None
    assert post_outage["net_change_pips"] is None
    assert post_outage["realizable_pnl_claim"] is False

    # 5. Missing post-event anchor on Event 2: status == "unknown" (not None / 0.0)
    no_anchor_ev = by_ev["ecb:press:noanchor:r1"]
    post_missing = no_anchor_ev["post_receipt_move"]
    assert post_missing["status"] == "unknown"
    assert post_missing["anchor_at"] is None
    assert post_missing["max_up_pips"] is None


# ==============================================================================
# Issue 4: Publication Timestamp & Latency Taxonomy + Missing Consensus
# ==============================================================================


def test_issue4_timestamp_taxonomy_revision_lag_schedule_delta_and_missing_consensus(
    tmp_path: Path,
) -> None:
    t0 = datetime(2026, 10, 29, 13, 53, 38, tzinfo=UTC)
    mod_t = datetime(2026, 10, 29, 14, 5, 12, tzinfo=UTC)
    rx_t = datetime(2026, 10, 29, 14, 5, 20, tzinfo=UTC)

    # 1. parse_investinglive_live_html preserves modified_at separately from publisher_display_at and leaves original_published_at=None
    rsc_article = {
        "contentItemId": "lagarde-qa-rev",
        "contentType": "Article",
        "displayText": "Lagarde Q&A: Inflation risks discussed",
        "path": "central-banks/lagarde-qa-rev",
        "published": True,
        "publishedUtc": "2026-10-29T13:53:38Z",
        "modifiedUtc": "2026-10-29T14:05:12Z",
        "isSponsored": False,
        "category": {"termContentItems": [{"displayText": "Central Banks"}]},
        "briefSummary": {"html": "<p>ECB press conference</p>"},
        "summary": {"html": ""},
        "expandedContent": "<p>Updated Q&A bullet.</p>",
    }
    escaped = json.dumps([rsc_article]).replace('"', '\\"')
    html_payload = f'<!doctype html><html><body><script>self.__next_f.push([1,"5:{escaped}"])</script></body></html>'.encode()
    recs, rej, err = parse_investinglive_live_html(
        html_payload, stream="news/live_feed", received_at=rx_t
    )
    assert err is None and not rej and len(recs) == 1
    rec = recs[0]
    assert rec.original_published_at is None
    assert rec.publisher_display_at == t0
    assert getattr(rec, "modified_at", None) == mod_t

    # 2. Missing consensus NEVER substitutes previous in ForexFactory OR WallStreetCN
    ff_raw = json.dumps(
        [
            {
                "title": "German Prelim CPI m/m",
                "country": "EUR",
                "date": "2026-10-29T13:00:00+00:00",
                "impact": "High",
                "actual": "0.4%",
                "forecast": "",
                "previous": "0.2%",
            }
        ]
    ).encode()
    ff_recs, _, ff_err = parse_forexfactory_calendar(ff_raw, received_at=rx_t)
    assert ff_err is None and len(ff_recs) == 1
    assert ff_recs[0].macro is not None
    assert ff_recs[0].macro.reported_forecast is None
    assert ff_recs[0].macro.previous == "0.2%"
    assert "Consensus Forecast: MISSING" in ff_recs[0].text

    wscn_raw = json.dumps(
        {
            "code": 20000,
            "data": {
                "items": [
                    {
                        "id": 99,
                        "public_date": int(datetime(2026, 10, 29, 12, 30, tzinfo=UTC).timestamp()),
                        "country": "美国",
                        "title": "美国9月核心PCE物价指数同比",
                        "importance": 3,
                        "actual": "2.7%",
                        "forecast": "",
                        "previous": "2.6%",
                    }
                ]
            },
        },
        ensure_ascii=False,
    ).encode()
    wscn_recs, _, wscn_err = parse_capture(
        wscn_raw, kind="calendar", received_at=rx_t, day=date(2026, 10, 29)
    )
    assert wscn_err is None and len(wscn_recs) == 1
    assert wscn_recs[0].macro is not None
    assert wscn_recs[0].macro.reported_forecast is None
    assert wscn_recs[0].macro.previous == "2.6%"
    assert "MISSING" in wscn_recs[0].text

    # 3. Diagnostic timestamp & latency taxonomy across calendar, r2 with modified_at, and r2 without modified_at
    run_dir = tmp_path / "tax_run"
    run_dir.mkdir()
    obs_clock = rx_t
    ostore = ObserverStore(run_dir / "observer.sqlite", clock=lambda: obs_clock)
    mem = SQLiteThesisMemory(
        run_dir / "shadow-memory.sqlite", clock=lambda: rx_t + timedelta(seconds=5)
    )
    state_store = SQLiteRuntimeStore(run_dir / "shadow-ledger.sqlite")
    try:
        # Calendar item: scheduled 12:30:00Z, received 12:30:18Z
        cal_sched = datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)
        cal_rx = datetime(2026, 10, 29, 12, 30, 18, tzinfo=UTC)
        obs_clock = cal_rx
        ostore.record(
            CaptureBatch(
                provider="forexfactory",
                stream="calendar/thisweek",
                kind="calendar",
                basis="live_poll",
                request_url="https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                request_started_at=cal_rx - timedelta(milliseconds=100),
                received_at=cal_rx,
                elapsed_ms=100.0,
                attempts=1,
                payload=ff_raw,
                records=(
                    SourceRecord(
                        source_id=f"de-cpi@{int(cal_sched.timestamp())}",
                        kind="calendar",
                        source_url="https://www.forexfactory.com/calendar",
                        title="German Prelim CPI m/m",
                        text="German Prelim CPI m/m | Consensus Forecast: MISSING",
                        scheduled_at=cal_sched,
                        macro=MacroValues(
                            actual="0.4%",
                            reported_forecast=None,
                            previous="0.2%",
                        ),
                        importance=3,
                    ),
                ),
            )
        )

        # News r1 at t0, then r2 with modified_at = mod_t, plus another news r2 WITHOUT modified_at
        obs_clock = t0 + timedelta(seconds=4)
        ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                basis="live_poll",
                request_url=INVESTINGLIVE_LIVE_FEED_URL,
                request_started_at=t0 + timedelta(seconds=3),
                received_at=t0 + timedelta(seconds=4),
                elapsed_ms=100.0,
                attempts=1,
                payload=b"<html/>",
                records=(
                    SourceRecord(
                        source_id=rec.source_id,
                        kind="news",
                        source_url=rec.source_url,
                        title="Lagarde Q&A",
                        text="Initial bullet.",
                        publisher_display_at=t0,
                        modified_at=t0,
                        importance=3,
                    ),
                    SourceRecord(
                        source_id="ecb-nomod",
                        kind="news",
                        source_url="https://www.ecb.europa.eu/press/nomod",
                        title="ECB Note",
                        text="Initial note.",
                        publisher_display_at=t0,
                        importance=3,
                    ),
                ),
            )
        )
        obs_clock = rx_t
        ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                basis="live_poll",
                request_url=INVESTINGLIVE_LIVE_FEED_URL,
                request_started_at=rx_t - timedelta(milliseconds=100),
                received_at=rx_t,
                elapsed_ms=100.0,
                attempts=1,
                payload=b"<html>r2</html>",
                records=(
                    rec,
                    SourceRecord(
                        source_id="ecb-nomod",
                        kind="news",
                        source_url="https://www.ecb.europa.eu/press/nomod",
                        title="ECB Note",
                        text="Updated note without modified_at.",
                        publisher_display_at=t0,
                        importance=3,
                    ),
                ),
            )
        )

        for ev_obj in (
            Event(
                event_id=f"forexfactory:calendar:de-cpi@{int(cal_sched.timestamp())}:r1",
                source="forexfactory_calendar",
                source_type="macro",
                source_reference="https://www.forexfactory.com/calendar",
                source_timestamp=cal_sched,
                received_timestamp=cal_rx,
                raw_text="German Prelim CPI m/m",
            ),
            Event(
                event_id=f"investinglive:news:{rec.source_id}:r2",
                source="investinglive_news",
                source_type="news",
                source_reference=rec.source_url,
                source_timestamp=mod_t,
                received_timestamp=rx_t,
                raw_text="Lagarde Q&A r2",
            ),
            Event(
                event_id="investinglive:news:ecb-nomod:r2",
                source="investinglive_news",
                source_type="news",
                source_reference="https://www.ecb.europa.eu/press/nomod",
                source_timestamp=t0,
                received_timestamp=rx_t,
                raw_text="ECB Note r2",
            ),
        ):
            asyncio.run(mem.record_event(ev_obj))
            state_store.upsert_event_state(
                event_id=ev_obj.event_id,
                source_reference=ev_obj.source_reference,
                received_at=ev_obj.received_timestamp,
                now=ev_obj.received_timestamp + timedelta(seconds=1),
                state="no_target",
                reason="portfolio_action_hold",
            )
    finally:
        ostore.close()
        state_store.close()

    tax_store = SQLiteRuntimeStore(run_dir / "shadow-ledger.sqlite")
    try:
        diag = _build_objective_diagnostics(
            state_store=tax_store,
            memory_db=run_dir / "shadow-memory.sqlite",
            observer_db=run_dir / "observer.sqlite",
            quotes_db=run_dir / "live-quotes.sqlite",
            basis="live_poll",
            now=rx_t + timedelta(seconds=5),
        )
    finally:
        tax_store.close()
    ev_map = {e["event_id"]: e for e in diag["recent_events"]}

    cal_info = ev_map[f"forexfactory:calendar:de-cpi@{int(cal_sched.timestamp())}:r1"][
        "timestamp_taxonomy"
    ]
    assert cal_info["original_published_at"] is None
    assert cal_info["scheduled_at"] == cal_sched.isoformat()
    assert cal_info["latency_basis"] == "schedule_to_receipt"
    assert cal_info["schedule_to_receipt_seconds"] == pytest.approx(18.0)
    assert cal_info["publication_lag_seconds"] is None

    r2_mod = ev_map[f"investinglive:news:{rec.source_id}:r2"]["timestamp_taxonomy"]
    assert r2_mod["original_published_at"] is None
    assert r2_mod["publisher_display_at"] == t0.isoformat()
    assert r2_mod["modified_at"] == mod_t.isoformat()
    assert r2_mod["latency_basis"] == "modified_to_receipt"
    assert r2_mod["verifiable_lag_seconds"] == pytest.approx(8.0)  # NOT 702s from t0!

    r2_nomod = ev_map["investinglive:news:ecb-nomod:r2"]["timestamp_taxonomy"]
    assert r2_nomod["modified_at"] is None
    assert r2_nomod["latency_basis"] == "revision_without_modified_timestamp"
    assert r2_nomod["verifiable_lag_seconds"] is None


# ==============================================================================
# Issue 5: Truly Read-Only status / report
# ==============================================================================


def test_issue5_status_and_report_strictly_readonly_across_missing_db_legacy_schema_and_cross_day_pending_orders(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # 1. Missing shadow_db MUST NOT create directory or file on status or report!
    missing_dir = tmp_path / "does_not_exist"
    missing_db = missing_dir / "shadow-ledger.sqlite"
    for cmd in ("status", "report"):
        shadow_main(["--shadow-db", str(missing_db), cmd])
        out = json.loads(capsys.readouterr().out)
        assert not missing_db.exists()
        assert not missing_dir.exists()
        assert out["readonly"] is True
        assert out["degraded"] is True

    # 2. Legacy / unsupported schema MUST NOT run ALTER TABLE migrations!
    legacy_db = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(legacy_db)
    try:
        conn.execute("PRAGMA application_id = 0x45565348")
        conn.execute(
            "CREATE TABLE shadow_account (id INTEGER PRIMARY KEY, cash_usd TEXT NOT NULL, quantity_eur TEXT NOT NULL)"
        )
        conn.execute("INSERT INTO shadow_account VALUES (1, '200.00', '0')")
        conn.commit()
    finally:
        conn.close()
    legacy_cols_before = (
        sqlite3.connect(legacy_db).execute("PRAGMA table_info(shadow_account)").fetchall()
    )
    legacy_hash_before = _sha256_file(legacy_db)

    for cmd in ("status", "report"):
        shadow_main(["--shadow-db", str(legacy_db), cmd])
        out = json.loads(capsys.readouterr().out)
        assert out["readonly"] is True
        assert out["degraded"] is True
    assert (
        sqlite3.connect(legacy_db).execute("PRAGMA table_info(shadow_account)").fetchall()
        == legacy_cols_before
    )
    assert _sha256_file(legacy_db) == legacy_hash_before

    # 3. Cross-day ledger with yesterday's shadow_daily_state, an open position, and a pending order:
    # querying status/report today MUST NOT insert today's shadow_daily_state, update shadow_account, or cancel pending orders!
    cross_dir = tmp_path / "cross_day"
    cross_db = cross_dir / "shadow-ledger.sqlite"
    yesterday = datetime(2026, 10, 27, 14, 0, 0, tzinfo=UTC)
    state_store = SQLiteRuntimeStore(cross_db)
    ledger = SQLiteShadowLedger(
        cross_db,
        risk_policy=RiskPolicy.pepperstone_razor_200(),
        initial_cash_usd=Decimal("200"),
        clock=lambda: yesterday,
    )
    try:
        mkt_y = _fresh_live_quote("1.1120", yesterday)
        port_y = ledger.snapshot_sync(market=mkt_y)
        th = Thesis(
            thesis_id="th-ro",
            revision=1,
            status="active",
            narrative="Readonly test",
            instruments=("EUR/USD",),
            supporting_event_ids=("ev-ro",),
            invalidation_conditions=("EUR/USD <= 1.1100",),
            confidence=0.7,
            created_at=yesterday - timedelta(seconds=2),
            updated_at=yesterday - timedelta(seconds=2),
            review_at=yesterday + timedelta(hours=1),
        )
        dec = AgentDecision(
            decision_id="dec-ro-1",
            event_id="ev-ro",
            based_on_memory_version=0,
            based_on_snapshot_id=port_y.snapshot_id,
            provider="mock",
            model_version="mock-v1",
            prompt_version="single-pass-agent-v2",
            started_at=yesterday - timedelta(seconds=2),
            completed_at=yesterday - timedelta(seconds=1),
            valid_until=yesterday + timedelta(minutes=2),
            thesis_changes=(ThesisChange(expected_revision=0, thesis=th, reason="init"),),
            target=TargetPortfolio(
                target_id="tgt-ro-1",
                based_on_snapshot_id=port_y.snapshot_id,
                positions=(
                    TargetPosition(
                        instrument="EUR/USD",
                        weight=Decimal("0.10"),
                        thesis_ids=("th-ro",),
                    ),
                ),
                rationale="Pending order test",
            ),
            portfolio_action="SET_TARGET",
            rationale="Pending order test",
        )
        assess = RiskAssessment(
            assessment_id="ass-ro-1",
            decision_id=dec.decision_id,
            target_id="tgt-ro-1",
            checked_snapshot_id=port_y.snapshot_id,
            policy_version="pepperstone-razor-200-v1",
            checked_at=yesterday - timedelta(milliseconds=500),
            valid_until=yesterday + timedelta(seconds=30),
            outcome="approve",
            reasons=("all_hard_risk_checks_passed",),
        )
        asyncio.run(ledger.record(dec, assess))
        state_store.update_runner_state(
            status="stopped",
            now=yesterday + timedelta(seconds=10),
            stop_reason="runner_stop_requested",
        )
        # Switch out of WAL into DELETE journal so single-file SHA-256 captures exact byte state
        ledger.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        ledger.close()
        state_store.close()

    chk = sqlite3.connect(cross_db)
    try:
        chk.execute("PRAGMA journal_mode=DELETE")
        chk.commit()
        daily_before = chk.execute("SELECT * FROM shadow_daily_state").fetchall()
        acct_before = chk.execute("SELECT * FROM shadow_account").fetchall()
        rcpts_before = chk.execute(
            "SELECT execution_id, order_status FROM shadow_receipts"
        ).fetchall()
    finally:
        chk.close()
    assert len(daily_before) == 1
    assert rcpts_before[0][1] == "pending"
    hash_before = _sha256_file(cross_db)

    for cmd in ("status", "report"):
        shadow_main(["--shadow-db", str(cross_db), cmd])
        out = json.loads(capsys.readouterr().out)
        assert out["readonly"] is True

    chk_after = sqlite3.connect(cross_db)
    try:
        assert chk_after.execute("SELECT * FROM shadow_daily_state").fetchall() == daily_before
        assert chk_after.execute("SELECT * FROM shadow_account").fetchall() == acct_before
        assert (
            chk_after.execute("SELECT execution_id, order_status FROM shadow_receipts").fetchall()
            == rcpts_before
        )
    finally:
        chk_after.close()
    assert _sha256_file(cross_db) == hash_before


# ==============================================================================
# Issue 6: Source Health Evaluation & Stale-Source Risk Gate
# ==============================================================================


def test_issue6_source_health_cache_quiet_timeout_dom_breakage_and_stale_risk_gate(
    tmp_path: Path,
) -> None:
    t0 = datetime(2026, 10, 29, 14, 0, 0, tzinfo=UTC)

    # 1. HTML DOM structure change (valid <html> with 0 RSC Articles and 0 legacy cards) MUST NOT silently succeed!
    broken_dom_html = b"""<!doctype html><html><body>
    <main class="redesigned-v3-feed"><div class="post-row">Headline moved</div></main>
    </body></html>"""
    b_recs, _b_rej, b_err = parse_investinglive_live_html(
        broken_dom_html, stream="news/live_feed", received_at=t0
    )
    assert b_recs == ()
    assert b_err == "investinglive_dom_or_rsc_structure_changed"

    # 2. Offline health evaluation across quiet_market, X-Cache: Miss disclaimer, high CDN Age, timeout, and DOM breakage
    import eventlens.observer as obs_mod

    eval_health = getattr(obs_mod, "evaluate_observer_stream_health", None)
    assert eval_health is not None, "Missing evaluate_observer_stream_health"

    clock_now = t0
    ostore = ObserverStore(tmp_path / "health.sqlite", clock=lambda: clock_now)
    try:
        # Quiet market: HTTP 200, X-Cache: Miss, Age: 0, fresh Date header, valid parse, 0 new items on second poll
        base_rec = SourceRecord(
            source_id="il-1",
            kind="news",
            source_url="https://investinglive.com/central-banks/il-1/",
            title="ECB unchanged",
            text="Rates unchanged.",
            publisher_display_at=t0 - timedelta(seconds=8),
            modified_at=t0 - timedelta(seconds=8),
            importance=3,
        )
        ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                basis="live_poll",
                request_url=INVESTINGLIVE_LIVE_FEED_URL,
                request_started_at=t0 - timedelta(milliseconds=120),
                received_at=t0,
                elapsed_ms=120.0,
                attempts=1,
                payload=b"<html>p1</html>",
                response_headers={
                    "x-cache": "Miss from cloudfront",
                    "age": "0",
                    "date": format_datetime(t0, usegmt=True),
                },
                records=(base_rec,),
            )
        )
        clock_now = t0 + timedelta(minutes=10)
        ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                basis="live_poll",
                request_url=INVESTINGLIVE_LIVE_FEED_URL,
                request_started_at=clock_now - timedelta(milliseconds=95),
                received_at=clock_now,
                elapsed_ms=95.0,
                attempts=1,
                payload=b"<html>p2-same</html>",
                response_headers={
                    "x-cache": "Miss from cloudfront",
                    "age": "0",
                    "date": format_datetime(clock_now, usegmt=True),
                },
                records=(base_rec,),
            )
        )
        quiet_reports = eval_health(ostore, as_of=clock_now)
        il_quiet = next(r for r in quiet_reports if r["provider"] == "investinglive")
        assert il_quiet["health_status"] == "quiet_market"
        assert il_quiet["cache_observation"]["cache_miss_proves_zero_lag"] is False
        assert il_quiet["live_forward_acceptance_status"] == "pending"

        # High CDN cache Age (Age: 313) -> degraded_cache_or_lag!
        clock_now = t0 + timedelta(minutes=11)
        ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                basis="live_poll",
                request_url=INVESTINGLIVE_LIVE_FEED_URL,
                request_started_at=clock_now - timedelta(milliseconds=90),
                received_at=clock_now,
                elapsed_ms=90.0,
                attempts=1,
                payload=b"<html>p3-cached</html>",
                response_headers={
                    "x-cache": "Hit from cloudfront",
                    "age": "313",
                    "date": format_datetime(clock_now - timedelta(seconds=313), usegmt=True),
                },
                records=(base_rec,),
            )
        )
        cached_reports = eval_health(ostore, as_of=clock_now)
        il_cached = next(r for r in cached_reports if r["provider"] == "investinglive")
        assert il_cached["health_status"] == "degraded_cache_or_lag"
        assert il_cached["allows_new_risk"] is False

        # Timeout failure on fed stream -> fault!
        clock_now = t0 + timedelta(minutes=12)
        ostore.failure("fed", "news/monetary", "ReadTimeout: timed out after 8.0s")
        fault_reports = eval_health(ostore, as_of=clock_now)
        fed_fault = next(r for r in fault_reports if r["provider"] == "fed")
        assert fed_fault["health_status"] == "fault"
        assert fed_fault["allows_new_risk"] is False
    finally:
        ostore.close()

    # 3. Stale/degraded source input MUST block SET_TARGET (new risk), while allowing CLOSE and protective exits!
    gate_clock = t0 + timedelta(minutes=20)
    mkt_now = _fresh_live_quote("1.1125", gate_clock)

    async def gate_llm(user_ctx: dict[str, Any], call_no: int) -> dict[str, Any]:
        if call_no == 1:
            return {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Fresh event opens bounded long.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1100"],
                "confidence": 0.75,
                "evidence_role": "support",
                "evidence_quote": "Fresh ECB rate hike",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.10",
                "target_thesis_ids": ["__current__"],
                "rationale": "Open long on fresh news.",
            }
        prior_id = user_ctx["prior_memory_theses_do_not_quote"][0]["thesis_id"]
        if call_no == 2:
            # Stale event attempts to ADD risk (0.10 -> 0.20) -> MUST be blocked by stale-source gate!
            return {
                "memory_action": "update",
                "thesis_id": prior_id,
                "status": "active",
                "narrative": "Stale wire tries to double long.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1100"],
                "confidence": 0.80,
                "evidence_role": "support",
                "evidence_quote": "Delayed cached wire",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.20",
                "target_thesis_ids": ["__current__"],
                "rationale": "Try adding risk on stale wire.",
            }
        # Call 3: Stale event proposes CLOSE (risk-reducing!) -> MUST still be permitted!
        return {
            "memory_action": "update",
            "thesis_id": prior_id,
            "status": "resolved",
            "narrative": "Close existing risk conservatively.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["EUR/USD <= 1.1100"],
            "confidence": 0.20,
            "evidence_role": "contradict",
            "evidence_quote": "Delayed adverse headline",
            "portfolio_action": "CLOSE",
            "target_direction": "flat",
            "target_weight": "0",
            "target_thesis_ids": [prior_id],
            "rationale": "Risk-reducing close must remain allowed.",
        }

    g_client = _ScriptedModelClient(gate_llm)
    g_mem = SQLiteThesisMemory(
        tmp_path / "gate-mem.sqlite", clock=lambda: gate_clock, allow_targets=True
    )
    g_state = SQLiteRuntimeStore(tmp_path / "gate-shadow.sqlite")
    g_ledger = SQLiteShadowLedger(
        tmp_path / "gate-shadow.sqlite",
        risk_policy=RiskPolicy.pepperstone_razor_200(),
        exit_policy=ProtectiveExitPolicy.pepperstone_razor_200(),
        initial_cash_usd=Decimal("200"),
        clock=lambda: gate_clock,
    )
    try:
        g_agent = SinglePassTradingAgent(
            g_client,
            max_position_weight=Decimal("0.25"),
            allow_commission_bearing_zero_spread=True,
            clock=lambda: gate_clock,
        )
        g_runtime = IntradayEventRuntime(
            memory=g_mem,
            agent=g_agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(
                RiskPolicy.pepperstone_razor_200(), clock=lambda: gate_clock
            ),
            shadow_executor=g_ledger,
            market_provider=lambda as_of: _fresh_live_quote("1.1125", as_of),
            state_store=g_state,
            clock=lambda: gate_clock,
            max_actionable_event_lag_seconds=180.0,
        )

        # Call 1: Fresh event (2s lag) opens +4,000 EUR
        ev_fresh = Event(
            event_id="ecb:press:fresh:r1",
            source="ecb_press",
            source_type="macro",
            source_reference="https://www.ecb.europa.eu/press/fresh",
            source_timestamp=gate_clock - timedelta(seconds=2),
            received_timestamp=gate_clock,
            raw_text="Fresh ECB rate hike announced.",
        )
        c1 = asyncio.run(g_runtime.run_cycle(ev_fresh))
        assert c1.status == "shadow_recorded"
        gate_clock = gate_clock + timedelta(seconds=1)
        g_ledger.fill_pending(_fresh_live_quote("1.1125", gate_clock))
        qty_after_open = g_ledger.snapshot_sync(market=mkt_now).positions[0].quantity
        assert qty_after_open > 0

        # Call 2: Stale event (600s > 180s lag) tries to increase risk -> REJECTED!
        gate_clock = gate_clock + timedelta(seconds=5)
        ev_stale_add = Event(
            event_id="investinglive:news:stale-add:r1",
            source="investinglive_news",
            source_type="news",
            source_reference="https://investinglive.com/news/stale-add",
            source_timestamp=gate_clock - timedelta(seconds=600),
            received_timestamp=gate_clock,
            raw_text="Delayed cached wire on ECB.",
        )
        c2 = asyncio.run(g_runtime.run_cycle(ev_stale_add))
        assert c2.status == "risk_rejected"
        assert "stale_or_degraded_source_blocks_new_risk" in c2.reason

        # Call 3: Stale event (600s lag) proposes CLOSE (risk-reducing) -> APPROVED & FILLED!
        gate_clock = gate_clock + timedelta(seconds=5)
        ev_stale_close = Event(
            event_id="investinglive:news:stale-close:r1",
            source="investinglive_news",
            source_type="news",
            source_reference="https://investinglive.com/news/stale-close",
            source_timestamp=gate_clock - timedelta(seconds=600),
            received_timestamp=gate_clock,
            raw_text="Delayed adverse headline.",
        )
        c3 = asyncio.run(g_runtime.run_cycle(ev_stale_close))
        assert c3.status == "shadow_recorded"
        gate_clock = gate_clock + timedelta(seconds=1)
        close_fills = g_ledger.fill_pending(_fresh_live_quote("1.1125", gate_clock))
        assert len(close_fills) == 1
        assert all(
            p.quantity == 0
            for p in g_ledger.snapshot_sync(
                market=_fresh_live_quote("1.1125", gate_clock)
            ).positions
        )
    finally:
        g_state.close()
        g_ledger.close()
