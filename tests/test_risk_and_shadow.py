"""Tests for deterministic target validation, hard risk engine, shadow ledger, and runtime."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from eventlens.contracts import (
    AgentDecision,
    Event,
    MarketSnapshot,
    PortfolioSnapshot,
    Quote,
    TargetPortfolio,
    TargetPosition,
    Thesis,
    ThesisChange,
)
from eventlens.deepseek import ModelResponse
from eventlens.runtime import InMemoryAuditSink, IntradayEventRuntime
from eventlens.sources.wallstreetcn import WallStreetCNFilter
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import (
    SinglePassProposal,
    SinglePassTradingAgent,
)
from eventlens.trading.execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    SQLiteShadowLedger,
)
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy

T0 = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)


class FakeJSONClient:
    provider = "deepseek"
    model = "deepseek-flash"

    def __init__(self, proposal: SinglePassProposal) -> None:
        self.proposal = proposal

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        return ModelResponse(
            content=self.proposal.model_dump_json(),
            input_tokens=150,
            output_tokens=60,
            model_id="deepseek-flash",
            system_fingerprint="test",
        )


def make_quote(
    *,
    bid: str = "1.08500",
    ask: str = "1.08510",
    received_at: datetime = T0,
) -> Quote:
    return Quote(
        instrument="EUR/USD",
        bid=Decimal(bid),
        ask=Decimal(ask),
        received_at=received_at,
    )


def make_market(*, quote: Quote | None = None, as_of: datetime = T0) -> MarketSnapshot:
    q = quote or make_quote(received_at=as_of)
    return MarketSnapshot(
        snapshot_id="market-1",
        as_of=as_of,
        quotes=(q,),
    )


def make_thesis(
    *,
    thesis_id: str = "t-eurusd-1",
    status: str = "active",
    instruments: tuple[str, ...] = ("EUR/USD",),
) -> Thesis:
    return Thesis(
        thesis_id=thesis_id,
        revision=1,
        status=status,  # type: ignore[arg-type]
        narrative="ECB hawkish repricing supports EUR/USD.",
        instruments=instruments,
        supporting_event_ids=("ev-1",),
        invalidation_conditions=("ECB walks back guidance",),
        confidence=0.75,
        created_at=T0,
        updated_at=T0,
        review_at=T0 + timedelta(hours=4),
    )


def make_decision(
    *,
    snapshot_id: str = "shadow-snapshot-0",
    weight: str = "0.20",
    thesis_id: str = "t-eurusd-1",
    started_at: datetime = T0,
    completed_at: datetime = T0 + timedelta(seconds=1),
    valid_until: datetime = T0 + timedelta(seconds=60),
) -> AgentDecision:
    thesis = make_thesis(thesis_id=thesis_id)
    target = TargetPortfolio(
        target_id="target-1",
        based_on_snapshot_id=snapshot_id,
        positions=(
            TargetPosition(
                instrument="EUR/USD",
                weight=Decimal(weight),
                thesis_ids=(thesis_id,),
            ),
        ),
        rationale="Long EUR/USD on ECB hawkish guidance.",
    )
    return AgentDecision(
        decision_id="decision-1",
        event_id="ev-1",
        based_on_memory_version=0,
        based_on_snapshot_id=snapshot_id,
        provider="deepseek",
        model_version="deepseek-flash",
        prompt_version="single-pass-agent-v1",
        started_at=started_at,
        completed_at=completed_at,
        valid_until=valid_until,
        thesis_changes=(
            ThesisChange(
                expected_revision=0,
                thesis=thesis,
                reason="Initial ECB thesis",
            ),
        ),
        target=target,
        rationale="Create thesis and open 20% long EUR/USD target.",
    )


def test_target_validator_enforces_active_thesis_and_instrument_bounds():
    validator = DeterministicTargetValidator(max_abs_weight=Decimal("0.5"))
    portfolio = PortfolioSnapshot(
        snapshot_id="shadow-snapshot-0",
        as_of=T0,
        base_currency="USD",
        equity=Decimal("10000"),
    )
    active_theses = {"t-eurusd-1": make_thesis()}
    decision = make_decision()
    assert decision.target is not None
    validator.validate(decision.target, portfolio, theses=active_theses, as_of=T0)

    # Stale snapshot id rejected
    stale_target = decision.target.model_copy(update={"based_on_snapshot_id": "other-snap"})
    with pytest.raises(ValueError, match="stale or mismatched portfolio snapshot"):
        validator.validate(stale_target, portfolio, theses=active_theses, as_of=T0)

    # Excessive weight rejected
    heavy_target = TargetPortfolio(
        target_id="target-heavy",
        based_on_snapshot_id="shadow-snapshot-0",
        positions=(
            TargetPosition(
                instrument="EUR/USD",
                weight=Decimal("0.75"),
                thesis_ids=("t-eurusd-1",),
            ),
        ),
        rationale="Too large.",
    )
    with pytest.raises(ValueError, match="exceeds bound"):
        validator.validate(heavy_target, portfolio, theses=active_theses, as_of=T0)

    # Inactive thesis rejected
    inactive_theses = {"t-eurusd-1": make_thesis(status="invalidated")}
    with pytest.raises(ValueError, match="inactive, expired or unrelated thesis"):
        validator.validate(decision.target, portfolio, theses=inactive_theses, as_of=T0)


def test_hard_risk_engine_approves_valid_and_rejects_violations():
    policy = RiskPolicy(
        max_position_weight=Decimal("0.25"),
        max_gross_weight=Decimal("0.25"),
        max_spread_pips=Decimal("1.5"),
        max_quote_age_ms=2000,
    )
    engine = DeterministicHardRiskEngine(policy=policy, clock=lambda: T0 + timedelta(seconds=1))
    portfolio = PortfolioSnapshot(
        snapshot_id="shadow-snapshot-0",
        as_of=T0,
        base_currency="USD",
        equity=Decimal("10000"),
    )
    market = make_market(as_of=T0 + timedelta(seconds=1))
    decision = make_decision()

    approved = asyncio.run(engine.assess(decision, portfolio, market))
    assert approved.outcome == "approve"
    assert approved.reasons == ("all_hard_risk_checks_passed",)

    # Wide spread rejected
    wide_quote = make_quote(bid="1.08500", ask="1.08530", received_at=T0 + timedelta(seconds=1))
    wide_market = make_market(quote=wide_quote, as_of=T0 + timedelta(seconds=1))
    rejected_spread = asyncio.run(engine.assess(decision, portfolio, wide_market))
    assert rejected_spread.outcome == "reject"
    assert "spread_limit_exceeded" in rejected_spread.reasons

    # Kill switch rejected
    kill_engine = DeterministicHardRiskEngine(
        policy=policy.model_copy(update={"kill_switch_active": True}),
        clock=lambda: T0 + timedelta(seconds=1),
    )
    rejected_kill = asyncio.run(kill_engine.assess(decision, portfolio, market))
    assert rejected_kill.outcome == "reject"
    assert "kill_switch_active" in rejected_kill.reasons

    # Pending order rejected
    busy_portfolio = portfolio.model_copy(update={"pending_order_ids": ("exec-1",)})
    rejected_pending = asyncio.run(engine.assess(decision, busy_portfolio, market))
    assert rejected_pending.outcome == "reject"
    assert "pending_orders_outstanding" in rejected_pending.reasons


def test_sqlite_shadow_ledger_separates_receipts_from_fills_and_runs_protective_exits(tmp_path):
    now = T0 + timedelta(seconds=2)
    ledger = SQLiteShadowLedger(
        tmp_path / "shadow.sqlite",
        initial_cash_usd=Decimal("10000"),
        cost_model=ShadowCostModel(),
        exit_policy=ProtectiveExitPolicy(
            max_holding_seconds=1800,
            max_position_loss_usd=Decimal("15.00"),
            daily_close_utc_hour=21,
        ),
        clock=lambda: now,
    )
    try:
        initial_snap = ledger.snapshot_sync()
        assert initial_snap.snapshot_id == "shadow-snapshot-0"
        assert initial_snap.positions == ()

        market = make_market(as_of=now)
        decision = make_decision(snapshot_id=initial_snap.snapshot_id)
        engine = DeterministicHardRiskEngine(clock=lambda: now)
        assessment = asyncio.run(engine.assess(decision, initial_snap, market))
        assert assessment.outcome == "approve"

        receipt = ledger.record_sync(decision, assessment)
        assert receipt.status == "recorded"
        # Deduplication returns identical receipt
        assert ledger.record_sync(decision, assessment) == receipt

        # Recording a receipt does NOT immediately change positions; it adds a pending order
        pending_snap = ledger.snapshot_sync()
        assert pending_snap.positions == ()
        assert pending_snap.pending_order_ids == (receipt.execution_id,)

        # Quote older than recorded_at cannot causally fill the order
        stale_fill_market = make_market(
            quote=make_quote(received_at=now - timedelta(seconds=1)),
            as_of=now,
        )
        assert ledger.fill_pending(stale_fill_market) == ()

        # Fresh quote at or after recorded_at fills the pending order
        fill_time = now + timedelta(seconds=1)
        ledger.clock = lambda: fill_time
        fill_market = make_market(
            quote=make_quote(bid="1.08500", ask="1.08510", received_at=fill_time),
            as_of=fill_time,
        )
        fills = ledger.fill_pending(fill_market)
        assert len(fills) == 1
        fill = fills[0]
        assert fill.kind == "target_fill"
        assert fill.delta_quantity > Decimal("0")
        assert fill.commission_usd > Decimal("0")

        post_fill_snap = ledger.snapshot_sync(market=fill_market)
        assert post_fill_snap.pending_order_ids == ()
        assert len(post_fill_snap.positions) == 1
        assert post_fill_snap.positions[0].quantity == fill.new_quantity

        # Protective stop-loss triggers when price drops sharply without calling any LLM
        crash_time = fill_time + timedelta(seconds=30)
        ledger.clock = lambda: crash_time
        crash_market = make_market(
            quote=make_quote(bid="1.06000", ask="1.06010", received_at=crash_time),
            as_of=crash_time,
        )
        exit_fills = ledger.evaluate_protective_exits(crash_market)
        assert len(exit_fills) == 1
        assert exit_fills[0].kind == "protective_exit"
        assert exit_fills[0].reason == "protective_stop_loss"
        assert exit_fills[0].new_quantity == Decimal("0")
        assert exit_fills[0].realized_pnl_usd < Decimal("0")

        flat_snap = ledger.snapshot_sync(market=crash_market)
        assert flat_snap.positions == ()
        report = ledger.report(market=crash_market)
        assert report["receipt_count"] == 1
        assert report["fill_count"] == 2
        assert report["execution_authorized"] is False
    finally:
        ledger.close()


def test_intraday_event_runtime_end_to_end_pipeline(tmp_path):
    db_path = tmp_path / "memory.sqlite"
    memory = SQLiteThesisMemory(
        db_path, clock=lambda: T0 + timedelta(seconds=1), allow_targets=True
    )
    ledger = SQLiteShadowLedger(tmp_path / "shadow.sqlite", clock=lambda: T0 + timedelta(seconds=1))
    audit = InMemoryAuditSink()

    proposal = SinglePassProposal(
        memory_action="create",
        thesis_id=None,
        status="active",
        narrative="ECB rate hike guidance strengthens EUR/USD.",
        instruments=("EUR/USD",),
        invalidation_conditions=("ECB pauses tightening",),
        confidence=0.8,
        evidence_role="support",
        evidence_quote="ECB Governing Council signals a 25 bps rate hike",
        target_direction="long",
        target_weight=Decimal("0.20"),
        target_thesis_ids=("__current__",),
        rationale="ECB signals rate hike; initiate 20% EUR/USD long exposure.",
    )
    agent = SinglePassTradingAgent(
        FakeJSONClient(proposal),
        clock=lambda: T0 + timedelta(seconds=1),
    )
    wscn_filter = WallStreetCNFilter(clock=lambda: T0 + timedelta(seconds=1))
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda now: make_market(as_of=now),
        agent=agent,
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: T0 + timedelta(seconds=1)),
        shadow_executor=ledger,
        audit_sink=audit,
        event_filter=wscn_filter,
        clock=lambda: T0 + timedelta(seconds=1),
    )

    try:
        # 1. Irrelevant equity noise is persisted but filtered out before LLM
        noise_event = Event(
            event_id="ev-noise",
            source="wallstreetcn",
            source_type="news",
            source_reference="wallstreetcn:news:1",
            source_timestamp=T0,
            received_timestamp=T0,
            raw_text="某A股上市公司发布季度财报。",
        )
        noise_res = asyncio.run(runtime.run_cycle(noise_event))
        assert noise_res.status == "filtered_out"
        assert noise_res.decision is None

        # 2. Relevant ECB macro headline runs full pipeline through risk and shadow ledger
        ecb_event = Event(
            event_id="ev-ecb",
            source="wallstreetcn",
            source_type="news",
            source_reference="wallstreetcn:news:2",
            source_timestamp=T0,
            received_timestamp=T0,
            raw_text="European Central Bank: ECB Governing Council signals a 25 bps rate hike for EUR/USD.",
        )
        res = asyncio.run(runtime.run_cycle(ecb_event))
        assert res.status == "shadow_recorded"
        assert res.committed_memory_version == 1
        assert res.assessment is not None and res.assessment.outcome == "approve"
        assert res.receipt is not None and res.receipt.status == "recorded"

        # Fill the recorded shadow target on next quote tick
        fill_time = T0 + timedelta(seconds=2)
        ledger.clock = lambda: fill_time
        fills = ledger.fill_pending(make_market(as_of=fill_time))
        assert len(fills) == 1
        assert fills[0].new_quantity > Decimal("0")

        # Max holding horizon protective exit triggers when 1800s elapse without LLM
        horizon_time = fill_time + timedelta(seconds=1801)
        ledger.clock = lambda: horizon_time
        horizon_fills = ledger.evaluate_protective_exits(make_market(as_of=horizon_time))
        assert len(horizon_fills) == 1
        assert horizon_fills[0].kind == "protective_exit"
        assert horizon_fills[0].reason == "protective_max_holding_horizon"
        assert horizon_fills[0].new_quantity == Decimal("0")
    finally:
        ledger.close()
