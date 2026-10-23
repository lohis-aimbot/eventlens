"""Regression and acceptance tests for v0.15.3 reliability fixes and automated Shadow loop."""

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from eventlens.cli import main
from eventlens.contracts import (
    AgentDecision,
    Event,
    MarketSnapshot,
    Quote,
    TargetPortfolio,
    TargetPosition,
    Thesis,
    ThesisChange,
)
from eventlens.deepseek import ModelResponse
from eventlens.quotes import (
    BridgeQuote,
    BridgeStart,
    QuoteQualityPolicy,
    QuoteStore,
    build_market_snapshot,
    validate_market_quote,
)
from eventlens.runtime import (
    IntradayEventRuntime,
    ShadowRunner,
    SQLiteRuntimeStore,
)
from eventlens.sources.wallstreetcn import WallStreetCNFilter
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import (
    SinglePassProposal,
    SinglePassTradingAgent,
)
from eventlens.trading.execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    ShadowFill,
    SQLiteShadowLedger,
)
from eventlens.trading.portfolio import (
    DeterministicTargetValidator,
    classify_exposure_change,
)
from eventlens.trading.risk import (
    DeterministicHardRiskEngine,
    RiskPolicy,
    is_in_daily_close_window,
)

T0 = datetime(2026, 4, 20, 12, 0, 0, tzinfo=UTC)
SESSION = UUID("22222222-3333-4444-5555-666666666666")


class SequencedJSONClient:
    provider = "deepseek"
    model = "deepseek-flash"

    def __init__(
        self,
        *proposals: SinglePassProposal | Exception,
        on_call=None,
    ) -> None:
        self.items = list(proposals)
        self.on_call = on_call
        self.calls = 0

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls += 1
        if self.on_call is not None:
            await self.on_call(self.calls, user)
        item = self.items.pop(0)
        if isinstance(item, Exception):
            raise item
        return ModelResponse(
            content=item.model_dump_json(),
            input_tokens=150,
            output_tokens=60,
            model_id="deepseek-flash",
            system_fingerprint="fp-test",
        )


def make_quote(
    *,
    bid: str = "1.08500",
    ask: str = "1.08510",
    received_at: datetime = T0,
    source_at: datetime | None = None,
    bridge_at: datetime | None = None,
    available_at: datetime | None = None,
    basis: str = "live_poll",
) -> Quote:
    src = source_at or (received_at - timedelta(milliseconds=30))
    brg = bridge_at or (received_at - timedelta(milliseconds=15))
    avl = available_at or received_at
    return Quote(
        instrument="EUR/USD",
        source_at=src,
        bridge_at=brg,
        received_at=received_at,
        available_at=avl,
        source_provider="ctrader_algo",
        basis=basis,  # type: ignore[arg-type]
        bid=Decimal(bid),
        ask=Decimal(ask),
    )


def make_market(*, quote: Quote | None = None, as_of: datetime = T0) -> MarketSnapshot:
    q = quote or make_quote(received_at=as_of)
    return MarketSnapshot(
        snapshot_id=f"market:{as_of.isoformat()}",
        as_of=as_of,
        quotes=(q,),
    )


def make_thesis(
    *,
    thesis_id: str = "t-eurusd-1",
    status: str = "active",
    instruments: tuple[str, ...] = ("EUR/USD",),
    updated_at: datetime = T0,
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
        created_at=updated_at,
        updated_at=updated_at,
        review_at=updated_at + timedelta(hours=4),
    )


def make_decision(
    *,
    decision_id: str = "decision-1",
    event_id: str = "ev-1",
    snapshot_id: str = "shadow-snapshot-0",
    weight: str = "0.20",
    thesis_id: str = "t-eurusd-1",
    portfolio_action: str = "SET_TARGET",
    started_at: datetime = T0,
    completed_at: datetime = T0 + timedelta(seconds=1),
    valid_until: datetime = T0 + timedelta(seconds=60),
) -> AgentDecision:
    thesis = make_thesis(thesis_id=thesis_id, updated_at=completed_at).model_copy(
        update={"supporting_event_ids": (event_id,)}
    )
    target = TargetPortfolio(
        target_id=f"target-{decision_id}",
        based_on_snapshot_id=snapshot_id,
        positions=(
            TargetPosition(
                instrument="EUR/USD",
                weight=Decimal(weight),
                thesis_ids=(thesis_id,),
            ),
        ),
        rationale="EUR/USD target exposure.",
    )
    return AgentDecision(
        decision_id=decision_id,
        event_id=event_id,
        based_on_memory_version=0,
        based_on_snapshot_id=snapshot_id,
        provider="deepseek",
        model_version="deepseek-flash",
        prompt_version="single-pass-agent-v2",
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
        portfolio_action=portfolio_action,  # type: ignore[arg-type]
        rationale="Create thesis and set EUR/USD target.",
    )


def test_unified_quote_quality_and_zero_spread_rejection(tmp_path):
    """Section 二: Preserve full quote clocks, base freshness on source_at, and reject zero spread by default."""
    qstore = QuoteStore(tmp_path / "quotes.sqlite", clock=lambda: T0)
    qstore.start(
        BridgeStart(
            version=1,
            kind="start",
            session_id=SESSION,
            account_mode="demo",
            running_mode="realtime",
            symbol="EURUSD",
            started_at=T0 - timedelta(seconds=5),
            sample_interval_ms=1000,
            duration_seconds=60,
        ),
        T0 - timedelta(seconds=5),
        basis="live_poll",
    )
    qstore.record(
        BridgeQuote(
            version=1,
            kind="quote",
            session_id=SESSION,
            sequence=1,
            symbol="EURUSD",
            source_at=T0 - timedelta(milliseconds=350),
            bridge_at=T0 - timedelta(milliseconds=200),
            bid=Decimal("1.08500"),
            ask=Decimal("1.08510"),
        ),
        T0 - timedelta(milliseconds=100),
        '{"kind":"quote"}',
    )
    snap = build_market_snapshot(qstore, T0)
    q = snap.quotes[0]
    assert q.source_at == T0 - timedelta(milliseconds=350)
    assert q.bridge_at == T0 - timedelta(milliseconds=200)
    assert q.received_at == T0 - timedelta(milliseconds=100)
    assert q.available_at == T0
    assert q.source_provider == "ctrader_algo"
    assert q.basis == "live_poll"
    assert q.has_verified_provenance is True
    assert q.receipt_lag_ms == pytest.approx(250.0)
    qstore.close()

    policy = QuoteQualityPolicy()
    assert policy.allow_zero_spread is False
    valid_res = validate_market_quote(snap, as_of=T0, policy=policy)
    assert valid_res.valid is True
    assert valid_res.source_age_ms == pytest.approx(350.0)

    # Fresh received_at (100ms) but stale source_at (2500ms > 2000ms) is rejected!
    stale_source_quote = make_quote(
        received_at=T0 - timedelta(milliseconds=100),
        source_at=T0 - timedelta(milliseconds=2500),
        bridge_at=T0 - timedelta(milliseconds=200),
    )
    stale_res = validate_market_quote(
        make_market(quote=stale_source_quote, as_of=T0), as_of=T0, policy=policy
    )
    assert stale_res.valid is False
    assert stale_res.reason == "stale_market_quote"

    # Zero spread is rejected by default across the unified validator and ShadowCostModel
    zero_quote = make_quote(bid="1.08500", ask="1.08500", received_at=T0)
    zero_res = validate_market_quote(
        make_market(quote=zero_quote, as_of=T0), as_of=T0, policy=policy
    )
    assert zero_res.valid is False
    assert zero_res.reason == "zero_spread_rejected"

    cost = ShadowCostModel()
    assert cost.spread_floor_pips == Decimal("0")
    with pytest.raises(ValueError, match="Zero-spread quote cannot be filled"):
        cost.execution_price(zero_quote, buy=True)
    with pytest.raises(ValueError, match="allow_synthetic_spread_floor=True"):
        ShadowCostModel(spread_floor_pips=Decimal("0.1"), allow_synthetic_spread_floor=False)


def test_ignore_and_hold_never_liquidate_open_position_and_close_requires_thesis(tmp_path):
    """Section 五: IGNORE_EVENT and HOLD keep open positions; CLOSE requires open position & explicit thesis."""
    now = T0
    memory = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=lambda: now, allow_targets=True)
    ledger = SQLiteShadowLedger(
        tmp_path / "shadow.sqlite",
        exit_policy=ProtectiveExitPolicy(max_position_loss_usd=Decimal("30")),
        clock=lambda: now,
    )

    open_prop = SinglePassProposal(
        memory_action="create",
        thesis_id=None,
        status="active",
        narrative="ECB hawkish hike supports EUR/USD.",
        instruments=("EUR/USD",),
        invalidation_conditions=("ECB turns dovish",),
        confidence=0.8,
        evidence_role="support",
        evidence_quote="ECB signals rate hike",
        portfolio_action="SET_TARGET",
        target_direction="long",
        target_weight=Decimal("0.20"),
        target_thesis_ids=("__current__",),
        rationale="Open 20% long EUR/USD.",
    )
    ignore_prop = SinglePassProposal(
        memory_action="ignore",
        portfolio_action="IGNORE_EVENT",
        target_direction="flat",
        target_weight=Decimal("0"),
        target_thesis_ids=(),
        rationale="Irrelevant noise; ignore event.",
    )
    hold_prop = SinglePassProposal(
        memory_action="ignore",
        portfolio_action="HOLD",
        target_direction="flat",
        target_weight=Decimal("0"),
        target_thesis_ids=(),
        rationale="Uncertain follow-up; hold existing EUR/USD position.",
    )
    client = SequencedJSONClient(open_prop, ignore_prop, hold_prop)
    agent = SinglePassTradingAgent(client, clock=lambda: now)
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: make_market(as_of=ts),
        agent=agent,
        shadow_executor=ledger,
        clock=lambda: now,
    )
    try:
        # 1. Open position on Event 1
        ev1 = Event(
            event_id="ev-open",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:1",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="European Central Bank: ECB signals rate hike for EUR/USD.",
        )
        r1 = asyncio.run(runtime.run_cycle(ev1))
        assert r1.status == "shadow_recorded"
        now = T0 + timedelta(seconds=1)
        ledger.clock = lambda: now
        fills = ledger.fill_pending(make_market(as_of=now))
        assert len(fills) == 1
        held_qty = fills[0].new_quantity
        assert held_qty > Decimal("0")

        # 2. Event 2 returns IGNORE_EVENT while holding position -> does NOT close position!
        now = T0 + timedelta(seconds=5)
        ev2 = Event(
            event_id="ev-ignore",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:2",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="Minor commentary on EUR/USD market session.",
        )
        r2 = asyncio.run(runtime.run_cycle(ev2))
        assert r2.status == "no_target"
        assert r2.decision is not None and r2.decision.target is None
        assert ledger.snapshot_sync(market=make_market(as_of=now)).positions[0].quantity == held_qty

        # 3. Event 3 returns HOLD while holding position -> does NOT close position!
        now = T0 + timedelta(seconds=10)
        ev3 = Event(
            event_id="ev-hold",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:3",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="Mixed macro commentary on EUR/USD.",
        )
        r3 = asyncio.run(runtime.run_cycle(ev3))
        assert r3.status == "no_target"
        assert r3.decision is not None and r3.decision.target is None
        assert ledger.snapshot_sync(market=make_market(as_of=now)).positions[0].quantity == held_qty

        # 4. CLOSE without target_thesis_ids is rejected by schema
        with pytest.raises(ValueError, match="CLOSE requires explicit target_thesis_ids"):
            SinglePassProposal(
                memory_action="ignore",
                evidence_quote="ECB signals",
                portfolio_action="CLOSE",
                target_direction="flat",
                target_weight=Decimal("0"),
                target_thesis_ids=(),
                rationale="Try close without thesis",
            )
    finally:
        ledger.close()


def test_post_llm_market_refresh_and_inference_state_change_invalidation(tmp_path):
    """Section 三: Refresh market/portfolio after >2s LLM call; reject if portfolio changed during inference."""
    now = T0
    memory = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=lambda: now, allow_targets=True)
    ledger = SQLiteShadowLedger(
        tmp_path / "shadow.sqlite",
        exit_policy=ProtectiveExitPolicy(max_position_loss_usd=Decimal("30")),
        clock=lambda: now,
    )

    open_prop = SinglePassProposal(
        memory_action="create",
        thesis_id=None,
        status="active",
        narrative="ECB rate hike supports EUR/USD.",
        instruments=("EUR/USD",),
        invalidation_conditions=("ECB pauses",),
        confidence=0.8,
        evidence_role="support",
        evidence_quote="ECB Governing Council signals a 25 bps rate hike",
        portfolio_action="SET_TARGET",
        target_direction="long",
        target_weight=Decimal("0.20"),
        target_thesis_ids=("__current__",),
        rationale="Open 20% long EUR/USD.",
    )
    add_prop = SinglePassProposal(
        memory_action="create",
        thesis_id=None,
        status="active",
        narrative="Follow-up ECB comment adds conviction.",
        instruments=("EUR/USD",),
        invalidation_conditions=("ECB pauses",),
        confidence=0.85,
        evidence_role="support",
        evidence_quote="Lagarde confirms further tightening",
        portfolio_action="SET_TARGET",
        target_direction="long",
        target_weight=Decimal("0.25"),
        target_thesis_ids=("__current__",),
        rationale="Increase long EUR/USD to 25%.",
    )

    async def simulate_slow_llm(call_num: int, user_json: str) -> None:
        nonlocal now
        # Advance clock by 4.5 seconds during LLM call (> 2.0s max_quote_age_ms!)
        now = now + timedelta(milliseconds=4500)
        if call_num == 2:
            # During the second LLM call, a protective exit flattens the open position!
            crash_quote = make_quote(bid="1.05000", ask="1.05010", received_at=now)
            ledger.evaluate_protective_exits(make_market(quote=crash_quote, as_of=now))

    client = SequencedJSONClient(open_prop, add_prop, on_call=simulate_slow_llm)
    agent = SinglePassTradingAgent(client, clock=lambda: now)
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: make_market(
            quote=make_quote(received_at=ts - timedelta(milliseconds=200)),
            as_of=ts,
        ),
        agent=agent,
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger,
        clock=lambda: now,
    )
    try:
        # 1. Event 1: LLM takes 4.5s. Pre-LLM quote is now 4.7s old, but post-LLM refresh
        # fetches a fresh quote (0.2s old) and preserves pre-LLM context in runtime_events!
        ev1 = Event(
            event_id="ev-slow-1",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:slow1",
            source_timestamp=T0,
            received_timestamp=T0,
            raw_text="ECB Governing Council signals a 25 bps rate hike for EUR/USD.",
        )
        r1 = asyncio.run(runtime.run_cycle(ev1))
        assert r1.status == "shadow_recorded"
        assert runtime.state_store is not None
        saved_row = runtime.state_store.get_event_state("ev-slow-1")
        assert saved_row is not None and saved_row["context_json"] is not None
        saved_ctx = json.loads(saved_row["context_json"])
        assert datetime.fromisoformat(saved_ctx["as_of"]) == T0

        # Fill Event 1 order so position is open at shadow-snapshot-1
        now = now + timedelta(seconds=1)
        fills = ledger.fill_pending(make_market(quote=make_quote(received_at=now), as_of=now))
        assert len(fills) == 1

        # 2. Event 2: During the 4.5s LLM inference, protective stop-loss fires and changes
        # portfolio state from shadow-snapshot-1 (long) to shadow-snapshot-2 (flat).
        # Post-LLM refresh detects the state change and rejects the stale target!
        ev2 = Event(
            event_id="ev-slow-2",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:slow2",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="Lagarde confirms further tightening for EUR/USD.",
        )
        r2 = asyncio.run(runtime.run_cycle(ev2))
        assert r2.status == "target_rejected"
        assert r2.reason == "portfolio_state_changed_during_inference"
    finally:
        ledger.close()


def test_pending_order_expiry_and_kill_switch_reverification_at_fill(tmp_path):
    """Section 四: Expired orders become 'expired' (not filled=1); kill switch blocks entry at fill."""
    now = T0 + timedelta(seconds=1)
    ledger = SQLiteShadowLedger(
        tmp_path / "shadow.sqlite",
        exit_policy=ProtectiveExitPolicy(max_position_loss_usd=Decimal("30")),
        clock=lambda: now,
    )
    try:
        # 1. Record an order that expires in 10 seconds
        snap0 = ledger.snapshot_sync()
        dec1 = make_decision(
            decision_id="dec-exp",
            event_id="ev-exp",
            snapshot_id=snap0.snapshot_id,
            started_at=T0,
            completed_at=now,
            valid_until=now + timedelta(seconds=10),
        )
        engine = DeterministicHardRiskEngine(clock=lambda: now)
        assess1 = asyncio.run(engine.assess(dec1, snap0, make_market(as_of=now)))
        rcpt1 = ledger.record_sync(dec1, assess1)
        assert rcpt1.status == "recorded"

        # Advance clock past decision.valid_until (15s later) and provide a valid quote
        now = now + timedelta(seconds=15)
        fills = ledger.fill_pending(make_market(as_of=now))
        assert fills == ()
        rep = ledger.report(market=make_market(as_of=now))
        assert rep["order_status_counts"].get("expired") == 1
        assert rep["order_status_counts"].get("filled", 0) == 0
        assert rep["receipts"][0]["order_status"] == "expired"
        assert rep["receipts"][0]["terminal_reason"] == "decision_expired_before_fill"

        # 2. Record a second order while kill switch is False, then enable kill switch before fill!
        snap1 = ledger.snapshot_sync()
        dec2 = make_decision(
            decision_id="dec-ks",
            event_id="ev-ks",
            snapshot_id=snap1.snapshot_id,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess2 = asyncio.run(engine.assess(dec2, snap1, make_market(as_of=now)))
        rcpt2 = ledger.record_sync(dec2, assess2)
        assert rcpt2.status == "recorded"

        now = now + timedelta(seconds=1)
        ks_policy = RiskPolicy(kill_switch_active=True)
        fills_ks = ledger.fill_pending(make_market(as_of=now), risk_policy=ks_policy)
        assert fills_ks == ()
        rep2 = ledger.report(market=make_market(as_of=now))
        assert rep2["order_status_counts"].get("rejected") == 1
        assert rep2["receipts"][1]["order_status"] == "rejected"
        assert rep2["receipts"][1]["terminal_reason"] == "kill_switch_active_at_fill"
    finally:
        ledger.close()


def test_protective_exit_defers_on_bad_quotes_and_executes_when_valid_quote_returns(tmp_path):
    """Section 二 & 六: When protective exit lacks a valid quote, persist pending_exit_reason and fault."""
    now = T0 + timedelta(seconds=1)
    ledger = SQLiteShadowLedger(
        tmp_path / "shadow.sqlite",
        exit_policy=ProtectiveExitPolicy(
            max_holding_seconds=60, max_position_loss_usd=Decimal("20")
        ),
        clock=lambda: now,
    )
    try:
        snap0 = ledger.snapshot_sync()
        dec = make_decision(snapshot_id=snap0.snapshot_id, completed_at=now)
        engine = DeterministicHardRiskEngine(clock=lambda: now)
        assess = asyncio.run(engine.assess(dec, snap0, make_market(as_of=now)))
        ledger.record_sync(dec, assess)

        now = now + timedelta(seconds=1)
        fills = ledger.fill_pending(make_market(as_of=now))
        assert len(fills) == 1
        open_qty = fills[0].new_quantity

        # Hold horizon expires (70s > 60s), but quotes are bad (missing, stale, zero-spread, wide-spread)
        now = now + timedelta(seconds=70)
        # (a) Missing market
        assert ledger.evaluate_protective_exits(None) == ()
        # (b) Zero-spread quote
        zero_mkt = make_market(
            quote=make_quote(bid="1.08400", ask="1.08400", received_at=now), as_of=now
        )
        assert ledger.evaluate_protective_exits(zero_mkt) == ()
        # (c) Stale quote
        stale_mkt = make_market(
            quote=make_quote(
                bid="1.08400",
                ask="1.08410",
                received_at=now - timedelta(seconds=5),
                source_at=now - timedelta(seconds=5),
                bridge_at=now - timedelta(seconds=5),
            ),
            as_of=now,
        )
        assert ledger.evaluate_protective_exits(stale_mkt) == ()

        # Position is STILL open (not faked as closed), and pending_exit_reason is persisted!
        snap_deferred = ledger.snapshot_sync(market=zero_mkt)
        assert snap_deferred.positions[0].quantity == open_qty
        assert snap_deferred.pending_exit_reason == "protective_max_holding_horizon"
        assert snap_deferred.valuation_valid is False

        # While pending_exit_reason is set, risk engine blocks any new risk-increasing proposal
        add_dec = make_decision(
            decision_id="dec-add",
            event_id="ev-add",
            snapshot_id=snap_deferred.snapshot_id,
            weight="0.25",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        valid_now_mkt = make_market(
            quote=make_quote(bid="1.08400", ask="1.08410", received_at=now), as_of=now
        )
        reject_add = asyncio.run(engine.assess(add_dec, snap_deferred, valid_now_mkt))
        assert reject_add.outcome == "reject"
        assert "protective_exit_pending" in reject_add.reasons

        # Once valid quote arrives, evaluate_protective_exits executes the deferred exit and clears pending_exit_reason!
        exit_fills = ledger.evaluate_protective_exits(valid_now_mkt)
        assert len(exit_fills) == 1
        assert exit_fills[0].kind == "protective_exit"
        assert exit_fills[0].reason == "protective_max_holding_horizon"
        assert exit_fills[0].new_quantity == Decimal("0")

        snap_after = ledger.snapshot_sync(market=valid_now_mkt)
        assert snap_after.positions == ()
        assert snap_after.pending_exit_reason is None
    finally:
        ledger.close()


def test_timezone_aware_daily_close_dst_missed_cutoff_and_daily_loss_persistence(tmp_path):
    """Section 六: DST-aware daily close window, missed cutoff recovery, and persistent daily loss limit."""
    # 1. Verify EDT (summer, UTC-4: 16:55 NY == 20:55 UTC) vs EST (winter, UTC-5: 16:55 NY == 21:55 UTC)
    summer_ts = datetime(2026, 7, 15, 20, 56, 0, tzinfo=UTC)
    winter_ts = datetime(2026, 1, 15, 20, 56, 0, tzinfo=UTC)
    winter_cutoff_ts = datetime(2026, 1, 15, 21, 56, 0, tzinfo=UTC)
    assert is_in_daily_close_window(summer_ts) is True
    assert is_in_daily_close_window(winter_ts) is False
    assert is_in_daily_close_window(winter_cutoff_ts) is True

    # 2. Open position before 16:55 NY (20:55 UTC in EDT), miss the cutoff window while offline,
    # and recover at 00:30 UTC (after midnight UTC!). Position must still close via protective_daily_close!
    open_time = datetime(2026, 7, 15, 19, 0, 0, tzinfo=UTC)
    now = open_time
    db_file = tmp_path / "daily_shadow.sqlite"
    ledger = SQLiteShadowLedger(
        db_file,
        exit_policy=ProtectiveExitPolicy(
            max_holding_seconds=86400,
            max_position_loss_usd=Decimal("500"),
            max_daily_loss_usd=Decimal("50"),
        ),
        risk_policy=RiskPolicy(max_daily_loss_usd=Decimal("50")),
        clock=lambda: now,
    )
    try:
        snap0 = ledger.snapshot_sync()
        dec = make_decision(
            snapshot_id=snap0.snapshot_id,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        engine = DeterministicHardRiskEngine(
            policy=RiskPolicy(max_daily_loss_usd=Decimal("50")), clock=lambda: now
        )
        assess = asyncio.run(engine.assess(dec, snap0, make_market(as_of=now)))
        ledger.record_sync(dec, assess)
        now = open_time + timedelta(seconds=1)
        assert len(ledger.fill_pending(make_market(as_of=now))) == 1

        # Jump past midnight UTC (00:30 UTC next calendar day, after missed 20:55 UTC close!)
        now = datetime(2026, 7, 16, 0, 30, 0, tzinfo=UTC)
        missed_fills = ledger.evaluate_protective_exits(make_market(as_of=now))
        assert len(missed_fills) == 1
        assert missed_fills[0].reason == "protective_daily_close"
        assert missed_fills[0].new_quantity == Decimal("0")

        # 3. Trigger daily loss limit (> $50 loss) and verify it persists across ledger restart!
        snap1 = ledger.snapshot_sync(market=make_market(as_of=now))
        dec2 = make_decision(
            decision_id="dec-loss",
            event_id="ev-loss",
            snapshot_id=snap1.snapshot_id,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess2 = asyncio.run(engine.assess(dec2, snap1, make_market(as_of=now)))
        ledger.record_sync(dec2, assess2)
        now = now + timedelta(seconds=1)
        assert len(ledger.fill_pending(make_market(as_of=now))) == 1

        # Price drops from 1.08510 to 1.05000 -> unrealized loss > $50 triggers daily loss limit!
        now = now + timedelta(seconds=10)
        drop_mkt = make_market(
            quote=make_quote(bid="1.05000", ask="1.05010", received_at=now), as_of=now
        )
        loss_exits = ledger.evaluate_protective_exits(drop_mkt)
        assert len(loss_exits) == 1
        assert loss_exits[0].new_quantity == Decimal("0")
    finally:
        ledger.close()

    # Reopen ledger on the same file (simulating process restart on the same trading day)
    reopened = SQLiteShadowLedger(
        db_file,
        exit_policy=ProtectiveExitPolicy(max_daily_loss_usd=Decimal("50")),
        risk_policy=RiskPolicy(max_daily_loss_usd=Decimal("50")),
        clock=lambda: now,
    )
    try:
        reopened_snap = reopened.snapshot_sync(market=make_market(as_of=now))
        assert reopened_snap.daily_loss_limit_triggered is True
        # New risk-increasing decision on the same trading day is rejected by HardRiskEngine!
        dec3 = make_decision(
            decision_id="dec-after-restart",
            event_id="ev-after-restart",
            snapshot_id=reopened_snap.snapshot_id,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        engine_reopened = DeterministicHardRiskEngine(
            policy=RiskPolicy(max_daily_loss_usd=Decimal("50")), clock=lambda: now
        )
        assess3 = asyncio.run(engine_reopened.assess(dec3, reopened_snap, make_market(as_of=now)))
        assert assess3.outcome == "reject"
        assert "daily_loss_limit_exceeded" in assess3.reasons
    finally:
        reopened.close()


def test_crash_after_memory_commit_recovers_and_transient_failure_retries(tmp_path):
    """Section 七: Transient failure retries without _seen lockup; crash after memory.commit recovers."""
    now = T0
    mem_path = tmp_path / "mem.sqlite"
    shd_path = tmp_path / "shadow.sqlite"
    memory = SQLiteThesisMemory(mem_path, clock=lambda: now, allow_targets=True)
    ledger = SQLiteShadowLedger(shd_path, clock=lambda: now)
    wscn_filter = WallStreetCNFilter(clock=lambda: now)

    good_prop = SinglePassProposal(
        memory_action="create",
        thesis_id=None,
        status="active",
        narrative="ECB rate hike supports EUR/USD.",
        instruments=("EUR/USD",),
        invalidation_conditions=("ECB pauses",),
        confidence=0.8,
        evidence_role="support",
        evidence_quote="ECB Governing Council signals a 25 bps rate hike",
        portfolio_action="SET_TARGET",
        target_direction="long",
        target_weight=Decimal("0.20"),
        target_thesis_ids=("__current__",),
        rationale="Open 20% long EUR/USD.",
    )
    # First call raises ConnectionError; second call succeeds
    client = SequencedJSONClient(ConnectionError("temporary LLM outage"), good_prop)
    agent = SinglePassTradingAgent(client, clock=lambda: now)
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: make_market(as_of=ts),
        agent=agent,
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger,
        event_filter=wscn_filter,
        clock=lambda: now,
    )
    try:
        ev = Event(
            event_id="ev-retry-1",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:retry1",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="European Central Bank: ECB Governing Council signals a 25 bps rate hike for EUR/USD.",
        )
        # Attempt 1 fails with transient LLM error -> status is reasoning_failed
        fail_res = asyncio.run(runtime.run_cycle(ev))
        assert fail_res.status == "reasoning_failed"
        assert client.calls == 1

        # Attempt 2 retries the SAME event -> not blocked by _seen, succeeds!
        now = T0 + timedelta(seconds=1)
        ok_res = asyncio.run(runtime.run_cycle(ev))
        assert ok_res.status == "shadow_recorded"
        assert client.calls == 2

        # Attempt 3 (duplicate delivery) -> returns persisted result with zero new LLM calls!
        dup_res = asyncio.run(runtime.run_cycle(ev))
        assert dup_res.status == "shadow_recorded"
        assert dup_res.reused is True
        assert client.calls == 2

        # Fill the first event's order so pending_order_ids is clear before crash recovery test
        now = T0 + timedelta(seconds=2)
        ledger.fill_pending(make_market(as_of=now))

        # Now simulate a crash right after memory.commit(decision) for Event 2!
        now = T0 + timedelta(seconds=5)
        snap1 = ledger.snapshot_sync(market=make_market(as_of=now))
        ev2 = Event(
            event_id="ev-crash-2",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:crash2",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="ECB Governing Council signals further tightening for EUR/USD.",
        )
        asyncio.run(memory.record_event(ev2))
        crash_dec = make_decision(
            decision_id="dec-crash-2",
            event_id="ev-crash-2",
            snapshot_id=snap1.snapshot_id,
            weight="0.25",
            thesis_id="t-crash-2",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        # Commit to memory, and write 'memory_committed' in runtime_events without calling shadow_executor.record!
        crash_dec = crash_dec.model_copy(update={"based_on_memory_version": 1})
        ver2 = asyncio.run(memory.commit(crash_dec))
        assert runtime.state_store is not None
        runtime.state_store.upsert_event_state(
            event_id=ev2.event_id,
            source_reference=ev2.source_reference,
            received_at=ev2.received_timestamp,
            now=now,
            state="memory_committed",
            reason="memory_committed",
            decision_id=crash_dec.decision_id,
            decision_json=crash_dec.model_dump_json(),
            memory_version=ver2,
        )

        # Recover incomplete events: recovers without calling LLM (client.calls stays 2) and records shadow receipt!
        now = T0 + timedelta(seconds=6)
        recovered_list = asyncio.run(runtime.recover_incomplete())
        assert len(recovered_list) == 1
        assert recovered_list[0].event_id == "ev-crash-2"
        assert recovered_list[0].recovered is True
        assert recovered_list[0].status == "shadow_recorded"
        assert client.calls == 2
    finally:
        if runtime.state_store is not None:
            runtime.state_store.close()
        ledger.close()


def test_full_pnl_and_fee_accounting_across_open_add_reduce_reverse_close(tmp_path):
    """Section 五 & 八: Reconcile cash, realized PnL, unrealized PnL, and fees across all position transitions."""
    now = T0
    ledger = SQLiteShadowLedger(
        tmp_path / "acct.sqlite",
        initial_cash_usd=Decimal("10000.00"),
        clock=lambda: now,
    )
    engine = DeterministicHardRiskEngine(
        policy=RiskPolicy(max_position_weight=Decimal("0.50"), max_gross_weight=Decimal("0.50")),
        clock=lambda: now,
    )

    def execute_target(
        idx: int, weight: str, bid: str, ask: str, action: str = "SET_TARGET"
    ) -> None:
        nonlocal now
        mkt = make_market(quote=make_quote(bid=bid, ask=ask, received_at=now), as_of=now)
        snap = ledger.snapshot_sync(market=mkt)
        dec = make_decision(
            decision_id=f"d-{idx}",
            event_id=f"e-{idx}",
            snapshot_id=snap.snapshot_id,
            weight=weight,
            portfolio_action=action,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess = asyncio.run(engine.assess(dec, snap, mkt))
        assert assess.outcome == "approve", assess.reasons
        rcpt = ledger.record_sync(dec, assess)
        assert rcpt.status == "recorded"
        now = now + timedelta(seconds=1)
        fill_mkt = make_market(quote=make_quote(bid=bid, ask=ask, received_at=now), as_of=now)
        fills = ledger.fill_pending(fill_mkt, risk_policy=engine.policy)
        assert len(fills) == 1
        rep = ledger.report(market=fill_mkt)
        cash = Decimal(rep["cash_usd"])
        realized = Decimal(rep["realized_pnl_usd"])
        fees = Decimal(rep["total_fees_usd"])
        # Accounting identity: cash == initial_cash + realized_pnl - total_fees
        assert cash == Decimal("10000.00") + realized - fees

    try:
        # 1. Open Long 20%
        execute_target(1, "0.20", "1.08000", "1.08010", "SET_TARGET")
        assert Decimal(ledger.report()["quantity_eur"]) > Decimal("0")
        qty_open = Decimal(ledger.report()["quantity_eur"])

        # 2. Add to Long 35%
        now = now + timedelta(seconds=5)
        execute_target(2, "0.35", "1.08100", "1.08110", "SET_TARGET")
        qty_added = Decimal(ledger.report()["quantity_eur"])
        assert qty_added > qty_open

        # 3. Partial reduction to Long 15%
        now = now + timedelta(seconds=5)
        execute_target(3, "0.15", "1.08200", "1.08210", "SET_TARGET")
        qty_reduced = Decimal(ledger.report()["quantity_eur"])
        assert Decimal("0") < qty_reduced < qty_added

        # 4. Reversal from Long (+15%) to Short (-20%)
        now = now + timedelta(seconds=5)
        execute_target(4, "-0.20", "1.08150", "1.08160", "SET_TARGET")
        qty_reversed = Decimal(ledger.report()["quantity_eur"])
        assert qty_reversed < Decimal("0")

        # 5. Explicit CLOSE to flat (0%)
        now = now + timedelta(seconds=5)
        execute_target(5, "0", "1.08050", "1.08060", "CLOSE")
        rep_final = ledger.report()
        assert Decimal(rep_final["quantity_eur"]) == Decimal("0")
        assert rep_final["avg_entry_price"] is None
        assert rep_final["fill_count"] == 5
    finally:
        ledger.close()


def test_automated_shadow_runner_protection_independent_of_hung_llm_and_cli(tmp_path, capsys):
    """Section 一 & 八: ShadowRunner injects real event, enters HungAgent, and still executes protective exits, stop, and duration timeout."""
    from eventlens.observer import CaptureBatch, ObserverStore, SourceRecord

    now = datetime.now(UTC)
    obs_db = tmp_path / "runner_obs.sqlite"
    shd_db = tmp_path / "runner_shadow.sqlite"
    mem_db = tmp_path / "runner_mem.sqlite"
    obs = ObserverStore(obs_db)
    memory = SQLiteThesisMemory(mem_db, allow_targets=True)
    state_store = SQLiteRuntimeStore(shd_db)
    ledger = SQLiteShadowLedger(
        shd_db,
        exit_policy=ProtectiveExitPolicy(max_holding_seconds=1),
    )

    # Record an eligible live EUR/USD macro event in ObserverStore so runner actually invokes HungAgent.decide()
    rec_live = SourceRecord(
        source_id="9001",
        kind="news",
        source_url="https://wallstreetcn.com/livenews/9001",
        title="ECB signals urgent EUR/USD policy rate recalibration",
        text="ECB Governing Council commentary triggers immediate EUR/USD rate reassessment.",
        publisher_display_at=now - timedelta(seconds=1),
        importance=3,
    )
    obs.record(
        CaptureBatch(
            provider="wallstreetcn",
            stream="news/global",
            kind="news",
            basis="live_poll",
            request_url="https://api-one-wscn.awtmt.com/apiv1/content/lives",
            request_started_at=now - timedelta(milliseconds=50),
            received_at=now,
            elapsed_ms=10.0,
            attempts=1,
            payload=b'{"live":9001}',
            records=(rec_live,),
        )
    )

    # Open a position in ledger first (already aged 2s > max_holding_seconds=1s)
    snap0 = ledger.snapshot_sync()
    dec = make_decision(
        snapshot_id=snap0.snapshot_id,
        started_at=now - timedelta(seconds=2),
        completed_at=now - timedelta(seconds=2),
        valid_until=now + timedelta(seconds=60),
    )
    engine = DeterministicHardRiskEngine()
    assess = asyncio.run(engine.assess(dec, snap0, make_market(as_of=now)))
    ledger.record_sync(dec, assess)
    fill_ts = datetime.now(UTC)
    fills = ledger.fill_pending(make_market(as_of=fill_ts))
    assert len(fills) == 1
    assert ledger.snapshot_sync().positions != ()

    class HungAgent:
        def __init__(self) -> None:
            self.calls = 0
            self.active_hangs = 0
            self.cancelled = 0
            self.entered = asyncio.Event()

        async def decide(self, context):
            self.calls += 1
            self.active_hangs += 1
            self.entered.set()
            try:
                await asyncio.sleep(30.0)
                raise RuntimeError("unreachable")
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
            finally:
                self.active_hangs -= 1

    hung_agent = HungAgent()
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: make_market(as_of=ts),
        agent=hung_agent,
        risk_engine=engine,
        shadow_executor=ledger,
        state_store=state_store,
    )
    runner = ShadowRunner(
        runtime=runtime,
        ledger=ledger,
        observer_store=obs,
        quote_interval_seconds=0.05,
        protection_interval_seconds=0.05,
        event_scan_interval_seconds=0.05,
        llm_timeout_seconds=10.0,
    )
    try:

        async def _verify_hung_loop_and_duration() -> dict:
            run_task = asyncio.create_task(runner.run(duration_seconds=1.25))
            # 1. Assert model actually entered hung state on the injected event
            await asyncio.wait_for(hung_agent.entered.wait(), timeout=1.0)
            assert hung_agent.calls == 1
            assert hung_agent.active_hangs == 1

            # 2. Wait until protective exit flattens the open position while HungAgent is STILL hung
            deadline = asyncio.get_running_loop().time() + 1.15
            while ledger.snapshot_sync().positions != ():
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.05)
            assert hung_agent.active_hangs == 1

            # 3. Runner stops via duration_seconds=1.25 even though llm_timeout_seconds=10.0
            return await asyncio.wait_for(run_task, timeout=1.5)

        summary = asyncio.run(_verify_hung_loop_and_duration())
        assert hung_agent.calls >= 1
        assert hung_agent.cancelled >= 1
        assert summary["open_quantity_eur"] == "0"
        assert summary["ledger"]["fill_count"] == 2
        assert summary["ledger"]["fills"][-1]["kind"] == "protective_exit"

        # 4. Also verify external stop request (state_store.request_stop) stops runner immediately while model is hung
        ts2 = datetime.now(UTC)
        rec_live_2 = SourceRecord(
            source_id="9002",
            kind="news",
            source_url="https://wallstreetcn.com/livenews/9002",
            title="Federal Reserve emergency statement on EUR/USD liquidity",
            text="Federal Reserve statement moves USD and EUR/USD intraday.",
            publisher_display_at=ts2 - timedelta(milliseconds=100),
            importance=3,
        )
        obs.record(
            CaptureBatch(
                provider="wallstreetcn",
                stream="news/global",
                kind="news",
                basis="live_poll",
                request_url="https://api-one-wscn.awtmt.com/apiv1/content/lives",
                request_started_at=ts2 - timedelta(milliseconds=50),
                received_at=ts2,
                elapsed_ms=10.0,
                attempts=1,
                payload=b'{"live":9002}',
                records=(rec_live_2,),
            )
        )

        async def _verify_stop_while_hung() -> None:
            hung_agent.entered = asyncio.Event()
            run_task = asyncio.create_task(runner.run(duration_seconds=10.0))
            await asyncio.wait_for(hung_agent.entered.wait(), timeout=1.0)
            assert hung_agent.active_hangs == 1
            state_store.request_stop(datetime.now(UTC))
            await asyncio.wait_for(run_task, timeout=1.0)

        asyncio.run(_verify_stop_while_hung())
    finally:
        obs.close()
        ledger.close()
        state_store.close()

    # Verify CLI status, stop, recover, and report commands
    for subcmd in ("status", "stop", "recover", "report"):
        main(["shadow", "--shadow-db", str(shd_db), "--memory-db", str(mem_db), subcmd])
        out = json.loads(capsys.readouterr().out)
        assert out["execution_authorized"] is False


def test_startup_recovery_non_blocking_hung_model_stop_timeout_retries_and_staleness(tmp_path):
    """Startup recovery enforces staleness, retry limits, LLM timeout, and never blocks protection/stop/duration."""
    now = datetime.now(UTC)
    shd_db = tmp_path / "rec_nonblock_shd.sqlite"
    mem_db = tmp_path / "rec_nonblock_mem.sqlite"
    memory = SQLiteThesisMemory(mem_db, allow_targets=True)
    state_store = SQLiteRuntimeStore(shd_db)
    ledger = SQLiteShadowLedger(
        shd_db,
        exit_policy=ProtectiveExitPolicy(max_holding_seconds=1),
    )

    class HungRecoveryAgent:
        def __init__(self) -> None:
            self.calls = 0
            self.active_hangs = 0
            self.entered = asyncio.Event()

        async def decide(self, context):
            self.calls += 1
            self.active_hangs += 1
            self.entered.set()
            try:
                await asyncio.sleep(30.0)
                raise RuntimeError("unreachable")
            finally:
                self.active_hangs -= 1

    hung_agent = HungRecoveryAgent()
    engine = DeterministicHardRiskEngine()
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: make_market(as_of=ts),
        agent=hung_agent,
        risk_engine=engine,
        shadow_executor=ledger,
        state_store=state_store,
        max_recovery_event_age=timedelta(seconds=60),
    )

    try:
        # 1. Stale pre-crash event (> 60s old) and retry-exhausted event (attempts >= 3) must NOT invoke LLM
        ev_stale = Event(
            event_id="ev-rec-stale",
            source="wallstreetcn_news",
            source_type="news",
            source_reference="https://wallstreetcn.com/livenews/stale",
            source_timestamp=now - timedelta(seconds=120),
            received_timestamp=now - timedelta(seconds=120),
            raw_text="Stale ECB EUR/USD headline from 2 minutes ago.",
        )
        ev_exhausted = Event(
            event_id="ev-rec-exhausted",
            source="wallstreetcn_news",
            source_type="news",
            source_reference="https://wallstreetcn.com/livenews/exhausted",
            source_timestamp=now - timedelta(seconds=5),
            received_timestamp=now - timedelta(seconds=5),
            raw_text="Fresh ECB EUR/USD headline that already exhausted retries.",
        )
        asyncio.run(memory.record_event(ev_stale))
        asyncio.run(memory.record_event(ev_exhausted))
        state_store.upsert_event_state(
            event_id=ev_stale.event_id,
            source_reference=ev_stale.source_reference,
            received_at=ev_stale.received_timestamp,
            now=now,
            state="received",
            reason="event_persisted",
        )
        for _ in range(3):
            state_store.upsert_event_state(
                event_id=ev_exhausted.event_id,
                source_reference=ev_exhausted.source_reference,
                received_at=ev_exhausted.received_timestamp,
                now=now,
                state="reasoning_failed",
                reason="transient_error",
                increment_attempts=True,
            )

        rec_results = asyncio.run(
            runtime.recover_incomplete(
                max_event_age=timedelta(seconds=60),
                max_attempts=3,
                llm_timeout_seconds=0.2,
            )
        )
        assert hung_agent.calls == 0
        by_id = {r.event_id: r for r in rec_results}
        assert by_id["ev-rec-stale"].status == "recovery_expired"
        assert by_id["ev-rec-stale"].reason == "recovery_event_stale"
        assert by_id["ev-rec-exhausted"].status == "recovery_abandoned"
        assert by_id["ev-rec-exhausted"].reason == "recovery_max_attempts_exceeded"

        # 2. Fresh incomplete event with LLM timeout during recovery increments attempts and records timeout
        ev_timeout = Event(
            event_id="ev-rec-timeout",
            source="wallstreetcn_news",
            source_type="news",
            source_reference="https://wallstreetcn.com/livenews/timeout",
            source_timestamp=now - timedelta(seconds=2),
            received_timestamp=now - timedelta(seconds=2),
            raw_text="ECB EUR/USD headline that times out during recovery.",
        )
        asyncio.run(memory.record_event(ev_timeout))
        state_store.upsert_event_state(
            event_id=ev_timeout.event_id,
            source_reference=ev_timeout.source_reference,
            received_at=ev_timeout.received_timestamp,
            now=now,
            state="received",
            reason="event_persisted",
        )
        to_results = asyncio.run(
            runtime.recover_incomplete(
                max_event_age=timedelta(seconds=60),
                max_attempts=3,
                llm_timeout_seconds=0.1,
            )
        )
        assert hung_agent.calls == 1
        assert len(to_results) == 1
        assert to_results[0].status == "reasoning_failed"
        assert to_results[0].reason == "llm_decision_timeout"
        row_to = state_store.get_event_state("ev-rec-timeout")
        assert row_to is not None and int(row_to["attempts"]) == 1

        # 3. Startup recovery inside ShadowRunner.run() with a hung model MUST NOT block protective exits,
        # stop requests, or duration_seconds!
        snap0 = ledger.snapshot_sync()
        dec = make_decision(
            snapshot_id=snap0.snapshot_id,
            started_at=now - timedelta(seconds=2),
            completed_at=now - timedelta(seconds=2),
            valid_until=now + timedelta(seconds=60),
        )
        assess = asyncio.run(engine.assess(dec, snap0, make_market(as_of=now)))
        ledger.record_sync(dec, assess)
        assert len(ledger.fill_pending(make_market(as_of=datetime.now(UTC)))) == 1
        assert ledger.snapshot_sync().positions != ()

        runner = ShadowRunner(
            runtime=runtime,
            ledger=ledger,
            quote_interval_seconds=0.05,
            protection_interval_seconds=0.05,
            event_scan_interval_seconds=0.05,
            llm_timeout_seconds=10.0,
            max_reasoning_attempts=3,
        )

        async def _verify_startup_recovery_non_blocking() -> None:
            hung_agent.entered = asyncio.Event()
            run_task = asyncio.create_task(runner.run(duration_seconds=5.0))
            # Wait for startup recover_incomplete() to enter HungRecoveryAgent.decide()
            await asyncio.wait_for(hung_agent.entered.wait(), timeout=1.0)
            assert hung_agent.active_hangs == 1

            # Protective exit must flatten the open position while startup recovery is STILL hung in LLM!
            deadline = asyncio.get_running_loop().time() + 1.2
            while ledger.snapshot_sync().positions != ():
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.05)
            assert hung_agent.active_hangs == 1

            # Request stop while startup recovery is STILL hung in LLM; runner must exit promptly (< 1.0s)
            state_store.request_stop(datetime.now(UTC))
            summary = await asyncio.wait_for(run_task, timeout=1.0)
            assert summary["open_quantity_eur"] == "0"
            assert summary["ledger"]["fills"][-1]["kind"] == "protective_exit"

        asyncio.run(_verify_startup_recovery_non_blocking())

        # Also verify duration_seconds alone stops hung startup recovery on schedule
        async def _verify_startup_recovery_duration_cap() -> None:
            hung_agent.entered = asyncio.Event()
            run_task = asyncio.create_task(runner.run(duration_seconds=0.25))
            await asyncio.wait_for(hung_agent.entered.wait(), timeout=1.0)
            assert hung_agent.active_hangs == 1
            await asyncio.wait_for(run_task, timeout=1.0)

        asyncio.run(_verify_startup_recovery_duration_cap())
    finally:
        ledger.close()
        state_store.close()


def test_kill_switch_allows_risk_reduction_blocks_reversal_and_shutdown_preserves_unclosed_position(
    tmp_path,
):
    """Section 一 & 六: Kill switch permits CLOSE/reduction, blocks reversal; runner shutdown never fakes exit fill."""
    assert (
        classify_exposure_change(
            Decimal("1000"), Decimal("10000"), Decimal("1.08500"), Decimal("0")
        )
        == "risk_reducing"
    )
    assert (
        classify_exposure_change(
            Decimal("1000"), Decimal("10000"), Decimal("1.08500"), Decimal("-0.10")
        )
        == "reversal_risk_increasing"
    )

    now = T0
    shd_db = tmp_path / "unclosed.sqlite"
    mem_db = tmp_path / "unclosed_mem.sqlite"
    memory = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    ledger = SQLiteShadowLedger(
        shd_db,
        exit_policy=ProtectiveExitPolicy(max_holding_seconds=10),
        clock=lambda: now,
    )
    validator = DeterministicTargetValidator()

    # Open position when kill switch is False
    snap0 = ledger.snapshot_sync()
    dec_open = make_decision(
        snapshot_id=snap0.snapshot_id,
        weight="0.20",
        started_at=now,
        completed_at=now,
        valid_until=now + timedelta(seconds=60),
    )
    validator.validate(dec_open.target, snap0)  # type: ignore[arg-type]
    engine_normal = DeterministicHardRiskEngine(clock=lambda: now)
    assess_open = asyncio.run(engine_normal.assess(dec_open, snap0, make_market(as_of=now)))
    ledger.record_sync(dec_open, assess_open)
    now = now + timedelta(seconds=1)
    assert len(ledger.fill_pending(make_market(as_of=now))) == 1

    # Now enable kill switch while position is open:
    # - Reversal (-0.10) MUST be rejected with reversal_blocked_by_kill_switch
    # - Explicit CLOSE (0) MUST be approved even with kill switch active!
    snap_open = ledger.snapshot_sync(market=make_market(as_of=now))
    engine_ks = DeterministicHardRiskEngine(
        policy=RiskPolicy(kill_switch_active=True), clock=lambda: now
    )
    dec_rev = make_decision(
        decision_id="dec-rev",
        event_id="ev-rev",
        snapshot_id=snap_open.snapshot_id,
        weight="-0.10",
        started_at=now,
        completed_at=now,
        valid_until=now + timedelta(seconds=60),
    )
    assess_rev = asyncio.run(engine_ks.assess(dec_rev, snap_open, make_market(as_of=now)))
    assert assess_rev.outcome == "reject"
    assert "reversal_blocked_by_kill_switch" in assess_rev.reasons

    dec_close = make_decision(
        decision_id="dec-close-ks",
        event_id="ev-close-ks",
        snapshot_id=snap_open.snapshot_id,
        weight="0",
        portfolio_action="CLOSE",
        started_at=now,
        completed_at=now,
        valid_until=now + timedelta(seconds=60),
    )
    assess_close = asyncio.run(engine_ks.assess(dec_close, snap_open, make_market(as_of=now)))
    assert assess_close.outcome == "approve"

    # Now advance time past max_holding_seconds (20s > 10s), but quotes are missing (empty MarketSnapshot)!
    # Running ShadowRunner to completion must NOT fabricate a close fill; it must persist open position & pending_exit_reason.
    now = now + timedelta(seconds=20)
    empty_market = MarketSnapshot(snapshot_id="empty", as_of=now, quotes=())

    class IdleAgent:
        async def decide(self, context):
            raise RuntimeError("unused")

    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: empty_market,
        agent=IdleAgent(),
        risk_engine=engine_normal,
        shadow_executor=ledger,
        clock=lambda: now,
    )
    runner = ShadowRunner(
        runtime=runtime,
        ledger=ledger,
        quote_interval_seconds=0.05,
        protection_interval_seconds=0.05,
        event_scan_interval_seconds=0.05,
        clock=lambda: now,
    )
    try:
        summary = asyncio.run(runner.run(duration_seconds=0.15))
        assert Decimal(summary["open_quantity_eur"]) > Decimal("0")
        assert summary["pending_exit_reason"] == "protective_max_holding_horizon"
    finally:
        if runtime.state_store is not None:
            runtime.state_store.close()
        ledger.close()


def test_shadow_runner_skips_baseline_snapshot_and_processes_new_live_arrival(tmp_path):
    """Section 九.5: Baseline snapshot revisions are not treated as live arrivals when allow_baseline_revisions=False."""
    from eventlens.observer import CaptureBatch, ObserverStore, SourceRecord

    now = datetime.now(UTC)
    obs_db = tmp_path / "obs_live.sqlite"
    shd_db = tmp_path / "shd_live.sqlite"
    mem_db = tmp_path / "mem_live.sqlite"

    obs = ObserverStore(obs_db, clock=lambda: now)
    memory = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    state_store = SQLiteRuntimeStore(shd_db)
    ledger = SQLiteShadowLedger(shd_db, clock=lambda: now)

    rec_base = SourceRecord(
        source_id="1001",
        kind="news",
        source_url="https://wallstreetcn.com/livenews/1001",
        title="ECB signals hawkish rate stance for EUR/USD",
        text="ECB Governing Council signals higher eurozone rates supporting EUR/USD.",
        publisher_display_at=now - timedelta(seconds=10),
        importance=3,
    )
    batch1 = CaptureBatch(
        provider="wallstreetcn",
        stream="news/global",
        kind="news",
        basis="live_poll",
        request_url="https://api-one-wscn.awtmt.com/apiv1/content/lives",
        request_started_at=now - timedelta(seconds=1),
        received_at=now,
        elapsed_ms=10.0,
        attempts=1,
        payload=b'{"baseline":1}',
        records=(rec_base,),
    )
    obs.record(batch1)

    now = now + timedelta(seconds=5)
    rec_new = SourceRecord(
        source_id="1002",
        kind="news",
        source_url="https://wallstreetcn.com/livenews/1002",
        title="Fed chair speech impacts EUR/USD outlook",
        text="Federal Reserve commentary shifts USD rate expectations against EUR/USD.",
        publisher_display_at=now - timedelta(seconds=1),
        importance=3,
    )
    batch2 = CaptureBatch(
        provider="wallstreetcn",
        stream="news/global",
        kind="news",
        basis="live_poll",
        request_url="https://api-one-wscn.awtmt.com/apiv1/content/lives",
        request_started_at=now - timedelta(seconds=1),
        received_at=now,
        elapsed_ms=10.0,
        attempts=1,
        payload=b'{"new":2}',
        records=(rec_new, rec_base),
    )
    obs.record(batch2)

    called_ids: list[str] = []

    class RecordingHoldAgent:
        async def decide(self, context):
            called_ids.append(context.event.event_id)
            return AgentDecision(
                decision_id=f"dec-{context.event.event_id}",
                event_id=context.event.event_id,
                based_on_memory_version=context.memory.memory_version,
                based_on_snapshot_id=context.portfolio.snapshot_id,
                provider="deepseek",
                model_version="deepseek-flash",
                prompt_version="single-pass-agent-v2",
                started_at=now,
                completed_at=now,
                valid_until=now + timedelta(seconds=60),
                portfolio_action="IGNORE_EVENT",
                thesis_changes=(),
                target=None,
                rationale="observed_live",
            )

    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: make_market(as_of=ts),
        agent=RecordingHoldAgent(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger,
        state_store=state_store,
        clock=lambda: now,
    )
    runner = ShadowRunner(
        runtime=runtime,
        ledger=ledger,
        observer_store=obs,
        allow_baseline_revisions=False,
        clock=lambda: now,
    )
    try:
        asyncio.run(runner.step_events())
        assert called_ids == ["wallstreetcn:news:1002:r1"]
        rep = state_store.summary_report()
        assert rep["reason_counts"]["baseline_snapshot_not_live_arrival"] == 1
        assert rep["state_counts"]["no_target"] == 1
    finally:
        obs.close()
        ledger.close()
        state_store.close()


def test_cross_day_unconfirmed_baseline_blocks_risk_persists_on_restart_and_confirms_conservatively(
    tmp_path,
):
    """Cross-day rollover with open position + missing quotes marks baseline unconfirmed, blocks new risk, persists across restart, and never masks losses upon quote recovery."""
    # Day 1 (2026-07-15 19:00 UTC == 15:00 EDT, trading day 2026-07-15)
    now = datetime(2026, 7, 15, 19, 0, 0, tzinfo=UTC)
    db_file = tmp_path / "cross_day_baseline.sqlite"
    policy = RiskPolicy(
        max_position_weight=Decimal("0.50"),
        max_gross_weight=Decimal("0.50"),
        max_daily_loss_usd=Decimal("50"),
    )
    ledger = SQLiteShadowLedger(
        db_file,
        exit_policy=ProtectiveExitPolicy(
            max_holding_seconds=86400 * 3,
            max_position_loss_usd=Decimal("500"),
            max_daily_loss_usd=Decimal("50"),
        ),
        risk_policy=policy,
        clock=lambda: now,
    )
    engine = DeterministicHardRiskEngine(policy=policy, clock=lambda: now)

    try:
        # 1. Open long position on Day 1 at 1.08500 / 1.08510
        mkt_open = make_market(
            quote=make_quote(bid="1.08500", ask="1.08510", received_at=now), as_of=now
        )
        snap0 = ledger.snapshot_sync(market=mkt_open)
        assert snap0.daily_baseline_valid is True
        dec_open = make_decision(
            decision_id="dec-day1-open",
            event_id="ev-day1-open",
            snapshot_id=snap0.snapshot_id,
            weight="0.20",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess_open = asyncio.run(engine.assess(dec_open, snap0, mkt_open))
        assert assess_open.outcome == "approve"
        ledger.record_sync(dec_open, assess_open)
        now = now + timedelta(seconds=1)
        assert (
            len(
                ledger.fill_pending(
                    make_market(
                        quote=make_quote(bid="1.08500", ask="1.08510", received_at=now),
                        as_of=now,
                    ),
                    risk_policy=policy,
                )
            )
            == 1
        )

        # 2. Still on Day 1 (20:00 UTC == 16:00 EDT), valid quote rallies to 1.12000 / 1.12010
        # so last_valid_equity_usd is ~$10,064.47 while raw cash_usd is $9,999.96.
        now = datetime(2026, 7, 15, 20, 0, 0, tzinfo=UTC)
        mkt_high = make_market(
            quote=make_quote(bid="1.12000", ask="1.12010", received_at=now), as_of=now
        )
        snap_high = ledger.snapshot_sync(market=mkt_high)
        assert snap_high.valuation_valid is True
        assert snap_high.daily_baseline_valid is True
        high_equity = snap_high.equity
        raw_cash = Decimal(ledger.report(market=mkt_high)["cash_usd"])
        assert high_equity > Decimal("10060")
        assert raw_cash < Decimal("10000")

        # 3. Rollover across 17:00 EDT into trading day 2026-07-16 (21:30 UTC == 17:30 EDT) DURING a quote outage!
        now = datetime(2026, 7, 15, 21, 30, 0, tzinfo=UTC)
        empty_market = MarketSnapshot(snapshot_id="empty-rollover", as_of=now, quotes=())
        snap_rollover = ledger.snapshot_sync(market=empty_market)
        rep_rollover = ledger.report(market=empty_market)
        assert rep_rollover["daily_states"][-1]["trading_day"] == "2026-07-16"
        assert snap_rollover.valuation_valid is False
        assert snap_rollover.daily_baseline_valid is False
        assert (
            snap_rollover.daily_baseline_reason == "unconfirmed_open_position_without_valid_quote"
        )
        # Raw cash_usd ($9,999.96) must NEVER be written as the confirmed baseline!
        assert snap_rollover.daily_baseline_equity_usd == high_equity
        assert snap_rollover.daily_baseline_equity_usd != raw_cash

        # Risk-increasing action while daily_baseline_valid is False must be rejected with daily_baseline_unconfirmed
        dec_add = make_decision(
            decision_id="dec-day2-add",
            event_id="ev-day2-add",
            snapshot_id=snap_rollover.snapshot_id,
            weight="0.35",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        valid_probe_mkt = make_market(
            quote=make_quote(bid="1.12000", ask="1.12010", received_at=now), as_of=now
        )
        assess_add = asyncio.run(engine.assess(dec_add, snap_rollover, valid_probe_mkt))
        assert assess_add.outcome == "reject"
        assert "daily_baseline_unconfirmed" in assess_add.reasons

        # Risk-reducing CLOSE remains permitted by risk engine even while baseline is unconfirmed
        dec_close = make_decision(
            decision_id="dec-day2-close",
            event_id="ev-day2-close",
            snapshot_id=snap_rollover.snapshot_id,
            weight="0",
            portfolio_action="CLOSE",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess_close = asyncio.run(engine.assess(dec_close, snap_rollover, valid_probe_mkt))
        assert assess_close.outcome == "approve"
    finally:
        ledger.close()

    # 4. Simulate process restart on Day 2 while quotes are STILL missing:
    # unconfirmed baseline state must persist across restart and not reset to cash_usd!
    now = datetime(2026, 7, 15, 21, 35, 0, tzinfo=UTC)
    reopened = SQLiteShadowLedger(
        db_file,
        exit_policy=ProtectiveExitPolicy(
            max_holding_seconds=86400 * 3,
            max_position_loss_usd=Decimal("500"),
            max_daily_loss_usd=Decimal("50"),
        ),
        risk_policy=policy,
        clock=lambda: now,
    )
    try:
        empty_restart = MarketSnapshot(snapshot_id="empty-restart", as_of=now, quotes=())
        snap_restart = reopened.snapshot_sync(market=empty_restart)
        assert snap_restart.daily_baseline_valid is False
        assert snap_restart.daily_baseline_reason == "unconfirmed_open_position_without_valid_quote"
        assert snap_restart.daily_baseline_equity_usd == high_equity

        # 5. Quotes recover at 21:40 UTC after a sharp drop (1.08000 / 1.08010, equity ~$9,990.57, down ~$73.73 from high_equity).
        # Conservative baseline confirmation uses max(provisional, last_valid_equity_usd, prior_baseline, current_equity) == high_equity,
        # so the ~$73.73 loss during the outage is NOT masked by the lower post-recovery equity and immediately triggers daily_loss_limit!
        now = datetime(2026, 7, 15, 21, 40, 0, tzinfo=UTC)
        mkt_recovered_low = make_market(
            quote=make_quote(bid="1.08000", ask="1.08010", received_at=now), as_of=now
        )
        snap_recovered = reopened.snapshot_sync(market=mkt_recovered_low)
        assert snap_recovered.valuation_valid is True
        assert snap_recovered.daily_baseline_valid is True
        assert snap_recovered.daily_baseline_reason == "confirmed_after_quote_recovery_conservative"
        assert snap_recovered.daily_baseline_equity_usd == high_equity
        assert snap_recovered.daily_pnl_usd <= Decimal("-50")
        assert snap_recovered.daily_loss_limit_triggered is True

        # Protective exit flattens the position and subsequent risk-increasing orders are blocked on Day 2
        exits = reopened.evaluate_protective_exits(mkt_recovered_low)
        assert len(exits) == 1
        assert exits[0].new_quantity == Decimal("0")

        snap_flat = reopened.snapshot_sync(market=mkt_recovered_low)
        assert snap_flat.daily_loss_limit_triggered is True
        dec_new_risk = make_decision(
            decision_id="dec-day2-blocked",
            event_id="ev-day2-blocked",
            snapshot_id=snap_flat.snapshot_id,
            weight="0.20",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        engine_reopened = DeterministicHardRiskEngine(policy=policy, clock=lambda: now)
        assess_blocked = asyncio.run(
            engine_reopened.assess(dec_new_risk, snap_flat, mkt_recovered_low)
        )
        assert assess_blocked.outcome == "reject"
        assert "daily_loss_limit_exceeded" in assess_blocked.reasons
    finally:
        reopened.close()


def test_stop_request_duration_expiry_and_concurrent_stop_fill_block_new_risk_while_allowing_exits(
    tmp_path,
):
    """Explicit stop, duration expiry, and concurrent stop+fill cancel pending risk-increasing orders and block new risk while allowing reductions and protective exits."""
    now = T0
    shd_db = tmp_path / "stop_guard_shd.sqlite"
    mem_db = tmp_path / "stop_guard_mem.sqlite"
    memory = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    state_store = SQLiteRuntimeStore(shd_db)
    policy = RiskPolicy(
        max_position_weight=Decimal("0.50"),
        max_gross_weight=Decimal("0.50"),
    )
    ledger = SQLiteShadowLedger(
        shd_db,
        exit_policy=ProtectiveExitPolicy(max_holding_seconds=10),
        risk_policy=policy,
        clock=lambda: now,
    )
    engine = DeterministicHardRiskEngine(policy=policy, clock=lambda: now)

    class StaticDecisionAgent:
        def __init__(self, decision: AgentDecision) -> None:
            self.decision = decision

        async def decide(self, context):
            return self.decision

    try:
        # --- Part 1: Explicit stop cancels pending open order and blocks new risk-increasing proposals ---
        mkt0 = make_market(as_of=now)
        snap0 = ledger.snapshot_sync(market=mkt0)
        dec_open_pending = make_decision(
            decision_id="dec-stop-open-1",
            event_id="ev-stop-open-1",
            snapshot_id=snap0.snapshot_id,
            weight="0.20",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        runtime = IntradayEventRuntime(
            memory=memory,
            portfolio_state=ledger,
            market_provider=lambda ts: make_market(as_of=ts),
            agent=StaticDecisionAgent(dec_open_pending),
            risk_engine=engine,
            shadow_executor=ledger,
            state_store=state_store,
            clock=lambda: now,
        )
        runner = ShadowRunner(runtime=runtime, ledger=ledger, clock=lambda: now)

        ev1 = Event(
            event_id="ev-stop-open-1",
            source="wallstreetcn_news",
            source_type="news",
            source_reference="https://wallstreetcn.com/livenews/stop1",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="ECB signals hawkish EUR/USD adjustment.",
        )
        res1 = asyncio.run(runtime.run_cycle(ev1))
        assert res1.status == "shadow_recorded"
        assert len(ledger.snapshot_sync(market=mkt0).pending_order_ids) == 1

        # Trigger explicit stop BEFORE fill_pending runs
        now = now + timedelta(seconds=1)
        runner.stop()
        # Pending risk-increasing order is immediately cancelled and recorded in receipts/faults/runtime
        rep_after_stop = ledger.report(market=make_market(as_of=now))
        assert rep_after_stop["order_status_counts"]["cancelled"] == 1
        assert (
            rep_after_stop["receipts"][0]["terminal_reason"] == "cancelled_on_runner_stop_requested"
        )
        ev1_row = state_store.get_event_state("ev-stop-open-1")
        assert ev1_row is not None
        assert ev1_row["state"] == "shadow_rejected"
        assert ev1_row["reason"] == "cancelled_on_runner_stop_requested"

        # Subsequent fill_pending with a valid fresh quote MUST NOT fill the cancelled order!
        assert ledger.fill_pending(make_market(as_of=now), risk_policy=policy) == ()
        assert Decimal(ledger.report()["quantity_eur"]) == Decimal("0")

        # Attempting a new risk-increasing cycle while stopped MUST be rejected with blocked_by_runner_stop_requested
        snap_stopped = ledger.snapshot_sync(market=make_market(as_of=now))
        dec_after_stop = make_decision(
            decision_id="dec-after-stop",
            event_id="ev-after-stop",
            snapshot_id=snap_stopped.snapshot_id,
            thesis_id="t-after-stop",
            weight="0.25",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        ).model_copy(update={"based_on_memory_version": 1})
        runtime.agent = StaticDecisionAgent(dec_after_stop)
        ev2 = Event(
            event_id="ev-after-stop",
            source="wallstreetcn_news",
            source_type="news",
            source_reference="https://wallstreetcn.com/livenews/stop2",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="Second ECB EUR/USD headline after stop.",
        )
        res2 = asyncio.run(runtime.run_cycle(ev2))
        assert res2.status == "risk_rejected"
        assert res2.reason == "blocked_by_runner_stop_requested"
        assert Decimal(ledger.report()["quantity_eur"]) == Decimal("0")

        # --- Part 2: Duration expiry cancels pending open order and never fills during shutdown pass ---
        # Start a new run where quotes are missing during the active window, a pending open order is recorded,
        # and a valid quote arrives right as duration_seconds expires.
        now = now + timedelta(seconds=5)
        state_store.update_runner_state(
            status="running",
            now=now,
            started_at=now,
            duration_seconds=1.0,
            reset_stop=True,
        )
        runner._stop_reason = None
        runner._stop_event = asyncio.Event()
        runner._run_started_at = now
        runner._run_duration_seconds = 1.0

        snap_run = ledger.snapshot_sync(market=make_market(as_of=now))
        dec_dur = make_decision(
            decision_id="dec-dur-open",
            event_id="ev-dur-open",
            snapshot_id=snap_run.snapshot_id,
            weight="0.20",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess_dur = asyncio.run(engine.assess(dec_dur, snap_run, make_market(as_of=now)))
        rcpt_dur = ledger.record_sync(dec_dur, assess_dur)
        assert rcpt_dur.status == "recorded"

        # Advance clock past duration_seconds (1.0s -> 1.5s) and call fill_pending with a valid quote:
        # Must cancel the pending open order with cancelled_on_runner_duration_expired and NOT fill!
        now = now + timedelta(milliseconds=1500)
        assert ledger.fill_pending(make_market(as_of=now), risk_policy=policy) == ()
        rep_dur = ledger.report(market=make_market(as_of=now))
        dur_rcpt = next(r for r in rep_dur["receipts"] if r["decision_id"] == "dec-dur-open")
        assert dur_rcpt["order_status"] == "cancelled"
        assert dur_rcpt["terminal_reason"] == "cancelled_on_runner_duration_expired"
        assert Decimal(rep_dur["quantity_eur"]) == Decimal("0")

        # --- Part 3: While stopped, risk-reducing orders (partial reduction & CLOSE) and protective exits STILL work ---
        # Start a fresh run, open a 30% long position, then trigger stop
        now = now + timedelta(seconds=2)
        state_store.update_runner_state(
            status="running",
            now=now,
            started_at=now,
            duration_seconds=60.0,
            reset_stop=True,
        )
        runner._stop_reason = None
        runner._stop_event = asyncio.Event()
        runner._run_started_at = now
        runner._run_duration_seconds = 60.0

        snap_active = ledger.snapshot_sync(market=make_market(as_of=now))
        dec_open_active = make_decision(
            decision_id="dec-active-30",
            event_id="ev-active-30",
            snapshot_id=snap_active.snapshot_id,
            weight="0.30",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess_active = asyncio.run(
            engine.assess(dec_open_active, snap_active, make_market(as_of=now))
        )
        ledger.record_sync(dec_open_active, assess_active)
        now = now + timedelta(seconds=1)
        fills_open = ledger.fill_pending(make_market(as_of=now), risk_policy=policy)
        assert len(fills_open) == 1
        initial_qty = Decimal(ledger.report()["quantity_eur"])
        assert initial_qty > Decimal("0")

        # Now request stop while position is open!
        runner.stop()

        # 3a. Reversal (-10%) is risk-increasing and MUST be blocked on stop
        snap_pos = ledger.snapshot_sync(market=make_market(as_of=now))
        dec_rev_stop = make_decision(
            decision_id="dec-rev-stop",
            event_id="ev-rev-stop",
            snapshot_id=snap_pos.snapshot_id,
            weight="-0.10",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess_rev_stop = asyncio.run(engine.assess(dec_rev_stop, snap_pos, make_market(as_of=now)))
        rcpt_rev_stop = ledger.record_sync(dec_rev_stop, assess_rev_stop)
        assert rcpt_rev_stop.status == "rejected"
        assert rcpt_rev_stop.reason == "blocked_by_runner_stop_requested"

        # 3b. Partial reduction (+10% < +30%) is risk-reducing and MUST still be recorded and filled while stopped!
        dec_reduce_stop = make_decision(
            decision_id="dec-reduce-stop",
            event_id="ev-reduce-stop",
            snapshot_id=snap_pos.snapshot_id,
            weight="0.10",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess_reduce_stop = asyncio.run(
            engine.assess(dec_reduce_stop, snap_pos, make_market(as_of=now))
        )
        rcpt_reduce_stop = ledger.record_sync(dec_reduce_stop, assess_reduce_stop)
        assert rcpt_reduce_stop.status == "recorded"
        now = now + timedelta(seconds=1)
        fills_reduce = ledger.fill_pending(make_market(as_of=now), risk_policy=policy)
        assert len(fills_reduce) == 1
        reduced_qty = Decimal(ledger.report()["quantity_eur"])
        assert Decimal("0") < reduced_qty < initial_qty

        # 3c. Protective exit (max_holding_seconds=10s elapsed) still executes while stopped!
        now = now + timedelta(seconds=15)
        exit_fills = ledger.evaluate_protective_exits(make_market(as_of=now))
        assert len(exit_fills) == 1
        assert exit_fills[0].kind == "protective_exit"
        assert Decimal(ledger.report()["quantity_eur"]) == Decimal("0")

        # --- Part 4: Concurrent stop and fill race ---
        # Verify that when stop fires concurrently with fill_pending, pending open order is cancelled and never fills.
        now = now + timedelta(seconds=2)
        state_store.update_runner_state(
            status="running",
            now=now,
            started_at=now,
            duration_seconds=60.0,
            reset_stop=True,
        )
        runner._stop_reason = None
        runner._stop_event = asyncio.Event()
        runner._run_started_at = now
        runner._run_duration_seconds = 60.0

        snap_race = ledger.snapshot_sync(market=make_market(as_of=now))
        dec_race = make_decision(
            decision_id="dec-race-open",
            event_id="ev-race-open",
            snapshot_id=snap_race.snapshot_id,
            weight="0.20",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
        )
        assess_race = asyncio.run(engine.assess(dec_race, snap_race, make_market(as_of=now)))
        rcpt_race = ledger.record_sync(dec_race, assess_race)
        assert rcpt_race.status == "recorded"
        now = now + timedelta(seconds=1)
        race_mkt = make_market(as_of=now)

        # Simulate stop firing during fill_pending's internal stop check (concurrent stop & fill)
        fill_reached_stop_check = threading.Event()

        def _concurrent_stop_during_fill(ts: datetime) -> str | None:
            fill_reached_stop_check.set()
            runner._stop_reason = "runner_stop_requested"
            runner._stop_event.set()
            return "runner_stop_requested"

        ledger.stop_reason_provider = _concurrent_stop_during_fill

        async def _run_concurrent_stop_and_fill() -> tuple[ShadowFill, ...]:
            def _delayed_stop() -> None:
                fill_reached_stop_check.wait(timeout=2.0)
                runner.stop()

            t_stop = asyncio.to_thread(_delayed_stop)
            t_fill = asyncio.to_thread(ledger.fill_pending, race_mkt, risk_policy=policy)
            _, fills_out = await asyncio.gather(t_stop, t_fill)
            return fills_out

        race_fills = asyncio.run(_run_concurrent_stop_and_fill())
        assert race_fills == ()
        rep_race = ledger.report(market=race_mkt)
        race_rcpt = next(r for r in rep_race["receipts"] if r["decision_id"] == "dec-race-open")
        assert race_rcpt["order_status"] == "cancelled"
        assert race_rcpt["terminal_reason"] == "cancelled_on_runner_stop_requested"
        assert Decimal(rep_race["quantity_eur"]) == Decimal("0")
    finally:
        ledger.close()
        state_store.close()


def test_pepperstone_razor_commission_bearing_ecn_zero_spread_quote_pipeline(tmp_path):
    """Verify commission-bearing Razor ECN 0.0-pip quotes pass validation, align, and fill with commission while unverified 0-pip quotes are still rejected."""
    now = T0
    qstore = QuoteStore(tmp_path / "razor_quotes.sqlite", clock=lambda: now)
    memory = SQLiteThesisMemory(
        tmp_path / "razor_mem.sqlite", clock=lambda: now, allow_targets=True
    )
    ledger = SQLiteShadowLedger(
        tmp_path / "razor_shadow.sqlite",
        exit_policy=ProtectiveExitPolicy(max_holding_seconds=10),
        clock=lambda: now,
    )
    try:
        # 1. Start a Pepperstone Razor ECN session with commission=3 UsdPerMillionUsdVolume
        qstore.start(
            BridgeStart(
                version=1,
                kind="start",
                session_id=SESSION,
                account_mode="demo",
                running_mode="realtime",
                symbol="EURUSD",
                started_at=now - timedelta(seconds=5),
                sample_interval_ms=200,
                duration_seconds=60,
                digits=5,
                pip_size=Decimal("0.0001"),
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
            now - timedelta(seconds=5),
            basis="live_poll",
        )
        qstore.record(
            BridgeQuote(
                version=1,
                kind="quote",
                session_id=SESSION,
                sequence=1,
                symbol="EURUSD",
                source_at=now - timedelta(milliseconds=150),
                bridge_at=now - timedelta(milliseconds=80),
                bid=Decimal("1.12403"),
                ask=Decimal("1.12403"),
            ),
            now - timedelta(milliseconds=40),
            '{"kind":"quote"}',
        )
        snap = build_market_snapshot(qstore, now)
        q = snap.quotes[0]
        assert q.bid == q.ask == Decimal("1.12403")
        assert q.commission == Decimal("3")
        assert q.commission_type == "UsdPerMillionUsdVolume"
        assert q.has_verified_commission is True

        # Default QuoteQualityPolicy accepts verified commission-bearing ECN 0.0-pip quote
        res_valid = validate_market_quote(snap, as_of=now)
        assert res_valid.valid is True
        assert res_valid.reason == "quote_valid"
        assert res_valid.spread_pips == Decimal("0")

        # Explicitly disabling allow_commission_bearing_zero_spread still rejects it
        res_strict = validate_market_quote(
            snap,
            as_of=now,
            policy=QuoteQualityPolicy(allow_commission_bearing_zero_spread=False),
        )
        assert res_strict.valid is False
        assert res_strict.reason == "zero_spread_rejected"

        # Unverified 0.0-pip quote (without commission metadata) is still rejected
        unverified_zero = make_quote(bid="1.12403", ask="1.12403", received_at=now)
        assert unverified_zero.has_verified_commission is False
        res_unverified = validate_market_quote(
            make_market(quote=unverified_zero, as_of=now), as_of=now
        )
        assert res_unverified.valid is False
        assert res_unverified.reason == "zero_spread_rejected"

        # 2. Full runtime cycle + fill + protective exit on Razor 0.0-pip ECN quotes
        open_prop = SinglePassProposal(
            memory_action="create",
            thesis_id=None,
            status="active",
            narrative="ECB hawkish surprise supports EUR/USD.",
            instruments=("EUR/USD",),
            invalidation_conditions=("ECB reverses stance",),
            confidence=0.8,
            evidence_role="support",
            evidence_quote="ECB Governing Council raises deposit facility rate",
            portfolio_action="SET_TARGET",
            target_direction="long",
            target_weight=Decimal("0.20"),
            target_thesis_ids=("__current__",),
            rationale="Open 20% long EUR/USD on Razor ECN feed.",
        )
        client = SequencedJSONClient(open_prop)
        agent = SinglePassTradingAgent(client, clock=lambda: now)
        runtime = IntradayEventRuntime(
            memory=memory,
            portfolio_state=ledger,
            market_provider=lambda ts: build_market_snapshot(qstore, ts),
            agent=agent,
            risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
            shadow_executor=ledger,
            clock=lambda: now,
        )
        ev = Event(
            event_id="ev-razor-1",
            source="wallstreetcn",
            source_type="news",
            source_reference="wscn:razor1",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="ECB Governing Council raises deposit facility rate for EUR/USD.",
        )
        cycle_res = asyncio.run(runtime.run_cycle(ev))
        assert cycle_res.status == "shadow_recorded"

        # Record a causal post-decision 0.0-pip Razor quote and fill the pending order
        now = now + timedelta(seconds=1)
        qstore.record(
            BridgeQuote(
                version=1,
                kind="quote",
                session_id=SESSION,
                sequence=2,
                symbol="EURUSD",
                source_at=now - timedelta(milliseconds=120),
                bridge_at=now - timedelta(milliseconds=60),
                bid=Decimal("1.12405"),
                ask=Decimal("1.12405"),
            ),
            now - timedelta(milliseconds=20),
            '{"kind":"quote"}',
        )
        fills = ledger.fill_pending(build_market_snapshot(qstore, now))
        assert len(fills) == 1
        assert fills[0].synthetic_assumption is False
        assert fills[0].commission_usd > Decimal("0")
        assert fills[0].new_quantity > Decimal("0")

        # Advance past max_holding_seconds (10s) and execute protective exit on a 0.0-pip Razor quote
        now = now + timedelta(seconds=15)
        qstore.record(
            BridgeQuote(
                version=1,
                kind="quote",
                session_id=SESSION,
                sequence=3,
                symbol="EURUSD",
                source_at=now - timedelta(milliseconds=100),
                bridge_at=now - timedelta(milliseconds=50),
                bid=Decimal("1.12420"),
                ask=Decimal("1.12420"),
            ),
            now - timedelta(milliseconds=10),
            '{"kind":"quote"}',
        )
        exit_fills = ledger.evaluate_protective_exits(build_market_snapshot(qstore, now))
        assert len(exit_fills) == 1
        assert exit_fills[0].kind == "protective_exit"
        assert exit_fills[0].new_quantity == Decimal("0")
        assert exit_fills[0].realized_pnl_usd > Decimal("0")
    finally:
        ledger.close()
        qstore.close()
