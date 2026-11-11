"""Regression suite for v0.15.7 audit remediations (G1-G6) and Docker cTrader CLI Demo lifecycle."""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from eventlens.contracts import (
    AgentContext,
    AgentDecision,
    Event,
    MarketSnapshot,
    MemorySnapshot,
    PortfolioSnapshot,
    PriceTrajectorySummary,
    Quote,
    RiskAssessment,
    TargetPortfolio,
    TargetPosition,
    Thesis,
)
from eventlens.deepseek import ModelResponse
from eventlens.observer import (
    CaptureBatch,
    MacroValues,
    ObserverStore,
    SourceRecord,
    analyze_macro_semantics,
)
from eventlens.quotes import BridgeQuote, BridgeStart, BridgeStop, QuoteStore
from eventlens.runtime import IntradayEventRuntime, ShadowRunner, SQLiteRuntimeStore
from eventlens.shadow_cli import inspect_shadow_ledger_readonly
from eventlens.shadow_cli import main as shadow_cli_main
from eventlens.sources.ctrader_demo import DemoQuoteCollector, DirectoryTail
from eventlens.sources.english_tier1 import EURUSDEventFilter, parse_rss_capture
from eventlens.sources.wallstreetcn import revision_to_event
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import (
    SINGLE_PASS_PROMPT_VERSION,
    SINGLE_PASS_SYSTEM_PROMPT,
    SinglePassTradingAgent,
    verify_decision_evidence,
)
from eventlens.trading.ctrader_cli_broker import (
    CTraderCliDemoBrokerAdapter,
    CTraderCliDemoExecutionEngine,
    DemoBrokerSafetyError,
    LiveAccountForbiddenError,
    SimulatedBridgeCrashError,
    SimulatedCTraderCliDaemon,
)
from eventlens.trading.execution import (
    ProtectiveExitPolicy,
    SQLiteShadowLedger,
    build_shadow_audit_metrics,
)
from eventlens.trading.portfolio import (
    DeterministicTargetValidator,
    compute_ex_ante_trade_risk,
    quantize_risk_budget_quantity_eur,
)
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy


def _fresh_market(
    ts: datetime,
    *,
    bid: Decimal = Decimal("1.12463"),
    ask: Decimal = Decimal("1.12465"),
    trajectory: PriceTrajectorySummary | None = None,
) -> MarketSnapshot:
    q = Quote(
        instrument="EUR/USD",
        source_at=ts,
        received_at=ts,
        available_at=ts,
        source_provider="ctrader_algo",
        basis="live_poll",
        bid=bid,
        ask=ask,
        commission=Decimal("30"),
        commission_type="UsdPerMillionUsdVolume",
    )
    return MarketSnapshot(
        snapshot_id=f"mkt:{ts.isoformat()}",
        as_of=ts,
        quotes=(q,),
        price_trajectory=trajectory,
    )


def _stale_market(ts: datetime, *, age_seconds: float = 1860.0) -> MarketSnapshot:
    old = ts - timedelta(seconds=age_seconds)
    q = Quote(
        instrument="EUR/USD",
        source_at=old,
        received_at=old,
        available_at=old,
        source_provider="ctrader_algo",
        basis="live_poll",
        bid=Decimal("1.12463"),
        ask=Decimal("1.12465"),
        commission=Decimal("30"),
        commission_type="UsdPerMillionUsdVolume",
    )
    return MarketSnapshot(snapshot_id=f"mkt-stale:{ts.isoformat()}", as_of=ts, quotes=(q,))


def _effective_memory_after_decision(
    base: MemorySnapshot, decision: AgentDecision
) -> MemorySnapshot:
    merged = {t.thesis_id: t for t in base.theses}
    for ch in decision.thesis_changes:
        merged[ch.thesis.thesis_id] = ch.thesis
    return MemorySnapshot(
        memory_version=base.memory_version + (1 if decision.thesis_changes else 0),
        as_of=decision.completed_at,
        theses=tuple(merged.values()),
    )


class _StaticPayloadModelClient:
    provider = "deepseek"
    model = "deepseek-chat"

    def __init__(self, payloads: list[dict[str, Any]]) -> None:
        self._payloads = list(payloads)
        self.calls = 0
        self.user_prompts: list[str] = []
        self.system_prompts: list[str] = []

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        idx = min(self.calls, len(self._payloads) - 1)
        self.calls += 1
        self.system_prompts.append(system)
        self.user_prompts.append(user)
        return ModelResponse(
            content=json.dumps(self._payloads[idx]),
            input_tokens=650,
            output_tokens=160,
            model_id="deepseek-chat",
        )


def test_g1_g2_macro_semantics_poll_gap_and_honest_publication_timestamps(tmp_path: Path) -> None:
    """Verify G1/G2: preliminary vs consensus disambiguation, poll_gap_seconds delayed catch-up,
    and honest unknown/article publication timestamps."""
    # 1. Nov 4 Final Services PMI: reported_forecast == previous == 57.9 -> preliminary_as_forecast_or_previous
    sem_nov04 = analyze_macro_semantics(
        "USD Final Services PMI",
        MacroValues(actual="56.1", reported_forecast="57.9", previous="57.9", country="USD"),
        raw_text="USD Final Services PMI released at 56.1 vs 57.9 prior.",
    )
    assert sem_nov04 is not None
    assert sem_nov04.semantics_status == "preliminary_as_forecast_or_previous"
    assert sem_nov04.preliminary == "57.9"
    assert sem_nov04.consensus is None

    # 2. Non-final equality mentioning prelim/flash -> preliminary_equality_needs_verification,
    # while ordinary unchanged macro rates stay clean
    sem_eq = analyze_macro_semantics(
        "USD Services PMI",
        MacroValues(actual="56.1", reported_forecast="57.9", previous="57.9", country="USD"),
        raw_text="Services PMI flash comparison came in at 56.1.",
    )
    assert sem_eq is not None
    assert sem_eq.semantics_status == "preliminary_equality_needs_verification"

    sem_unchanged = analyze_macro_semantics(
        "USD Unemployment Rate",
        MacroValues(actual="4.2%", reported_forecast="4.1%", previous="4.1%", country="USD"),
        raw_text="US unemployment rate ticked up to 4.2%.",
    )
    assert sem_unchanged is not None
    assert sem_unchanged.semantics_status == "clean"

    # 3. Clean initial macro release -> clean
    sem_clean = analyze_macro_semantics(
        "USD ISM Services PMI",
        MacroValues(actual="58.5", reported_forecast="56.0", previous="55.2", country="USD"),
        raw_text="USD ISM Services PMI beats at 58.5 vs 56.0 consensus.",
    )
    assert sem_clean is not None
    assert sem_clean.semantics_status == "clean"
    assert sem_clean.consensus == "56.0"

    # 4. ObserverStore poll_gap_seconds & delayed catch-up detection (14:45Z scheduled, 15:00:37Z received)
    t_prev_poll = datetime(2026, 11, 4, 14, 45, 35, tzinfo=UTC)
    t_sched = datetime(2026, 11, 4, 14, 45, 0, tzinfo=UTC)
    t_late_poll = datetime(2026, 11, 4, 15, 0, 37, tzinfo=UTC)
    obs_clock = [t_prev_poll]

    obs = ObserverStore(tmp_path / "obs_g1.sqlite", clock=lambda: obs_clock[0])
    try:
        obs.record(
            CaptureBatch(
                provider="forexfactory",
                stream="calendar/live",
                kind="calendar",
                request_url="https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                request_started_at=t_prev_poll,
                received_at=t_prev_poll,
                elapsed_ms=12.0,
                attempts=1,
                payload=b"[]",
                records=(),
            ),
        )
        obs_clock[0] = t_late_poll
        res_late = obs.record(
            CaptureBatch(
                provider="forexfactory",
                stream="calendar/live",
                kind="calendar",
                request_url="https://nfs.faireconomy.media/ff_calendar_thisweek.json",
                request_started_at=t_late_poll,
                received_at=t_late_poll,
                elapsed_ms=15.0,
                attempts=1,
                payload=b"[1]",
                records=(
                    SourceRecord(
                        source_id="ff:final_services_pmi",
                        kind="calendar",
                        source_url="https://www.forexfactory.com/calendar",
                        title="USD Final Services PMI",
                        text="USD Final Services PMI actual 56.1",
                        scheduled_at=t_sched,
                        importance=3,
                        macro=MacroValues(
                            actual="56.1",
                            reported_forecast="57.9",
                            previous="57.9",
                            country="USD",
                        ),
                    ),
                ),
            ),
        )
        assert res_late["poll_gap_seconds"] == pytest.approx(902.0, abs=0.5)
        published_revs = obs.history("calendar", "ff:final_services_pmi", provider="forexfactory")
        assert len(published_revs) == 1
        rev_late = published_revs[0]
        assert rev_late.poll_gap_seconds == pytest.approx(902.0, abs=0.5)
        ev_late = revision_to_event(rev_late)
        assert ev_late.is_delayed_catchup is True
        assert ev_late.publication_at is None
        assert ev_late.publication_time_status == "unknown"
        assert ev_late.macro_semantics is not None
        assert ev_late.macro_semantics.semantics_status == "preliminary_as_forecast_or_previous"
    finally:
        obs.close()

    # 5. RSS / Atom capture keeps publisher_display_at distinct from modified_at (never overwrites original/display time)
    rss_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom"><channel>
      <item>
        <title>US S&amp;P Global Final Services PMI 56.1 vs 57.9 prelim</title>
        <link>https://investinglive.com/news/us-services-pmi-final-20261104/</link>
        <guid>https://investinglive.com/news/us-services-pmi-final-20261104/</guid>
        <pubDate>Wed, 04 Nov 2026 14:50:12 GMT</pubDate>
        <atom:updated>2026-11-04T15:00:05Z</atom:updated>
        <description>Prices charged jumped sharply.</description>
      </item>
    </channel></rss>"""
    recs, rejs, _ = parse_rss_capture(
        rss_xml,
        provider="investinglive",
        stream="news/rss",
        received_at=datetime(2026, 11, 4, 15, 0, 50, tzinfo=UTC),
    )
    assert rejs == ()
    assert len(recs) == 1
    assert recs[0].publisher_display_at == datetime(2026, 11, 4, 14, 50, 12, tzinfo=UTC)
    assert recs[0].modified_at == datetime(2026, 11, 4, 15, 0, 5, tzinfo=UTC)


def test_g3_g5_nov04_fault_sequence_blocked_and_evidence_verified() -> None:
    """Replay the Nov 4, 2026 overnight PMI Final fault sequence:
    - Model returns long SET_TARGET +0.12 without cited_evidence_ids claiming 'consistent with drift'
    - verify_decision_evidence flags contradicted_price_direction (or missing_short_window_coverage without prose)
    - cited_evidence_ids stays empty (never auto-injected)
    - DeterministicHardRiskEngine blocks new risk with all 3 independent integrity reasons."""
    assert SINGLE_PASS_PROMPT_VERSION == "single-pass-agent-v3.1"
    assert "scales leveraged buying power" in SINGLE_PASS_SYSTEM_PROMPT

    now = datetime(2026, 11, 4, 15, 0, 41, tzinfo=UTC)
    sched = datetime(2026, 11, 4, 14, 45, 0, tzinfo=UTC)
    ev = Event(
        event_id="forexfactory:calendar:pmi_final:r1",
        source="forexfactory_calendar",
        source_type="macro",
        source_reference="https://www.forexfactory.com/calendar",
        source_timestamp=sched,
        received_timestamp=now,
        scheduled_at=sched,
        publication_at=None,
        receipt_at=now,
        available_at=now,
        timestamp_source="scheduled",
        publication_time_status="unknown",
        poll_gap_seconds=901.7,
        is_delayed_catchup=True,
        macro_semantics=analyze_macro_semantics(
            "USD Final Services PMI",
            MacroValues(actual="56.1", reported_forecast="57.9", previous="57.9", country="USD"),
        ),
        raw_text=(
            "USD Final Services PMI\n"
            "USD Final Services PMI released at 56.1 vs 57.9 preliminary reading."
        ),
    )
    traj_nov04 = PriceTrajectorySummary(
        instrument="EUR/USD",
        as_of=now,
        current_mid=Decimal("1.12464"),
        change_1m_pips=None,
        change_5m_pips=None,
        change_15m_pips=None,
        change_30m_pips=Decimal("-12.7"),
        max_gap_30m_seconds=1506.6,
        sample_count_30m=12,
        quote_freshness_ms=250,
    )
    market = _fresh_market(now, trajectory=traj_nov04)
    portfolio = PortfolioSnapshot(
        snapshot_id="port-flat",
        as_of=now,
        base_currency="USD",
        equity=Decimal("200"),
        positions=(),
    )
    memory_snap = MemorySnapshot(as_of=now, memory_version=0, theses=())

    client = _StaticPayloadModelClient(
        [
            {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Weaker US Final Services PMI weighs on USD; EUR/USD price action consistent with drift.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1230"],
                "confidence": 0.62,
                "evidence_quote": "USD Final Services PMI released at 56.1 vs 57.9 preliminary reading.",
                "evidence_role": "support",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.12",
                "target_thesis_ids": ["__current__"],
                "rationale": "Immediate SET_TARGET long EUR/USD consistent with post-release drift.",
            }
        ]
    )
    policy = RiskPolicy.pepperstone_razor_200_guarded()
    agent = SinglePassTradingAgent(
        client,
        leverage=policy.leverage,
        max_risk_per_trade_pct=policy.max_risk_per_trade_pct,
        ex_ante_stop_loss_pips=policy.ex_ante_stop_loss_pips,
        allow_commission_bearing_zero_spread=True,
        clock=lambda: now,
    )
    ctx = AgentContext(
        event=ev,
        memory=memory_snap,
        portfolio=portfolio,
        market=market,
        as_of=now,
    )
    decision = asyncio.run(agent.decide(ctx))

    # Honest empty cited_evidence_ids (no fake default auto-injection)
    assert decision.cited_evidence_ids == ()
    assert decision.evidence_verification_status == "contradicted_price_direction"

    # Transparent account leverage & ex-ante risk metadata in _context_json
    sent_ctx = json.loads(client.user_prompts[0])
    assert sent_ctx["risk_constraints"]["account_leverage"] == "200"
    assert sent_ctx["risk_constraints"]["max_risk_per_trade_pct"] == "0.02"

    # Missing prose claim still blocks on missing_short_window_coverage after quote gap
    decision_silent = decision.model_copy(
        update={
            "rationale": "Initiate tactical long EUR/USD on weaker services print.",
            "thesis_changes": (),
        }
    )
    ev_silent = verify_decision_evidence(ctx, decision_silent)
    assert ev_silent.valid is False
    assert ev_silent.status == "missing_short_window_coverage"

    # No trajectory attached -> status == "not_applicable" (never faking "verified")
    mkt_no_traj = _fresh_market(now, trajectory=None)
    ctx_no_traj = ctx.model_copy(update={"market": mkt_no_traj})
    ev_na = verify_decision_evidence(ctx_no_traj, decision_silent)
    assert ev_na.status == "not_applicable"

    # Fabricated cited_evidence_ids -> rejected as unverified_numeric_claim
    decision_fake_id = decision_silent.model_copy(
        update={"cited_evidence_ids": ("fabricated:id:999",)}
    )
    ev_fake = verify_decision_evidence(ctx, decision_fake_id)
    assert ev_fake.valid is False
    assert ev_fake.status == "unverified_numeric_claim"

    # Hard risk gate blocks new risk with all 3 independent reasons
    assert decision.target is not None
    DeterministicTargetValidator().validate(decision.target, portfolio)
    risk_engine = DeterministicHardRiskEngine(policy=policy, clock=lambda: now)
    assessment = asyncio.run(
        risk_engine.assess(
            decision,
            portfolio,
            market,
            event=ev,
        )
    )
    assert assessment.outcome == "reject"
    assert "unverified_decision_evidence:contradicted_price_direction" in assessment.reasons
    assert "ambiguous_macro_semantics:preliminary_as_forecast_or_previous" in assessment.reasons
    assert "delayed_event_catchup_blocks_new_risk" in assessment.reasons


def test_golden_clean_macro_1m_breakout_against_30m_drift_and_unscheduled_news_allowed() -> None:
    """Golden non-regression:
    1. Clean timely macro surprise + 1m/5m upward breakout reversing 30m downward drift is APPROVED.
    2. Timely unscheduled central-bank news without macro_semantics is NOT falsely blocked."""
    now = datetime(2026, 11, 5, 15, 0, 3, tzinfo=UTC)
    sched = datetime(2026, 11, 5, 15, 0, 0, tzinfo=UTC)
    ev_clean = Event(
        event_id="forexfactory:calendar:ism_services:r1",
        source="forexfactory_calendar",
        source_type="macro",
        source_reference="https://www.forexfactory.com/calendar",
        source_timestamp=sched,
        received_timestamp=now,
        scheduled_at=sched,
        publication_at=None,
        receipt_at=now,
        available_at=now,
        timestamp_source="scheduled",
        publication_time_status="unknown",
        poll_gap_seconds=4.0,
        is_delayed_catchup=False,
        macro_semantics=analyze_macro_semantics(
            "USD ISM Services PMI",
            MacroValues(actual="48.2", reported_forecast="51.5", previous="51.8", country="USD"),
        ),
        raw_text="USD ISM Services PMI\nUSD ISM Services PMI drops sharply to 48.2 vs 51.5 consensus.",
    )
    traj_breakout = PriceTrajectorySummary(
        instrument="EUR/USD",
        as_of=now,
        current_mid=Decimal("1.12510"),
        change_1m_pips=Decimal("4.8"),
        change_5m_pips=Decimal("4.2"),
        change_15m_pips=Decimal("3.5"),
        change_30m_pips=Decimal("-8.5"),
        max_gap_30m_seconds=1.1,
        sample_count_30m=420,
        quote_freshness_ms=120,
    )
    market = _fresh_market(
        now,
        bid=Decimal("1.12509"),
        ask=Decimal("1.12511"),
        trajectory=traj_breakout,
    )
    portfolio = PortfolioSnapshot(
        snapshot_id="port-flat",
        as_of=now,
        base_currency="USD",
        equity=Decimal("200"),
        positions=(),
    )
    memory_snap = MemorySnapshot(as_of=now, memory_version=0, theses=())
    policy = RiskPolicy.pepperstone_razor_200_guarded()

    client = _StaticPayloadModelClient(
        [
            {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Sharp ISM Services contraction triggers 1m +4.8 pips upward breakout reversing earlier 30m -8.5 pips drift.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1238"],
                "confidence": 0.72,
                "evidence_quote": "USD ISM Services PMI drops sharply to 48.2 vs 51.5 consensus.",
                "evidence_role": "support",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.12",
                "target_thesis_ids": ["__current__"],
                "strategy_hypothesis": "fundamental_breakout",
                "cited_evidence_ids": [ev_clean.event_id, str(traj_breakout.evidence_id)],
                "rationale": "Confirmed 1m +4.8 pips upward breakout reversing prior 30m -8.5 pips drift.",
            }
        ]
    )
    agent = SinglePassTradingAgent(
        client,
        leverage=policy.leverage,
        max_risk_per_trade_pct=policy.max_risk_per_trade_pct,
        ex_ante_stop_loss_pips=policy.ex_ante_stop_loss_pips,
        allow_commission_bearing_zero_spread=True,
        clock=lambda: now,
    )
    decision = asyncio.run(
        agent.decide(
            AgentContext(
                event=ev_clean,
                memory=memory_snap,
                portfolio=portfolio,
                market=market,
                as_of=now,
            )
        )
    )
    assert decision.evidence_verification_status == "verified"
    assert decision.target is not None
    # Guarded $200 / 2% ($4.00) risk budget quantizes 0.12 weight to 3,000 EUR instead of 4,000 EUR
    assert policy.quantize_target_quantity_eur(
        Decimal("200"), Decimal("1.12510"), decision.target.positions[0].weight
    ) == Decimal("3000")

    DeterministicTargetValidator().validate(decision.target, portfolio)
    risk_engine = DeterministicHardRiskEngine(policy=policy, clock=lambda: now)
    assessment = asyncio.run(risk_engine.assess(decision, portfolio, market, event=ev_clean))
    assert assessment.outcome == "approve"

    # Unscheduled central-bank headline with macro_semantics=None is also permitted
    ev_news = Event(
        event_id="ecb:news:press_1:r1",
        source="ecb_news",
        source_type="news",
        source_reference="https://www.ecb.europa.eu/press/pr/date/2026/html/index.en.html",
        source_timestamp=now - timedelta(seconds=5),
        received_timestamp=now,
        publication_at=now - timedelta(seconds=5),
        receipt_at=now,
        available_at=now,
        timestamp_source="official_publication",
        publication_time_status="verified",
        poll_gap_seconds=5.0,
        is_delayed_catchup=False,
        macro_semantics=None,
        raw_text="ECB signals rates will remain restrictive for longer, lifting EUR/USD.",
    )
    decision_news = decision.model_copy(
        update={"event_id": ev_news.event_id, "cited_evidence_ids": (ev_news.event_id,)}
    )
    assessment_news = asyncio.run(
        risk_engine.assess(decision_news, portfolio, market, event=ev_news)
    )
    assert assessment_news.outcome == "approve"


def test_g5_q_dependent_ex_ante_risk_budget_sizing_and_hard_block() -> None:
    """Verify G5 q-dependent ex-ante risk sizing:
    q*stop_distance + spread(q) + slippage(q) + commission(q) + fixed_fees <= risk_budget,
    ROUND_DOWN to 1,000 EUR lot step, and hard risk block when exceeded."""
    equity = Decimal("200")
    mid = Decimal("1.12464")

    r_3k = compute_ex_ante_trade_risk(
        quantity_eur=Decimal("3000"),
        current_equity=equity,
        mid_price=mid,
        leverage=Decimal("200"),
        stop_loss_pips=Decimal("11"),
        spread_pips=Decimal("0.2"),
        estimated_slippage_bps=Decimal("0.25"),
        estimated_commission_round_trip_per_million_usd=Decimal("60"),
        risk_budget_usd=Decimal("4.00"),
    )
    r_4k = compute_ex_ante_trade_risk(
        quantity_eur=Decimal("4000"),
        current_equity=equity,
        mid_price=mid,
        leverage=Decimal("200"),
        stop_loss_pips=Decimal("11"),
        spread_pips=Decimal("0.2"),
        estimated_slippage_bps=Decimal("0.25"),
        estimated_commission_round_trip_per_million_usd=Decimal("60"),
        risk_budget_usd=Decimal("4.00"),
    )
    assert r_4k.estimated_commission_usd > r_3k.estimated_commission_usd
    assert r_4k.spread_cost_usd + r_4k.estimated_slippage_usd > (
        r_3k.spread_cost_usd + r_3k.estimated_slippage_usd
    )
    assert r_3k.within_risk_budget is True
    assert r_3k.ex_ante_total_risk_usd <= Decimal("4.00")
    assert r_4k.within_risk_budget is False
    assert r_4k.ex_ante_total_risk_usd > Decimal("4.00")

    capped_q = quantize_risk_budget_quantity_eur(
        current_equity=equity,
        mid_price=mid,
        risk_budget_usd=Decimal("4.00"),
        stop_loss_pips=Decimal("11"),
        direction_sign=1,
        leverage=Decimal("200"),
        spread_pips=Decimal("0.2"),
        estimated_slippage_bps=Decimal("0.25"),
        estimated_commission_round_trip_per_million_usd=Decimal("60"),
    )
    assert capped_q == Decimal("3000")

    # If cap_quantity_by_risk_budget=False and target requests 4,000 EUR against a 2% budget, hard risk rejects it
    now = datetime(2026, 11, 5, 15, 0, 0, tzinfo=UTC)
    strict_policy = RiskPolicy.pepperstone_razor_200(
        max_risk_per_trade_pct=Decimal("0.02"),
        ex_ante_stop_loss_pips=Decimal("11.0"),
        cap_quantity_by_risk_budget=False,
    )
    target_4k = TargetPortfolio(
        target_id="tgt-4k",
        based_on_snapshot_id="p0",
        rationale="Oversized 4,000 EUR target",
        positions=(
            TargetPosition(
                instrument="EUR/USD",
                weight=Decimal("0.12"),
                thesis_ids=("t1",),
            ),
        ),
    )
    mem = MemorySnapshot(
        as_of=now,
        memory_version=1,
        theses=(
            Thesis(
                thesis_id="t1",
                revision=1,
                status="active",
                narrative="Active bullish EUR/USD thesis",
                instruments=("EUR/USD",),
                invalidation_conditions=("EUR/USD <= 1.1200",),
                confidence=0.7,
                created_at=now,
                updated_at=now,
                review_at=now + timedelta(minutes=30),
            ),
        ),
    )
    dec = AgentDecision(
        decision_id="dec-4k",
        event_id="ev-4k",
        based_on_memory_version=1,
        based_on_snapshot_id="p0",
        provider="deepseek",
        model_version="deepseek-chat",
        prompt_version=SINGLE_PASS_PROMPT_VERSION,
        started_at=now,
        completed_at=now,
        valid_until=now + timedelta(minutes=5),
        portfolio_action="SET_TARGET",
        target=target_4k,
        rationale="Oversized 4,000 EUR target",
    )
    port = PortfolioSnapshot(
        snapshot_id="p0",
        as_of=now,
        base_currency="USD",
        equity=Decimal("200"),
        positions=(),
    )
    mkt = _fresh_market(now)
    DeterministicTargetValidator().validate(target_4k, port)
    assess = asyncio.run(
        DeterministicHardRiskEngine(policy=strict_policy, clock=lambda: now).assess(
            dec, port, mkt, prior_theses=mem.theses
        )
    )
    assert assess.outcome == "reject"
    assert "ex_ante_stop_risk_exceeds_budget" in assess.reasons


def test_g4_close_cancels_pending_entry_dedup_pending_target_and_pre_llm_fill_reconcile(
    tmp_path: Path,
) -> None:
    """Verify G4:
    1. Follow-up CLOSE cancels an unfilled risk-increasing pending order instead of failing with pending_order_in_flight.
    2. Duplicate same-direction pending target is rejected cleanly with duplicate_pending_target_already_queued."""
    now = datetime(2026, 11, 4, 15, 0, 45, tzinfo=UTC)
    traj_ok = PriceTrajectorySummary(
        instrument="EUR/USD",
        as_of=now,
        current_mid=Decimal("1.12464"),
        change_1m_pips=Decimal("2.5"),
        change_5m_pips=Decimal("2.0"),
        change_15m_pips=Decimal("1.5"),
        change_30m_pips=Decimal("3.0"),
        max_gap_30m_seconds=1.0,
        sample_count_30m=300,
    )
    mem = SQLiteThesisMemory(tmp_path / "g4_mem.sqlite", clock=lambda: now, allow_targets=True)
    shd_db = tmp_path / "g4_shd.sqlite"
    store = SQLiteRuntimeStore(shd_db)
    ledger = SQLiteShadowLedger(shd_db, clock=lambda: now)

    client = _StaticPayloadModelClient(
        [
            {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Initial release lifts EUR/USD.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1230"],
                "confidence": 0.70,
                "evidence_quote": "Eurozone output beats consensus.",
                "evidence_role": "support",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.12",
                "target_thesis_ids": ["__current__"],
                "rationale": "Initiate long EUR/USD.",
            },
            {
                "memory_action": "ignore",
                "thesis_id": None,
                "status": None,
                "narrative": None,
                "instruments": None,
                "invalidation_conditions": None,
                "confidence": None,
                "evidence_quote": "Secondary wire repeats beat.",
                "evidence_role": None,
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.12",
                "target_thesis_ids": [],
                "cited_evidence_ids": ["wire:2"],
                "rationale": "Repeat same target while pending.",
            },
            {
                "memory_action": "update",
                "thesis_id": None,
                "status": "invalidated",
                "narrative": "Inflation jump invalidates long EUR/USD.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1230"],
                "confidence": 0.25,
                "evidence_quote": "Prices charged jumped sharply in latest survey.",
                "evidence_role": "contradict",
                "portfolio_action": "CLOSE",
                "target_direction": "flat",
                "target_weight": "0",
                "target_thesis_ids": [],
                "cited_evidence_ids": ["wire:3"],
                "rationale": "Cancel/exit long exposure immediately on adverse inflation details.",
            },
        ]
    )

    def _mkt_provider(ts: datetime) -> MarketSnapshot:
        return _fresh_market(ts, trajectory=traj_ok.model_copy(update={"as_of": ts}))

    rt = IntradayEventRuntime(
        memory=mem,
        portfolio_state=ledger,
        market_provider=_mkt_provider,
        agent=SinglePassTradingAgent(client, clock=lambda: now),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger,
        state_store=store,
        clock=lambda: now,
    )
    try:
        ev1 = Event(
            event_id="wire:1",
            source="ecb_news",
            source_type="news",
            source_reference="https://www.ecb.europa.eu/1",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="Eurozone output beats consensus.",
        )
        r1 = asyncio.run(rt.run_cycle(ev1))
        assert r1.status == "shadow_recorded"
        assert len(asyncio.run(ledger.snapshot()).pending_order_ids) == 1

        active_tid = asyncio.run(mem.snapshot(as_of=now)).theses[0].thesis_id
        client._payloads[1]["target_thesis_ids"] = [active_tid]
        client._payloads[2]["thesis_id"] = active_tid
        client._payloads[2]["target_thesis_ids"] = [active_tid]
        ev2 = Event(
            event_id="wire:2",
            source="ecb_news",
            source_type="news",
            source_reference="https://www.ecb.europa.eu/2",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="Secondary wire repeats beat.",
        )
        r2 = asyncio.run(rt.run_cycle(ev2))
        assert r2.status == "target_rejected"
        assert r2.reason == "duplicate_pending_target_already_queued"
        assert len(asyncio.run(ledger.snapshot()).pending_order_ids) == 1

        # Follow-up proposes CLOSE while entry is STILL pending -> cancels pending entry cleanly to flat!
        ev3 = Event(
            event_id="wire:3",
            source="investinglive_news",
            source_type="news",
            source_reference="https://investinglive.com/3",
            source_timestamp=now,
            received_timestamp=now,
            raw_text="Prices charged jumped sharply in latest survey.",
        )
        r3 = asyncio.run(rt.run_cycle(ev3))
        assert r3.status == "no_target"
        assert r3.reason == "pending_risk_increasing_order_cancelled_to_flat"
        rep = ledger.report(market=_mkt_provider(now))
        assert rep["order_status_counts"].get("cancelled", 0) == 1
        assert asyncio.run(ledger.snapshot()).pending_order_ids == ()
    finally:
        ledger.close()
        store.close()


def test_g6_24x5_bridge_rotation_restart_wallclock_deadline_outage_alert_and_nov04_ro_audit(
    tmp_path: Path,
) -> None:
    """Verify G6:
    1. >4 rotated 12h bridge files (8 sessions including unclean crash rollover) never hit 'Too many new bridge files'.
    2. ShadowRunner restart attaches idempotently to an already-recorded active bridge session.
    3. Wall-clock deadline expires promptly on simulated sleep clock jump.
    4. 31m quote outage records open_position_quote_outage_alert, never fabricates fills, and marks report fault_affected.
    5. Strict read-only (?mode=ro) audit of Nov 4 overnight SQLite verifies exact 4-part PnL identity."""
    bridge_dir = tmp_path / "bridge"
    bridge_dir.mkdir()
    q_db = tmp_path / "quotes.sqlite"
    t0 = datetime(2026, 11, 5, 0, 0, 0, tzinfo=UTC)
    q_clock = [t0]

    q_store = QuoteStore(q_db, clock=lambda: q_clock[0])
    tail = DirectoryTail(bridge_dir)
    collector = DemoQuoteCollector(
        q_store,
        t0,
        allow_multiple_sessions=True,
        allow_unclean_session_rollover=True,
    )

    try:
        # 1. Stream 8 rotated sessions (session #3 crashes without writing "stop")
        for idx in range(8):
            sid = uuid4()
            st_ts = t0 + timedelta(hours=12 * idx)
            q_ts = st_ts + timedelta(seconds=1)
            sp_ts = st_ts + timedelta(seconds=2)
            q_clock[0] = sp_ts + timedelta(milliseconds=100)
            fpath = bridge_dir / f"quotes-{st_ts.strftime('%Y%m%dT%H%M%S')}-{sid.hex[:8]}.jsonl"
            lines = [
                BridgeStart(
                    version=1,
                    kind="start",
                    session_id=sid,
                    account_mode="demo",
                    running_mode="realtime",
                    symbol="EURUSD",
                    started_at=st_ts,
                    sample_interval_ms=200,
                    duration_seconds=43200,
                    digits=5,
                    pip_size=Decimal("0.0001"),
                    commission=Decimal("30"),
                    commission_type="UsdPerMillionUsdVolume",
                ).model_dump_json(),
                BridgeQuote(
                    version=1,
                    kind="quote",
                    session_id=sid,
                    sequence=1,
                    symbol="EURUSD",
                    source_at=q_ts,
                    bridge_at=q_ts,
                    bid=Decimal("1.12400"),
                    ask=Decimal("1.12402"),
                ).model_dump_json(),
            ]
            if idx != 3:
                lines.append(
                    BridgeStop(
                        version=1,
                        kind="stop",
                        session_id=sid,
                        stopped_at=sp_ts,
                        quotes=1,
                        raw_ticks=1,
                        raw_zero_spread_ticks=0,
                        symbol_mismatch_ticks=0,
                        throttled_ticks=0,
                    ).model_dump_json()
                )
            fpath.write_text("\n".join(lines) + "\n", encoding="utf-8")
            for raw_ln in tail.poll():
                collector.accept(raw_ln, sp_ts + timedelta(milliseconds=50))

        # Leave an active 9th session open and restart ShadowRunner twice against the same QuoteStore
        active_sid = uuid4()
        active_ts = t0 + timedelta(hours=96)
        active_file = bridge_dir / f"quotes-{active_ts.strftime('%Y%m%dT%H%M%S')}-active.jsonl"
        active_file.write_text(
            BridgeStart(
                version=1,
                kind="start",
                session_id=active_sid,
                account_mode="demo",
                running_mode="realtime",
                symbol="EURUSD",
                started_at=active_ts,
                sample_interval_ms=200,
                duration_seconds=43200,
                digits=5,
                pip_size=Decimal("0.0001"),
                commission=Decimal("30"),
                commission_type="UsdPerMillionUsdVolume",
            ).model_dump_json()
            + "\n"
            + BridgeQuote(
                version=1,
                kind="quote",
                session_id=active_sid,
                sequence=1,
                symbol="EURUSD",
                source_at=active_ts + timedelta(seconds=1),
                bridge_at=active_ts + timedelta(seconds=1),
                bid=Decimal("1.12500"),
                ask=Decimal("1.12502"),
            ).model_dump_json()
            + "\n",
            encoding="utf-8",
        )

        clock_now = active_ts + timedelta(seconds=2)
        q_clock[0] = clock_now + timedelta(seconds=1)
        mem = SQLiteThesisMemory(
            tmp_path / "r_mem.sqlite", clock=lambda: clock_now, allow_targets=True
        )
        shd_db = tmp_path / "r_shd.sqlite"
        r_store = SQLiteRuntimeStore(shd_db)
        r_ledger = SQLiteShadowLedger(
            shd_db,
            exit_policy=ProtectiveExitPolicy(max_holding_seconds=600),
            clock=lambda: clock_now,
        )
        rt = IntradayEventRuntime(
            memory=mem,
            portfolio_state=r_ledger,
            market_provider=lambda ts: _fresh_market(ts),
            agent=SinglePassTradingAgent(_StaticPayloadModelClient([]), clock=lambda: clock_now),
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(clock=lambda: clock_now),
            shadow_executor=r_ledger,
            state_store=r_store,
            event_filter=EURUSDEventFilter(clock=lambda: clock_now),
            clock=lambda: clock_now,
        )

        _runner_a = ShadowRunner(
            runtime=rt,
            ledger=r_ledger,
            quote_store=q_store,
            quote_bridge_dir=bridge_dir,
            clock=lambda: clock_now,
        )
        assert q_store.has_session(str(active_sid)) is True
        runner_b = ShadowRunner(
            runtime=rt,
            ledger=r_ledger,
            quote_store=q_store,
            quote_bridge_dir=bridge_dir,
            open_position_quote_outage_alert_seconds=30.0,
            clock=lambda: clock_now,
        )
        with active_file.open("a", encoding="utf-8") as f:
            f.write(
                BridgeQuote(
                    version=1,
                    kind="quote",
                    session_id=active_sid,
                    sequence=2,
                    symbol="EURUSD",
                    source_at=active_ts + timedelta(seconds=2),
                    bridge_at=active_ts + timedelta(seconds=2),
                    bid=Decimal("1.12510"),
                    ask=Decimal("1.12512"),
                ).model_dump_json()
                + "\n"
            )
        runner_b._poll_quote_bridge_sync()
        assert q_store.last_sequence(str(active_sid)) == 2

        # 3. Wall-clock jump across simulated sleep expires runner_duration_expired promptly
        jump_clock = [clock_now]

        async def _jump_wall_clock_scenario() -> dict[str, Any]:
            jump_ledger = SQLiteShadowLedger(
                tmp_path / "jump_shd.sqlite", clock=lambda: jump_clock[0]
            )
            jump_rt = IntradayEventRuntime(
                memory=mem,
                portfolio_state=jump_ledger,
                market_provider=lambda ts: _fresh_market(ts),
                agent=SinglePassTradingAgent(
                    _StaticPayloadModelClient([]), clock=lambda: jump_clock[0]
                ),
                target_validator=DeterministicTargetValidator(),
                risk_engine=DeterministicHardRiskEngine(clock=lambda: jump_clock[0]),
                shadow_executor=jump_ledger,
                clock=lambda: jump_clock[0],
            )
            try:
                runner_jump = ShadowRunner(
                    runtime=jump_rt,
                    ledger=jump_ledger,
                    strict_window_coverage=False,
                    clock=lambda: jump_clock[0],
                )
                t_run = asyncio.create_task(runner_jump.run(duration_seconds=3600.0))
                await asyncio.sleep(0.05)
                jump_clock[0] = clock_now + timedelta(seconds=3605)
                return await asyncio.wait_for(t_run, timeout=1.5)
            finally:
                jump_ledger.close()

        jump_summary = asyncio.run(_jump_wall_clock_scenario())
        assert jump_summary["stop_reason"] == "runner_duration_expired"

        # 4. 31m quote outage with open position -> pending_exit_reason set, outage fault recorded, fills on recovery
        now_trade = active_ts + timedelta(seconds=10)
        r_ledger.clock = lambda: now_trade
        snap_before = asyncio.run(
            r_ledger.snapshot(
                market=_fresh_market(now_trade, bid=Decimal("1.12500"), ask=Decimal("1.12502"))
            )
        )
        dec_open = AgentDecision(
            decision_id="d-open",
            event_id="e-open",
            based_on_memory_version=0,
            based_on_snapshot_id=snap_before.snapshot_id,
            provider="deepseek",
            model_version="deepseek-chat",
            prompt_version=SINGLE_PASS_PROMPT_VERSION,
            started_at=now_trade,
            completed_at=now_trade,
            valid_until=now_trade + timedelta(minutes=5),
            portfolio_action="SET_TARGET",
            target=TargetPortfolio(
                target_id="t-open",
                based_on_snapshot_id=snap_before.snapshot_id,
                rationale="Open long",
                positions=(
                    TargetPosition(
                        instrument="EUR/USD",
                        weight=Decimal("0.10"),
                        thesis_ids=("th1",),
                    ),
                ),
            ),
            rationale="Open long",
        )
        asyncio.run(
            r_ledger.record(
                dec_open,
                RiskAssessment(
                    assessment_id="a-open",
                    decision_id="d-open",
                    target_id="t-open",
                    checked_snapshot_id=snap_before.snapshot_id,
                    policy_version="test",
                    checked_at=now_trade,
                    valid_until=now_trade + timedelta(minutes=5),
                    outcome="approve",
                    reasons=("within_risk_limits",),
                ),
            )
        )
        r_ledger.fill_pending(
            _fresh_market(now_trade, bid=Decimal("1.12500"), ask=Decimal("1.12502"))
        )

        # Advance 31m into a quote outage: max_holding_seconds=600 triggers pending_exit_reason, ZERO fabricated fills
        outage_t1 = now_trade + timedelta(minutes=11)
        outage_t2 = now_trade + timedelta(minutes=31)
        r_ledger.clock = lambda: outage_t1
        assert r_ledger.evaluate_protective_exits(_stale_market(outage_t1, age_seconds=660)) == ()
        runner_outage = ShadowRunner(
            runtime=rt,
            ledger=r_ledger,
            open_position_quote_outage_alert_seconds=30.0,
            clock=lambda: outage_t1,
        )
        runner_outage.runtime.market_provider = lambda ts: _stale_market(ts, age_seconds=660)
        asyncio.run(runner_outage.step_protection())
        runner_outage.clock = lambda: outage_t2
        r_ledger.clock = lambda: outage_t2
        runner_outage.runtime.market_provider = lambda ts: _stale_market(ts, age_seconds=1860)
        asyncio.run(runner_outage.step_protection())

        snap_mid = asyncio.run(r_ledger.snapshot())
        assert snap_mid.positions[0].quantity != Decimal("0")
        rep_mid = r_ledger.report(market=_stale_market(outage_t2, age_seconds=1860))
        assert any(f["reason"] == "open_position_quote_outage_alert" for f in rep_mid["faults"])

        # Quote recovers -> protective exit fills immediately, report marks fault_affected with verified accounting
        rec_ts = outage_t2 + timedelta(seconds=5)
        r_ledger.clock = lambda: rec_ts
        exits = r_ledger.evaluate_protective_exits(
            _fresh_market(rec_ts, bid=Decimal("1.12420"), ask=Decimal("1.12422"))
        )
        assert len(exits) == 1
        ro_rep = inspect_shadow_ledger_readonly(shd_db)
        audit_m = build_shadow_audit_metrics(
            fills=ro_rep["fills"],
            faults=ro_rep["faults"],
            receipts=ro_rep["receipts"],
            realized_pnl_usd=Decimal(ro_rep["realized_pnl_usd"]),
            total_fees_usd=Decimal(ro_rep["total_fees_usd"]),
            unclosed_risk=bool(ro_rep["unclosed_risk"]),
            max_holding_seconds=600,
        )
        assert audit_m["quality_classification"] == "fault_affected"
        assert audit_m["pnl_decomposition"]["accounting_identity_verified"] is True
        assert len(audit_m["round_trips"]) == 1
        assert float(audit_m["round_trips"][0]["holding_overdue_seconds"]) > 1200.0
    finally:
        q_store.close()
        if "r_ledger" in locals():
            r_ledger.close()
        if "r_store" in locals():
            r_store.close()

    # 5. Read-only check against preserved 2026-11-04 overnight SQLite if present locally
    nov04_db = Path("data/shadow-observation-20261104-overnight/shadow-ledger.sqlite")
    nov04_summary = Path("data/shadow-observation-20261104-overnight/run-summary.json")
    if nov04_db.exists() and nov04_summary.exists():
        mtime_before = nov04_db.stat().st_mtime_ns
        run_sum = json.loads(nov04_summary.read_text(encoding="utf-8"))
        nov04_ro = inspect_shadow_ledger_readonly(nov04_db)
        nov04_audit = build_shadow_audit_metrics(
            fills=nov04_ro["fills"],
            faults=nov04_ro["faults"],
            receipts=nov04_ro["receipts"],
            realized_pnl_usd=Decimal(nov04_ro["realized_pnl_usd"]),
            total_fees_usd=Decimal(nov04_ro["total_fees_usd"]),
            unclosed_risk=bool(nov04_ro["unclosed_risk"]),
            max_holding_seconds=1200,
            protection_coverage=run_sum.get("protection_coverage"),
            started_at=run_sum.get("started_at"),
            stopped_at=run_sum.get("stopped_at"),
            configured_duration_seconds=36000.0,
        )
        pnl = nov04_audit["pnl_decomposition"]
        assert pnl["pure_raw_quote_pnl_usd"] == "-3.96"
        assert pnl["embedded_slippage_and_spread_cost_usd"] == "0.22"
        assert pnl["realized_fill_price_pnl_usd"] == "-4.18"
        assert pnl["explicit_commission_fees_usd"] == "0.26"
        assert pnl["net_realized_pnl_after_fees_usd"] == "-4.44"
        assert pnl["accounting_identity_verified"] is True
        assert nov04_audit["quality_classification"] == "fault_affected"
        assert (
            nov04_audit["timing_and_quote_check_diagnostics"][
                "unknown_inter_check_gaps_cannot_infer_uptime"
            ]
            is True
        )
        assert nov04_db.stat().st_mtime_ns == mtime_before


def test_stage_f_ctrader_cli_demo_broker_full_lifecycle_reconcile_and_live_lock(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify CTraderCliDemoBrokerAdapter + SimulatedCTraderCliDaemon end-to-end:
    - Zero test hooks in production ctrader_openapi.py
    - RiskAssessment gate required
    - submitted -> accepted (not open!) -> partial -> filled -> close by broker_position_id
    - client_intent_id idempotency across adapter + daemon restarts
    - Order rejection & pending order cancellation
    - Missing SL attachment & orphan position halt new risk while still permitting close_position
    - Broker-side SL close reconciliation
    - Live account hard refusal (LiveAccountForbiddenError)
    - `eventlens-shadow demo-reconcile` offline CLI check."""
    openapi_src = Path("src/eventlens/ctrader_openapi.py").read_text(encoding="utf-8")
    assert "_LOOPBACK_TEST_SERVERS" not in openapi_src

    now = datetime(2026, 11, 11, 10, 0, 0, tzinfo=UTC)
    bridge_dir = tmp_path / "cli_bridge"
    state_db = tmp_path / "cli_state.sqlite"

    daemon = SimulatedCTraderCliDaemon(
        bridge_dir,
        is_live=False,
        partial_fill_ratio=Decimal("0.5"),
        clock=lambda: now,
    )
    daemon.start()

    adapter = CTraderCliDemoBrokerAdapter(
        bridge_dir=bridge_dir,
        state_db_path=state_db,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number="1000001",
        clock=lambda: now,
    )
    try:
        init_rec = adapter.reconcile()
        assert init_rec.clean is True
        assert init_rec.blocks_new_risk is False

        dec = AgentDecision(
            decision_id="dec-demo-1",
            event_id="ev-demo-1",
            based_on_memory_version=1,
            based_on_snapshot_id="snap-demo-1",
            provider="deepseek",
            model_version="deepseek-chat",
            prompt_version=SINGLE_PASS_PROMPT_VERSION,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(minutes=5),
            portfolio_action="SET_TARGET",
            target=TargetPortfolio(
                target_id="tgt-demo-1",
                based_on_snapshot_id="snap-demo-1",
                rationale="Demo test long",
                positions=(
                    TargetPosition(
                        instrument="EUR/USD",
                        weight=Decimal("0.10"),
                        thesis_ids=("t1",),
                    ),
                ),
            ),
            rationale="Demo test long",
        )
        unapproved = RiskAssessment(
            assessment_id="assess-no",
            decision_id="dec-demo-1",
            target_id="tgt-demo-1",
            checked_snapshot_id="snap-demo-1",
            policy_version="test",
            checked_at=now,
            valid_until=now + timedelta(minutes=5),
            outcome="reject",
            reasons=("blocked",),
        )
        with pytest.raises(DemoBrokerSafetyError):
            adapter.submit_market_order(
                decision=dec,
                assessment=unapproved,
                side="buy",
                volume_units=2000,
                stop_loss_pips=11.0,
            )

        approved = unapproved.model_copy(
            update={
                "assessment_id": "assess-ok",
                "outcome": "approve",
                "reasons": ("within_risk_limits",),
                "approved_quantity_eur": Decimal("2000"),
            }
        )
        approved_1000 = approved.model_copy(
            update={
                "assessment_id": "assess-ok-1000",
                "approved_quantity_eur": Decimal("1000"),
            }
        )
        receipt = adapter.submit_market_order(
            decision=dec,
            assessment=approved,
            side="buy",
            volume_units=2000,
            client_intent_id="intent-buy-1",
            stop_loss_pips=11.0,
            take_profit_pips=22.0,
        )
        assert receipt.mode == "ctrader_cli_demo"
        assert adapter.get_intent("intent-buy-1")["status"] == "submitted"  # type: ignore[index]
        assert adapter.open_positions() == []

        # Verify 'accepted' event alone NEVER marks a position open before fill confirmation
        with (bridge_dir / "execution-events.jsonl").open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "version": 1,
                        "kind": "order_event",
                        "bridge_session_id": daemon.bridge_session_id,
                        "event_seq": 99,
                        "emitted_at": now.isoformat(),
                        "account_mode": "demo",
                        "is_live": False,
                        "account_number": daemon.account_number,
                        "account_currency": daemon.account_currency,
                        "client_intent_id": "intent-buy-1",
                        "intent_type": "market_order",
                        "status": "accepted",
                        "requested_units": 2000,
                        "filled_units": 0,
                    }
                )
                + "\n"
            )
        adapter.poll_events()
        assert adapter.get_intent("intent-buy-1")["status"] == "accepted"  # type: ignore[index]
        assert adapter.open_positions() == []

        # Daemon processes intent-buy-1 (partial fill: 1,000 of 2,000 units)
        daemon._event_seq = 100
        assert daemon.step() == 1
        adapter.poll_events()
        intent_row = adapter.get_intent("intent-buy-1")
        assert intent_row is not None
        assert intent_row["status"] == "partial"
        assert intent_row["filled_units"] == 1000
        open_pos = adapter.open_positions()
        assert len(open_pos) == 1
        pos_id = int(open_pos[0]["broker_position_id"])
        assert int(open_pos[0]["volume_units"]) == 1000
        assert int(open_pos[0]["sl_confirmed"]) == 1

        # Restart BOTH adapter and daemon and replay same client_intent_id -> strictly idempotent!
        adapter.close()
        daemon_restarted = SimulatedCTraderCliDaemon(bridge_dir, clock=lambda: now)
        daemon_restarted.start()
        adapter = CTraderCliDemoBrokerAdapter(
            bridge_dir=bridge_dir,
            state_db_path=state_db,
            allow_orders=True,
            confirm_demo_execution=True,
            expected_demo_account_number="1000001",
            clock=lambda: now,
        )
        dup_receipt = adapter.submit_market_order(
            decision=dec,
            assessment=approved,
            side="buy",
            volume_units=2000,
            client_intent_id="intent-buy-1",
            stop_loss_pips=11.0,
        )
        assert dup_receipt.reason == "idempotent_existing_intent_reused"
        # Restarted daemon re-scans order-intents.jsonl from offset 0 and ignores already-processed intent-buy-1
        assert daemon_restarted.step() == 1
        dup_events = adapter.poll_events()
        assert any(e.get("status") == "duplicate_ignored" for e in dup_events)
        assert adapter.get_intent("intent-buy-1")["status"] == "partial"  # type: ignore[index]
        assert len(adapter.open_positions()) == 1

        # Close open position strictly by broker_position_id
        close_cid = adapter.submit_close_position(
            broker_position_id=pos_id,
            client_intent_id="intent-close-1",
        )
        assert close_cid == "intent-close-1"
        assert daemon_restarted.step() == 1
        adapter.poll_events()
        assert adapter.open_positions() == []
        assert adapter.reconcile().clean is True

        # Rejected order & cancel_pending_order
        daemon_restarted.reject_next_reason = "NOT_ENOUGH_MONEY"
        adapter.submit_market_order(
            decision=dec,
            assessment=approved_1000,
            side="buy",
            volume_units=1000,
            client_intent_id="intent-rej-1",
            stop_loss_pips=11.0,
        )
        daemon_restarted.step()
        adapter.poll_events()
        assert adapter.get_intent("intent-rej-1")["status"] == "rejected"  # type: ignore[index]

        daemon_restarted.inject_pending_order(broker_order_id=8801, side="buy", volume_units=1000)
        adapter.submit_cancel_pending_order(
            broker_order_id=8801, client_intent_id="intent-cancel-1"
        )
        daemon_restarted.step()
        adapter.poll_events()
        assert adapter.get_intent("intent-cancel-1")["status"] == "cancelled"  # type: ignore[index]

        # Missing SL attachment -> reconcile halts new risk while permitting close_position!
        daemon_restarted.drop_stop_loss_attachment = True
        adapter.submit_market_order(
            decision=dec,
            assessment=approved_1000,
            side="buy",
            volume_units=1000,
            client_intent_id="intent-nosl-1",
            stop_loss_pips=11.0,
        )
        daemon_restarted.step()
        rec_nosl = adapter.reconcile()
        assert rec_nosl.blocks_new_risk is True
        assert len(rec_nosl.unconfirmed_sl_position_ids) == 1
        nosl_pos_id = rec_nosl.unconfirmed_sl_position_ids[0]
        with pytest.raises(DemoBrokerSafetyError, match="New risk halted"):
            adapter.submit_market_order(
                decision=dec,
                assessment=approved_1000,
                side="buy",
                volume_units=1000,
                client_intent_id="intent-blocked-after-nosl",
                stop_loss_pips=11.0,
            )
        adapter.submit_close_position(broker_position_id=nosl_pos_id, client_intent_id="close-nosl")
        daemon_restarted.step()
        adapter.poll_events()
        assert adapter.open_positions() == []

        # Broker-side SL close & orphan detection (use explicit operator unlock after nosl close)
        daemon_restarted.drop_stop_loss_attachment = False
        unlock_rec = adapter.acknowledge_manual_unlock(
            note="Verified unconfirmed-SL position closed cleanly at broker"
        )
        assert unlock_rec.blocks_new_risk is False
        adapter.submit_market_order(
            decision=dec,
            assessment=approved_1000,
            side="buy",
            volume_units=1000,
            client_intent_id="intent-slclose-1",
            stop_loss_pips=11.0,
        )
        daemon_restarted.step()
        adapter.poll_events()
        sl_pos_id = int(adapter.open_positions()[0]["broker_position_id"])
        daemon_restarted.trigger_broker_stop_loss_close(sl_pos_id)
        rec_sl = adapter.reconcile()
        assert sl_pos_id in rec_sl.closed_at_broker_position_ids
        assert adapter.open_positions() == []

        daemon_restarted.inject_orphan_position(broker_position_id=9911, volume_units=1000)
        daemon_restarted.emit_reconcile_snapshot("orphan_injected")
        rec_orphan = adapter.reconcile()
        assert rec_orphan.blocks_new_risk is True
        assert 9911 in rec_orphan.orphan_broker_position_ids
    finally:
        adapter.close()

    # CLI `eventlens-shadow demo-reconcile` smoke check
    shadow_cli_main(
        [
            "demo-reconcile",
            "--quote-bridge-dir",
            str(bridge_dir),
            "--shadow-db",
            str(state_db),
        ]
    )
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["scope"] == "ctrader_cli_demo_reconciliation"
    assert cli_out["reconciliation"]["blocks_new_risk"] is True
    assert 9911 in cli_out["reconciliation"]["orphan_broker_position_ids"]

    # Live account refusal hard lock
    live_dir = tmp_path / "live_bridge"
    live_daemon = SimulatedCTraderCliDaemon(live_dir, is_live=True, clock=lambda: now)
    live_daemon.start()
    live_adapter = CTraderCliDemoBrokerAdapter(
        bridge_dir=live_dir,
        state_db_path=tmp_path / "live_state.sqlite",
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number="1000001",
        clock=lambda: now,
    )
    try:
        with pytest.raises(LiveAccountForbiddenError):
            live_adapter.poll_events()
        assert live_adapter.halted_reason is not None
        assert "live_account" in live_adapter.halted_reason
    finally:
        live_adapter.close()


def test_stage_f_runtime_and_shadow_runner_cli_demo_confirmed_fills_and_demo_smoke(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Review Item 1:
    - Risk-approved targets in IntradayEventRuntime / ShadowRunner dispatch to CTraderCliDemoBrokerAdapter.
    - SQLiteShadowLedger has ZERO simulated orders/fills; Demo positions open/close strictly on broker-confirmed fills.
    - ProtectiveExitPolicy triggers independent close_position by broker_position_id.
    - CLI `demo-smoke` executes bounded open -> SL-confirmed -> close -> flat without paid LLM calls.
    - CLI `start --ctrader-cli-demo-orders` without `--call-model` fails loudly (no silent non-trading)."""
    t_now = [datetime(2026, 11, 11, 14, 0, 0, tzinfo=UTC)]
    bridge_dir = tmp_path / "rt_demo_bridge"
    state_db = tmp_path / "rt_demo_state.sqlite"
    shd_db = tmp_path / "rt_shadow.sqlite"
    mem_db = tmp_path / "rt_mem.sqlite"
    quotes_db = tmp_path / "rt_quotes.sqlite"

    daemon = SimulatedCTraderCliDaemon(
        bridge_dir,
        account_number=5100201,
        account_currency="USD",
        allow_demo_orders=True,
        clock=lambda: t_now[0],
    )
    daemon.start()
    daemon.emit_quote_session_and_tick()

    qstore = QuoteStore(quotes_db, clock=lambda: t_now[0])
    mem = SQLiteThesisMemory(mem_db, clock=lambda: t_now[0], allow_targets=True)
    r_store = SQLiteRuntimeStore(shd_db)
    exit_pol = ProtectiveExitPolicy(
        stop_loss_pips=Decimal("11"),
        take_profit_pips=Decimal("22"),
        max_holding_seconds=20,
    )
    risk_pol = RiskPolicy.pepperstone_razor_200(
        max_risk_per_trade_pct=Decimal("0.02"),
        ex_ante_stop_loss_pips=Decimal("11"),
        cap_quantity_by_risk_budget=True,
    )
    ledger = SQLiteShadowLedger(
        shd_db,
        initial_cash_usd=Decimal("200"),
        exit_policy=exit_pol,
        risk_policy=risk_pol,
        clock=lambda: t_now[0],
    )
    adapter = CTraderCliDemoBrokerAdapter(
        state_db_path=state_db,
        bridge_dir=bridge_dir,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )

    traj_ok = PriceTrajectorySummary(
        instrument="EUR/USD",
        as_of=t_now[0],
        current_mid=Decimal("1.12464"),
        change_1m_pips=Decimal("2.5"),
        change_5m_pips=Decimal("2.0"),
        change_15m_pips=Decimal("1.5"),
        change_30m_pips=Decimal("3.0"),
        max_gap_30m_seconds=1.0,
        sample_count_30m=300,
    )
    client = _StaticPayloadModelClient(
        [
            {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Eurozone output surprise supports EUR/USD.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1230"],
                "confidence": 0.72,
                "evidence_quote": "Eurozone industrial output rose 0.8% m/m vs 0.2% expected.",
                "evidence_role": "support",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.20",
                "target_thesis_ids": ["__current__"],
                "rationale": "Initiate risk-budgeted long EUR/USD on beat.",
            },
            {
                "memory_action": "update",
                "thesis_id": None,
                "status": "invalidated",
                "narrative": "Take off long exposure cleanly.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1230"],
                "confidence": 0.30,
                "evidence_quote": "Follow-up wire offsets initial impulse.",
                "evidence_role": "contradict",
                "portfolio_action": "CLOSE",
                "target_direction": "flat",
                "target_weight": "0",
                "target_thesis_ids": [],
                "cited_evidence_ids": ["demo-ev-2"],
                "rationale": "Flatten Demo position by broker Position.Id.",
            },
            {
                "memory_action": "create",
                "thesis_id": None,
                "status": "active",
                "narrative": "Second macro surprise for protective exit test.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD <= 1.1230"],
                "confidence": 0.70,
                "evidence_quote": "ECB speaker signals firmer rate path.",
                "evidence_role": "support",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.20",
                "target_thesis_ids": ["__current__"],
                "rationale": "Open second Demo position to verify protective close.",
            },
        ]
    )

    rt = IntradayEventRuntime(
        memory=mem,
        portfolio_state=ledger,
        market_provider=lambda ts: _fresh_market(
            ts, trajectory=traj_ok.model_copy(update={"as_of": ts})
        ),
        agent=SinglePassTradingAgent(client, clock=lambda: t_now[0]),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(policy=risk_pol, clock=lambda: t_now[0]),
        shadow_executor=ledger,
        state_store=r_store,
        clock=lambda: t_now[0],
    )
    runner = ShadowRunner(
        runtime=rt,
        ledger=ledger,
        quote_store=qstore,
        quote_bridge_dir=bridge_dir,
        ctrader_cli_demo_adapter=adapter,
        clock=lambda: t_now[0],
    )
    try:
        assert isinstance(runner.demo_execution_engine, CTraderCliDemoExecutionEngine)
        assert rt.portfolio_state is runner.demo_execution_engine
        assert rt.shadow_executor is runner.demo_execution_engine

        asyncio.run(runner.step_quotes_and_fills())

        # 1. Dispatch SET_TARGET -> approved at 3,000 EUR (2% risk budget on $200 with 11p SL)
        ev1 = Event(
            event_id="demo-ev-1",
            source="ecb_news",
            source_type="news",
            source_reference="https://www.ecb.europa.eu/demo1",
            source_timestamp=t_now[0],
            received_timestamp=t_now[0],
            raw_text="Eurozone industrial output rose 0.8% m/m vs 0.2% expected.",
        )
        res1 = asyncio.run(rt.run_cycle(ev1))
        assert res1.status == "shadow_recorded"
        assert res1.assessment is not None
        assert res1.assessment.approved_quantity_eur == Decimal("3000")
        assert res1.receipt is not None and res1.receipt.mode == "ctrader_cli_demo"
        entry_cid = res1.receipt.execution_id

        # Crucial invariant: SQLiteShadowLedger has ZERO simulated orders/fills!
        assert ledger.report()["order_status_counts"] == {}
        assert ledger.report()["fills"] == []

        # While order is only 'submitted', PortfolioSnapshot is strictly flat (0 EUR) with pending_order_ids
        snap_pre = asyncio.run(rt.portfolio_state.snapshot())
        assert snap_pre.positions == ()
        assert entry_cid in snap_pre.pending_order_ids

        # Inject 'accepted' frame -> still 0 EUR held!
        with (bridge_dir / "execution-events.jsonl").open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "version": 1,
                        "kind": "order_event",
                        "bridge_session_id": daemon.bridge_session_id,
                        "event_seq": 50,
                        "emitted_at": t_now[0].isoformat(),
                        "account_mode": "demo",
                        "is_live": False,
                        "account_number": 5100201,
                        "account_currency": "USD",
                        "client_intent_id": entry_cid,
                        "intent_type": "market_order",
                        "status": "accepted",
                        "requested_units": 3000,
                        "filled_units": 0,
                    }
                )
                + "\n"
            )
        asyncio.run(runner.step_quotes_and_fills())
        snap_acc = asyncio.run(rt.portfolio_state.snapshot())
        assert snap_acc.positions == ()
        assert entry_cid in snap_acc.pending_order_ids

        # Daemon executes & confirms broker fill -> PortfolioSnapshot reflects +3,000 EUR
        daemon._event_seq = 51
        assert daemon.step() == 1
        exit_fills_1, target_fills_1 = asyncio.run(runner.step_quotes_and_fills())
        assert len(exit_fills_1) + len(target_fills_1) == 1
        snap_filled = asyncio.run(rt.portfolio_state.snapshot())
        assert len(snap_filled.positions) == 1
        assert snap_filled.positions[0].quantity == Decimal("3000")
        assert snap_filled.pending_order_ids == ()
        assert ledger.report()["fills"] == []

        # 2. Follow-up CLOSE decision -> submits close_position by broker_position_id
        active_tid = asyncio.run(mem.snapshot(as_of=t_now[0])).theses[0].thesis_id
        client._payloads[1]["thesis_id"] = active_tid
        client._payloads[1]["target_thesis_ids"] = [active_tid]
        ev2 = Event(
            event_id="demo-ev-2",
            source="investinglive_news",
            source_type="news",
            source_reference="https://investinglive.com/demo2",
            source_timestamp=t_now[0],
            received_timestamp=t_now[0],
            raw_text="Follow-up wire offsets initial impulse.",
        )
        res2 = asyncio.run(rt.run_cycle(ev2))
        assert res2.status == "shadow_recorded"
        # Position remains +3,000 EUR until broker confirms close!
        snap_closing = asyncio.run(rt.portfolio_state.snapshot())
        assert len(snap_closing.positions) == 1
        assert len(snap_closing.pending_order_ids) == 1

        assert daemon.step() == 1
        exit_fills_2, target_fills_2 = asyncio.run(runner.step_quotes_and_fills())
        assert len(exit_fills_2) + len(target_fills_2) == 1
        snap_flat = asyncio.run(rt.portfolio_state.snapshot())
        assert snap_flat.positions == ()
        assert snap_flat.pending_order_ids == ()
        assert ledger.report()["fills"] == []

        # 3. Second entry closed via independent ProtectiveExitPolicy (max_holding_seconds=20)
        t_now[0] = t_now[0] + timedelta(seconds=2)
        daemon.emit_quote_session_and_tick()
        daemon.emit_reconcile_snapshot("before_ev3")
        asyncio.run(runner.step_quotes_and_fills())
        ev3 = Event(
            event_id="demo-ev-3",
            source="ecb_news",
            source_type="news",
            source_reference="https://www.ecb.europa.eu/demo3",
            source_timestamp=t_now[0],
            received_timestamp=t_now[0],
            raw_text="ECB speaker signals firmer rate path.",
        )
        res3 = asyncio.run(rt.run_cycle(ev3))
        assert res3.status == "shadow_recorded"
        assert daemon.step() == 1
        asyncio.run(runner.step_quotes_and_fills())
        assert len(asyncio.run(rt.portfolio_state.snapshot()).positions) == 1

        t_now[0] = t_now[0] + timedelta(seconds=25)
        daemon.emit_quote_session_and_tick()
        daemon.emit_reconcile_snapshot("holding_expired")
        asyncio.run(runner.step_quotes_and_fills())
        asyncio.run(runner.step_protection())
        assert daemon.step() == 1
        exit_fills_3, target_fills_3 = asyncio.run(runner.step_quotes_and_fills())
        assert len(exit_fills_3) + len(target_fills_3) == 1
        assert asyncio.run(rt.portfolio_state.snapshot()).positions == ()
        assert ledger.report()["fills"] == []
    finally:
        adapter.close()
        qstore.close()
        ledger.close()
        r_store.close()

    # 4. Silent non-trading forbidden: `start --ctrader-cli-demo-orders` without `--call-model` exits with error
    with pytest.raises(SystemExit) as exc_info:
        shadow_cli_main(
            [
                "start",
                "--duration",
                "5",
                "--quote-bridge-dir",
                str(bridge_dir),
                "--ctrader-cli-demo-orders",
                "--confirm-demo-execution",
            ]
        )
    assert exc_info.value.code == 2
    capsys.readouterr()

    # 5. Bounded model-free `eventlens-shadow demo-smoke` CLI end-to-end check
    smoke_bridge = tmp_path / "smoke_bridge"
    smoke_shd = tmp_path / "smoke_data" / "shadow-ledger.sqlite"
    smoke_quotes = tmp_path / "smoke_data" / "live-quotes.sqlite"
    smoke_daemon = SimulatedCTraderCliDaemon(
        smoke_bridge,
        account_number=5100201,
        account_currency="USD",
        allow_demo_orders=True,
    )
    smoke_daemon.start()
    smoke_daemon.emit_quote_session_and_tick()
    stop_evt = threading.Event()

    def _drive_smoke_daemon() -> None:
        while not stop_evt.is_set():
            smoke_daemon.step()
            time.sleep(0.01)

    drv = threading.Thread(target=_drive_smoke_daemon, daemon=True)
    drv.start()
    try:
        shadow_cli_main(
            [
                "demo-smoke",
                "--quote-bridge-dir",
                str(smoke_bridge),
                "--shadow-db",
                str(smoke_shd),
                "--quotes-db",
                str(smoke_quotes),
                "--expected-demo-account-number",
                "5100201",
                "--confirm-demo-execution",
                "--volume-units",
                "1000",
                "--hold-seconds",
                "0.05",
                "--timeout-seconds",
                "10",
            ]
        )
    finally:
        stop_evt.set()
        drv.join(timeout=2.0)

    smoke_out = json.loads(capsys.readouterr().out)
    assert smoke_out["scope"] == "ctrader_cli_demo_smoke"
    assert smoke_out["passed"] is True
    assert smoke_out["approved_quantity_eur"] == "1000"
    assert smoke_out["sl_confirmed"] is True
    assert smoke_out["flat_after_close"] is True


def test_stage_f_bridge_restart_seq_reset_and_snapshot_safety_gates(tmp_path: Path) -> None:
    """Review Item 2:
    - SimulatedCTraderCliDaemon resets _event_seq = 0 on every restart (matching C# EventLensCliBridge).
    - Adapter keys events by (bridge_session_id, event_seq) and selects latest reconcile_snapshot by append order,
      never picking a stale snapshot from an older session with a higher event_seq.
    - Missing, stale, wrong account_number, non-USD account_currency, disabled allow_demo_orders,
      and side/volume target mismatches ALL reject submit_market_order."""
    t_now = [datetime(2026, 11, 11, 15, 0, 0, tzinfo=UTC)]
    bridge_dir = tmp_path / "seq_bridge"
    state_db = tmp_path / "seq_state.sqlite"

    dec = AgentDecision(
        decision_id="dec-seq-1",
        event_id="ev-seq-1",
        based_on_memory_version=1,
        based_on_snapshot_id="snap-seq-1",
        provider="deepseek",
        model_version="deepseek-chat",
        prompt_version=SINGLE_PASS_PROMPT_VERSION,
        started_at=t_now[0],
        completed_at=t_now[0],
        valid_until=t_now[0] + timedelta(minutes=5),
        portfolio_action="SET_TARGET",
        target=TargetPortfolio(
            target_id="tgt-seq-1",
            based_on_snapshot_id="snap-seq-1",
            rationale="Long target",
            positions=(
                TargetPosition(
                    instrument="EUR/USD",
                    weight=Decimal("0.15"),
                    thesis_ids=("t1",),
                ),
            ),
        ),
        rationale="Long target",
    )
    approved_3k = RiskAssessment(
        assessment_id="assess-seq-3k",
        decision_id="dec-seq-1",
        target_id="tgt-seq-1",
        checked_snapshot_id="snap-seq-1",
        policy_version="test",
        checked_at=t_now[0],
        valid_until=t_now[0] + timedelta(minutes=5),
        outcome="approve",
        reasons=("within_risk_limits",),
        approved_quantity_eur=Decimal("3000"),
    )

    # 1. Missing snapshot -> submit_market_order rejects immediately
    empty_adapter = CTraderCliDemoBrokerAdapter(
        state_db_path=tmp_path / "empty_state.sqlite",
        bridge_dir=tmp_path / "empty_bridge",
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    try:
        with pytest.raises(DemoBrokerSafetyError, match="no_reconcile_snapshot"):
            empty_adapter.submit_market_order(
                decision=dec,
                assessment=approved_3k,
                side="buy",
                volume_units=3000,
                stop_loss_pips=11.0,
            )
    finally:
        empty_adapter.close()

    # 2. Session 1 advances event_seq to 7; Session 2 restarts with _event_seq == 0 -> event_seq == 1 wins!
    daemon1 = SimulatedCTraderCliDaemon(
        bridge_dir,
        account_number=5100201,
        account_currency="USD",
        allow_demo_orders=True,
        clock=lambda: t_now[0],
    )
    assert daemon1._event_seq == 0
    daemon1.start()
    for idx in range(6):
        daemon1.emit_reconcile_snapshot(f"s1_hb_{idx}")
    assert daemon1._event_seq == 7

    adapter = CTraderCliDemoBrokerAdapter(
        state_db_path=state_db,
        bridge_dir=bridge_dir,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        max_snapshot_age_seconds=45.0,
        clock=lambda: t_now[0],
    )
    try:
        rec1 = adapter.reconcile()
        assert rec1.bridge_session_id == daemon1.bridge_session_id
        assert rec1.snapshot_event_seq == 7

        t_now[0] = t_now[0] + timedelta(seconds=10)
        daemon2 = SimulatedCTraderCliDaemon(
            bridge_dir,
            account_number=5100201,
            account_currency="USD",
            allow_demo_orders=True,
            clock=lambda: t_now[0],
        )
        assert daemon2._event_seq == 0
        assert daemon2.bridge_session_id != daemon1.bridge_session_id
        daemon2.start()
        assert daemon2._event_seq == 1

        rec2 = adapter.reconcile()
        assert rec2.bridge_session_id == daemon2.bridge_session_id
        assert rec2.snapshot_event_seq == 1
        assert rec2.clean is True

        # 3. Side mismatch & volume_units mismatch against approved target are rejected
        with pytest.raises(
            DemoBrokerSafetyError, match="contradicts approved decision.target weight"
        ):
            adapter.submit_market_order(
                decision=dec,
                assessment=approved_3k,
                side="sell",
                volume_units=3000,
                stop_loss_pips=11.0,
            )
        with pytest.raises(DemoBrokerSafetyError, match="does not match approved target delta"):
            adapter.submit_market_order(
                decision=dec,
                assessment=approved_3k,
                side="buy",
                volume_units=4000,
                stop_loss_pips=11.0,
            )

        # 4. Stale snapshot (> 45s old) blocks new risk
        t_now[0] = t_now[0] + timedelta(seconds=60)
        with pytest.raises(DemoBrokerSafetyError, match="stale_reconcile_snapshot"):
            adapter.submit_market_order(
                decision=dec,
                assessment=approved_3k,
                side="buy",
                volume_units=3000,
                stop_loss_pips=11.0,
            )

        # 5. allow_demo_orders=False on snapshot blocks new risk
        daemon2.allow_demo_orders = False
        daemon2.emit_reconcile_snapshot("orders_disabled")
        with pytest.raises(DemoBrokerSafetyError, match="allow_demo_orders=False"):
            adapter.submit_market_order(
                decision=dec,
                assessment=approved_3k,
                side="buy",
                volume_units=3000,
                stop_loss_pips=11.0,
            )
    finally:
        adapter.close()

    # 6. Wrong account_number and wrong account_currency ("EUR") block new risk
    dec_6 = dec.model_copy(update={"valid_until": t_now[0] + timedelta(minutes=5)})
    approved_6 = approved_3k.model_copy(
        update={"checked_at": t_now[0], "valid_until": t_now[0] + timedelta(minutes=5)}
    )
    for sub_name, acct_num, acct_ccy, expected_match in (
        ("wrong_acct", 9999999, "USD", "unexpected_demo_account_number"),
        ("wrong_ccy", 5100201, "EUR", "unexpected_account_currency"),
    ):
        b_dir = tmp_path / sub_name
        d_sub = SimulatedCTraderCliDaemon(
            b_dir,
            account_number=acct_num,
            account_currency=acct_ccy,
            allow_demo_orders=True,
            clock=lambda: t_now[0],
        )
        d_sub.start()
        a_sub = CTraderCliDemoBrokerAdapter(
            state_db_path=tmp_path / f"{sub_name}.sqlite",
            bridge_dir=b_dir,
            allow_orders=True,
            confirm_demo_execution=True,
            expected_demo_account_number=5100201,
            expected_account_currency="USD",
            clock=lambda: t_now[0],
        )
        try:
            with pytest.raises(DemoBrokerSafetyError, match=expected_match):
                a_sub.submit_market_order(
                    decision=dec_6,
                    assessment=approved_6,
                    side="buy",
                    volume_units=3000,
                    stop_loss_pips=11.0,
                )
        finally:
            a_sub.close()


def test_stage_f_intent_ttl_expiry_and_independent_protective_close(tmp_path: Path) -> None:
    """Review Item 3:
    - Every order-intents.jsonl frame carries mandatory UTC expires_at.
    - Intents arriving while AllowDemoOrders=false are rejected immediately and NEVER replayed when toggled true.
    - Intents arriving past expires_at are marked 'expired' without opening a position.
    - Protective close_position uses an independent fresh TTL channel and executes cleanly after entry TTL passes."""
    t_now = [datetime(2026, 11, 11, 16, 0, 0, tzinfo=UTC)]
    bridge_dir = tmp_path / "ttl_bridge"
    state_db = tmp_path / "ttl_state.sqlite"

    daemon = SimulatedCTraderCliDaemon(
        bridge_dir,
        account_number=5100201,
        account_currency="USD",
        allow_demo_orders=True,
        clock=lambda: t_now[0],
    )
    daemon.start()
    adapter = CTraderCliDemoBrokerAdapter(
        state_db_path=state_db,
        bridge_dir=bridge_dir,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        max_intent_ttl_seconds=20.0,
        protective_intent_ttl_seconds=60.0,
        clock=lambda: t_now[0],
    )
    dec = AgentDecision(
        decision_id="dec-ttl-1",
        event_id="ev-ttl-1",
        based_on_memory_version=1,
        based_on_snapshot_id="snap-ttl-1",
        provider="deepseek",
        model_version="deepseek-chat",
        prompt_version=SINGLE_PASS_PROMPT_VERSION,
        started_at=t_now[0],
        completed_at=t_now[0],
        valid_until=t_now[0] + timedelta(minutes=5),
        portfolio_action="SET_TARGET",
        target=TargetPortfolio(
            target_id="tgt-ttl-1",
            based_on_snapshot_id="snap-ttl-1",
            rationale="TTL test long",
            positions=(
                TargetPosition(
                    instrument="EUR/USD",
                    weight=Decimal("0.10"),
                    thesis_ids=("t1",),
                ),
            ),
        ),
        rationale="TTL test long",
    )
    approved_2k = RiskAssessment(
        assessment_id="assess-ttl-2k",
        decision_id="dec-ttl-1",
        target_id="tgt-ttl-1",
        checked_snapshot_id="snap-ttl-1",
        policy_version="test",
        checked_at=t_now[0],
        valid_until=t_now[0] + timedelta(minutes=5),
        outcome="approve",
        reasons=("within_risk_limits",),
        approved_quantity_eur=Decimal("2000"),
    )
    try:
        # 1. Submit while adapter still sees last snapshot (allow_demo_orders=True), then bridge flips allow_demo_orders=False
        adapter.submit_market_order(
            decision=dec,
            assessment=approved_2k,
            side="buy",
            volume_units=2000,
            client_intent_id="intent-disabled-1",
            stop_loss_pips=11.0,
        )
        daemon.allow_demo_orders = False
        assert daemon.step() == 1
        adapter.poll_events()
        row_dis = adapter.get_intent("intent-disabled-1")
        assert row_dis is not None and row_dis["status"] == "rejected"
        assert row_dis["error_code"] == "bridge_demo_orders_disabled"

        # Re-enabling allow_demo_orders NEVER replays the rejected intent!
        daemon.allow_demo_orders = True
        daemon.emit_reconcile_snapshot("reenabled")
        assert daemon.step() == 0
        adapter.poll_events()
        assert adapter.open_positions() == []

        # 2. Submit valid entry, then advance clock past expires_at (20s) before daemon polls
        adapter.submit_market_order(
            decision=dec,
            assessment=approved_2k,
            side="buy",
            volume_units=2000,
            client_intent_id="intent-stale-1",
            stop_loss_pips=11.0,
        )
        t_now[0] = t_now[0] + timedelta(seconds=25)
        assert daemon.step() == 1
        adapter.poll_events()
        row_exp = adapter.get_intent("intent-stale-1")
        assert row_exp is not None and row_exp["status"] == "expired"
        assert row_exp["error_code"] == "intent_expired_before_execution"
        assert adapter.open_positions() == []

        # 3. Open live Demo position, advance clock 35s (past entry TTL), and close on independent protective channel
        daemon.emit_reconcile_snapshot("fresh_for_open")
        dec_fresh = dec.model_copy(update={"valid_until": t_now[0] + timedelta(minutes=5)})
        approved_fresh = approved_2k.model_copy(
            update={"checked_at": t_now[0], "valid_until": t_now[0] + timedelta(minutes=5)}
        )
        adapter.submit_market_order(
            decision=dec_fresh,
            assessment=approved_fresh,
            side="buy",
            volume_units=2000,
            client_intent_id="intent-live-open",
            stop_loss_pips=11.0,
        )
        assert daemon.step() == 1
        adapter.poll_events()
        pos_id = int(adapter.open_positions()[0]["broker_position_id"])

        t_now[0] = t_now[0] + timedelta(seconds=35)
        close_cid = adapter.submit_close_position(
            broker_position_id=pos_id,
            client_intent_id="intent-prot-close",
            protective_reason="max_holding_seconds",
        )
        assert close_cid == "intent-prot-close"
        assert daemon.step() == 1
        adapter.poll_events()
        assert adapter.open_positions() == []
    finally:
        adapter.close()


def test_stage_f_partial_line_offsets_durable_outbox_and_unknown_execution_manual_unlock(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Review Item 4:
    - Partial-line JSONL writes (without '\\n') across restarts never advance offsets past incomplete bytes or drop frames.
    - SQLite durable outbox aborts unpublished market_order on restart, repairs torn half-lines, deduplicates crash-after-write,
      and flushes unexpired protective close_position on restart.
    - Two-stage intent-journal recovers post-broker crashes via broker Positions/History without double-executing,
      and transitions ambiguous pre-broker crashes to `unknown_execution` + `manual_unlock_required` (`demo-unlock`)."""
    t_now = [datetime(2026, 11, 11, 17, 0, 0, tzinfo=UTC)]

    # --- Part A: Partial-line JSONL safety across BOTH Python adapter & daemon restarts ---
    pl_bridge = tmp_path / "partial_bridge"
    pl_bridge.mkdir(parents=True)
    pl_state = tmp_path / "partial_state.sqlite"
    snap_frame = json.dumps(
        {
            "version": 1,
            "kind": "reconcile_snapshot",
            "bridge_session_id": "sess-partial-1",
            "event_seq": 1,
            "emitted_at": t_now[0].isoformat(),
            "account_mode": "demo",
            "is_live": False,
            "account_number": 5100201,
            "account_currency": "USD",
            "allow_demo_orders": True,
            "trigger": "partial_line_test",
            "balance_usd": "200.00",
            "equity_usd": "200.00",
            "margin_used_usd": "0.00",
            "open_positions": [],
            "pending_orders": [],
        }
    )
    half = len(snap_frame) // 2
    ev_path = pl_bridge / "execution-events.jsonl"
    ev_path.write_text(snap_frame[:half], encoding="utf-8")

    a1 = CTraderCliDemoBrokerAdapter(
        state_db_path=pl_state,
        bridge_dir=pl_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    assert a1.poll_events() == []
    a1.close()

    with ev_path.open("a", encoding="utf-8") as f:
        f.write(snap_frame[half:] + "\n")
    a2 = CTraderCliDemoBrokerAdapter(
        state_db_path=pl_state,
        bridge_dir=pl_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    try:
        polled = a2.poll_events()
        assert len(polled) == 1
        assert polled[0]["trigger"] == "partial_line_test"
    finally:
        a2.close()

    # Partial line on order-intents.jsonl across daemon restart
    d_pl_1 = SimulatedCTraderCliDaemon(pl_bridge, account_number=5100201, clock=lambda: t_now[0])
    intent_frame = json.dumps(
        {
            "version": 1,
            "client_intent_id": "rec-partial-1",
            "intent_type": "reconcile",
            "symbol": "EURUSD",
            "created_at": t_now[0].isoformat(),
            "expires_at": (t_now[0] + timedelta(seconds=30)).isoformat(),
        }
    )
    intents_path = pl_bridge / "order-intents.jsonl"
    intents_path.write_text(intent_frame[:25], encoding="utf-8")
    assert d_pl_1.step() == 0
    with intents_path.open("a", encoding="utf-8") as f:
        f.write(intent_frame[25:] + "\n")
    d_pl_2 = SimulatedCTraderCliDaemon(pl_bridge, account_number=5100201, clock=lambda: t_now[0])
    assert d_pl_2.step() == 1

    # --- Part B: Python SQLite durable outbox fault modes ---
    ob_bridge = tmp_path / "outbox_bridge"
    ob_state = tmp_path / "outbox_state.sqlite"
    d_ob = SimulatedCTraderCliDaemon(
        ob_bridge,
        account_number=5100201,
        account_currency="USD",
        allow_demo_orders=True,
        clock=lambda: t_now[0],
    )
    d_ob.start()

    dec = AgentDecision(
        decision_id="dec-ob-1",
        event_id="ev-ob-1",
        based_on_memory_version=1,
        based_on_snapshot_id="snap-ob-1",
        provider="deepseek",
        model_version="deepseek-chat",
        prompt_version=SINGLE_PASS_PROMPT_VERSION,
        started_at=t_now[0],
        completed_at=t_now[0],
        valid_until=t_now[0] + timedelta(minutes=5),
        portfolio_action="SET_TARGET",
        target=TargetPortfolio(
            target_id="tgt-ob-1",
            based_on_snapshot_id="snap-ob-1",
            rationale="Outbox test long",
            positions=(
                TargetPosition(
                    instrument="EUR/USD",
                    weight=Decimal("0.10"),
                    thesis_ids=("t1",),
                ),
            ),
        ),
        rationale="Outbox test long",
    )
    approved_2k = RiskAssessment(
        assessment_id="assess-ob-2k",
        decision_id="dec-ob-1",
        target_id="tgt-ob-1",
        checked_snapshot_id="snap-ob-1",
        policy_version="test",
        checked_at=t_now[0],
        valid_until=t_now[0] + timedelta(minutes=5),
        outcome="approve",
        reasons=("within_risk_limits",),
        approved_quantity_eur=Decimal("2000"),
    )

    # B1. skip_file_write_after_db_commit on market_order -> aborted on restart (never replayed!)
    a_skip = CTraderCliDemoBrokerAdapter(
        state_db_path=ob_state,
        bridge_dir=ob_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        publish_fault_mode="skip_file_write_after_db_commit",
        clock=lambda: t_now[0],
    )
    a_skip.reconcile()
    a_skip.submit_market_order(
        decision=dec,
        assessment=approved_2k,
        side="buy",
        volume_units=2000,
        client_intent_id="ob-unpublished-mkt",
        stop_loss_pips=11.0,
    )
    assert not (ob_bridge / "order-intents.jsonl").exists()
    a_skip.close()

    a_rec1 = CTraderCliDemoBrokerAdapter(
        state_db_path=ob_state,
        bridge_dir=ob_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    row_unpub = a_rec1.get_intent("ob-unpublished-mkt")
    assert row_unpub is not None and row_unpub["status"] == "cancelled"
    assert row_unpub["error_code"] == "aborted_unpublished_market_order_on_restart"
    assert not (ob_bridge / "order-intents.jsonl").exists()

    # B2. write_half_line_after_db_commit -> truncates torn tail cleanly on restart
    a_rec1.publish_fault_mode = "write_half_line_after_db_commit"
    a_rec1.submit_market_order(
        decision=dec,
        assessment=approved_2k,
        side="buy",
        volume_units=2000,
        client_intent_id="ob-halfline-mkt",
        stop_loss_pips=11.0,
    )
    raw_torn = (ob_bridge / "order-intents.jsonl").read_bytes()
    assert len(raw_torn) > 0 and not raw_torn.endswith(b"\n")
    a_rec1.close()

    a_rec2 = CTraderCliDemoBrokerAdapter(
        state_db_path=ob_state,
        bridge_dir=ob_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    assert (ob_bridge / "order-intents.jsonl").read_bytes() == b""
    assert a_rec2.get_intent("ob-halfline-mkt")["status"] == "cancelled"  # type: ignore[index]

    # B3. crash_after_file_write_before_db_mark -> promotes to submitted on restart without duplicating JSONL line
    a_rec2.publish_fault_mode = "crash_after_file_write_before_db_mark"
    with pytest.raises(RuntimeError, match="Simulated crash after order-intents.jsonl write"):
        a_rec2.submit_market_order(
            decision=dec,
            assessment=approved_2k,
            side="buy",
            volume_units=2000,
            client_intent_id="ob-written-mkt",
            stop_loss_pips=11.0,
        )
    a_rec2.close()

    a_rec3 = CTraderCliDemoBrokerAdapter(
        state_db_path=ob_state,
        bridge_dir=ob_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    lines_after = (ob_bridge / "order-intents.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines_after) == 1
    assert a_rec3.get_intent("ob-written-mkt")["status"] == "submitted"  # type: ignore[index]

    # --- Part C: Bridge crash AFTER broker call -> recovers 'filled' without double-executing! ---
    d_ob.crash_mode = "after_broker_call_before_event"
    with pytest.raises(SimulatedBridgeCrashError):
        d_ob.step()
    a_rec3.poll_events()
    assert a_rec3.open_positions() == []  # Only 'accepted' was emitted before crash

    d_ob_restarted = SimulatedCTraderCliDaemon(
        ob_bridge,
        account_number=5100201,
        account_currency="USD",
        allow_demo_orders=True,
        clock=lambda: t_now[0],
    )
    d_ob_restarted.start()
    assert d_ob_restarted.step() == 1
    rec_events = a_rec3.poll_events()
    assert any(e.get("recovered_after_crash") is True for e in rec_events)
    assert len(a_rec3.open_positions()) == 1
    recovered_pos_id = int(a_rec3.open_positions()[0]["broker_position_id"])

    # Also verify protective close_position staged in outbox_pending IS flushed on adapter restart!
    a_rec3.publish_fault_mode = "skip_file_write_after_db_commit"
    a_rec3.submit_close_position(
        broker_position_id=recovered_pos_id,
        client_intent_id="ob-protective-close",
        protective_reason="protective_outbox_flush_test",
    )
    assert len((ob_bridge / "order-intents.jsonl").read_text(encoding="utf-8").splitlines()) == 1
    a_rec3.close()

    a_rec4 = CTraderCliDemoBrokerAdapter(
        state_db_path=ob_state,
        bridge_dir=ob_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    assert len((ob_bridge / "order-intents.jsonl").read_text(encoding="utf-8").splitlines()) == 2
    assert d_ob_restarted.step() == 1  # reads only newly appended ob-protective-close!
    a_rec4.poll_events()
    assert a_rec4.open_positions() == []

    # --- Part D: Bridge crash BEFORE broker call -> unknown_execution + manual_unlock_required (`demo-unlock`) ---
    d_ob_restarted.emit_reconcile_snapshot("flat_before_ambig")
    a_rec4.reconcile()
    a_rec4.submit_market_order(
        decision=dec,
        assessment=approved_2k,
        side="buy",
        volume_units=2000,
        client_intent_id="intent-ambig-1",
        stop_loss_pips=11.0,
    )
    d_ob_restarted.crash_mode = "before_broker_call"
    with pytest.raises(SimulatedBridgeCrashError):
        d_ob_restarted.step()

    d_ob_after_ambig = SimulatedCTraderCliDaemon(
        ob_bridge,
        account_number=5100201,
        account_currency="USD",
        allow_demo_orders=True,
        clock=lambda: t_now[0],
    )
    d_ob_after_ambig.start()
    assert d_ob_after_ambig.step() == 3  # 2 terminal duplicates + 1 in_flight -> unknown_execution
    ambig_rec = a_rec4.reconcile()
    assert ambig_rec.blocks_new_risk is True
    assert ambig_rec.manual_unlock_required is True
    assert "intent-ambig-1" in ambig_rec.unknown_intent_ids
    assert a_rec4.get_intent("intent-ambig-1")["status"] == "unknown_execution"  # type: ignore[index]

    # New risk is hard-blocked (never auto-retried!)
    with pytest.raises(DemoBrokerSafetyError, match="unknown_intent_outcome:intent-ambig-1"):
        a_rec4.submit_market_order(
            decision=dec,
            assessment=approved_2k,
            side="buy",
            volume_units=2000,
            client_intent_id="intent-blocked-by-unknown",
            stop_loss_pips=11.0,
        )

    # Inject an unresolved orphan -> `demo-unlock` refuses to unlock until Demo account is clean!
    d_ob_after_ambig.inject_orphan_position(broker_position_id=9988, volume_units=1000)
    d_ob_after_ambig.emit_reconcile_snapshot("orphan_blocks_unlock")
    with pytest.raises(DemoBrokerSafetyError, match="unresolved drift"):
        a_rec4.acknowledge_manual_unlock(note="Attempt premature unlock with orphan")
    a_rec4.close()

    # Clear orphan at broker, emit clean snapshot at real UTC now, and run `eventlens-shadow demo-unlock --note ...` CLI
    t_now[0] = datetime.now(UTC)
    d_ob_after_ambig._positions.clear()
    d_ob_after_ambig._persist_server_state()
    d_ob_after_ambig.emit_reconcile_snapshot("verified_flat_after_ambig")
    shadow_cli_main(
        [
            "demo-unlock",
            "--quote-bridge-dir",
            str(ob_bridge),
            "--cli-demo-state-db",
            str(ob_state),
            "--expected-demo-account-number",
            "5100201",
            "--note",
            "Verified Pepperstone Demo flat after pre-broker crash",
        ]
    )
    unlock_out = json.loads(capsys.readouterr().out)
    assert unlock_out["scope"] == "ctrader_cli_demo_unlock"
    assert unlock_out["manual_unlock_required"] is False
    assert unlock_out["halted_reason"] is None

    a_unlocked = CTraderCliDemoBrokerAdapter(
        state_db_path=ob_state,
        bridge_dir=ob_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number=5100201,
        clock=lambda: t_now[0],
    )
    try:
        assert a_unlocked.manual_unlock_required is False
        assert a_unlocked.get_intent("intent-ambig-1")["status"] == "reconciled_after_unknown"  # type: ignore[index]
        assert a_unlocked.reconcile().blocks_new_risk is False
    finally:
        a_unlocked.close()


def test_stage_f_expected_demo_account_string_int64_precision_and_csproj_pin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Cloud Build Feedback #1 & #2 regression guard:
    - EventLensCliBridge.cs uses `using File = System.IO.File;`, `ProtectionType.Absolute`,
      `public string ExpectedDemoAccountNumber` (zero `long` [Parameter]), and `long.TryParse`
      with `NumberStyles.None` (never `double`).
    - EventLensCliBridge.csproj pins `cTrader.Automate` to `[1.0.21]` with `RestorePackagesWithLockFile=true`
      and `AlgoBuild=true`, and `build_ctrader_algo.sh` verifies both `packages.lock.json` and `.algo`.
    - Demo execution requires an explicit positive 64-bit integer account number (`> 0`), rejects
      `"0"`, empty, non-digit, negative, float/double, bool, and >Int64.MaxValue overflow, allows `"0"`
      ONLY in read-only mode, and preserves full 64-bit precision beyond IEEE-754 double (`2^53 + 1`)."""
    cs_src = Path("ctrader/EventLensCliBridge/EventLensCliBridge.cs").read_text(encoding="utf-8")
    csproj_src = Path("ctrader/EventLensCliBridge/EventLensCliBridge.csproj").read_text(
        encoding="utf-8"
    )
    build_sh = Path("deploy/gcp/build_ctrader_algo.sh").read_text(encoding="utf-8")
    setup_sh = Path("deploy/gcp/setup_ubuntu.sh").read_text(encoding="utf-8")
    compose_yml = Path("deploy/gcp/docker-compose.ctrader-cli.yml").read_text(encoding="utf-8")
    env_example = Path("deploy/gcp/eventlens.env.example").read_text(encoding="utf-8")

    assert "using File = System.IO.File;" in cs_src
    assert "ProtectionType.Absolute" in cs_src
    assert "public string ExpectedDemoAccountNumber" in cs_src
    assert "public long ExpectedDemoAccountNumber" not in cs_src
    assert "long.TryParse(" in cs_src
    assert "NumberStyles.None" in cs_src
    assert "<TargetFramework>net6.0</TargetFramework>" in csproj_src
    assert "<TargetFramework>net8.0</TargetFramework>" not in csproj_src
    assert 'Version="[1.0.21]"' in csproj_src
    assert "<RestorePackagesWithLockFile>true</RestorePackagesWithLockFile>" in csproj_src
    assert "<AlgoBuild>true</AlgoBuild>" in csproj_src
    assert "<AlgoPublish>false</AlgoPublish>" in csproj_src
    assert "packages.lock.json" in build_sh
    assert "EventLensCliBridge.algo" in build_sh
    assert "net6" in build_sh
    assert "--network none" in build_sh
    assert "metadata /algo/EventLensCliBridge.algo" in build_sh
    assert re.search(r"(^|\s)HOME=", build_sh) is None
    assert "DOTNET_CLI_HOME=" in build_sh
    assert "NUGET_PACKAGES=" in build_sh
    assert "XDG_CONFIG_HOME=" in build_sh
    assert "XDG_DATA_HOME=" in build_sh
    assert "-p:AlgoPublish=false" in build_sh
    assert "1654" not in setup_sh
    assert "CTRADER_CONTAINER_UID" not in setup_sh
    assert 'chown root:root "${PWD_FILE}"' in setup_sh
    assert 'setfacl -b "${PWD_FILE}"' in setup_sh
    assert 'setfacl -m "u:' not in setup_sh
    assert re.search(r"^\s*HOME\s*:", compose_yml, re.MULTILINE) is None
    assert re.search(r"^\s*user\s*:", compose_yml, re.MULTILINE) is None
    assert "DOTNET_CLI_HOME:" in compose_yml
    assert "NUGET_PACKAGES:" in compose_yml
    assert "XDG_CONFIG_HOME:" in compose_yml
    assert "XDG_DATA_HOME:" in compose_yml
    assert "--full-access" in compose_yml
    assert "--exit-on-stop" in compose_yml
    assert "CTRADER_CONTAINER_UID_GID" not in compose_yml
    assert "1654" not in env_example
    assert "CTRADER_CONTAINER_UID_GID" not in env_example

    now = datetime(2026, 11, 11, 18, 0, 0, tzinfo=UTC)

    # 1. Read-only mode (`allow_demo_orders=False`) permits "0" (any Demo), but Demo execution (`True`) refuses "0"!
    ro_bridge = tmp_path / "ro_zero"
    d_ro = SimulatedCTraderCliDaemon(
        ro_bridge,
        account_number=5100201,
        expected_demo_account_number="0",
        allow_demo_orders=False,
        clock=lambda: now,
    )
    d_ro.start()
    assert d_ro._enabled is True
    ro_adapter = CTraderCliDemoBrokerAdapter(
        state_db_path=tmp_path / "ro_zero.sqlite",
        bridge_dir=ro_bridge,
        allow_orders=False,
        confirm_demo_execution=False,
        expected_demo_account_number="0",
        clock=lambda: now,
    )
    try:
        ro_rec = ro_adapter.reconcile()
        assert ro_rec.account_number == 5100201
        assert ro_rec.allow_demo_orders is False
        assert ro_rec.blocks_new_risk is True
    finally:
        ro_adapter.close()

    zero_exec_bridge = tmp_path / "exec_zero"
    d_zero_exec = SimulatedCTraderCliDaemon(
        zero_exec_bridge,
        account_number=5100201,
        expected_demo_account_number="0",
        allow_demo_orders=True,
        clock=lambda: now,
    )
    d_zero_exec.start()
    assert d_zero_exec._enabled is False
    ev_zero = [
        json.loads(ln)
        for ln in (zero_exec_bridge / "execution-events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert ev_zero[0]["kind"] == "safety_refusal"
    assert ev_zero[0]["reason"] == "demo_orders_require_explicit_expected_account_number"

    # 2. Invalid strings, floats (`double`), booleans, and >Int64.MaxValue overflow are strictly rejected
    for idx, bad_val in enumerate(
        ("", "   ", "abc", "-5100201", "+5100201", "5100201.0", "99999999999999999999", 5100201.0)
    ):
        b_bad = tmp_path / f"bad_acct_{idx}"
        d_bad = SimulatedCTraderCliDaemon(
            b_bad,
            account_number=5100201,
            expected_demo_account_number=bad_val,  # type: ignore[arg-type]
            allow_demo_orders=False,
            clock=lambda: now,
        )
        d_bad.start()
        assert d_bad._enabled is False
        ev_bad = json.loads(
            (b_bad / "execution-events.jsonl").read_text(encoding="utf-8").splitlines()[0]
        )
        assert ev_bad["reason"] == "invalid_expected_demo_account_number"
        with pytest.raises(DemoBrokerSafetyError):
            CTraderCliDemoBrokerAdapter(
                state_db_path=tmp_path / f"bad_ad_{idx}.sqlite",
                bridge_dir=b_bad,
                allow_orders=False,
                expected_demo_account_number=bad_val,  # type: ignore[arg-type]
                clock=lambda: now,
            )

    for bad_exec_acct in (None, 0, "0", True, 5100201.0):
        with pytest.raises(DemoBrokerSafetyError):
            CTraderCliDemoBrokerAdapter(
                state_db_path=tmp_path / "exec_missing_acct.sqlite",
                bridge_dir=ro_bridge,
                allow_orders=True,
                confirm_demo_execution=True,
                expected_demo_account_number=bad_exec_acct,  # type: ignore[arg-type]
                clock=lambda: now,
            )

    # 3. Full 64-bit integer precision (`2^53 + 1 == 9007199254740993`, which `double` corrupts to `9007199254740992`)
    large_acct = 9_007_199_254_740_993
    assert int(float(large_acct)) != large_acct  # Proves IEEE-754 double loses precision here!
    hi_bridge = tmp_path / "hi64_bridge"
    d_hi = SimulatedCTraderCliDaemon(
        hi_bridge,
        account_number=large_acct,
        expected_demo_account_number="9007199254740993",
        allow_demo_orders=True,
        clock=lambda: now,
    )
    d_hi.start()
    assert d_hi._enabled is True
    ad_hi_ok = CTraderCliDemoBrokerAdapter(
        state_db_path=tmp_path / "hi64_ok.sqlite",
        bridge_dir=hi_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number="9007199254740993",
        clock=lambda: now,
    )
    try:
        rec_hi = ad_hi_ok.reconcile()
        assert rec_hi.clean is True
        assert rec_hi.account_number == large_acct
    finally:
        ad_hi_ok.close()

    ad_hi_off_by_one = CTraderCliDemoBrokerAdapter(
        state_db_path=tmp_path / "hi64_bad.sqlite",
        bridge_dir=hi_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number="9007199254740992",
        clock=lambda: now,
    )
    try:
        rec_off = ad_hi_off_by_one.reconcile()
        assert rec_off.blocks_new_risk is True
        assert any("unexpected_demo_account_number" in r for r in rec_off.drift_reasons)
    finally:
        ad_hi_off_by_one.close()

    # 4. CLI `demo-smoke` and `start --ctrader-cli-demo-orders` reject missing / "0" / malformed account numbers
    monkeypatch.delenv("CTRADER_DEMO_ACCOUNT_NUMBER", raising=False)
    for cli_argv in (
        [
            "demo-smoke",
            "--quote-bridge-dir",
            str(hi_bridge),
            "--confirm-demo-execution",
        ],
        [
            "demo-smoke",
            "--quote-bridge-dir",
            str(hi_bridge),
            "--confirm-demo-execution",
            "--expected-demo-account-number",
            "0",
        ],
        [
            "demo-smoke",
            "--quote-bridge-dir",
            str(hi_bridge),
            "--confirm-demo-execution",
            "--expected-demo-account-number",
            "5100201.0",
        ],
        [
            "start",
            "--duration",
            "5",
            "--quote-bridge-dir",
            str(hi_bridge),
            "--ctrader-cli-demo-orders",
            "--confirm-demo-execution",
            "--call-model",
        ],
    ):
        with pytest.raises(SystemExit) as exc_info:
            shadow_cli_main(cli_argv)
        assert exc_info.value.code == 2
        capsys.readouterr()

    monkeypatch.setenv("CTRADER_DEMO_ACCOUNT_NUMBER", "not-an-int")
    with pytest.raises(SystemExit) as exc_info_env:
        shadow_cli_main(["demo-reconcile", "--quote-bridge-dir", str(hi_bridge)])
    assert exc_info_env.value.code == 2
    capsys.readouterr()

    # 5. Mismatched / zero account_number on an incoming `order_event` halts with manual_unlock_required
    #    and NEVER mutates open_positions() or marks the intent filled!
    mis_bridge = tmp_path / "mis_event_bridge"
    d_mis = SimulatedCTraderCliDaemon(
        mis_bridge,
        account_number=5100201,
        expected_demo_account_number="5100201",
        allow_demo_orders=True,
        clock=lambda: now,
    )
    d_mis.start()
    ad_mis = CTraderCliDemoBrokerAdapter(
        state_db_path=tmp_path / "mis_event.sqlite",
        bridge_dir=mis_bridge,
        allow_orders=True,
        confirm_demo_execution=True,
        expected_demo_account_number="5100201",
        clock=lambda: now,
    )
    try:
        dec_m = AgentDecision(
            decision_id="dec-mis-1",
            event_id="ev-mis-1",
            based_on_memory_version=1,
            based_on_snapshot_id="snap-mis-1",
            provider="deepseek",
            model_version="deepseek-chat",
            prompt_version=SINGLE_PASS_PROMPT_VERSION,
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(minutes=5),
            portfolio_action="SET_TARGET",
            target=TargetPortfolio(
                target_id="tgt-mis-1",
                based_on_snapshot_id="snap-mis-1",
                rationale="Test mismatched event isolation",
                positions=(
                    TargetPosition(
                        instrument="EUR/USD",
                        weight=Decimal("0.10"),
                        thesis_ids=("t1",),
                    ),
                ),
            ),
            rationale="Test mismatched event isolation",
        )
        app_m = RiskAssessment(
            assessment_id="app-mis-1",
            decision_id="dec-mis-1",
            target_id="tgt-mis-1",
            checked_snapshot_id="snap-mis-1",
            policy_version="test",
            checked_at=now,
            valid_until=now + timedelta(minutes=5),
            outcome="approve",
            reasons=("within_risk_limits",),
            approved_quantity_eur=Decimal("1000"),
        )
        ad_mis.submit_market_order(
            decision=dec_m,
            assessment=app_m,
            side="buy",
            volume_units=1000,
            client_intent_id="intent-mis-1",
            stop_loss_pips=11.0,
        )
        with (mis_bridge / "execution-events.jsonl").open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "version": 1,
                        "kind": "order_event",
                        "bridge_session_id": d_mis.bridge_session_id,
                        "event_seq": 77,
                        "emitted_at": now.isoformat(),
                        "account_mode": "demo",
                        "is_live": False,
                        "account_number": 9999999,
                        "account_currency": "USD",
                        "client_intent_id": "intent-mis-1",
                        "intent_type": "market_order",
                        "broker_position_id": 7701,
                        "side": "buy",
                        "status": "filled",
                        "requested_units": 1000,
                        "filled_units": 1000,
                        "fill_price": "1.12465",
                        "sl_confirmed": True,
                    }
                )
                + "\n"
            )
        ad_mis.poll_events()
        assert ad_mis.manual_unlock_required is True
        assert ad_mis.halted_reason is not None
        assert "unexpected_demo_account_number" in ad_mis.halted_reason
        assert ad_mis.open_positions() == []
        assert ad_mis.get_intent("intent-mis-1")["status"] == "submitted"  # type: ignore[index]
    finally:
        ad_mis.close()
