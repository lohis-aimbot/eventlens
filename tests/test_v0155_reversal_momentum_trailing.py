"""Tests for v0.15.5: atomic thesis reversal, causal price trajectory, revision marginal delta,
central bank wire deduplication, and position-scaled pip SL/TP + trailing profit lock.
"""

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from eventlens.contracts import (
    AgentContext,
    Event,
    MarketSnapshot,
    PortfolioSnapshot,
    Quote,
    TargetPortfolio,
    TargetPosition,
)
from eventlens.deepseek import ModelResponse
from eventlens.observer import CaptureBatch, ObserverStore, SourceRecord
from eventlens.quotes import (
    BridgeQuote,
    BridgeStart,
    BridgeStop,
    QuoteStore,
    build_market_snapshot,
)
from eventlens.runtime import IntradayEventRuntime, ShadowRunner, SQLiteRuntimeStore
from eventlens.shadow_cli import _build_policies
from eventlens.sources.english_tier1 import (
    DEFAULT_TIER1_STREAMS,
    INVESTINGLIVE_CENTRALBANK_RSS_URL,
    EURUSDEventFilter,
    format_revision_marginal_delta,
)
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import SinglePassTradingAgent, resolve_verbatim_quote
from eventlens.trading.execution import ProtectiveExitPolicy, SQLiteShadowLedger
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy

T0 = datetime(2026, 10, 28, 18, 0, 0, tzinfo=UTC)
SESSION_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class _StubModelClient:
    provider = "deepseek"
    model = "deepseek-flash"

    def __init__(self, *payloads: dict) -> None:
        self._payloads = list(payloads)
        self.calls = 0
        self.contexts: list[dict] = []

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        idx = min(self.calls, len(self._payloads) - 1)
        self.calls += 1
        ctx = json.loads(user)
        self.contexts.append(ctx)
        raw = json.dumps(self._payloads[idx], ensure_ascii=False)
        return ModelResponse(
            content=raw,
            input_tokens=300,
            output_tokens=120,
        )


def _razor_quote(
    price: str,
    ts: datetime,
    *,
    spread_pips: str = "0.0",
) -> Quote:
    mid = Decimal(price)
    half = (Decimal(spread_pips) * Decimal("0.0001")) / Decimal("2")
    return Quote(
        instrument="EUR/USD",
        received_at=ts,
        source_at=ts - timedelta(milliseconds=80),
        bridge_at=ts - timedelta(milliseconds=40),
        available_at=ts,
        bid=mid - half,
        ask=mid + half,
        source_provider="ctrader_algo",
        basis="live_poll",
        commission=Decimal("3"),
        commission_type="UsdPerMillionUsdVolume",
    )


def _razor_market(price: str, ts: datetime, *, spread_pips: str = "0.0") -> MarketSnapshot:
    return MarketSnapshot(
        snapshot_id=f"mkt:{ts.isoformat()}",
        as_of=ts,
        quotes=(_razor_quote(price, ts, spread_pips=spread_pips),),
    )


def test_atomic_thesis_reversal_invalidates_prior_and_creates_new_target(tmp_path: Path) -> None:
    """SinglePassTradingAgent can atomically invalidate contradicted prior active theses AND create a new active reversal thesis backing SET_TARGET."""
    now = T0
    mem = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=lambda: now, allow_targets=True)
    shd = SQLiteShadowLedger(
        tmp_path / "shd.sqlite",
        initial_cash_usd=Decimal("200.00"),
        risk_policy=RiskPolicy.pepperstone_razor_200(),
        exit_policy=ProtectiveExitPolicy.pepperstone_razor_200(),
        clock=lambda: now,
    )
    ev1 = Event(
        event_id="fed:news:statement:r1",
        source="fed_news",
        source_type="news",
        source_reference="https://www.federalreserve.gov/newsevents/pressreleases/monetary20261028a.htm",
        source_timestamp=T0,
        received_timestamp=T0 + timedelta(seconds=5),
        raw_text="The Committee decided to maintain the target range for the federal funds rate at 3-1/2 to 3-3/4 percent.",
    )
    ev2 = Event(
        event_id="investinglive:news:warsh-qa:r2",
        source="investinglive_news",
        source_type="news",
        source_reference="https://investinglive.com/centralbank/warsh-press-conference-live/",
        source_timestamp=T0 + timedelta(minutes=39),
        received_timestamp=T0 + timedelta(minutes=39, seconds=5),
        raw_text="Fed Chair Warsh Q&A: Further rate hikes are not our base case and inflation progress continues.",
    )

    class _DynamicReversalClient(_StubModelClient):
        async def complete_json(self, *, system: str, user: str) -> ModelResponse:
            ctx = json.loads(user)
            if self.calls == 1:
                prior_id = ctx["prior_memory_theses_do_not_quote"][0]["thesis_id"]
                self._payloads[1] = {
                    **self._payloads[1],
                    "memory_action": "update",
                    "thesis_id": prior_id,
                    "status": "invalidated",
                    "evidence_role": "contradict",
                    "invalidate_thesis_ids": [prior_id],
                }
            return await super().complete_json(system=system, user=user)

    client = _DynamicReversalClient(
        {
            "memory_action": "create",
            "thesis_id": None,
            "invalidate_thesis_ids": [],
            "status": "active",
            "narrative": "FOMC rate hold supports near-term USD firmness against EUR.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["Dovish press conference Q&A reversal above 1.1150"],
            "confidence": 0.72,
            "evidence_quote": "maintain the target range for the federal funds rate at 3-1/2 to 3-3/4 percent.",
            "evidence_role": "support",
            "portfolio_action": "SET_TARGET",
            "target_direction": "short",
            "target_weight": "-0.10",
            "target_thesis_ids": ["__current__"],
            "rationale": "Hawkish hold favors tactical short EUR/USD.",
        },
        {
            "memory_action": "create",
            "thesis_id": None,
            "invalidate_thesis_ids": ["thesis_1"],
            "status": "active",
            "narrative": "Warsh Q&A rules out hikes as base case, triggering USD long squeeze and EUR/USD upside reversal.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["EUR/USD falls back below 1.1140"],
            "confidence": 0.80,
            "evidence_quote": "Further rate hikes are not our base case and inflation progress continues.",
            "evidence_role": "support",
            "portfolio_action": "SET_TARGET",
            "target_direction": "long",
            "target_weight": "0.20",
            "target_thesis_ids": ["__current__"],
            "rationale": "Dovish Q&A pivot invalidates prior short thesis and justifies reversing long.",
        },
    )
    agent = SinglePassTradingAgent(
        client,
        clock=lambda: now,
        allow_commission_bearing_zero_spread=True,
        max_position_weight=Decimal("0.25"),
    )
    runtime = IntradayEventRuntime(
        memory=mem,
        portfolio_state=shd,
        market_provider=lambda ts: _razor_market(
            "1.11420" if ts < T0 + timedelta(minutes=20) else "1.11510", ts
        ),
        agent=agent,
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(
            policy=RiskPolicy.pepperstone_razor_200(),
            clock=lambda: now,
        ),
        shadow_executor=shd,
        clock=lambda: now,
    )

    try:
        now = T0 + timedelta(seconds=6)
        res1 = asyncio.run(runtime.run_cycle(ev1))
        assert res1.status == "shadow_recorded"
        snap1 = asyncio.run(mem.snapshot(as_of=now))
        assert len(snap1.theses) == 1
        first_id = snap1.theses[0].thesis_id
        assert snap1.theses[0].status == "active"

        # Fill first short leg
        now = T0 + timedelta(seconds=7)
        assert len(shd.fill_pending(_razor_market("1.11420", now))) == 1

        # Advance to Warsh Q&A at +39m06s and execute atomic reversal
        now = T0 + timedelta(minutes=39, seconds=6)
        res2 = asyncio.run(runtime.run_cycle(ev2))
        assert res2.status == "shadow_recorded"
        assert res2.decision is not None
        assert res2.decision.target is not None
        assert res2.decision.target.positions[0].weight == Decimal("0.20")
        second_id = res2.decision.target.positions[0].thesis_ids[0]
        assert second_id != first_id

        # Check memory snapshot: first_id is invalidated (with ev2 in contradicting_event_ids) and second_id is active!
        snap2 = asyncio.run(mem.snapshot(as_of=now))
        by_id = {t.thesis_id: t for t in snap2.theses}
        assert by_id[first_id].status == "invalidated"
        assert by_id[first_id].revision == 2
        assert ev2.event_id in by_id[first_id].contradicting_event_ids
        assert by_id[second_id].status == "active"
        assert by_id[second_id].revision == 1
        assert ev2.event_id in by_id[second_id].supporting_event_ids

        # Context JSON exposed max_abs_target_weight = 0.25
        assert client.contexts[-1]["risk_constraints"]["max_abs_target_weight"] == "0.25"
    finally:
        shd.close()


def test_atomic_thesis_reversal_rejects_unknown_or_inactive_invalidation_id() -> None:
    """invalidate_thesis_ids rejects unknown or already invalidated thesis IDs cleanly."""
    now = T0 + timedelta(seconds=5)
    ev = Event(
        event_id="fed:news:1:r1",
        source="fed_news",
        source_type="news",
        source_reference="https://www.federalreserve.gov/1",
        source_timestamp=T0,
        received_timestamp=now - timedelta(seconds=1),
        raw_text="Warsh signals dovish shift on EUR/USD outlook.",
    )
    client = _StubModelClient(
        {
            "memory_action": "create",
            "thesis_id": None,
            "invalidate_thesis_ids": ["thesis_999"],
            "status": "active",
            "narrative": "Bullish EUR/USD reversal.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["Below 1.1100"],
            "confidence": 0.7,
            "evidence_quote": "Warsh signals dovish shift on EUR/USD outlook.",
            "evidence_role": "support",
            "portfolio_action": "SET_TARGET",
            "target_direction": "long",
            "target_weight": "0.10",
            "target_thesis_ids": ["__current__"],
            "rationale": "Reversal.",
        }
    )
    agent = SinglePassTradingAgent(
        client,
        clock=lambda: now,
        allow_commission_bearing_zero_spread=True,
    )
    from eventlens.contracts import MemorySnapshot

    valid_ctx = AgentContext(
        event=ev,
        memory=MemorySnapshot(memory_version=0, as_of=now),
        portfolio=PortfolioSnapshot(
            snapshot_id="p0",
            as_of=now,
            base_currency="USD",
            equity=Decimal("200"),
        ),
        market=_razor_market("1.11500", now),
        as_of=now,
    )
    with pytest.raises(ValueError, match="unknown thesis ID"):
        asyncio.run(agent.decide(valid_ctx))


def test_causal_price_trajectory_summary_and_strict_anti_lookahead(tmp_path: Path) -> None:
    """build_market_snapshot computes exact 1m/5m/15m/30m pip changes and high/low ranges strictly <= as_of."""
    start_ts = T0
    as_of = T0 + timedelta(minutes=30)
    q_now = start_ts - timedelta(minutes=5)
    qstore = QuoteStore(tmp_path / "quotes.sqlite", clock=lambda: q_now)
    try:
        qstore.start(
            BridgeStart(
                version=1,
                kind="start",
                session_id=SESSION_ID,
                account_mode="demo",
                running_mode="realtime",
                symbol="EURUSD",
                started_at=start_ts - timedelta(minutes=5),
                sample_interval_ms=1000,
                duration_seconds=3600,
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
            start_ts - timedelta(minutes=5),
            basis="live_poll",
        )
        samples = [
            (start_ts, "1.11300"),  # -30m
            (start_ts + timedelta(minutes=10), "1.11250"),  # -20m (30m low)
            (start_ts + timedelta(minutes=15), "1.11320"),  # -15m
            (start_ts + timedelta(minutes=20), "1.11310"),  # -10m (15m low)
            (start_ts + timedelta(minutes=25), "1.11408"),  # -5m
            (start_ts + timedelta(minutes=28), "1.11530"),  # -2m (15m & 30m high)
            (start_ts + timedelta(minutes=29), "1.11488"),  # -1m
            (as_of, "1.11508"),  # as_of (0m)
            (as_of + timedelta(minutes=1), "1.12500"),  # FUTURE quote (+1m) — MUST BE IGNORED
        ]
        for seq, (ts, px) in enumerate(samples, start=1):
            q_now = ts
            qstore.record(
                BridgeQuote(
                    version=1,
                    kind="quote",
                    session_id=SESSION_ID,
                    sequence=seq,
                    symbol="EURUSD",
                    source_at=ts - timedelta(milliseconds=50),
                    bridge_at=ts - timedelta(milliseconds=20),
                    bid=Decimal(px),
                    ask=Decimal(px),
                ),
                ts,
                '{"kind":"quote"}',
            )
        q_now = as_of + timedelta(minutes=2)
        qstore.stop(
            BridgeStop(
                version=1,
                kind="stop",
                session_id=SESSION_ID,
                stopped_at=as_of + timedelta(minutes=2),
                quotes=len(samples),
            ),
            as_of + timedelta(minutes=2),
        )

        snap = build_market_snapshot(qstore, as_of, basis="live_poll")
        traj = snap.price_trajectory
        assert traj is not None
        assert traj.as_of == as_of
        assert traj.current_mid == Decimal("1.11508")
        assert traj.change_1m_pips == Decimal("2.0")  # 1.11508 - 1.11488
        assert traj.change_5m_pips == Decimal("10.0")  # 1.11508 - 1.11408
        assert traj.change_15m_pips == Decimal("18.8")  # 1.11508 - 1.11320
        assert traj.change_30m_pips == Decimal("20.8")  # 1.11508 - 1.11300
        assert traj.window_15m_high_mid == Decimal("1.11530")  # Future 1.12500 excluded!
        assert traj.window_15m_low_mid == Decimal("1.11310")
        assert traj.window_30m_high_mid == Decimal("1.11530")
        assert traj.window_30m_low_mid == Decimal("1.11250")
        assert traj.sample_count_30m == 8
    finally:
        qstore.close()


def test_revision_marginal_delta_in_shadow_runner_and_centralbank_dedup(tmp_path: Path) -> None:
    """ShadowRunner prepends verbatim [REVISION r2 MARGINAL UPDATE] lines on updated liveblogs, and news/centralbank deduplicates cleanly."""
    streams = {(s.provider, s.stream): s.url for s in DEFAULT_TIER1_STREAMS}
    assert ("investinglive", "news/centralbank") in streams
    assert streams[("investinglive", "news/centralbank")] == INVESTINGLIVE_CENTRALBANK_RSS_URL

    now = T0
    ostore = ObserverStore(tmp_path / "obs.sqlite", clock=lambda: now)
    mem = SQLiteThesisMemory(tmp_path / "mem.sqlite", clock=lambda: now, allow_targets=True)
    shd = SQLiteShadowLedger(
        tmp_path / "shd.sqlite",
        initial_cash_usd=Decimal("200.00"),
        risk_policy=RiskPolicy.pepperstone_razor_200(),
        exit_policy=ProtectiveExitPolicy.pepperstone_razor_200(),
        clock=lambda: now,
    )
    state_store = SQLiteRuntimeStore(tmp_path / "shd.sqlite")

    try:
        # Baseline poll so subsequent live arrivals are non-baseline
        ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/rss",
                basis="live_poll",
                kind="news",
                request_url="https://investinglive.com/feed",
                request_started_at=now - timedelta(seconds=20),
                received_at=now - timedelta(seconds=19),
                elapsed_ms=10.0,
                attempts=1,
                payload=b"<rss/>",
                records=(
                    SourceRecord(
                        source_id="il:baseline-1",
                        kind="news",
                        source_url="https://investinglive.com/news/base/",
                        title="Baseline item",
                        text="Earlier market wrap.",
                        publisher_display_at=now - timedelta(minutes=10),
                        importance=2,
                    ),
                ),
            )
        )

        # r1: Opening remarks at T0 + 34m on news/rss
        now = T0 + timedelta(minutes=34)
        rec_r1 = SourceRecord(
            source_id="il:warsh-live-qa",
            kind="news",
            source_url="https://investinglive.com/centralbank/warsh-press-conference/",
            title="Fed Chair Warsh press conference live: Inflation remains somewhat elevated",
            text=(
                "Opening statement highlights:\n"
                "- Labor market conditions have stabilized.\n"
                "- Policy is well positioned to respond to risks."
            ),
            publisher_display_at=now,
            importance=3,
        )
        ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/rss",
                basis="live_poll",
                kind="news",
                request_url="https://investinglive.com/feed",
                request_started_at=now,
                received_at=now,
                elapsed_ms=10.0,
                attempts=1,
                payload=b"<rss>r1</rss>",
                records=(rec_r1,),
            )
        )
        # Same r1 arriving on news/centralbank stream must deduplicate as repeat (no duplicate revision)
        now = now + timedelta(seconds=2)
        dedup_res = ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/centralbank",
                basis="live_poll",
                kind="news",
                request_url=INVESTINGLIVE_CENTRALBANK_RSS_URL,
                request_started_at=now,
                received_at=now,
                elapsed_ms=10.0,
                attempts=1,
                payload=b"<rss>r1-cb</rss>",
                records=(rec_r1,),
            )
        )
        assert dedup_res["counts"]["repeat"] == 1
        assert dedup_res["counts"]["new"] == 0
        assert dedup_res["counts"]["revised"] == 0

        # r2: Q&A appended at T0 + 39m on news/centralbank
        now = T0 + timedelta(minutes=39)
        rec_r2 = SourceRecord(
            source_id="il:warsh-live-qa",
            kind="news",
            source_url="https://investinglive.com/centralbank/warsh-press-conference/",
            title="Fed Chair Warsh press conference live: Rate hikes are not our base case",
            text=(
                "Opening statement highlights:\n"
                "- Labor market conditions have stabilized.\n"
                "- Policy is well positioned to respond to risks.\n"
                "Q&A session updates:\n"
                "- Warsh says further rate hikes are not our base case for EUR/USD rates divergence.\n"
                "- Balance sheet runoff remains on autopilot without pressuring reserves."
            ),
            publisher_display_at=now,
            importance=3,
        )
        upd_res = ostore.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/centralbank",
                basis="live_poll",
                kind="news",
                request_url=INVESTINGLIVE_CENTRALBANK_RSS_URL,
                request_started_at=now,
                received_at=now,
                elapsed_ms=10.0,
                attempts=1,
                payload=b"<rss>r2-cb</rss>",
                records=(rec_r2,),
            )
        )
        assert upd_res["counts"]["revised"] == 1

        hist = ostore.history("news", "il:warsh-live-qa", provider="investinglive", as_of=now)
        assert len(hist) == 2
        delta_text = format_revision_marginal_delta(hist[1], hist[0])
        assert delta_text is not None
        assert "[REVISION r2 MARGINAL UPDATE" in delta_text
        assert "Warsh says further rate hikes are not our base case" in delta_text
        assert "Labor market conditions have stabilized." not in delta_text

        client = _StubModelClient(
            {
                "memory_action": "none",
                "thesis_id": None,
                "invalidate_thesis_ids": [],
                "status": None,
                "narrative": None,
                "instruments": ["EUR/USD"],
                "invalidation_conditions": [],
                "confidence": None,
                "evidence_quote": "Policy is well positioned to respond to risks.",
                "evidence_role": "none",
                "portfolio_action": "HOLD",
                "target_direction": None,
                "target_weight": None,
                "target_thesis_ids": [],
                "rationale": "Wait for Q&A.",
            },
            {
                "memory_action": "create",
                "thesis_id": None,
                "invalidate_thesis_ids": [],
                "status": "active",
                "narrative": "Warsh Q&A rules out hikes, lifting EUR/USD.",
                "instruments": ["EUR/USD"],
                "invalidation_conditions": ["EUR/USD below 1.1135"],
                "confidence": 0.78,
                "evidence_quote": "Warsh says further rate hikes are not our base case for EUR/USD rates divergence.",
                "evidence_role": "support",
                "portfolio_action": "SET_TARGET",
                "target_direction": "long",
                "target_weight": "0.15",
                "target_thesis_ids": ["__current__"],
                "rationale": "Marginal Q&A delta triggers long EUR/USD.",
            },
        )
        agent = SinglePassTradingAgent(
            client,
            clock=lambda: now,
            allow_commission_bearing_zero_spread=True,
        )
        runtime = IntradayEventRuntime(
            memory=mem,
            portfolio_state=shd,
            market_provider=lambda ts: _razor_market("1.11510", ts),
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(
                policy=RiskPolicy.pepperstone_razor_200(),
                clock=lambda: now,
            ),
            shadow_executor=shd,
            state_store=state_store,
            event_filter=EURUSDEventFilter(clock=lambda: now),
            clock=lambda: now,
        )
        runner = ShadowRunner(
            runtime=runtime,
            ledger=shd,
            observer_store=ostore,
            basis="live_poll",
            bundle_simultaneous=False,
            strict_window_coverage=False,
            clock=lambda: now,
        )
        runner._run_started_at = T0

        cycles = asyncio.run(runner.step_events())
        assert len(cycles) == 2
        r2_verbatim = client.contexts[1]["current_event_verbatim_text"]
        assert r2_verbatim.startswith("[REVISION r2 MARGINAL UPDATE")
        assert "[FULL UPDATED ARTICLE TEXT]" in r2_verbatim
        assert resolve_verbatim_quote(
            r2_verbatim,
            "Warsh says further rate hikes are not our base case for EUR/USD rates divergence.",
        )
        with pytest.raises(ValueError, match="not adapter metadata tags"):
            resolve_verbatim_quote(
                "[INVESTINGLIVE] [Categories: Central Banks]\nSubstantive body",
                "[INVESTINGLIVE] [Categories: Central Banks]",
            )
    finally:
        shd.close()
        state_store.close()
        ostore.close()


def test_position_scaled_pip_sl_tp_and_durable_trailing_profit_lock(tmp_path: Path) -> None:
    """ProtectiveExitPolicy scales SL/TP by pips across lot sizes and durably persists peak_favorable_mark for trailing profit locks."""
    now = T0
    risk = RiskPolicy.pepperstone_razor_200()
    exit_pol = ProtectiveExitPolicy.pepperstone_razor_200()
    shd_path = tmp_path / "trailing.sqlite"

    ledger = SQLiteShadowLedger(
        shd_path,
        initial_cash_usd=Decimal("200.00"),
        risk_policy=risk,
        exit_policy=exit_pol,
        clock=lambda: now,
    )
    risk_engine = DeterministicHardRiskEngine(policy=risk, clock=lambda: now)

    def _open_position(weight: str, price: str) -> None:
        nonlocal now
        snap = ledger.snapshot_sync(market=_razor_market(price, now))
        from eventlens.contracts import AgentDecision, Thesis, ThesisChange

        backing_thesis = Thesis(
            thesis_id="t1",
            revision=1,
            status="active",
            narrative="Active macro thesis",
            instruments=("EUR/USD",),
            supporting_event_ids=("ev-test",),
            invalidation_conditions=("Invalidated on reversal",),
            confidence=0.75,
            created_at=now - timedelta(seconds=1),
            updated_at=now,
            review_at=now + timedelta(hours=1),
        )
        dec = AgentDecision(
            decision_id=f"dec-{now.timestamp()}",
            event_id="ev-test",
            based_on_memory_version=0,
            based_on_snapshot_id=snap.snapshot_id,
            provider="deepseek",
            model_version="deepseek-flash",
            prompt_version="single-pass-agent-v3",
            started_at=now - timedelta(seconds=1),
            completed_at=now,
            valid_until=now + timedelta(seconds=60),
            thesis_changes=(
                ThesisChange(
                    expected_revision=0,
                    thesis=backing_thesis,
                    reason="Create backing thesis",
                ),
            ),
            portfolio_action="SET_TARGET",
            target=TargetPortfolio(
                target_id=f"tgt-{now.timestamp()}",
                based_on_snapshot_id=snap.snapshot_id,
                positions=(
                    TargetPosition(
                        instrument="EUR/USD",
                        weight=Decimal(weight),
                        thesis_ids=("t1",),
                    ),
                ),
                rationale="Test position",
            ),
            rationale="Test position",
        )
        assess = asyncio.run(risk_engine.assess(dec, snap, _razor_market(price, now)))
        assert assess.outcome == "approve"
        ledger.record_sync(dec, assess)
        now = now + timedelta(seconds=1)
        fills = ledger.fill_pending(_razor_market(price, now))
        assert len(fills) == 1

    try:
        # 1. Open max-conviction +0.08 lot (+8,000 EUR, weight=0.25) at 1.11510 (fill ~1.115128)
        _open_position("0.25", "1.11510")
        assert ledger.report()["quantity_eur"] == "8000"

        # A -5.5 pip wiggle to 1.11458 (-$4.38 unrealized) does NOT whipsaw out (old $4.00 SL would have exited!)
        now = now + timedelta(seconds=30)
        assert ledger.evaluate_protective_exits(_razor_market("1.11458", now)) == ()

        # Rally +12.7 pips above entry to 1.11640 (>= 9.0 activation_pips) -> updates peak_favorable_mark without exiting
        now = now + timedelta(seconds=60)
        assert ledger.evaluate_protective_exits(_razor_market("1.11640", now)) == ()
        assert ledger.report()["peak_favorable_mark"] == "1.11640"

        # Simulate process restart: close & reopen SQLiteShadowLedger and verify peak_favorable_mark survived
        ledger.close()
        ledger = SQLiteShadowLedger(
            shd_path,
            initial_cash_usd=Decimal("200.00"),
            risk_policy=risk,
            exit_policy=exit_pol,
            clock=lambda: now,
        )
        assert ledger.report()["peak_favorable_mark"] == "1.11640"

        # Minor 3.0-pip pullback to 1.11610 (< 5.0 trailing_distance_pips) keeps position open to run
        now = now + timedelta(seconds=20)
        assert ledger.evaluate_protective_exits(_razor_market("1.11610", now)) == ()

        # 5.2-pip pullback from 1.11640 peak to 1.11588 (>= 5.0 trailing_distance_pips) fires protective_trailing_stop!
        now = now + timedelta(seconds=20)
        trail_fills = ledger.evaluate_protective_exits(_razor_market("1.11588", now))
        assert len(trail_fills) == 1
        assert trail_fills[0].reason == "protective_trailing_stop"
        assert trail_fills[0].new_quantity == Decimal("0")
        assert trail_fills[0].realized_pnl_usd > Decimal("5.50")
        assert ledger.report()["peak_favorable_mark"] is None

        # 2. Low-conviction -0.02 lot (-2,000 EUR, weight=-0.05) stops out at 11.0 pips (~-$2.20) instead of bleeding -$4.00 (-20 pips)
        now = now + timedelta(seconds=30)
        _open_position("-0.05", "1.11600")
        assert ledger.report()["quantity_eur"] == "-2000"
        now = now + timedelta(seconds=30)
        sl_fills = ledger.evaluate_protective_exits(_razor_market("1.11710", now))
        assert len(sl_fills) == 1
        assert sl_fills[0].reason == "protective_stop_loss"
        assert Decimal("-2.40") <= sl_fills[0].realized_pnl_usd <= Decimal("-2.15")
    finally:
        ledger.close()


def test_cli_pip_and_trailing_policy_overrides() -> None:
    """shadow_cli._build_policies wires pip SL/TP and trailing flags cleanly and enforces trailing distance < activation."""
    ns = argparse.Namespace(
        account_profile="pepperstone_razor_200",
        stop_loss_pips="12.0",
        take_profit_pips="24.0",
        trailing_activation_pips="10.0",
        trailing_distance_pips="5.0",
    )
    _, _, exit_pol, _ = _build_policies(ns)
    assert exit_pol.stop_loss_pips == Decimal("12.0")
    assert exit_pol.take_profit_pips == Decimal("24.0")
    assert exit_pol.trailing_activation_pips == Decimal("10.0")
    assert exit_pol.trailing_distance_pips == Decimal("5.0")

    ns_disable = argparse.Namespace(
        account_profile="pepperstone_razor_200",
        trailing_activation_pips="0",
    )
    _, _, exit_disabled, _ = _build_policies(ns_disable)
    assert exit_disabled.trailing_activation_pips is None
    assert exit_disabled.trailing_distance_pips is None

    ns_invalid = argparse.Namespace(
        account_profile="pepperstone_razor_200",
        trailing_activation_pips="6.0",
        trailing_distance_pips="6.0",
    )
    with pytest.raises(ValueError, match="trailing_distance_pips"):
        _build_policies(ns_invalid)
