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
    Event,
    MarketSnapshot,
    PriceTrajectorySummary,
    Quote,
    ReviewContext,
)
from eventlens.deepseek import ModelResponse
from eventlens.observer import CaptureBatch, ObserverStore, SourceRecord
from eventlens.quotes import BridgeQuote, BridgeStart, BridgeStop, QuoteStore
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
    MAX_INPUT_CHARACTERS,
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
    assert SINGLE_PASS_PROMPT_VERSION == "single-pass-agent-v4.0"
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
        del payload
        if call_no == 1:
            return {
                "portfolio_action": "HOLD",
                "target_direction": "flat",
                "target_weight": "0",
                "strategy_hypothesis": "fundamental_breakout",
                "market_bias": "bearish_eur",
                "market_state_note": "Hawkish Fed speech watches 1.15580 downside breakout.",
                "watch_condition": "EUR/USD <= 1.15580",
                "rationale": "Monitor 15m low 1.15580 for downside breakout confirmation.",
            }
        return {
            "portfolio_action": "SET_TARGET",
            "target_direction": "short",
            "target_weight": "-0.15",
            "strategy_hypothesis": "fundamental_breakout",
            "market_bias": "bearish_eur",
            "market_state_note": "Downside breakout below 1.15580 confirmed.",
            "watch_condition": None,
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


def test_observer_store_skips_redundant_repeats_prunes_clutter_and_bounds_revisions(
    tmp_path: Path,
) -> None:
    t0 = datetime(2026, 11, 17, 10, 0, 0, tzinfo=UTC)
    now_box = [t0]
    db_path = tmp_path / "observer.sqlite"
    obs = ObserverStore(db_path, clock=lambda: now_box[0])
    try:
        r1 = SourceRecord(
            source_id="ev-1",
            kind="news",
            source_url="https://example.com/1",
            title="ECB holds rates steady",
            text="ECB left deposit rate unchanged as expected.",
            publisher_display_at=t0 - timedelta(seconds=5),
        )
        r2 = SourceRecord(
            source_id="ev-2",
            kind="news",
            source_url="https://example.com/2",
            title="Eurozone CPI flash prints 2.1%",
            text="Flash HICP prints 2.1% y/y.",
            publisher_display_at=t0 + timedelta(seconds=1),
        )
        r3 = SourceRecord(
            source_id="ev-3",
            kind="news",
            source_url="https://example.com/3",
            title="US payrolls beat consensus",
            text="Nonfarm payrolls rose 210k vs 170k expected.",
            publisher_display_at=t0 + timedelta(seconds=8),
        )

        def _batch(ts: datetime, records: tuple[SourceRecord, ...], tag: str) -> CaptureBatch:
            return CaptureBatch(
                provider="investinglive_fx",
                stream="forex",
                kind="news",
                request_url="https://example.com/feed",
                request_started_at=ts - timedelta(milliseconds=50),
                received_at=ts,
                elapsed_ms=25.0,
                attempts=1,
                payload=tag.encode("utf-8"),
                records=records,
            )

        res1 = obs.record(_batch(t0, (r1,), "p1"))
        assert res1["counts"]["baseline"] == 1
        now_box[0] = t0 + timedelta(seconds=2)
        res2 = obs.record(_batch(now_box[0], (r1,), "p2"))
        assert res2["counts"]["repeat"] == 1
        sight_cnt = obs.db.execute(
            "SELECT COUNT(*) FROM observer_sightings WHERE source_id='ev-1'"
        ).fetchone()[0]
        assert sight_cnt == 1

        # Crash between data commit and available_at publication -> subsequent repeat publishes it
        now_box[0] = t0 + timedelta(seconds=3)
        res_unpub = obs.record(_batch(now_box[0], (r1, r2), "p3"))
        assert res_unpub["counts"]["new"] == 1
        with obs.db:
            obs.db.execute(
                "UPDATE observer_polls SET available_at=NULL WHERE poll_id=?",
                (res_unpub["poll_id"],),
            )
        assert [x.record.source_id for x in obs.revisions(limit=10)] == ["ev-1"]
        now_box[0] = t0 + timedelta(seconds=4)
        res_pub = obs.record(_batch(now_box[0], (r1, r2), "p4"))
        assert res_pub["counts"]["repeat"] == 2
        assert [x.record.source_id for x in obs.revisions(limit=10)] == ["ev-1", "ev-2"]

        now_box[0] = t0 + timedelta(seconds=10)
        obs.record(_batch(now_box[0], (r1, r2, r3), "p5"))

        # Inject legacy redundant repeat sighting on published ev-1 + >48h & <48h failures and reopen
        with obs.db:
            obs.db.execute(
                "INSERT INTO observer_sightings VALUES (?, ?, ?, ?)",
                (res_pub["poll_id"], "ev-1", 1, "repeat"),
            )
            obs.db.execute(
                "INSERT INTO observer_failures VALUES (?, ?, ?, ?, ?)",
                (
                    "old-fail",
                    "investinglive_fx",
                    "forex",
                    (t0 - timedelta(hours=60)).isoformat(),
                    "old",
                ),
            )
            obs.db.execute(
                "INSERT INTO observer_failures VALUES (?, ?, ?, ?, ?)",
                (
                    "new-fail",
                    "investinglive_fx",
                    "forex",
                    (t0 + timedelta(hours=3)).isoformat(),
                    "recent",
                ),
            )
    finally:
        obs.close()

    reopened = ObserverStore(db_path, clock=lambda: t0 + timedelta(hours=5))
    try:
        # Both legacy redundant repeat sightings pruned cleanly; unpublished/published preserved
        repeat_rows = reopened.db.execute(
            "SELECT source_id FROM observer_sightings WHERE state='repeat'"
        ).fetchall()
        assert [r[0] for r in repeat_rows] == ["ev-2"]
        fail_ids = [
            r[0]
            for r in reopened.db.execute(
                "SELECT failure_id FROM observer_failures ORDER BY recorded_at"
            ).fetchall()
        ]
        assert fail_ids == ["new-fail"]

        bounded_all = reopened.revisions(limit=2)
        assert [x.record.source_id for x in bounded_all] == ["ev-2", "ev-3"]
        bounded_as_of = reopened.revisions(as_of=t0 + timedelta(seconds=5), limit=1)
        assert [x.record.source_id for x in bounded_as_of] == ["ev-2"]
        # Deterministic ascending order with full slice
        all_revs = reopened.revisions(limit=10)
        assert [x.record.source_id for x in all_revs] == ["ev-1", "ev-2", "ev-3"]
    finally:
        reopened.close()
