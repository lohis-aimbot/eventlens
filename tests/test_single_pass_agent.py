"""Single-pass agent, verbatim evidence recovery, quote alignment and memory commit."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from eventlens.contracts import (
    AgentContext,
    Event,
    MarketSnapshot,
    MemorySnapshot,
    PortfolioSnapshot,
    Position,
    Quote,
)
from eventlens.deepseek import ModelResponse
from eventlens.quotes import BridgeQuote, BridgeStart, BridgeStop, QuoteStore, build_market_snapshot
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import (
    MARKET_STATE_WATCH_THESIS_ID,
    ObservationReasoner,
    SinglePassTradingAgent,
)

T = datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)
SESSION = UUID("11111111-2222-3333-4444-555555555555")


def run(coro):
    return asyncio.run(coro)


def sample_event(
    event_id: str = "wallstreetcn:news:104:r1",
    raw_text: str = "The Committee decided, in support of its goals,\nto maintain the target range for the federal funds rate.",
) -> Event:
    return Event(
        event_id=event_id,
        source="wallstreetcn_news",
        source_type="news",
        source_reference="https://wallstreetcn.com/livenews/104",
        source_timestamp=T - timedelta(seconds=2),
        received_timestamp=T - timedelta(seconds=1),
        raw_text=raw_text,
    )


def sample_market(received_at: datetime = T - timedelta(milliseconds=400)) -> MarketSnapshot:
    return MarketSnapshot(
        snapshot_id="m1",
        as_of=max(T, received_at),
        quotes=(
            Quote(
                instrument="EUR/USD",
                received_at=received_at,
                bid=Decimal("1.08500"),
                ask=Decimal("1.08502"),
            ),
        ),
    )


def sample_portfolio(quantity: Decimal = Decimal("0")) -> PortfolioSnapshot:
    positions = (Position(instrument="EUR/USD", quantity=quantity),) if quantity != 0 else ()
    return PortfolioSnapshot(
        snapshot_id="p1",
        as_of=T,
        base_currency="USD",
        equity=Decimal("10000"),
        positions=positions,
    )


class FakeSinglePassClient:
    provider = "deepseek"
    model = "deepseek-flash"

    def __init__(self, *responses: dict | str) -> None:
        self.responses = [
            item if isinstance(item, str) else json.dumps(item, ensure_ascii=False)
            for item in responses
        ]
        self.calls = 0
        self.users: list[dict] = []

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls += 1
        payload = json.loads(user)
        assert "current_event_verbatim_text" in payload
        assert "current_market_state_card" in payload
        self.users.append(payload)
        return ModelResponse(
            content=self.responses.pop(0),
            input_tokens=80,
            output_tokens=45,
            model_id="deepseek-flash",
            system_fingerprint="fp-single",
        )


def test_single_pass_create_and_directional_target_links_current_thesis_and_commits(tmp_path):
    clock_now = T + timedelta(milliseconds=500)
    memory = SQLiteThesisMemory(
        tmp_path / "memory.sqlite", clock=lambda: clock_now, allow_targets=True
    )
    client = FakeSinglePassClient(
        {
            "portfolio_action": "SET_TARGET",
            "target_direction": "short",
            "target_weight": "-0.15",
            "strategy_hypothesis": "fundamental_breakout",
            "market_bias": "bearish_eur",
            "market_state_note": "Fed holds rates steady while inflation remains elevated.",
            "watch_condition": None,
            "rationale": "Rate hold supports USD yield advantage against EUR.",
        }
    )
    agent = SinglePassTradingAgent(client, clock=lambda: clock_now)
    reasoner = ObservationReasoner(agent, memory)
    ev = sample_event()
    first = run(reasoner.process(ev, market=sample_market(), portfolio=sample_portfolio(), as_of=T))
    assert first.memory_version == 1
    assert first.reused is False
    assert first.decision.model_version == "deepseek-flash@fp-single"
    assert len(first.decision.thesis_changes) == 1
    assert first.decision.thesis_changes[0].thesis.thesis_id == MARKET_STATE_WATCH_THESIS_ID
    assert first.decision.target is not None
    assert first.decision.target.positions[0].weight == Decimal("-0.15")

    # Re-processing the same event reuses the committed decision with zero new model calls.
    repeated = run(
        reasoner.process(ev, market=sample_market(), portfolio=sample_portfolio(), as_of=T)
    )
    assert repeated.reused is True
    assert repeated.decision.decision_id == first.decision.decision_id
    assert client.calls == 1


def test_single_pass_flat_abstention_never_liquidates_and_explicit_close_exits(tmp_path):
    clock_now = T + timedelta(milliseconds=500)
    memory = SQLiteThesisMemory(
        tmp_path / "memory.sqlite", clock=lambda: clock_now, allow_targets=True
    )
    client = FakeSinglePassClient(
        {
            "portfolio_action": "IGNORE_EVENT",
            "target_direction": "flat",
            "target_weight": "0",
            "strategy_hypothesis": "unspecified",
            "market_bias": "neutral",
            "market_state_note": None,
            "watch_condition": None,
            "rationale": "Euro-area stance is unstated in this release; ignore.",
        },
        {
            "portfolio_action": "HOLD",
            "target_direction": "flat",
            "target_weight": "0",
            "strategy_hypothesis": "unspecified",
            "market_bias": "neutral",
            "market_state_note": None,
            "watch_condition": None,
            "rationale": "Low confidence headline; hold existing position unchanged.",
        },
        {
            "portfolio_action": "CLOSE",
            "target_direction": "flat",
            "target_weight": "0",
            "strategy_hypothesis": "unspecified",
            "market_bias": "bearish_eur",
            "market_state_note": "Prior USD strength thesis invalidated.",
            "watch_condition": None,
            "rationale": "Explicitly close existing position as thesis is invalidated.",
        },
    )
    agent = SinglePassTradingAgent(client, clock=lambda: clock_now)
    reasoner = ObservationReasoner(agent, memory)

    # 1. Held position + IGNORE_EVENT -> target is None (does NOT liquidate open position!).
    abstain = run(
        reasoner.process(
            sample_event("e1"),
            market=sample_market(),
            portfolio=sample_portfolio(quantity=Decimal("1000")),
            as_of=T,
        )
    )
    assert abstain.decision.portfolio_action == "IGNORE_EVENT"
    assert abstain.decision.target is None
    assert abstain.decision.thesis_changes == ()

    # 2. Held position + HOLD -> target is None (keeps open position unchanged!).
    clock_now = T + timedelta(seconds=1)
    hold_run = run(
        reasoner.process(
            sample_event("e2"),
            market=sample_market(received_at=clock_now - timedelta(milliseconds=400)),
            portfolio=sample_portfolio(quantity=Decimal("1000")),
            as_of=clock_now,
        )
    )
    assert hold_run.decision.portfolio_action == "HOLD"
    assert hold_run.decision.target is None

    # 3. Held position + explicit CLOSE citing invalidated thesis -> produces zero-weight exit target.
    clock_now = T + timedelta(seconds=2)
    exit_run = run(
        reasoner.process(
            sample_event("e3"),
            market=sample_market(received_at=clock_now - timedelta(milliseconds=400)),
            portfolio=sample_portfolio(quantity=Decimal("1000")),
            as_of=clock_now,
        )
    )
    assert exit_run.decision.portfolio_action == "CLOSE"
    assert exit_run.decision.target is not None
    assert exit_run.decision.target.positions[0].weight == Decimal("0")


def test_single_pass_rejects_stale_quote():
    client = FakeSinglePassClient(
        {
            "portfolio_action": "HOLD",
            "target_direction": "flat",
            "target_weight": "0",
            "strategy_hypothesis": "unspecified",
            "market_bias": "neutral",
            "market_state_note": None,
            "watch_condition": None,
            "rationale": "Should not be called on stale quote",
        }
    )
    agent = SinglePassTradingAgent(client, clock=lambda: T)
    stale_context = AgentContext(
        event=sample_event(),
        memory=MemorySnapshot(memory_version=0, as_of=T),
        portfolio=sample_portfolio(),
        market=sample_market(received_at=T - timedelta(seconds=5)),
        as_of=T,
    )
    with pytest.raises(ValueError, match="Missing or stale"):
        run(agent.decide(stale_context))
    assert client.calls == 0


def test_build_market_snapshot_from_quote_store(tmp_path):
    quote_db = tmp_path / "quotes.sqlite"
    qstore = QuoteStore(quote_db, clock=lambda: T)
    qstore.start(
        BridgeStart(
            version=1,
            kind="start",
            session_id=SESSION,
            account_mode="demo",
            running_mode="realtime",
            symbol="EURUSD",
            started_at=T - timedelta(seconds=5),
            sample_interval_ms=1000,
            duration_seconds=60,
        ),
        T - timedelta(seconds=5),
        basis="live_poll",
    )
    qstore.record(
        BridgeQuote(
            version=1,
            kind="quote",
            session_id=SESSION,
            sequence=1,
            symbol="EURUSD",
            source_at=T - timedelta(milliseconds=600),
            bridge_at=T - timedelta(milliseconds=500),
            bid=Decimal("1.08510"),
            ask=Decimal("1.08512"),
        ),
        T - timedelta(milliseconds=400),
        '{"kind":"quote"}',
    )
    qstore.stop(
        BridgeStop(
            version=1,
            kind="stop",
            session_id=SESSION,
            stopped_at=T,
            quotes=1,
        ),
        T,
    )
    snap = build_market_snapshot(qstore, T)
    assert len(snap.quotes) == 1
    assert snap.quotes[0].instrument == "EUR/USD"
    assert snap.quotes[0].bid == Decimal("1.08510")
    qstore.close()
