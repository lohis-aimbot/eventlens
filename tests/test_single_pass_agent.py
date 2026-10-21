"""Single-pass agent, verbatim evidence recovery, quote alignment and memory commit."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from eventlens.cli import main
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
    ObservationReasoner,
    SinglePassTradingAgent,
    resolve_verbatim_quote,
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
        assert "prior_memory_theses_do_not_quote" in payload
        self.users.append(payload)
        return ModelResponse(
            content=self.responses.pop(0),
            input_tokens=80,
            output_tokens=45,
            model_id="deepseek-flash",
            system_fingerprint="fp-single",
        )


def test_resolve_verbatim_quote_recovers_mid_sentence_capitalization_and_line_wraps():
    raw = "The Committee decided, in support of its goals,\nto maintain the target range."
    # Exact match
    assert resolve_verbatim_quote(raw, "in support of its goals") == "in support of its goals"
    # Mid-sentence capitalization (the January FOMC failure mode)
    recovered = resolve_verbatim_quote(raw, "In support of its goals, to maintain")
    assert recovered == "in support of its goals,\nto maintain"
    assert recovered in raw
    # Rejects thesis-narrative or fabricated text (the March/May/July/October failure mode)
    with pytest.raises(ValueError, match="absent"):
        resolve_verbatim_quote(raw, "Euro-area/ECB policy stance is unknown")
    with pytest.raises(ValueError, match="absent"):
        resolve_verbatim_quote(raw, "in support of its goals to lower")


def test_single_pass_create_and_directional_target_links_current_thesis_and_commits(tmp_path):
    clock_now = T + timedelta(milliseconds=500)
    memory = SQLiteThesisMemory(
        tmp_path / "memory.sqlite", clock=lambda: clock_now, allow_targets=True
    )
    client = FakeSinglePassClient(
        {
            "memory_action": "create",
            "thesis_id": None,
            "status": "active",
            "narrative": "Fed holds rates steady while inflation remains elevated.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["Dovish guidance shift"],
            "confidence": 0.65,
            "evidence_role": "support",
            # Capitalized first letter of mid-sentence phrase + space instead of newline:
            "evidence_quote": "In support of its goals, to maintain the target range",
            "target_direction": "short",
            "target_weight": "-0.15",
            "target_thesis_ids": ["__current__"],
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
    created_thesis = first.decision.thesis_changes[0].thesis
    assert first.decision.target is not None
    assert first.decision.target.positions[0].weight == Decimal("-0.15")
    assert first.decision.target.positions[0].thesis_ids == (created_thesis.thesis_id,)

    # Re-processing the same event reuses the committed decision with zero new model calls.
    repeated = run(
        reasoner.process(ev, market=sample_market(), portfolio=sample_portfolio(), as_of=T)
    )
    assert repeated.reused is True
    assert repeated.decision.decision_id == first.decision.decision_id
    assert client.calls == 1


def test_single_pass_flat_abstention_allows_null_quote_and_closes_held_position(tmp_path):
    clock_now = T + timedelta(milliseconds=500)
    memory = SQLiteThesisMemory(
        tmp_path / "memory.sqlite", clock=lambda: clock_now, allow_targets=True
    )
    client = FakeSinglePassClient(
        {
            "memory_action": "ignore",
            "thesis_id": None,
            "status": None,
            "narrative": None,
            "instruments": None,
            "invalidation_conditions": None,
            "confidence": None,
            "evidence_role": None,
            "evidence_quote": None,
            "target_direction": "flat",
            "target_weight": "0",
            "target_thesis_ids": [],
            "rationale": "Euro-area stance is unstated in this release; stay flat.",
        },
        {
            "memory_action": "create",
            "thesis_id": None,
            "status": "invalidated",
            "narrative": "Prior USD strength thesis invalidated.",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["Rate hike resumed"],
            "confidence": 0.2,
            "evidence_role": "contradict",
            "evidence_quote": "maintain the target range",
            "target_direction": "flat",
            "target_weight": "0",
            "target_thesis_ids": ["__current__"],
            "rationale": "Exit existing position as thesis is invalidated.",
        },
    )
    agent = SinglePassTradingAgent(client, clock=lambda: clock_now)
    reasoner = ObservationReasoner(agent, memory)

    # 1. Flat portfolio + flat abstention -> target is None, no quote required.
    abstain = run(
        reasoner.process(
            sample_event("e1"), market=sample_market(), portfolio=sample_portfolio(), as_of=T
        )
    )
    assert abstain.decision.target is None
    assert abstain.decision.thesis_changes == ()

    # 2. Held position + flat proposal -> produces a zero-weight exit target.
    clock_now = T + timedelta(seconds=2)
    exit_run = run(
        reasoner.process(
            sample_event("e2"),
            market=sample_market(received_at=clock_now - timedelta(milliseconds=400)),
            portfolio=sample_portfolio(quantity=Decimal("1000")),
            as_of=clock_now,
        )
    )
    assert exit_run.decision.target is not None
    assert exit_run.decision.target.positions[0].weight == Decimal("0")


def test_single_pass_rejects_stale_quote_and_thesis_narrative_quote():
    client = FakeSinglePassClient(
        {
            "memory_action": "create",
            "thesis_id": None,
            "status": "active",
            "narrative": "Test",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["None"],
            "confidence": 0.5,
            "evidence_role": "support",
            "evidence_quote": "Copied from prior thesis narrative, not current event",
            "target_direction": "flat",
            "target_weight": "0",
            "target_thesis_ids": [],
            "rationale": "Bad quote",
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

    fresh_context = stale_context.model_copy(update={"market": sample_market()})
    with pytest.raises(ValueError, match="absent"):
        run(agent.decide(fresh_context))
    assert client.calls == 1


def test_build_market_snapshot_and_memory_decide_cli_preview(tmp_path, capsys):
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

    event_file = tmp_path / "event.json"
    event_file.write_text(sample_event().model_dump_json(), encoding="utf-8")
    mem_db = tmp_path / "memory.sqlite"
    main(
        [
            "memory",
            "--database",
            str(mem_db),
            "decide",
            str(event_file),
            "--quotes",
            str(quote_db),
            "--as-of",
            T.isoformat(),
        ]
    )
    out = json.loads(capsys.readouterr().out)
    assert out["scope"] == "single_pass_context_preview"
    assert out["execution_authorized"] is False
    assert out["model_called"] is False
    assert out["context"]["market"]["quotes"][0]["bid"] == "1.08510"
