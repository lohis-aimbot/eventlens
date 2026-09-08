import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from eventlens.backtesting import replay
from eventlens.core import Quote, SourceRecord, Stance
from eventlens.parsing import LLMParser, RuleParser
from eventlens.research import event_study, similar_events
from eventlens.risk import RiskConfig, entry_rejection
from eventlens.signals import score_event
from eventlens.storage import EventStore
from pydantic import ValidationError

T = datetime(2025, 1, 1, 14, tzinfo=timezone.utc)


def event(
    seconds=0, text="Inflation remains elevated.", episode="meeting-1", kind="fomc_statement"
):
    record = SourceRecord(
        source="synthetic",
        source_id=str(seconds),
        published_at=T + timedelta(seconds=seconds),
        received_at=T + timedelta(seconds=seconds),
        text=text,
        episode_id=episode,
        kind=kind,
    )
    return RuleParser().parse(record, available_at=record.received_at)


def quote(seconds, bid=1.1, ask=1.1001):
    return Quote(timestamp=T + timedelta(seconds=seconds), bid=bid, ask=ask)


def test_schema_rejects_naive_time_and_bad_spread():
    with pytest.raises(ValidationError):
        Quote(timestamp=datetime(2025, 1, 1), bid=1, ask=2)
    with pytest.raises(ValidationError):
        quote(0, bid=2, ask=1)
    with pytest.raises(ValidationError):
        quote(0, bid=float("nan"))


def test_parser_evidence_unknown_negation_and_conflict():
    assert event().extraction.stance == Stance.HAWKISH
    assert event(text="Inflation is not elevated.").extraction.stance == Stance.UNKNOWN
    assert (
        event(text="Inflation remains elevated. Begin reducing the target range.").extraction.stance
        == Stance.UNKNOWN
    )
    with pytest.raises(ValidationError):
        RuleParser().parse(event().record, available_at=T - timedelta(seconds=1))
    with pytest.raises(ValueError):
        event(kind="other")


def test_llm_boundary_validates_evidence_and_output():
    class FakeClient:
        def __init__(self, output):
            self.output = output

        def complete(self, **kwargs):
            assert "untrusted" in kwargs["system"]
            return self.output

    valid = json.dumps(
        dict(stance="hawkish", evidence=["Inflation remains elevated."], rationale="test")
    )
    parsed = LLMParser(FakeClient(valid), "fake-v1").parse(event().record, available_at=T)
    assert parsed.parser_version == "llm:fake-v1:prompt-1"
    for output in ("invalid", valid.replace("Inflation remains elevated.", "Invented evidence")):
        with pytest.raises(ValueError):
            LLMParser(FakeClient(output), "fake-v1").parse(event().record, available_at=T)


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_storage_idempotency_and_point_in_time(tmp_path, backend):
    url = f"sqlite:///{tmp_path / 'events.db'}"
    if backend == "postgres":
        url = os.environ.get("EVENTLENS_TEST_POSTGRES")
        if not url:
            pytest.skip("Set EVENTLENS_TEST_POSTGRES for a disposable PostgreSQL database")
    store = EventStore(url)
    sample = event(episode=str(uuid4()))
    try:
        assert store.put(sample)
        assert not store.put(sample)
        assert sample.event_id in {e.event_id for e in store.list(as_of=T)}
        assert sample.event_id not in {
            e.event_id for e in store.list(as_of=T - timedelta(seconds=1))
        }
    finally:
        store.close()


def test_research_horizon_cannot_use_future_or_cross_gap():
    e = event()
    qs = [quote(0), quote(900, 1.11, 1.1101)]
    assert event_study([e], qs, as_of=T + timedelta(seconds=899)) == []
    observation = event_study([e], qs, as_of=T + timedelta(seconds=900))[0]
    assert observation.return_bps == pytest.approx((1.11005 / 1.10005 - 1) * 10000)
    assert event_study([e], [quote(86400)], as_of=T + timedelta(days=2)) == []
    with pytest.raises(ValueError):
        event_study([e], qs, as_of=T, horizon=timedelta(0))


def test_analogues_exclude_current_episode_and_future():
    target = event(100, episode="new")
    older = event(0, episode="old")
    same = event(10, episode="new")
    future = event(200, episode="future")
    assert similar_events(target, [future, older, same], as_of=target.available_at) == [older]


def test_next_quote_fill_and_reversal_exit():
    first = event()
    second = event(10, "Downside risks to employment have increased.", kind="fomc_followup")
    qs = [quote(0), quote(1), quote(10), quote(11, 1.099, 1.0991), quote(20)]
    result = replay([first, second], qs, RiskConfig(slippage_bps=0))
    assert [f.action for f in result.fills] == ["open", "close"]
    assert result.fills[0].timestamp == T + timedelta(seconds=1)
    assert result.fills[0].side == -1
    assert result.fills[1].timestamp == T + timedelta(seconds=11)
    assert result.fills[1].reason == "thesis_reversal"
    assert result.net_pnl_usd == pytest.approx(0.9)


def test_duplicate_unknown_and_unrelated_do_not_change_position():
    events = [
        event(),
        event(3),
        event(5, "We will review incoming data."),
        event(7, "Begin reducing the target range.", episode="other"),
    ]
    result = replay(events, [quote(i) for i in (1, 4, 6, 8, 10)])
    assert len(result.fills) == 2
    assert result.fills[-1].reason == "end_of_data"
    assert {a.reason for a in result.audit} >= {
        "duplicate_content",
        "unknown_or_neutral",
        "unrelated_episode_position_open",
    }


@pytest.mark.parametrize(
    "cfg,reason",
    [
        (RiskConfig(kill_switch=True), "kill_switch"),
        (RiskConfig(units=2000, max_units=1000), "position_limit"),
        (RiskConfig(max_spread_bps=0.1), "spread_limit"),
    ],
)
def test_entry_limits(cfg, reason):
    result = replay([event()], [quote(1), quote(2)], cfg)
    assert result.fills == []
    assert result.audit[0].reason == reason


def test_stale_signals_and_terminal_data_abstain():
    assert replay([event()], [quote(400), quote(401)]).audit[0].reason == "stale_signal"
    assert replay([event()], [quote(0)]).audit[0].reason == "no_later_quote"
    assert replay([event()], []).fills == []
    assert replay([event()], [quote(1)]).fills == []


def test_quote_freshness_gate():
    signal = score_event(event(), now=T)
    assert (
        entry_rejection(
            signal, quote(0), now=T + timedelta(seconds=31), equity_change=0, config=RiskConfig()
        )
        == "stale_or_future_quote"
    )


@pytest.mark.parametrize(
    "cfg,quotes,reason",
    [
        (RiskConfig(stop_loss_bps=5), [quote(1), quote(2, 1.102, 1.1021)], "stop_loss"),
        (RiskConfig(max_holding_seconds=5), [quote(1), quote(7)], "holding_limit"),
        (
            RiskConfig(max_loss_usd=0.2, stop_loss_bps=100),
            [quote(1), quote(2, 1.101, 1.1011)],
            "loss_limit",
        ),
    ],
)
def test_exit_limits(cfg, quotes, reason):
    result = replay([event()], quotes, cfg)
    assert result.fills[-1].reason == reason


def test_spread_slippage_and_commission_are_charged():
    result = replay(
        [event()], [quote(1), quote(2)], RiskConfig(slippage_bps=1, commission_per_order_usd=2)
    )
    expected = (1.1 * (1 - 0.0001) - 1.1001 * (1 + 0.0001)) * 1000 - 4
    assert result.net_pnl_usd == pytest.approx(expected)
    assert result.fills[-1].net_pnl_usd == pytest.approx(expected)


def test_ambiguous_timestamps_rejected():
    with pytest.raises(ValueError):
        replay([event()], [quote(1), quote(1)])
    with pytest.raises(ValueError):
        replay([event(), event(episode="other")], [quote(1)])


def test_cli_round_trip(tmp_path):
    root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        "-m",
        "eventlens.cli",
        "demo",
        "--database-url",
        f"sqlite:///{tmp_path / 'cli.db'}",
    ]
    first = json.loads(subprocess.check_output(command, cwd=root))
    second = json.loads(subprocess.check_output(command, cwd=root))
    assert first["new_events_stored"] == 4
    assert second["new_events_stored"] == 0
    assert first["replay"] == second["replay"]
    assert first["replay"]["closed_trades"] == 2
