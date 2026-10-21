"""No-order Demo bridge validation, durable quote clocks and causal news alignment."""

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from eventlens.observer import CaptureBatch, ObserverStore, SourceRecord
from eventlens.quotes import BridgeQuote, BridgeStart, BridgeStop, QuoteStore, align_news
from eventlens.sources.ctrader_demo import DemoQuoteCollector, DirectoryTail

NOW = datetime(2026, 10, 15, 6, tzinfo=UTC)
SESSION = "00000000-0000-4000-8000-000000000001"


def start(**changes):
    return BridgeStart.model_validate(
        {
            "version": 1,
            "kind": "start",
            "session_id": SESSION,
            "account_mode": "demo",
            "running_mode": "realtime",
            "symbol": "EURUSD",
            "started_at": NOW,
            "sample_interval_ms": 1000,
            "duration_seconds": 30,
            **changes,
        }
    )


def quote(**changes):
    return BridgeQuote.model_validate(
        {
            "version": 1,
            "kind": "quote",
            "session_id": SESSION,
            "sequence": 1,
            "symbol": "EURUSD",
            "source_at": NOW + timedelta(seconds=1),
            "bridge_at": NOW + timedelta(seconds=1, milliseconds=10),
            "bid": "1.10001",
            "ask": "1.10003",
            **changes,
        }
    )


def make_store(tmp_path, clock=None):
    store = QuoteStore(
        tmp_path / "quotes.sqlite", clock=clock or (lambda: NOW + timedelta(seconds=2))
    )
    store.start(start(), NOW, basis="live_poll")
    return store


def test_quote_publication_cutoff_and_exact_decimal_evidence(tmp_path):
    clocks = iter(
        [NOW + timedelta(seconds=1, milliseconds=30), NOW + timedelta(seconds=1, milliseconds=40)]
    )
    store = make_store(tmp_path, lambda: next(clocks))
    item = quote()
    store.record(item, NOW + timedelta(seconds=1, milliseconds=20), item.model_dump_json())
    assert store.latest("EURUSD", NOW + timedelta(seconds=1, milliseconds=39)) is None
    observed = store.latest("EURUSD", NOW + timedelta(seconds=1, milliseconds=40))
    assert observed.quote.ask - observed.quote.bid == Decimal("0.00002")
    assert observed.available_at > observed.recorded_at > observed.received_at
    row = store.db.execute("SELECT raw_line FROM quote_observations").fetchone()
    assert row[0] == item.model_dump_json()
    store.close()


def test_failed_publication_retains_raw_but_never_returns_quote(tmp_path):
    clocks = iter([NOW + timedelta(seconds=2), NOW + timedelta(seconds=1)])
    store = make_store(tmp_path, lambda: next(clocks))
    item = quote()
    with pytest.raises(ValueError, match="unpublished"):
        store.record(item, NOW + timedelta(seconds=1, milliseconds=20), item.model_dump_json())
    assert store.report()["unpublished"] == 1
    assert store.latest("EURUSD", NOW + timedelta(seconds=10)) is None
    store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"bid": "0"},
        {"ask": "1"},
        {"bid": "NaN"},
        {"ask": "Infinity"},
        {"source_at": NOW.replace(tzinfo=None)},
        {"source_at": NOW + timedelta(seconds=3)},
        {"sequence": True},
        {"symbol": "BTCUSD"},
    ],
)
def test_quote_contract_rejects_bad_prices_times_and_identity(changes):
    with pytest.raises(ValidationError):
        quote(**changes)


def test_store_rejects_future_receipt_duplicate_unknown_and_backward_quotes(tmp_path):
    store = make_store(tmp_path)
    item = quote()
    with pytest.raises(ValueError, match="Future"):
        store.record(item, NOW, "future")
    store.record(item, NOW + timedelta(seconds=1, milliseconds=20), item.model_dump_json())
    with pytest.raises(ValueError, match="Duplicate"):
        store.record(item, NOW + timedelta(seconds=1, milliseconds=20), item.model_dump_json())
    with pytest.raises(ValueError, match="open Demo"):
        store.record(
            quote(session_id=UUID(int=2)), NOW + timedelta(seconds=1, milliseconds=20), "unknown"
        )
    with pytest.raises(ValueError, match="backward"):
        store.record(
            quote(sequence=2, source_at=NOW), NOW + timedelta(seconds=1, milliseconds=20), "old"
        )
    assert store.report()["quotes"] == 1
    store.close()


def test_synthetic_quotes_cannot_enter_default_live_lookup(tmp_path):
    store = QuoteStore(tmp_path / "quotes.sqlite", clock=lambda: NOW + timedelta(seconds=2))
    store.start(start(), NOW, basis="synthetic_fixture")
    store.record(quote(), NOW + timedelta(seconds=1, milliseconds=20), quote().model_dump_json())
    assert store.latest("EURUSD", NOW + timedelta(seconds=3)) is None
    assert store.latest("EURUSD", NOW + timedelta(seconds=3), basis="synthetic_fixture")
    store.close()


def collector(tmp_path):
    store = QuoteStore(tmp_path / "quotes.sqlite", clock=lambda: NOW + timedelta(seconds=2))
    return store, DemoQuoteCollector(store, NOW - timedelta(seconds=1))


def test_fresh_handshake_quote_stop_and_post_stop_rejection(tmp_path):
    store, receiver = collector(tmp_path)
    assert receiver.accept(start().model_dump_json(), NOW) == "start"
    assert (
        receiver.accept(quote().model_dump_json(), NOW + timedelta(seconds=1, milliseconds=20))
        == "quote"
    )
    stop = {
        "version": 1,
        "kind": "stop",
        "session_id": SESSION,
        "stopped_at": (NOW + timedelta(seconds=2)).isoformat(),
        "quotes": 1,
    }
    assert receiver.accept(json.dumps(stop), NOW + timedelta(seconds=2)) == "stop"
    assert receiver.accept(quote().model_dump_json(), NOW + timedelta(seconds=2)) == "rejected"
    assert store.report()["quotes"] == store.report()["failures"] == 1
    store.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("account_mode", "live"),
        ("running_mode", "backtest"),
        ("symbol", "XAUUSD"),
        ("version", True),
        ("started_at", "2020-01-01T00:00:00Z"),
        ("started_at", "2026-10-15T06:00:01Z"),
    ],
)
def test_bad_or_old_handshake_never_creates_session(tmp_path, field, value):
    store, receiver = collector(tmp_path)
    data = start().model_dump(mode="json")
    data[field] = value
    assert receiver.accept(json.dumps(data), NOW) == "rejected"
    assert store.report()["sessions"] == store.report()["quotes"] == 0
    store.close()


def test_receiver_records_malformed_unknown_stale_and_numeric_prices(tmp_path):
    store, receiver = collector(tmp_path)
    assert receiver.accept("{bad", NOW) == "rejected"
    assert receiver.accept(quote().model_dump_json(), NOW + timedelta(seconds=1)) == "rejected"
    assert receiver.accept(start().model_dump_json(), NOW) == "start"
    assert receiver.accept(quote().model_dump_json(), NOW + timedelta(seconds=20)) == "rejected"
    item = quote().model_dump(mode="json")
    item["bid"] = 1.10001
    assert (
        receiver.accept(json.dumps(item), NOW + timedelta(seconds=1, milliseconds=20)) == "rejected"
    )
    assert store.report()["failures"] == 4
    store.close()


def test_tail_skips_existing_files_and_waits_for_complete_new_lines(tmp_path):
    (tmp_path / "quotes-old.jsonl").write_text("old\n")
    tail = DirectoryTail(tmp_path)
    path = tmp_path / "quotes-new.jsonl"
    path.write_bytes(b"first\npart")
    assert list(tail.poll()) == ["first"]
    with path.open("ab") as out:
        out.write(b"ial\n")
    assert list(tail.poll()) == ["partial"]
    assert list(tail.poll()) == []
    path.write_text("x")
    with pytest.raises(ValueError, match="truncated"):
        list(tail.poll())


def test_tail_rejects_linked_and_oversized_new_inputs(tmp_path):
    tail = DirectoryTail(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    link = tmp_path / "quotes-linked.jsonl"
    link.symlink_to(outside)
    with pytest.raises(ValueError, match="linked"):
        list(tail.poll())
    link.unlink()
    (tmp_path / "quotes-huge.jsonl").write_bytes(b"x" * 16385)
    with pytest.raises(ValueError, match="size"):
        list(tail.poll())


def add_event(store, source_id="test-news", received=NOW):
    store.record(
        CaptureBatch(
            provider="wallstreetcn",
            stream="news/global",
            kind="news",
            request_url="https://example.test",
            request_started_at=received,
            received_at=received,
            elapsed_ms=0,
            attempts=1,
            payload=b"fixture",
            records=(
                SourceRecord(
                    source_id=source_id,
                    kind="news",
                    source_url="https://example.test",
                    title="synthetic",
                    text="synthetic",
                ),
            ),
        )
    )


def test_alignment_never_uses_future_news_or_future_quote_and_expires(tmp_path):
    events = ObserverStore(
        tmp_path / "events.sqlite", clock=lambda: NOW + timedelta(milliseconds=100)
    )
    add_event(events)
    quotes = make_store(tmp_path)
    quotes.record(quote(), NOW + timedelta(seconds=1, milliseconds=20), quote().model_dump_json())
    assert align_news(events, quotes, "test-news", NOW)["status"] == "missing_event"
    assert (
        align_news(events, quotes, "test-news", NOW + timedelta(seconds=1))["status"]
        == "missing_quote"
    )
    result = align_news(events, quotes, "test-news", NOW + timedelta(seconds=2))
    assert result["status"] == "aligned_observation" and not result["execution_authorized"]
    assert result["event"]["record"]["original_published_at"] is None
    assert (
        align_news(events, quotes, "test-news", NOW + timedelta(seconds=4))["status"]
        == "stale_quote"
    )
    assert (
        align_news(events, quotes, "test-news", NOW + timedelta(seconds=70))["status"]
        == "stale_event"
    )
    with pytest.raises(ValueError, match="Timezone"):
        align_news(events, quotes, "test-news", NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="Freshness"):
        align_news(events, quotes, "test-news", NOW, max_quote_age=float("nan"))
    events.close()
    quotes.close()


def test_zero_spread_is_observed_but_not_accepted_alignment(tmp_path):
    events = ObserverStore(tmp_path / "events.sqlite", clock=lambda: NOW)
    add_event(events)
    store = make_store(tmp_path)
    item = quote(ask="1.10001")
    store.record(item, NOW + timedelta(seconds=1, milliseconds=20), item.model_dump_json())
    assert (
        align_news(events, store, "test-news", NOW + timedelta(seconds=2))["status"]
        == "zero_spread"
    )
    events.close()
    store.close()


def test_foreign_database_readonly_and_lock_guards(tmp_path):
    missing = tmp_path / "missing" / "quotes.sqlite"
    with pytest.raises(FileNotFoundError):
        QuoteStore(missing, readonly=True)
    assert not missing.parent.exists()
    wrong = tmp_path / "observer.sqlite"
    observer = ObserverStore(wrong)
    observer.close()
    before = wrong.read_bytes()
    with pytest.raises(ValueError, match="Not an EventLens"):
        QuoteStore(wrong)
    assert wrong.read_bytes() == before
    store = make_store(tmp_path)
    with store.exclusive(), pytest.raises(RuntimeError, match="Another"):
        with store.exclusive():
            pass
    store.close()
    path = tmp_path / "quotes.sqlite"
    before = path.read_bytes()
    reader = QuoteStore(path, readonly=True)
    reader.report()
    reader.latest("EURUSD", NOW)
    with pytest.raises(sqlite3.OperationalError):
        reader.failure(NOW, "test", "test")
    reader.close()
    assert path.read_bytes() == before


def test_bridge_source_has_no_order_api_and_keeps_historical_probe_separate():
    code = Path("ctrader/EventLensQuoteBridge.cs").read_text()
    assert (
        "Account.IsLive" in code and "RunningMode.RealTime" in code and "AccessRights.None" in code
    )
    assert "FlushPendingIfDue" in code and "raw_zero_spread_ticks" in code
    assert "symbol_mismatch_ticks" in code and "commission_type" in code
    for operation in (
        "ExecuteMarketOrder",
        "PlaceLimitOrder",
        "ModifyPosition",
        "ClosePosition",
        "Account.Number",
    ):
        assert operation not in code
    assert "IsBacktesting" in Path("ctrader/EventLensTickProbe.cs").read_text()


@pytest.mark.parametrize("seconds", ["0", "nan", "601"])
def test_quote_cli_rejects_bad_duration_without_creating_database(tmp_path, seconds):
    from eventlens.quotes_cli import main

    database = tmp_path / "quotes.sqlite"
    with pytest.raises(SystemExit) as exc:
        main(["--database", str(database), "watch", str(tmp_path), "--seconds", seconds])
    assert exc.value.code == 2 and not database.exists()


def test_quote_group_help_and_misplaced_database_are_explicit(tmp_path, capsys):
    from eventlens.cli import main

    with pytest.raises(SystemExit) as help_result:
        main(["observe", "quotes", "--help"])
    assert help_result.value.code == 0
    assert "watch" in capsys.readouterr().out
    path = tmp_path / "wrong.sqlite"
    with pytest.raises(SystemExit) as misplaced:
        main(["observe", "--database", str(path), "quotes", "report"])
    assert misplaced.value.code == 2 and not path.exists()


@pytest.mark.parametrize("include_stop", [True, False])
def test_watch_cli_requires_clean_stop_and_reports_sample_gaps(
    tmp_path, monkeypatch, capsys, include_stop
):
    from eventlens import quotes_cli

    stop = {
        "version": 1,
        "kind": "stop",
        "session_id": SESSION,
        "stopped_at": (NOW + timedelta(seconds=2)).isoformat(),
        "quotes": 2,
    }
    lines = [start().model_dump_json(), quote(sequence=2).model_dump_json()]
    if include_stop:
        lines.append(json.dumps(stop))

    class Tail:
        def __init__(self, directory):
            pass

        def poll(self):
            return iter(lines)

    clocks = iter(
        [
            NOW - timedelta(seconds=1),
            NOW,
            NOW + timedelta(seconds=1, milliseconds=20),
            NOW + timedelta(seconds=2),
        ]
    )
    ticks = iter([0, 1, 20])
    monkeypatch.setattr(quotes_cli, "DirectoryTail", Tail)
    monkeypatch.setattr(
        quotes_cli,
        "QuoteStore",
        lambda path, readonly: QuoteStore(path, clock=lambda: NOW + timedelta(seconds=2)),
    )
    monkeypatch.setattr(quotes_cli, "now_utc", lambda: next(clocks))
    monkeypatch.setattr(quotes_cli.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(quotes_cli.time, "sleep", lambda seconds: None)
    args = ["--database", str(tmp_path / "watch.sqlite"), "watch", str(tmp_path), "--seconds", "10"]
    if include_stop:
        quotes_cli.main(args)
    else:
        with pytest.raises(SystemExit) as unclosed:
            quotes_cli.main(args)
        assert unclosed.value.code == 1
    result = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert result["status"] == ("observed_closed" if include_stop else "observed_unclosed")
    assert result["report"]["session_details"][0]["missing_sequences"] == 1


def test_alignment_does_not_fall_back_past_newest_zero_spread_quote(tmp_path):
    events = ObserverStore(tmp_path / "events.sqlite", clock=lambda: NOW)
    add_event(events)
    store = make_store(tmp_path, lambda: NOW + timedelta(seconds=3))
    first = quote()
    second = quote(
        sequence=2,
        source_at=NOW + timedelta(seconds=2),
        bridge_at=NOW + timedelta(seconds=2),
        ask="1.10001",
    )
    store.record(first, NOW + timedelta(seconds=1, milliseconds=20), first.model_dump_json())
    store.record(second, NOW + timedelta(seconds=2), second.model_dump_json())
    result = align_news(events, store, "test-news", NOW + timedelta(seconds=3))
    assert result["status"] == "zero_spread" and result["quote"]["quote"]["sequence"] == 2
    assert store.report()["quality_by_basis"]["live_poll"]["zero_spread"] == 1
    events.close()
    store.close()


def test_bridge_diagnostics_and_clock_decomposition_in_report_and_alignment(tmp_path):
    events = ObserverStore(tmp_path / "events.sqlite", clock=lambda: NOW + timedelta(seconds=5))
    add_event(events, received=NOW + timedelta(seconds=5))
    store, receiver = collector(tmp_path)
    meta_start = {
        **start().model_dump(mode="json"),
        "digits": 5,
        "pip_size": "0.0001",
        "commission": "3",
        "commission_type": "UsdPerMillionUsdVolume",
    }
    assert receiver.accept(json.dumps(meta_start), NOW) == "start"
    first = quote(
        sequence=1,
        source_at=NOW - timedelta(milliseconds=500),
        bridge_at=NOW + timedelta(milliseconds=100),
        bid="1.10001",
        ask="1.10001",
    )
    second = quote(
        sequence=2,
        source_at=NOW + timedelta(seconds=1),
        bridge_at=NOW + timedelta(seconds=1, milliseconds=250),
        bid="1.10001",
        ask="1.10002",
    )
    assert receiver.accept(first.model_dump_json(), NOW + timedelta(milliseconds=200)) == "quote"
    assert (
        receiver.accept(second.model_dump_json(), NOW + timedelta(seconds=1, milliseconds=400))
        == "quote"
    )
    stop = {
        "version": 1,
        "kind": "stop",
        "session_id": SESSION,
        "stopped_at": (NOW + timedelta(seconds=2)).isoformat(),
        "quotes": 2,
        "raw_ticks": 5,
        "raw_zero_spread_ticks": 4,
        "symbol_mismatch_ticks": 0,
        "throttled_ticks": 3,
    }
    assert receiver.accept(json.dumps(stop), NOW + timedelta(seconds=2)) == "stop"
    report = store.report()
    detail = report["session_details"][0]
    assert detail["first_source_before_start"] is True
    assert detail["symbol_metadata"] == {
        "digits": 5,
        "pip_size": "0.0001",
        "commission": "3",
        "commission_type": "UsdPerMillionUsdVolume",
    }
    assert detail["bridge_diagnostics"] == {
        "raw_ticks": 5,
        "raw_zero_spread_ticks": 4,
        "symbol_mismatch_ticks": 0,
        "throttled_ticks": 3,
    }
    quality = report["quality_by_basis"]["live_poll"]
    assert quality["zero_spread"] == 1 and quality["positive_spread"] == 1
    assert quality["min_positive_spread"] == "0.00001" and quality["max_spread"] == "0.00001"
    assert quality["max_sample_source_gap_seconds"] == pytest.approx(1.5)
    assert quality["max_source_to_bridge_seconds"] == pytest.approx(0.6)
    aligned = align_news(events, store, "test-news", NOW + timedelta(seconds=5))
    assert aligned["status"] == "stale_quote"
    assert aligned["quote_source_to_bridge_seconds"] == pytest.approx(0.25)
    assert aligned["quote_bridge_to_receipt_seconds"] == pytest.approx(0.15)
    assert aligned["quote_receipt_age_seconds"] == pytest.approx(3.6)
    assert aligned["quote_source_age_seconds"] == pytest.approx(
        aligned["quote_source_to_bridge_seconds"]
        + aligned["quote_bridge_to_receipt_seconds"]
        + aligned["quote_receipt_age_seconds"]
    )
    events.close()
    store.close()


@pytest.mark.parametrize(
    "bad_stop",
    [
        {"quotes": 3, "raw_ticks": 2},
        {"quotes": 2, "raw_ticks": 3, "raw_zero_spread_ticks": 4},
        {"quotes": 2, "raw_ticks": 3, "symbol_mismatch_ticks": 4},
        {"quotes": 2, "raw_ticks": 5, "throttled_ticks": 2},
        {"quotes": 2, "raw_zero_spread_ticks": 1},
    ],
)
def test_stop_contract_rejects_inconsistent_tick_diagnostics(bad_stop):
    with pytest.raises(ValidationError):
        BridgeStop.model_validate(
            {
                "version": 1,
                "kind": "stop",
                "session_id": SESSION,
                "stopped_at": NOW + timedelta(seconds=2),
                **bad_stop,
            }
        )


def test_receiver_rejects_numeric_symbol_metadata_decimals(tmp_path):
    store, receiver = collector(tmp_path)
    bad = {**start().model_dump(mode="json"), "pip_size": 0.0001}
    assert receiver.accept(json.dumps(bad), NOW) == "rejected"
    assert store.report()["sessions"] == 0 and store.report()["failures"] == 1
    store.close()
