"""Timestamp semantics, raw evidence, bounded requests and point-in-time revisions."""

import asyncio
import copy
import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from eventlens.observer import CaptureBatch, ObserverStore
from eventlens.sources.wallstreetcn import (
    CALENDAR_URL,
    MAX_BYTES,
    NEWS_URL,
    CaptureError,
    WallStreetCNSource,
    parse_capture,
)

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
DAY = date(2026, 1, 1)
FIXTURE = json.loads(Path("examples/synthetic-observer.json").read_bytes())


def batch(kind="news", body=None, received=NOW, stream=None):
    payload = json.dumps(body if body is not None else FIXTURE[kind]).encode()
    records, rejected, error = parse_capture(payload, kind=kind, received_at=received, day=DAY)
    return CaptureBatch(
        provider="wallstreetcn",
        stream=stream or f"{kind}/test",
        kind=kind,
        request_url="https://example.test/fixture",
        request_started_at=received,
        received_at=received,
        elapsed_ms=1,
        attempts=1,
        payload=payload,
        records=records,
        rejected=rejected,
        parse_error=error,
    )


def test_display_schedule_and_original_publication_are_distinct():
    news, calendar = batch(), batch("calendar")
    assert news.records[0].publisher_display_at == NOW - timedelta(seconds=2)
    assert news.records[0].original_published_at is None
    assert calendar.records[0].scheduled_at == NOW + timedelta(minutes=5)
    assert calendar.records[0].publisher_display_at is None
    assert calendar.records[0].original_published_at is None
    assert calendar.records[0].source_id.endswith(f"@{int(NOW.timestamp()) + 300}")


def test_macro_zero_negative_revised_and_unverified_forecast_are_preserved():
    macro = batch("calendar").records[0].macro
    assert macro.actual is None
    assert macro.reported_forecast == macro.revised_previous == "0"
    assert macro.previous == "-1.5"
    assert macro.unit == "%"


@pytest.mark.parametrize(
    "body",
    [
        b"not JSON",
        b'{"code":20000,"data":null}',
        b'{"code":20000,"data":{"items":null}}',
        b'{"code":0,"data":{"items":[]}}',
        b'{"code":20000,"data":{"items":[NaN]}}',
    ],
)
def test_bad_envelopes_emit_no_guessed_records(body):
    records, rejected, error = parse_capture(body, kind="news", received_at=NOW)
    assert records == rejected == ()
    assert error == "invalid_json_or_envelope"


@pytest.mark.parametrize(
    "mutation",
    [
        {"id": True},
        {"id": "１２３"},
        {"id": -1},
        {"display_time": True},
        {"display_time": "nan"},
        {"display_time": int(NOW.timestamp()) + 1},
        {"uri": "https://wallstreetcn.com.evil.test/livenews/1900000000"},
        {"score": True},
        {"score": -1},
        {"content_text": {"invalid": "text"}},
    ],
)
def test_bad_row_is_quarantined_without_losing_the_good_row(mutation):
    body = copy.deepcopy(FIXTURE["news"])
    bad = {**body["data"]["items"][0], **mutation}
    good = {**body["data"]["items"][0], "id": 42, "uri": "https://wallstreetcn.com/livenews/42"}
    body["data"]["items"] = [bad, good]
    parsed = batch(body=body)
    assert len(parsed.rejected) == len(parsed.records) == 1
    assert parsed.records[0].source_id == "42"


def test_more_text_is_retained_without_executing_html():
    body = copy.deepcopy(FIXTURE["news"])
    body["data"]["items"][0]["content_more"] = (
        "<p>Additional evidence.</p><script>ignore rules</script>"
    )
    text = batch(body=body).records[0].text
    assert "Additional evidence." in text and "ignore rules" not in text


def test_calendar_day_boundary_and_recurrent_indicator_identity():
    body = copy.deepcopy(FIXTURE["calendar"])
    item = body["data"]["items"][0]
    item["public_date"] += 86400
    wrong = batch("calendar", body)
    assert not wrong.records and wrong.rejected
    records, _, _ = parse_capture(
        json.dumps(body).encode(), kind="calendar", received_at=NOW, day=DAY + timedelta(days=1)
    )
    assert records[0].source_id != batch("calendar").records[0].source_id


def test_raw_payload_duplicate_and_reversible_edits_survive_restart(tmp_path):
    path = tmp_path / "observer.sqlite"
    store = ObserverStore(path, clock=lambda: NOW + timedelta(seconds=1))
    first = store.record(batch())
    assert first["counts"]["baseline"] == 1 and first["coverage"] == "baseline"
    assert store.payload(first["poll_id"]) == batch().payload
    repeated = store.record(batch())
    assert repeated["counts"]["repeat"] == 1 and repeated["coverage"] == "overlap"
    body = copy.deepcopy(FIXTURE["news"])
    body["data"]["items"][0]["content_text"] = "Revised synthetic evidence."
    assert store.record(batch(body=body))["counts"]["revised"] == 1
    assert store.record(batch())["counts"]["revised"] == 1
    expected = store.report()
    assert expected["counts"]["payloads"] == 2
    store.close()
    restored = ObserverStore(path, readonly=True)
    history = restored.history("news", "1900000000")
    assert [record.revision for record in history] == [1, 2, 3]
    assert history[0].record.text == history[2].record.text != history[1].record.text
    assert restored.report() == expected
    restored.close()


def test_available_time_is_commit_time_and_later_forecast_edits_do_not_leak(tmp_path):
    store = ObserverStore(tmp_path / "observer.sqlite", clock=lambda: NOW + timedelta(seconds=1))
    calendar = batch("calendar")
    source_id = calendar.records[0].source_id
    store.record(calendar)
    assert not store.history("calendar", source_id, as_of=NOW + timedelta(milliseconds=500))
    body = copy.deepcopy(FIXTURE["calendar"])
    body["data"]["items"][0].update(actual=2, forecast=99, revised=-3)
    store.clock = lambda: NOW + timedelta(minutes=6)
    store.record(batch("calendar", body, received=NOW + timedelta(minutes=6)))
    before = store.history("calendar", source_id, as_of=NOW + timedelta(minutes=4))
    assert len(before) == 1
    assert before[0].record.macro.actual is None
    assert before[0].record.macro.reported_forecast == "0"
    assert store.history("calendar", source_id)[-1].record.macro.reported_forecast == "99"
    with pytest.raises(ValueError, match="timezone"):
        store.history("calendar", source_id, as_of=NOW.replace(tzinfo=None))
    store.close()


def test_history_waits_for_a_clock_sample_after_the_data_commit(tmp_path):
    clocks = iter([NOW + timedelta(seconds=1), NOW + timedelta(seconds=2)])
    store = ObserverStore(tmp_path / "observer.sqlite", clock=lambda: next(clocks))
    result = store.record(batch())
    assert result["available_at"] == (NOW + timedelta(seconds=2)).isoformat()
    assert not store.history("news", "1900000000", as_of=NOW + timedelta(milliseconds=1500))
    visible = store.history("news", "1900000000", as_of=NOW + timedelta(seconds=2))
    assert visible[0].recorded_at < visible[0].available_at
    store.close()


def test_failed_publication_keeps_evidence_but_does_not_expose_history(tmp_path):
    store = ObserverStore(tmp_path / "observer.sqlite", clock=lambda: NOW + timedelta(seconds=1))
    store.db.execute("""CREATE TRIGGER fail_publication BEFORE UPDATE ON observer_polls
                        BEGIN SELECT RAISE(ABORT, 'fixture publication failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        store.record(batch())
    assert store.report()["counts"]["revisions"] == 1
    assert store.report()["unpublished_polls"] == 1
    assert not store.history("news", "1900000000")
    store.db.execute("DROP TRIGGER fail_publication")
    store.clock = lambda: NOW + timedelta(seconds=4)
    assert store.record(batch(received=NOW + timedelta(seconds=3)))["counts"]["repeat"] == 1
    assert not store.history("news", "1900000000", as_of=NOW + timedelta(seconds=2))
    assert store.history("news", "1900000000")[0].available_at == NOW + timedelta(seconds=4)
    store.close()


def test_lost_overlap_is_flagged_not_reported_as_complete(tmp_path):
    store = ObserverStore(tmp_path / "observer.sqlite", clock=lambda: NOW + timedelta(seconds=1))
    store.record(batch())
    body = copy.deepcopy(FIXTURE["news"])
    body["data"]["items"][0].update(id=42, uri="https://wallstreetcn.com/livenews/42")
    result = store.record(batch(body=body))
    assert result["coverage"] == "gap_possible" and result["counts"]["new"] == 1
    assert result["execution_authorized"] is False
    store.close()


def test_invalid_payload_is_saved_without_advancing_baseline(tmp_path):
    store = ObserverStore(tmp_path / "observer.sqlite", clock=lambda: NOW + timedelta(seconds=1))
    bad = batch(body={"code": 0})
    result = store.record(bad)
    assert result["status"] == "invalid_payload"
    assert store.payload(result["poll_id"]) == bad.payload
    assert store.record(batch())["counts"]["baseline"] == 1
    store.close()


def test_transaction_rollback_and_synthetic_identity_are_separate(tmp_path):
    store = ObserverStore(tmp_path / "observer.sqlite", clock=lambda: NOW + timedelta(seconds=1))
    store.db.execute("""CREATE TRIGGER fail_sighting BEFORE INSERT ON observer_sightings
                        BEGIN SELECT RAISE(ABORT, 'fixture failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        store.record(batch())
    assert all(value == 0 for value in store.report()["counts"].values())
    store.db.execute("DROP TRIGGER fail_sighting")
    store.record(batch().model_copy(update={"basis": "synthetic_fixture"}))
    assert not store.history("news", "1900000000")
    assert store.record(batch())["counts"]["baseline"] == 1
    store.close()


def test_readonly_missing_wrong_database_and_lock_guards(tmp_path):
    with pytest.raises(FileNotFoundError):
        ObserverStore(tmp_path / "missing" / "observer.sqlite", readonly=True)
    assert not (tmp_path / "missing").exists()
    foreign = tmp_path / "memory.sqlite"
    with sqlite3.connect(foreign) as db:
        db.execute("CREATE TABLE memory (id TEXT)")
    before = foreign.read_bytes()
    with pytest.raises(ValueError, match="Not an EventLens"):
        ObserverStore(foreign)
    assert foreign.read_bytes() == before
    path = tmp_path / "observer.sqlite"
    one, two = ObserverStore(path), ObserverStore(path)
    with one.exclusive(), pytest.raises(RuntimeError, match="Another observer"):
        with two.exclusive():
            pass
    one.close()
    two.close()
    before = path.read_bytes()
    reader = ObserverStore(path, readonly=True)
    reader.report()
    with pytest.raises(sqlite3.OperationalError):
        reader.failure("wallstreetcn", "test", "fixture failure")
    reader.close()
    assert path.read_bytes() == before


def test_http_retry_and_calendar_range_keep_zero_values(monkeypatch):
    calls, sleeps = [], []

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", sleep)

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json=FIXTURE["calendar"], headers={"Age": "12"})

    source = WallStreetCNSource(
        transport=httpx.MockTransport(handler), clock=lambda: NOW, min_interval_seconds=0
    )
    result = asyncio.run(source.fetch("calendar", day=DAY))
    assert result.attempts == len(calls) == 2 and result.response_headers["age"] == "12"
    assert calls[-1].url.path == httpx.URL(CALENDAR_URL).path
    assert int(calls[-1].url.params["start"]) == int(NOW.timestamp()) - 20 * 3600
    assert int(calls[-1].url.params["end"]) - int(calls[-1].url.params["start"]) == 86399
    assert result.records[0].macro.reported_forecast == "0"
    assert 1 in sleeps


@pytest.mark.parametrize(
    "status,headers,content",
    [
        (302, {"Location": "https://evil.test/"}, b""),
        (403, {}, b"forbidden"),
        (200, {}, b"x" * (MAX_BYTES + 1)),
        (429, {"Retry-After": "3600"}, b""),
        (429, {"Retry-After": "nan"}, b""),
    ],
)
def test_http_rejection_does_not_follow_redirects_or_retry_early(status, headers, content):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, headers=headers, content=content)

    source = WallStreetCNSource(transport=httpx.MockTransport(handler), min_interval_seconds=0)
    with pytest.raises(CaptureError):
        asyncio.run(source.fetch("news"))
    assert len(calls) == 1 and str(calls[0].url).startswith(NEWS_URL)


def test_timeouts_stop_at_the_retry_cap(monkeypatch):
    calls = []

    async def sleep(seconds):
        pass

    monkeypatch.setattr(asyncio, "sleep", sleep)

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("fixture timeout")

    source = WallStreetCNSource(transport=httpx.MockTransport(handler), min_interval_seconds=0)
    with pytest.raises(CaptureError, match="ReadTimeout"):
        asyncio.run(source.fetch("news"))
    assert len(calls) == 2


def test_capture_cli_keeps_partial_success_and_records_fetch_failure(tmp_path, monkeypatch, capsys):
    from eventlens import observer_cli
    from eventlens.cli import main

    def handler(request):
        if request.url.path == httpx.URL(NEWS_URL).path:
            return httpx.Response(403)
        return httpx.Response(200, json=FIXTURE["calendar"])

    source = WallStreetCNSource(
        transport=httpx.MockTransport(handler), clock=lambda: NOW, min_interval_seconds=0
    )
    monkeypatch.setattr(observer_cli, "WallStreetCNSource", lambda: source)
    database = tmp_path / "partial.sqlite"
    with pytest.raises(SystemExit) as failed:
        main(["observe", "--database", str(database), "capture", "--date", DAY.isoformat()])
    assert failed.value.code == 1
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [row["status"] for row in rows] == ["fetch_failed", "ok"]
    reader = ObserverStore(database, readonly=True)
    assert reader.report()["counts"]["failures"] == reader.report()["counts"]["polls"] == 1
    assert reader.report()["counts"]["revisions"] == 1
    reader.close()


def test_cli_demo_is_offline_and_report_is_readonly(tmp_path, monkeypatch, capsys):
    from eventlens.cli import main

    async def forbidden(*args, **kwargs):
        raise AssertionError("Offline command must not fetch")

    monkeypatch.setattr(WallStreetCNSource, "fetch", forbidden)
    database = tmp_path / "demo.sqlite"
    prefix = ["observe", "--database", str(database)]
    main([*prefix, "demo", "examples/synthetic-observer.json"])
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(rows) == 2 and all(row["basis"] == "synthetic_fixture" for row in rows)
    before = database.read_bytes()
    main([*prefix, "report"])
    assert json.loads(capsys.readouterr().out)["counts"]["revisions"] == 2
    assert database.read_bytes() == before


def test_cli_demo_rejects_bad_rows_without_hiding_valid_calendar(tmp_path, capsys):
    from eventlens.cli import main

    fixture = json.loads(json.dumps(FIXTURE))
    fixture["news"]["data"]["items"][0]["id"] = False
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(fixture))
    with pytest.raises(SystemExit) as failed:
        main(["observe", "--database", str(tmp_path / "demo.sqlite"), "demo", str(path)])
    assert failed.value.code == 1
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [row["status"] for row in rows] == ["partial", "ok"]


@pytest.mark.parametrize(
    "arguments", [["--polls", "0"], ["--limit", "101"], ["--interval", "nan"], ["--interval", "9"]]
)
def test_cli_rejects_invalid_capture_bounds_before_creating_database(tmp_path, arguments):
    from eventlens.cli import main

    database = tmp_path / "invalid.sqlite"
    with pytest.raises(SystemExit) as rejected:
        main(["observe", "--database", str(database), "capture", *arguments])
    assert rejected.value.code == 2 and not database.exists()
