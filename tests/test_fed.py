import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from eventlens.contracts import Event
from eventlens.deepseek import ModelResponse
from eventlens.fed import (
    FEED_URL,
    FedInbox,
    FedMonetarySource,
    FedWatcher,
    FeedError,
    FeedItem,
    SourceRejected,
    extract_article,
    validate_item,
)
from eventlens.sqlite_memory import SQLiteThesisMemory

OLD = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20240918a.htm"
NEW = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20241107a.htm"
BODY = (
    "The Committee decided to lower the target range for the federal funds rate "
    "by one quarter percentage point. The Committee will assess incoming data "
    "and the evolving outlook before considering any further adjustment."
)


def run(coro):
    return asyncio.run(coro)


def feed(links: list[str]) -> bytes:
    items = []
    for link in links:
        released = (
            "Wed, 18 Sep 2024 18:00:00 GMT" if link == OLD else "Thu, 07 Nov 2024 19:00:00 GMT"
        )
        items.append(
            f"<item><title>Federal Reserve issues FOMC statement</title>"
            f"<link><![CDATA[{link}]]></link><guid><![CDATA[{link}]]></guid>"
            f"<pubDate>{released}</pubDate></item>"
        )
    return ("<rss><channel>" + "".join(items) + "</channel></rss>").encode()


def article() -> bytes:
    return (
        '<html><body><nav><p>Navigation not evidence</p></nav><div id="article">'
        '<div class="heading col-xs-12 col-sm-8 col-md-8"><p>Release time</p></div>'
        '<div class="col-xs-12 col-sm-8 col-md-8"><p>' + BODY + "</p></div></div></body></html>"
    ).encode()


class FakeModel:
    provider = "fake"
    model = "fake-v1"

    def __init__(self, response: str | None = None) -> None:
        self.calls = 0
        self.response = response or json.dumps(
            {
                "action": "ignore",
                "thesis_id": None,
                "status": None,
                "narrative": None,
                "instruments": None,
                "invalidation_conditions": None,
                "confidence": None,
                "evidence_role": None,
                "evidence_quote": None,
                "reason": "No thesis change",
            }
        )

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls += 1
        return ModelResponse(content=self.response, input_tokens=20, output_tokens=10)


def setup(tmp_path: Path, links: list[str], *, model: FakeModel | None = None):
    database = tmp_path / "fed.sqlite"
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        if str(request.url) == FEED_URL:
            return httpx.Response(200, content=feed(links))
        if str(request.url) in {OLD, NEW}:
            return httpx.Response(200, content=article())
        raise AssertionError(f"Unexpected fetch: {request.url}")

    source = FedMonetarySource(
        transport=httpx.MockTransport(handler), min_request_interval_seconds=0
    )
    memory = SQLiteThesisMemory(database)
    inbox = FedInbox(database)
    return FedWatcher(source, memory, inbox, model=model), memory, inbox, requests


def test_article_extraction_ignores_navigation_and_heading():
    text = extract_article(article())
    assert BODY == text
    assert "Navigation" not in text
    assert "Release time" not in text


def test_initial_baseline_then_collect_reason_and_restart(tmp_path):
    links = [OLD]
    watcher, memory, inbox, requests = setup(tmp_path, links)
    first = run(watcher.poll_once())
    assert first.bootstrapped and first.baseline == 1
    assert requests == [FEED_URL]

    links.append(NEW)
    second = run(watcher.poll_once())
    new_id = validate_item(
        FeedItem("Federal Reserve issues FOMC statement", NEW, NEW, "Thu, 07 Nov 2024 19:00:00 GMT")
    ).event_id
    assert second.collected == 1 and second.reasoned == 0
    assert inbox.status(new_id) == "pending"
    stored = run(memory.get_event(new_id))
    assert stored is not None
    assert stored.source_timestamp == datetime(2024, 11, 7, 19, tzinfo=UTC)
    assert stored.received_timestamp > stored.source_timestamp
    assert stored.raw_text == BODY
    assert requests.count(NEW) == 1

    # The item is no longer in the current feed, but the durable pending record survives.
    links[:] = [OLD]
    model = FakeModel()
    restarted, _, restarted_inbox, new_requests = setup(tmp_path, links, model=model)
    third = run(restarted.poll_once())
    assert third.reasoned == 1 and model.calls == 1
    assert restarted_inbox.status(new_id) == "processed"
    assert new_requests == [FEED_URL]
    assert run(memory.decision_for_event(new_id)) is not None
    run(restarted.poll_once())
    assert model.calls == 1


def test_model_failure_quarantines_but_retains_raw_evidence(tmp_path):
    links = [OLD]
    model = FakeModel(response="{}")
    watcher, memory, inbox, _ = setup(tmp_path, links, model=model)
    run(watcher.poll_once())
    links.append(NEW)
    result = run(watcher.poll_once())
    new_id = validate_item(
        FeedItem("Federal Reserve issues FOMC statement", NEW, NEW, "Thu, 07 Nov 2024 19:00:00 GMT")
    ).event_id
    assert result.collected == 1 and result.quarantined == 1
    assert inbox.status(new_id) == "quarantined"
    assert run(memory.get_event(new_id)) is not None
    assert run(memory.decision_for_event(new_id)) is None
    run(watcher.poll_once())
    assert model.calls == 1


def test_spoofed_url_is_quarantined_without_article_request(tmp_path):
    links = [
        "https://www.federalreserve.gov.evil.test/newsevents/pressreleases/monetary20240918a.htm"
    ]
    watcher, _, _, requests = setup(tmp_path, links)
    result = run(watcher.poll_once())
    assert result.bootstrapped and result.baseline == 0 and result.quarantined == 1
    assert requests == [FEED_URL]


def test_duplicate_feed_link_has_one_article_request(tmp_path):
    links = [OLD]
    watcher, _, _, requests = setup(tmp_path, links)
    run(watcher.poll_once())
    links[:] = [OLD, NEW, NEW]
    result = run(watcher.poll_once())
    assert result.collected == 1
    assert requests.count(NEW) == 1


def test_crash_after_raw_commit_recovers_without_refetch(tmp_path):
    links = [OLD]
    watcher, memory, inbox, requests = setup(tmp_path, links, model=FakeModel())
    run(watcher.poll_once())
    item = validate_item(
        FeedItem("Federal Reserve issues FOMC statement", NEW, NEW, "Thu, 07 Nov 2024 19:00:00 GMT")
    )
    now = datetime.now(UTC)
    run(
        memory.record_event(
            Event(
                event_id=item.event_id,
                source="Federal Reserve",
                source_type="macro",
                source_reference=NEW,
                source_timestamp=item.source_timestamp,
                received_timestamp=now,
                raw_text=BODY,
                author="Federal Open Market Committee",
            )
        )
    )
    links.append(NEW)
    result = run(watcher.poll_once())
    assert result.reasoned == 1 and result.collected == 0
    assert inbox.status(item.event_id) == "processed"
    assert requests.count(NEW) == 0


def test_invalid_guid_or_timestamp_rejected():
    item = FeedItem(
        "Federal Reserve issues FOMC statement", OLD, NEW, "Wed, 18 Sep 2024 18:00:00 GMT"
    )
    try:
        validate_item(item)
    except SourceRejected:
        pass
    else:
        raise AssertionError("GUID mismatch must be rejected")

    with pytest.raises(SourceRejected, match="date disagrees"):
        validate_item(
            FeedItem(
                "Federal Reserve issues FOMC statement", OLD, OLD, "Thu, 07 Nov 2024 19:00:00 GMT"
            )
        )


def test_second_local_watcher_is_rejected(tmp_path):
    watcher, _, inbox, _ = setup(tmp_path, [OLD])
    with inbox.exclusive(), pytest.raises(FeedError, match="Another Fed watcher"):
        run(watcher.poll_once())


def test_temporary_feed_http_failure_is_retried():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503)
        return httpx.Response(200, content=feed([OLD]))

    source = FedMonetarySource(
        transport=httpx.MockTransport(handler), max_retries=1, min_request_interval_seconds=0
    )
    assert len(run(source.fetch_items())) == 1
    assert calls == 2


def test_article_redirect_is_quarantined_without_following(tmp_path):
    links = [OLD]
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        if str(request.url) == FEED_URL:
            return httpx.Response(200, content=feed(links))
        if str(request.url) == NEW:
            return httpx.Response(302, headers={"Location": "https://evil.test/"})
        raise AssertionError("Unexpected request")

    database = tmp_path / "fed.sqlite"
    memory = SQLiteThesisMemory(database)
    inbox = FedInbox(database)
    watcher = FedWatcher(
        FedMonetarySource(transport=httpx.MockTransport(handler), min_request_interval_seconds=0),
        memory,
        inbox,
    )
    run(watcher.poll_once())
    links.append(NEW)
    result = run(watcher.poll_once())
    new_id = validate_item(
        FeedItem("Federal Reserve issues FOMC statement", NEW, NEW, "Thu, 07 Nov 2024 19:00:00 GMT")
    ).event_id
    assert result.quarantined == 1
    assert inbox.status(new_id) == "quarantined"
    assert run(memory.get_event(new_id)) is None
    assert "https://evil.test/" not in requests
