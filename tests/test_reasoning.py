import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from eventlens.contracts import Event
from eventlens.deepseek import DeepSeekClient, ModelCallError, ModelResponse
from eventlens.reasoning import MemoryReasoner
from eventlens.sqlite_memory import SQLiteThesisMemory


def run(coro):
    return asyncio.run(coro)


def event(key: str, text: str) -> Event:
    received = datetime.now(UTC) - timedelta(seconds=1)
    return Event(
        event_id=key,
        source="test-adapter",
        source_type="news",
        source_reference=key,
        source_timestamp=received,
        received_timestamp=received,
        raw_text=text,
    )


def proposal(action: str, quote: str, *, thesis_id: str | None = None,
             role: str = "support", status: str = "active") -> str:
    return json.dumps({
        "action": action,
        "thesis_id": thesis_id,
        "status": status,
        "narrative": "Supply disruption remains under review",
        "instruments": ["USO"],
        "invalidation_conditions": ["Verified restoration"],
        "confidence": 0.6,
        "evidence_role": role,
        "evidence_quote": quote,
        "reason": "The new report changes the supply thesis",
    })


class FakeClient:
    provider = "fixture"
    model = "fixture-v1"

    def __init__(self, *responses: str):
        self.responses = list(responses)
        self.calls = 0

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls += 1
        assert "No BUY/SELL" in system
        assert "received_timestamp" in user
        return ModelResponse(content=self.responses.pop(0), input_tokens=40, output_tokens=20)


def test_create_update_and_duplicate_without_extra_paid_call(tmp_path):
    store = SQLiteThesisMemory(tmp_path / "memory.sqlite")
    first = event("e1", "Pipeline shutdown confirmed by operator")
    client = FakeClient(proposal("create", "Pipeline shutdown confirmed"))
    reasoner = MemoryReasoner(client, store)
    initial = run(reasoner.process(first))
    thesis = initial.decision.thesis_changes[0].thesis
    assert initial.memory_version == 1
    assert initial.decision.target is None
    assert initial.input_tokens == 40
    assert thesis.supporting_event_ids == ("e1",)
    assert thesis.revision == 1

    repeated = run(reasoner.process(first))
    assert repeated.reused is True
    assert repeated.decision.decision_id == initial.decision.decision_id
    assert client.calls == 1

    second = event("e2", "Operator reports verified restoration")
    client.responses.append(proposal("update", "verified restoration", thesis_id=thesis.thesis_id,
                                     role="contradict", status="invalidated"))
    updated = run(reasoner.process(second))
    changed = updated.decision.thesis_changes[0].thesis
    assert updated.memory_version == 2
    assert changed.revision == 2
    assert changed.status == "invalidated"
    assert changed.supporting_event_ids == ("e1",)
    assert changed.contradicting_event_ids == ("e2",)
    assert [row.thesis.revision for row in run(store.history(thesis.thesis_id))] == [1, 2]


def test_ignore_records_decision_without_thesis_change(tmp_path):
    store = SQLiteThesisMemory(tmp_path / "memory.sqlite")
    response = json.dumps({"action": "ignore", "thesis_id": None, "status": None,
                           "narrative": None, "instruments": None,
                           "invalidation_conditions": None, "confidence": None,
                           "evidence_role": None, "evidence_quote": None,
                           "reason": "Irrelevant"})
    result = run(MemoryReasoner(FakeClient(response), store).process(event("e1", "Sports score")))
    assert result.decision.thesis_changes == ()
    assert run(store.snapshot(as_of=datetime.now(UTC))).theses == ()


@pytest.mark.parametrize("response,match", [
    ("not json", "JSON"),
    (proposal("create", "fabricated words"), "absent"),
    (proposal("update", "Pipeline shutdown", thesis_id="nonexistent"), "unknown"),
])
def test_invalid_proposal_keeps_raw_event_but_no_belief(tmp_path, response, match):
    store = SQLiteThesisMemory(tmp_path / "memory.sqlite")
    item = event("e1", "Pipeline shutdown confirmed")
    with pytest.raises(ValueError, match=match):
        run(MemoryReasoner(FakeClient(response), store).process(item))
    assert run(store.get_event("e1")) == item
    assert run(store.snapshot(as_of=datetime.now(UTC))).memory_version == 0


def test_deepseek_request_format_and_usage():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = json.loads(request.content)
        assert body["model"] == "deepseek-flash"
        assert body["response_format"] == {"type": "json_object"}
        assert body["thinking"] == {"type": "disabled"}
        return httpx.Response(200, json={"choices": [{"finish_reason": "stop",
                                                   "message": {"content": '{"action":"ignore"}'}}],
                                          "usage": {"prompt_tokens": 14, "completion_tokens": 8}})

    client = DeepSeekClient("test-secret", transport=httpx.MockTransport(handler))
    result = run(client.complete_json(system="JSON", user="event"))
    assert result.input_tokens == 14
    assert result.output_tokens == 8
    assert len(seen) == 1
    assert seen[0].headers["Authorization"] == "Bearer test-secret"


@pytest.mark.parametrize("status,attempts", [(401, 1), (429, 2)])
def test_deepseek_rejects_http_errors_without_leaking_body(status, attempts):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, text="secret from provider")

    client = DeepSeekClient("test-secret", transport=httpx.MockTransport(handler), max_retries=1)
    with pytest.raises(ModelCallError) as captured:
        run(client.complete_json(system="JSON", user="event"))
    assert calls == attempts
    assert "secret from provider" not in str(captured.value)


def test_deepseek_rejects_truncated_json():
    client = DeepSeekClient("test-secret", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json={"choices": [{"finish_reason": "length",
                                                               "message": {"content": "{}"}}],
                                                   "usage": {"prompt_tokens": 1,
                                                             "completion_tokens": 1}})))
    with pytest.raises(ModelCallError, match="incomplete"):
        run(client.complete_json(system="JSON", user="event"))
