import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from eventlens.deepseek import JSONRequestSettings, ModelCallError, ModelResponse
from eventlens.research.replay_inference import MEMORY_PROMPT
from eventlens.research.series_replay import SeriesBlocked, SeriesPlan, SeriesRunner, SeriesStore


class Clock:
    def __init__(self):
        self.ms = 0

    def now(self):
        return datetime(2027, 1, 1, tzinfo=UTC) + timedelta(milliseconds=self.ms)

    def monotonic_ns(self):
        return self.ms * 1_000_000

    async def sleep(self, seconds):
        self.ms += int(seconds * 1000)


class Crash(BaseException):
    pass


class Client:
    provider = "deepseek"
    model = "deepseek-flash"
    request_settings = JSONRequestSettings(timeout_seconds=45, max_retries=0)

    def __init__(self, clock, *, crash_at=None, invalid_at=None, fail_at=None):
        self.clock = clock
        self.crash_at, self.invalid_at, self.fail_at = crash_at, invalid_at, fail_at
        self.inputs = []

    async def complete_json(self, *, system, user):
        context = json.loads(user)["context"]
        self.inputs.append(context)
        self.clock.ms += 500
        number = len(self.inputs)
        if number == self.crash_at:
            raise Crash("simulated uncertain paid call")
        if number == self.fail_at:
            raise ModelCallError("fixture failure")
        if number == self.invalid_at:
            content = "malformed"
        elif system == MEMORY_PROMPT:
            existing = context["theses"]
            content = json.dumps(
                {
                    "action": "update" if existing else "create",
                    "thesis_id": existing[0]["thesis_id"] if existing else None,
                    "status": "active",
                    "narrative": "Fixture-only evolving policy belief",
                    "instruments": ["EUR/USD"],
                    "invalidation_conditions": ["Contradictory evidence"],
                    "confidence": 0.3,
                    "evidence_role": "support",
                    "evidence_quote": "Fixture policy evidence.",
                    "reason": "Fixture chronological memory update",
                }
            )
        else:
            content = json.dumps(
                {
                    "target_direction": "flat",
                    "thesis_references": [],
                    "evidence_quotes": ["Fixture policy evidence."],
                    "rationale": "Fixture abstention",
                    "confidence": 0.3,
                    "invalidation_conditions": [],
                }
            )
        return ModelResponse(
            content=content, input_tokens=100, output_tokens=30, model_id=self.model
        )


def plan():
    return SeriesPlan.model_validate_json(Path("examples/synthetic-series-plan.json").read_bytes())


def execute(store, client, clock, selected=None, **kwargs):
    return asyncio.run(SeriesRunner(store, client, clock=clock).run(selected or plan(), **kwargs))


def test_complete_chronological_memory_chain_and_zero_call_repeat(tmp_path):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock)
    report = execute(store, client, clock)
    assert len(client.inputs) == report["api_calls_reserved"] == 16
    assert report["stored_revisions"] == 8
    assert all(row["direction"] == "flat" for row in report["episodes"])
    assert [row["input_revisions"] for row in report["episodes"]] == list(range(8))
    assert [row["memory_action"] for row in report["episodes"]] == ["create"] + ["update"] * 7
    records = store.history(plan().run_id)
    assert [r.thesis.revision for r in records] == list(range(1, 9))
    for index in range(8):
        memory_input, portfolio_input = client.inputs[index * 2 : index * 2 + 2]
        assert portfolio_input["theses"][0]["revision"] == index + 1
        assert portfolio_input["theses"][0]["supporting_event_ids"] == [
            e.event_id for e in plan().episodes[: index + 1]
        ]
        if index:
            assert memory_input["theses"][0]["revision"] == index
        assert all(
            datetime.fromisoformat(t["updated_at"].replace("Z", "+00:00"))
            <= datetime.fromisoformat(memory_input["as_of"].replace("Z", "+00:00"))
            for t in memory_input["theses"]
        )
    assert execute(store, client, clock) == report
    assert len(client.inputs) == 16
    assert not report["execution_authorized"]
    store.close()


def test_clean_pause_reopen_and_resume_without_rebilling_completed_events(tmp_path):
    path = tmp_path / "series.sqlite"
    store = SeriesStore(path)
    clock = Clock()
    client = Client(clock)
    partial = execute(store, client, clock, max_events=2)
    assert len(client.inputs) == 4 and partial["stored_revisions"] == 2
    store.close()
    store = SeriesStore(path)
    resumed = Client(clock)
    report = execute(store, resumed, clock)
    assert len(resumed.inputs) == 12 and report["api_calls_reserved"] == 16
    assert resumed.inputs[0]["theses"][0]["revision"] == 2
    assert report["stored_revisions"] == 8
    store.close()


def test_uncertain_call_blocks_resume_until_explicit_abandon(tmp_path):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock, crash_at=3)
    with pytest.raises(Crash):
        execute(store, client, clock)
    assert store.report(plan().run_id)["unknown_calls"] == 1
    resumed = Client(clock)
    with pytest.raises(SeriesBlocked, match="Interrupted"):
        execute(store, resumed, clock)
    assert resumed.inputs == []
    store.abandon_interrupted(plan().run_id, plan().episodes[1].event_id)
    report = execute(store, resumed, clock)
    assert report["episodes"][1]["status"] == "interrupted_unknown"
    assert report["episodes"][1]["direction"] is None
    assert report["unknown_calls"] == 1  # Unknown billing remains unknown.
    assert len(resumed.inputs) == 12
    assert resumed.inputs[0]["theses"][0]["revision"] == 1
    store.close()


def test_failure_retained_and_valid_memory_survives_portfolio_failure(tmp_path):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock, invalid_at=4)
    report = execute(store, client, clock)
    assert report["episodes"][1]["status"] == "invalid_output"
    assert report["episodes"][1]["direction"] is None
    assert report["episodes"][1]["generated_revisions"] == 1
    assert client.inputs[4]["theses"][0]["revision"] == 2
    assert report["stored_revisions"] == 8
    assert report["known_input_tokens"] == 1600
    store.close()


def test_missing_data_is_an_explicit_row_and_does_not_spend(tmp_path):
    data = plan().model_dump(mode="json")
    data["episodes"][0]["window"] = None
    selected = SeriesPlan.model_validate(data)
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock)
    report = execute(store, client, clock, selected)
    assert report["episodes"][0]["status"] == "data_unavailable"
    assert len(client.inputs) == 14 and report["stored_revisions"] == 7
    assert client.inputs[0]["theses"] == []
    store.close()


def test_series_call_limit_prevents_extra_paid_requests(tmp_path):
    data = plan().model_dump(mode="json")
    data["max_api_calls"] = 1
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock)
    report = execute(store, client, clock, SeriesPlan.model_validate(data))
    assert len(client.inputs) == report["api_calls_reserved"] == 1
    assert report["episodes"][0]["status"] == "model_failed"
    assert all(row["status"] == "call_limit_reached" for row in report["episodes"][1:])
    store.close()


def test_immutable_manifest_blocks_changed_input_before_any_calls(tmp_path):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock)
    execute(store, client, clock, max_events=1)
    data = plan().model_dump(mode="json")
    data["episodes"][-1]["event"]["raw_text"] = "Changed later archive"
    changed = SeriesPlan.model_validate(data)
    with pytest.raises(ValueError, match="different data"):
        execute(store, client, clock, changed)
    assert len(client.inputs) == 2
    store.close()


def test_registry_lock_prevents_a_second_runner(tmp_path):
    path = tmp_path / "series.sqlite"
    first, second = SeriesStore(path), SeriesStore(path)
    clock = Clock()
    client = Client(clock)
    with first.lock():
        with pytest.raises(SeriesBlocked, match="Another runner"):
            execute(second, client, clock)
    assert client.inputs == []
    first.close()
    second.close()


@pytest.mark.parametrize("change", ["remove", "reorder", "catalog", "source", "window", "scenario"])
def test_incomplete_or_inconsistent_catalogs_are_rejected(change):
    data = plan().model_dump(mode="json")
    if change == "remove":
        data["episodes"].pop()
    elif change == "reorder":
        data["episodes"].reverse()
    elif change == "catalog":
        data["catalog"]["events"].pop()
        data["episodes"].pop()
    elif change == "source":
        data["episodes"][0]["event"]["source_timestamp"] = "2025-01-29T19:00:01Z"
    elif change == "window":
        data["episodes"][0]["window"]["event_id"] = "other"
    else:
        data["latency_scenario"] = "unbudgeted"
    with pytest.raises(ValueError):
        SeriesPlan.model_validate(data)


def test_readonly_report_requires_existing_registry_and_cannot_mutate(tmp_path):
    path = tmp_path / "series.sqlite"
    store = SeriesStore(path)
    clock = Clock()
    execute(store, Client(clock), clock, max_events=1)
    report = store.report(plan().run_id)
    store.close()
    before = path.read_bytes()
    readonly = SeriesStore(path, readonly=True)
    assert readonly.report(plan().run_id) == report
    with pytest.raises(ValueError, match="Read-only"):
        with readonly.lock():
            pass
    readonly.close()
    assert path.read_bytes() == before


def test_cli_plan_is_free_and_run_requires_opt_in(tmp_path, monkeypatch, capsys):
    from eventlens.series_cli import main

    database = tmp_path / "not-created.sqlite"
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        ["series", "--database", str(database), "plan", "examples/synthetic-series-plan.json"],
    )
    main()
    assert json.loads(capsys.readouterr().out)["run_id"] == "synthetic-series"
    assert not database.exists()
    monkeypatch.setattr("sys.argv", ["series", "run", "examples/synthetic-series-plan.json"])
    with pytest.raises(SystemExit):
        main()


def test_failed_memory_does_not_create_default_flat_or_revision(tmp_path):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock, invalid_at=1)
    report = execute(store, client, clock)
    assert report["episodes"][0]["status"] == "invalid_output"
    assert report["episodes"][0]["direction"] is None
    assert report["episodes"][0]["generated_revisions"] == 0
    assert report["stored_revisions"] == 7
    assert client.inputs[1]["theses"] == []
    store.close()


def test_crash_after_known_replies_still_blocks_uncommitted_case(tmp_path, monkeypatch):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock)
    finish = store.finish_episode

    def crash_before_commit(*args, **kwargs):
        raise Crash("Local finalization failed after responses")

    monkeypatch.setattr(store, "finish_episode", crash_before_commit)
    with pytest.raises(Crash):
        execute(store, client, clock)
    assert store.report(plan().run_id)["api_calls_reserved"] == 2
    assert store.report(plan().run_id)["unknown_calls"] == 0
    assert store.history(plan().run_id) == ()
    monkeypatch.setattr(store, "finish_episode", finish)
    with pytest.raises(SeriesBlocked):
        execute(store, Client(clock), clock)
    store.abandon_interrupted(plan().run_id, plan().episodes[0].event_id)
    resumed = Client(clock)
    execute(store, resumed, clock)
    assert resumed.inputs[0]["theses"] == []
    store.close()


def test_provider_failure_retains_unknown_usage_and_continues(tmp_path):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock, fail_at=1)
    report = execute(store, client, clock)
    assert report["episodes"][0]["status"] == "model_failed"
    assert report["calls_without_usage"] == 1 and report["api_calls_reserved"] == 15
    assert report["known_input_tokens"] == 1400
    assert report["stored_revisions"] == 7
    store.close()


def test_completed_episode_cannot_be_overwritten_or_abandoned(tmp_path):
    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    execute(store, Client(clock), clock, max_events=1)
    with pytest.raises(ValueError, match="immutable"):
        store.finish_episode(plan().run_id, plan().episodes[0].event_id, "changed", "changed")
    with pytest.raises(ValueError, match="Only interrupted"):
        store.abandon_interrupted(plan().run_id, plan().episodes[0].event_id)
    store.close()


def test_result_and_memory_roll_back_together_on_commit_failure(tmp_path):
    import sqlite3

    from eventlens.research.replay_inference import (
        ReplayInferenceCase,
        ReplayInferenceRunner,
        freeze_manifest,
    )

    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = Client(clock)
    selected = plan()
    execute(store, client, clock, max_events=1)
    second = selected.episodes[1]
    case = ReplayInferenceCase(
        run_id=selected.run_id,
        current_event_id=second.event_id,
        archived_events=tuple(e.event for e in selected.episodes[:2]),
        window=second.window,
        latency_scenario=selected.latency_scenario,
        assumed_market_delay_ms=selected.assumed_market_delay_ms,
        memory_history=store.history(selected.run_id),
    )
    manifest = freeze_manifest(case).model_copy(update={"created_at": clock.now()})
    store.begin_episode(selected.run_id, second.event_id, manifest)
    result = asyncio.run(ReplayInferenceRunner(client, clock=clock).run(case, manifest))
    duplicate = result.model_copy(update={"generated_memory": result.generated_memory * 2})
    with pytest.raises(sqlite3.IntegrityError):
        store.finish_episode(
            selected.run_id, second.event_id, duplicate.status, duplicate.reason, duplicate
        )
    assert len(store.history(selected.run_id)) == 1
    assert store.episode(selected.run_id, second.event_id)["state"] == "running"
    assert store.episode(selected.run_id, second.event_id)["result"] is None
    store.close()


def test_cancelled_request_cannot_be_automatically_billed_again(tmp_path):
    from eventlens.research.replay_inference import ReplayInferenceConfig

    class CancelledClient(Client):
        request_settings = JSONRequestSettings(timeout_seconds=0.005, max_retries=0)

        async def complete_json(self, *, system, user):
            self.inputs.append(json.loads(user)["context"])
            await asyncio.sleep(1)
            raise AssertionError("Cancelled request should not finish")

    store = SeriesStore(tmp_path / "series.sqlite")
    clock = Clock()
    client = CancelledClient(clock)
    config = ReplayInferenceConfig(client_settings=client.request_settings)
    with pytest.raises(SeriesBlocked, match="unknown"):
        asyncio.run(SeriesRunner(store, client, clock=clock, config=config).run(plan()))
    assert len(client.inputs) == 1
    assert store.report(plan().run_id)["api_calls_reserved"] == 1
    assert store.report(plan().run_id)["unknown_calls"] == 1
    store.close()
