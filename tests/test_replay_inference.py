import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from eventlens.decision_protocol import ThesisReference
from eventlens.deepseek import DeepSeekClient, JSONRequestSettings, ModelCallError, ModelResponse
from eventlens.replay_inference import (
    ReplayInferenceCase,
    ReplayInferenceConfig,
    ReplayInferenceRunner,
    check_replay_proposal,
    freeze_manifest,
)


class FakeClock:
    def __init__(self):
        self.offset_ms = 0

    def now(self):
        return datetime(2027, 1, 1, tzinfo=UTC) + timedelta(milliseconds=self.offset_ms)

    def monotonic_ns(self):
        return self.offset_ms * 1_000_000

    async def sleep(self, seconds):
        self.offset_ms += int(seconds * 1000)


def case_data():
    data = json.loads(Path("examples/synthetic-inference-case.json").read_text())
    start = datetime(2025, 1, 29, 19, 0, 59, tzinfo=UTC)
    data["window"]["observations"] = [
        {
            "timestamp": (start + timedelta(milliseconds=i * 500)).isoformat(),
            "bid": "1.1000",
            "ask": "1.1002",
        }
        for i in range(125)
    ]
    return data


def memory_response(action="create", **changes):
    data = {
        "action": action,
        "thesis_id": None,
        "status": "active",
        "narrative": "Fixture policy thesis",
        "instruments": ["EUR/USD"],
        "invalidation_conditions": ["Verified policy reversal"],
        "confidence": 0.6,
        "evidence_role": "support",
        "evidence_quote": "Policy stance remains restrictive.",
        "reason": "Fixture evidence supports a policy hypothesis",
    }
    if action == "ignore":
        data = {key: None for key in data}
        data.update(action="ignore", reason="Evidence insufficient")
    data.update(changes)
    return json.dumps(data)


def portfolio_response(context, **changes):
    theses = context["theses"]
    data = {
        "target_direction": "short" if theses else "flat",
        "thesis_references": [
            {"thesis_id": t["thesis_id"], "revision": t["revision"]} for t in theses
        ],
        "evidence_quotes": ["Policy stance remains restrictive."],
        "rationale": "Fixture response, not predictive evidence",
        "confidence": 0.6,
        "invalidation_conditions": ["Policy reversal"] if theses else [],
    }
    data.update(changes)
    return json.dumps(data)


class FakeClient:
    provider = "fixture"
    model = "fixture-model"
    request_settings = JSONRequestSettings(timeout_seconds=45, max_retries=0)

    def __init__(self, clock, *, durations=(1000, 1000), memory=None, portfolio=None, errors=None):
        self.clock = clock
        self.durations = list(durations)
        self.memory = memory
        self.portfolio = portfolio
        self.errors = dict(errors or {})
        self.inputs = []

    async def complete_json(self, *, system, user):
        data = json.loads(user)
        assert "source_timestamp" in user
        assert "received_timestamp" not in user and "captured_at" not in user
        assert "observations" not in user and "future_return" not in user
        self.inputs.append(data["context"])
        call_number = len(self.inputs)
        self.clock.offset_ms += self.durations[call_number - 1]
        if call_number in self.errors:
            raise self.errors[call_number]
        if "No BUY/SELL" in system:
            content = self.memory if self.memory is not None else memory_response()
        else:
            content = (
                self.portfolio
                if self.portfolio is not None
                else (portfolio_response(data["context"]))
            )
        return ModelResponse(
            content=content,
            input_tokens=100,
            output_tokens=60,
            model_id="fixture-served-model",
            system_fingerprint="fp-fixture",
        )


def run_case(data=None, **client_kwargs):
    case = ReplayInferenceCase.model_validate(data or case_data())
    clock = FakeClock()
    client = FakeClient(clock, **client_kwargs)
    config = ReplayInferenceConfig(provider=client.provider, requested_model=client.model)
    manifest = freeze_manifest(case, config).model_copy(update={"created_at": clock.now()})
    result = asyncio.run(ReplayInferenceRunner(client, clock=clock).run(case, manifest))
    return result, client


def test_two_calls_use_updated_memory_and_measured_cutoffs_without_execution():
    result, client = run_case()
    assert result.status == "validated_proposal"
    assert len(client.inputs) == len(result.attempts) == 2
    assert client.inputs[0]["theses"] == []
    assert client.inputs[1]["theses"][0]["revision"] == 1
    assert result.generated_memory[0].replay_committed_at == datetime(
        2025, 1, 29, 19, 1, 1, tzinfo=UTC
    )
    assert result.generated_memory[0].recorded_at.year == 2027
    assert (
        result.portfolio_context.model_input.as_of == result.generated_memory[0].replay_committed_at
    )
    assert result.hypothetical_completed_at == datetime(2025, 1, 29, 19, 1, 2, tzinfo=UTC)
    assert result.total_processing_ms == 2000
    assert result.proposal_check.proposed_quantity_eur == -1000
    assert result.proposal_check.context_id == result.portfolio_context.model_input_id
    assert not result.proposal_check.execution_authorized and not result.execution_authorized
    assert result.attempts[0].response.model_id == "fixture-served-model"
    assert result.attempts[1].response.system_fingerprint == "fp-fixture"
    assert result.manifest.config.requested_model == "fixture-model"


def test_retry_backoff_counts_in_budget_and_each_attempt_has_audit():
    result, client = run_case(
        durations=(250, 250, 500), errors={1: ModelCallError("temporary", retryable=True)}
    )
    assert result.status == "validated_proposal"
    assert len(client.inputs) == 3
    assert [a.outcome for a in result.attempts] == ["retryable_error", "response", "response"]
    assert [a.attempt for a in result.attempts] == [1, 2, 1]
    assert result.total_processing_ms == 1500  # Includes 500 ms retry sleep.
    assert result.attempts[0].input_id == result.attempts[1].input_id
    assert result.portfolio_context.model_input.as_of == datetime(2025, 1, 29, 19, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    "error", [ModelCallError("fatal"), ModelCallError("retry", retryable=True)]
)
def test_provider_failures_are_not_default_flat_or_unknown_zero_cost(error):
    result, client = run_case(durations=(500, 500), errors={1: error, 2: error})
    expected = 2 if error.retryable else 1
    assert result.status == "model_failed"
    assert len(client.inputs) == expected
    assert result.proposal is None and result.generated_memory == ()
    assert all(a.response is None for a in result.attempts)  # Token charges unknown.


@pytest.mark.parametrize(
    "response",
    [
        "not json",
        memory_response(evidence_quote="fabricated"),
        memory_response(evidence_quote="policy stance remains restrictive."),
        memory_response(instruments=["BTC/USD"]),
        memory_response("update", thesis_id="unknown"),
        memory_response("create", target={"BUY": "EUR/USD"}),
    ],
)
def test_invalid_memory_output_stops_before_portfolio_call(response):
    result, client = run_case(memory=response)
    assert result.status == "invalid_output"
    assert len(client.inputs) == 1
    assert result.attempts[-1].outcome == "invalid_output"
    assert result.attempts[-1].response.input_tokens == 100
    assert result.generated_memory == () and result.proposal is None


def test_ignore_still_requires_an_explicit_valid_portfolio_response():
    result, _ = run_case(memory=memory_response("ignore"))
    assert result.status == "validated_proposal" and result.proposal.target_direction == "flat"
    assert result.generated_memory == () and len(result.attempts) == 2
    failed, _ = run_case(memory=memory_response("ignore"), portfolio="not json")
    assert failed.status == "invalid_output" and failed.proposal is None


def test_memory_over_budget_stops_without_stamp_or_portfolio_call():
    data = case_data()
    data["latency_scenario"] = "poll_plus_reasoning_assumed"
    result, client = run_case(data, durations=(5000,))
    assert result.status == "budget_exceeded" and len(client.inputs) == 1
    assert result.memory_proposal is not None and result.generated_memory == ()
    assert result.portfolio_context is None


def test_portfolio_over_budget_keeps_proposal_but_cannot_pass():
    data = case_data()
    data["latency_scenario"] = "poll_plus_reasoning_assumed"
    result, _ = run_case(data, durations=(2000, 3000))
    assert result.status == "budget_exceeded" and result.proposal is not None
    assert result.proposal_check is not None and not result.proposal_check.execution_authorized
    assert result.hypothetical_completed_at > result.portfolio_context.clock.reasoning_deadline
    diagnostics = {d.scenario: d for d in result.budget_diagnostics}
    assert not diagnostics["poll_plus_reasoning_assumed"].fits_processing_budget
    assert diagnostics["slow_reasoning_assumed"].fits_processing_budget
    assert diagnostics["slow_reasoning_assumed"].scope == "processing_only_not_a_replayed_decision"


def test_exact_budget_boundary_is_allowed_and_retry_crossing_budget_stops():
    data = case_data()
    data["latency_scenario"] = "poll_plus_reasoning_assumed"
    passed, _ = run_case(data, durations=(2000, 2850))
    assert passed.status == "validated_proposal" and passed.total_processing_ms == 4850
    failed, client = run_case(
        data, durations=(4800,), errors={1: ModelCallError("temporary", retryable=True)}
    )
    assert failed.status == "budget_exceeded" and len(client.inputs) == 1
    assert failed.total_processing_ms == 5300


def test_context_rejection_does_not_spend_or_read_next_quote():
    data = case_data()
    data["window"]["observations"] = data["window"]["observations"][10:]
    result, client = run_case(data)
    assert result.status == "context_rejected" and client.inputs == []
    data = case_data()
    data["archived_events"][0]["received_timestamp"] = "2028-01-01T00:00:00Z"
    result, client = run_case(data)
    assert result.reason == "archive_not_yet_captured" and client.inputs == []


def test_future_prices_and_extra_future_events_are_never_sent_to_model():
    first, first_client = run_case()
    data = case_data()
    data["window"]["observations"][-1].update(bid="9.0", ask="9.1")
    later = dict(data["archived_events"][0])
    later.update(
        event_id="future-event",
        source_timestamp="2025-03-19T18:00:00Z",
        source_reference="https://www.federalreserve.gov/newsevents/pressreleases/monetary20250319a.htm",
        raw_text="Future outcome should be excluded",
    )
    data["archived_events"].append(later)
    changed, client = run_case(data)
    assert changed.manifest.case_id != first.manifest.case_id
    assert client.inputs == first_client.inputs
    assert changed.portfolio_context.model_input_id == first.portfolio_context.model_input_id


def test_unknown_thesis_directional_evidence_or_model_controls_are_rejected():
    result, _ = run_case()
    proposal = result.proposal
    for change in (
        {"evidence_quotes": ("not in event",)},
        {"thesis_references": (ThesisReference(thesis_id="unknown", revision=1),)},
    ):
        bad = proposal.model_copy(update=change)
        with pytest.raises(ValueError):
            check_replay_proposal(bad, result.portfolio_context)
    failed, _ = run_case(portfolio=json.dumps({"BUY": "EUR/USD", "quantity": 1000000}))
    assert failed.status == "invalid_output" and failed.proposal_check is None


def test_run_and_manifest_cannot_silently_change_inputs_prompts_or_client_settings():
    case = ReplayInferenceCase.model_validate(case_data())
    clock = FakeClock()
    client = FakeClient(clock)
    config = ReplayInferenceConfig(provider=client.provider, requested_model=client.model)
    manifest = freeze_manifest(case, config).model_copy(update={"created_at": clock.now()})
    for modified in (
        manifest.model_copy(update={"memory_prompt_hash": "different"}),
        manifest.model_copy(update={"case_id": "different"}),
    ):
        with pytest.raises(ValueError, match="mismatch"):
            asyncio.run(ReplayInferenceRunner(client, clock=clock).run(case, modified))
    assert client.inputs == []
    assert freeze_manifest(case, config).experiment_id == manifest.experiment_id
    client.request_settings = client.request_settings.model_copy(update={"max_retries": 2})
    with pytest.raises(ValueError, match="mismatch"):
        asyncio.run(ReplayInferenceRunner(client, clock=clock).run(case, manifest))


def test_clock_anomaly_retains_attempt_and_cannot_pass_latency_diagnostics():
    result, client = run_case(durations=(-100,))
    assert result.status == "clock_anomaly" and len(client.inputs) == 1
    assert result.attempts[0].outcome == "clock_anomaly"
    assert result.attempts[0].response is not None
    assert not any(d.fits_processing_budget for d in result.budget_diagnostics)


def test_timeout_is_recorded_and_bounded():
    result, client = run_case(durations=(500, 500), errors={1: TimeoutError(), 2: TimeoutError()})
    assert result.status == "model_failed" and len(client.inputs) == 2
    assert all(a.outcome == "retryable_error" for a in result.attempts)


def test_deepseek_exposes_settings_and_preserves_nonretryable_auth_failure():
    client = DeepSeekClient(
        "test-secret",
        max_retries=0,
        transport=httpx.MockTransport(lambda request: httpx.Response(401, text="secret")),
    )
    assert client.request_settings == JSONRequestSettings(timeout_seconds=45, max_retries=0)
    with pytest.raises(ModelCallError) as captured:
        asyncio.run(client.complete_json(system="json", user="{}"))
    assert not captured.value.retryable and "secret" not in str(captured.value)
    with pytest.raises(ValueError, match="hidden"):
        ReplayInferenceConfig(
            client_settings=JSONRequestSettings(timeout_seconds=45, max_retries=1)
        )


def test_exported_config_is_exact_and_paid_cli_requires_explicit_opt_in(tmp_path, monkeypatch):
    from eventlens.inference_cli import main

    assert json.loads(Path("configs/replay-inference-v1.json").read_text()) == (
        ReplayInferenceConfig().model_dump(mode="json")
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "eventlens-infer",
            "run",
            "examples/synthetic-inference-case.json",
            "--output",
            str(tmp_path),
        ],
    )
    with pytest.raises(SystemExit) as captured:
        main()
    assert captured.value.code == 2


def test_cli_plan_without_credentials_and_existing_run_directory_never_calls_model(
    tmp_path, monkeypatch, capsys
):
    from eventlens.inference_cli import main

    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.setattr(
        "sys.argv", ["eventlens-infer", "plan", "examples/synthetic-inference-case.json"]
    )
    main()
    assert json.loads(capsys.readouterr().out)["config"]["client_settings"]["max_retries"] == 0
    monkeypatch.setenv("DEEPSEEK_API_KEY", "fixture-key")
    monkeypatch.setattr(
        "sys.argv",
        [
            "eventlens-infer",
            "run",
            "examples/synthetic-inference-case.json",
            "--call-model",
            "--output",
            str(tmp_path),
        ],
    )
    with pytest.raises(FileExistsError):
        main()


def test_quote_gap_after_memory_update_stops_before_portfolio_call():
    data = case_data()
    data["window"]["observations"] = data["window"]["observations"][:2]
    result, client = run_case(data, durations=(3000,))
    assert result.status == "context_rejected" and len(client.inputs) == 1
    assert result.generated_memory and result.portfolio_context is None


def test_large_input_is_rejected_without_spending():
    data = case_data()
    data["archived_events"][0]["raw_text"] = "x" * 25000
    result, client = run_case(data)
    assert result.reason == "model_input_size_exceeded" and client.inputs == []


def test_nonflat_requires_active_unexpired_thesis_but_flat_may_cite_invalidated_memory():
    from eventlens.decision_protocol import ResearchPositionProposal

    result, _ = run_case()
    context = result.portfolio_context
    thesis = context.model_input.theses[0]
    for changes in ({"status": "invalidated"}, {"review_at": context.model_input.as_of}):
        altered = context.model_copy(
            update={
                "model_input": context.model_input.model_copy(
                    update={"theses": (thesis.model_copy(update=changes),)}
                )
            }
        )
        with pytest.raises(ValueError, match="ineligible"):
            check_replay_proposal(result.proposal, altered)
        flat = ResearchPositionProposal.model_validate(
            {**result.proposal.model_dump(), "target_direction": "flat"}
        )
        assert check_replay_proposal(flat, altered).proposed_quantity_eur == 0
