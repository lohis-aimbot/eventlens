import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from eventlens.research.historical_context import (
    HistoricalContextInput,
    ReplayModelInput,
    build_historical_context,
)


def payload():
    return json.loads(Path("examples/synthetic-historical-context.json").read_text())


def build(data):
    return build_historical_context(HistoricalContextInput.model_validate(data))


def next_revision(data):
    record = copy.deepcopy(data["memory_history"][0])
    record["thesis"].update(
        revision=2,
        narrative="A later revision must not leak into the earlier decision.",
        updated_at="2025-01-29T19:01:01.900Z",
    )
    record["replay_committed_at"] = "2025-01-29T19:01:03Z"
    record["recorded_at"] = "2026-10-14T12:03:00Z"
    data["memory_history"].append(record)
    return record


def test_archive_clocks_preserved_but_not_passed_as_live_receipt():
    data = payload()
    parsed = HistoricalContextInput.model_validate(data)
    before = parsed.model_dump_json()
    context = build_historical_context(parsed)
    assert parsed.model_dump_json() == before
    assert context.provenance.original_events[0].received_timestamp.year == 2026
    assert context.provenance.market_captured_at.year == 2026
    assert context.clock.event_available_at == datetime(2025, 1, 29, 19, 1, tzinfo=UTC)
    assert context.clock.reasoning_deadline == datetime(2025, 1, 29, 19, 1, 4, 850000, tzinfo=UTC)
    assert context.model_input.event.assumed_available_at.year == 2025
    assert context.model_input.portfolio.equity == 10000
    assert not context.model_input.portfolio.positions
    assert not context.execution_authorized
    view = context.model_input.model_dump()
    assert "received_timestamp" not in view["event"]
    assert "captured_at" not in context.model_input.model_dump_json()
    assert "2026" not in context.model_input.model_dump_json()


def test_quote_selection_uses_assumed_delivery_and_never_next_quote():
    context = build(payload())
    quote = context.model_input.quote
    assert quote.observation.timestamp == datetime(2025, 1, 29, 19, 1, 1, 500000, tzinfo=UTC)
    assert quote.assumed_available_at == datetime(2025, 1, 29, 19, 1, 1, 750000, tzinfo=UTC)
    assert quote.assumed_available_at <= context.model_input.as_of
    data = payload()
    data["assumed_market_delay_ms"] = 0
    assert build(data).model_input.quote.observation.timestamp == context.model_input.as_of


def test_future_prices_and_events_cannot_change_the_model_input():
    original = build(payload())
    data = payload()
    data["window"]["observations"][-1].update(bid="9.0", ask="9.1")
    future = copy.deepcopy(data["archived_events"][0])
    future.update(
        event_id="future-fomc",
        source_timestamp="2025-03-19T18:00:00Z",
        source_reference="https://www.federalreserve.gov/newsevents/pressreleases/monetary20250319a.htm",
        raw_text="Future outcome must never enter this decision.",
    )
    data["archived_events"].append(future)
    changed = build(data)
    assert original.model_input == changed.model_input
    assert original.model_input_id == changed.model_input_id
    assert original.provenance.market_data_id != changed.provenance.market_data_id
    assert len(changed.provenance.original_events) == 1
    assert "Future outcome" not in changed.model_dump_json()


def test_memory_selection_uses_commit_time_not_backdated_update():
    data = payload()
    next_revision(data)
    original = build(payload())
    before = build(data)
    assert before.model_input.theses[0].revision == 1
    assert before.model_input_id == original.model_input_id
    assert before.model_input.selected_revision_count == 1
    data["portfolio_reasoning_start"] = "2025-01-29T19:01:03Z"
    after = build(data)
    assert after.model_input.theses[0].revision == 2
    assert after.model_input.selected_revision_count == 2
    assert (
        after.provenance.selected_memory_records[0].replay_committed_at == after.model_input.as_of
    )


def test_late_actual_recording_is_separate_from_hypothetical_commit():
    data = payload()
    data["memory_history"][0]["recorded_at"] = "2026-10-15T12:00:00Z"
    assert build(data).model_input_id == build(payload()).model_input_id


def test_empty_memory_is_valid_and_expired_or_invalidated_beliefs_remain_visible():
    data = payload()
    data["memory_history"] = []
    assert build(data).model_input.theses == ()
    assert build(data).model_input.selected_revision_count == 0
    data = payload()
    data["memory_history"][0]["thesis"].update(
        status="invalidated", review_at="2025-01-29T19:01:01Z"
    )
    assert build(data).model_input.theses[0].status == "invalidated"


@pytest.mark.parametrize("cutoff", ["2025-01-29T19:00:59Z", "2025-01-29T19:01:05Z"])
def test_cutoff_must_fit_pinned_scenario(cutoff):
    data = payload()
    data["portfolio_reasoning_start"] = cutoff
    with pytest.raises(ValidationError, match="budget"):
        build(data)


@pytest.mark.parametrize("age_ms", [2000, 2001])
def test_quote_age_boundary_checks_observation_not_only_delivery(age_ms):
    data = payload()
    quote = data["window"]["observations"][0]
    quote["timestamp"] = "2025-01-29T19:01:00Z" if age_ms == 2000 else ("2025-01-29T19:00:59.999Z")
    data["window"]["observations"] = [quote]
    data["assumed_market_delay_ms"] = age_ms
    if age_ms == 2000:
        assert build(data).model_input.quote.assumed_available_at == build(data).model_input.as_of
    else:
        with pytest.raises(ValueError, match="Stale"):
            build(data)


def test_missing_quotes_do_not_select_future_observations():
    data = payload()
    data["window"]["observations"] = data["window"]["observations"][2:]
    with pytest.raises(ValueError, match="No market observation"):
        build(data)


@pytest.mark.parametrize(
    ("field", "value"),
    [("event_id", "different"), ("source_time", "2025-01-29T19:00:01Z")],
)
def test_unrelated_window_rejected(field, value):
    data = payload()
    data["window"][field] = value
    with pytest.raises(ValueError, match="does not match"):
        build(data)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"run_id": "other-run"}, "different replay run"),
        ({"latency_scenario": "slow_reasoning_assumed"}, "different latency scenario"),
        ({"recorded_at": "2026-10-14T11:00:00Z"}, "archive receipt"),
        ({"replay_committed_at": "2025-01-29T19:01:00Z"}, "noncausal"),
    ],
)
def test_memory_wrapper_causality(change, message):
    data = payload()
    data["memory_history"][0].update(change)
    with pytest.raises(ValueError, match=message):
        build(data)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"revision": 2}, "complete unique chain"),
        ({"supporting_event_ids": ["unknown"]}, "unknown archived evidence"),
        ({"supporting_event_ids": []}, "distinct evidence"),
        ({"contradicting_event_ids": ["synthetic-fomc-event"]}, "distinct evidence"),
        (
            {"created_at": "2025-01-29T19:00:59Z", "updated_at": "2025-01-29T19:00:59Z"},
            "unavailable at update",
        ),
        ({"created_at": "2025-01-29T19:00:59Z"}, "creation precedes"),
    ],
)
def test_memory_evidence_and_revision_checks(change, message):
    data = payload()
    data["memory_history"][0]["thesis"].update(change)
    with pytest.raises(ValueError, match=message):
        build(data)


def test_past_revision_cannot_cite_future_event():
    data = payload()
    future = copy.deepcopy(data["archived_events"][0])
    future.update(
        event_id="future-fomc",
        source_timestamp="2025-03-19T18:00:00Z",
        source_reference="https://www.federalreserve.gov/newsevents/pressreleases/monetary20250319a.htm",
    )
    data["archived_events"].append(future)
    data["memory_history"][0]["thesis"]["supporting_event_ids"].append("future-fomc")
    with pytest.raises(ValueError, match="unavailable at update"):
        build(data)


def test_live_2026_memory_is_not_imported_as_2025_memory():
    data = payload()
    record = data["memory_history"][0]
    record["thesis"].update(
        created_at="2026-10-14T12:01:00Z",
        updated_at="2026-10-14T12:01:00Z",
        review_at="2026-10-14T13:01:00Z",
    )
    record["replay_committed_at"] = "2026-10-14T12:01:01Z"
    assert build(data).model_input.theses == ()
    record["replay_committed_at"] = "2025-01-29T19:01:01Z"
    with pytest.raises(ValidationError, match="noncausal"):
        build(data)


def test_memory_persists_across_events_with_original_archive_provenance():
    data = payload()
    earlier = copy.deepcopy(data["archived_events"][0])
    earlier.update(
        event_id="earlier-fomc",
        source_timestamp="2024-11-07T19:00:00Z",
        source_reference="https://www.federalreserve.gov/newsevents/pressreleases/monetary20241107a.htm",
        raw_text="Invented earlier evidence.",
    )
    data["archived_events"].append(earlier)
    record = data["memory_history"][0]
    record["thesis"].update(
        created_at="2024-11-07T19:01:01Z",
        updated_at="2024-11-07T19:01:01Z",
        supporting_event_ids=["earlier-fomc"],
    )
    record["replay_committed_at"] = "2024-11-07T19:01:02Z"
    context = build(data)
    assert context.model_input.theses[0].supporting_event_ids == ("earlier-fomc",)
    assert {e.event_id for e in context.provenance.original_events} == {
        "earlier-fomc",
        "synthetic-fomc-event",
    }
    assert all(e.received_timestamp.year == 2026 for e in context.provenance.original_events)


@pytest.mark.parametrize("case", ["duplicate", "created", "updated", "committed", "recorded"])
def test_invalid_revision_chains_rejected(case):
    data = payload()
    record = next_revision(data)
    if case == "duplicate":
        data["memory_history"].append(copy.deepcopy(record))
    elif case == "created":
        record["thesis"]["created_at"] = "2025-01-29T19:01:00.900Z"
    elif case == "updated":
        record["thesis"]["updated_at"] = "2025-01-29T19:01:00.800Z"
    elif case == "committed":
        record["thesis"]["updated_at"] = "2025-01-29T19:01:00.900Z"
        record["replay_committed_at"] = "2025-01-29T19:01:01Z"
    else:
        record["recorded_at"] = "2026-10-14T12:01:00Z"
    with pytest.raises(ValueError, match="chain|inconsistent"):
        build(data)


@pytest.mark.parametrize("field", ["portfolio_reasoning_start", "assumed_market_delay_ms"])
def test_naive_clock_or_negative_delay_rejected(field):
    data = payload()
    data[field] = "2025-01-29T19:01:02" if field == "portfolio_reasoning_start" else -1
    with pytest.raises(ValidationError):
        build(data)


def test_utc_normalization_and_deterministic_input_identity():
    data = payload()
    data["portfolio_reasoning_start"] = "2025-01-30T03:01:02+08:00"
    assert build(data).model_input_id == build(payload()).model_input_id
    assert build(payload()) == build(payload())


@pytest.mark.parametrize("case", ["event", "scenario", "duplicate", "source"])
def test_bad_event_and_scenario_metadata_rejected(case):
    data = payload()
    if case == "event":
        data["current_event_id"] = "unknown"
    elif case == "scenario":
        data["latency_scenario"] = "unbudgeted"
    elif case == "duplicate":
        data["archived_events"].append(copy.deepcopy(data["archived_events"][0]))
    else:
        data["archived_events"][0]["source_type"] = "social"
    with pytest.raises(ValueError):
        build(data)


def test_unknown_outcome_fields_and_unchecked_copies_are_rejected():
    data = payload()
    data["future_return"] = 1.0
    with pytest.raises(ValidationError):
        build(data)
    parsed = HistoricalContextInput.model_validate(payload())
    copied = parsed.model_copy(update={"portfolio_reasoning_start": datetime(2025, 1, 29)})
    with pytest.raises(ValidationError):
        build_historical_context(copied)
    view = build(payload()).model_input.model_dump(mode="json")
    view["future_label"] = 0.5
    with pytest.raises(ValidationError):
        ReplayModelInput.model_validate(view)


def test_model_view_rejects_future_quote_or_revision():
    view = build(payload()).model_input.model_dump(mode="json")
    view["quote"]["assumed_available_at"] = "2025-01-29T19:01:03Z"
    with pytest.raises(ValidationError, match="future information"):
        ReplayModelInput.model_validate(view)
