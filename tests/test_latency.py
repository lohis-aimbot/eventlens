from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from eventlens.contracts import AgentDecision, Event
from eventlens.latency import processing_latency

T = datetime(2026, 1, 1, tzinfo=UTC)


def event():
    return Event(
        event_id="e1",
        source="fixture",
        source_type="macro",
        source_reference="fixture",
        source_timestamp=T - timedelta(days=365),
        received_timestamp=T,
        raw_text="Historical input",
    )


def decision(**changes):
    values = dict(
        decision_id="d1",
        event_id="e1",
        based_on_memory_version=0,
        based_on_snapshot_id="memory-only",
        provider="fixture",
        model_version="fixture-v1",
        prompt_version="test",
        started_at=T + timedelta(milliseconds=100),
        completed_at=T + timedelta(milliseconds=2100),
        valid_until=T + timedelta(seconds=60),
        rationale="fixture",
    )
    return AgentDecision(**(values | changes))


def test_processing_latency_is_distinct_from_publisher_age():
    trace = processing_latency(event(), decision())
    assert trace.receipt_to_start_ms == 100
    assert trace.reasoning_ms == 2000
    assert trace.receipt_to_completion_ms == 2100
    assert trace.source_age_at_receipt_ms == 365 * 86400 * 1000
    assert trace.basis == "observed_process_timestamps"
    assert trace.model_dump(mode="json")["reasoning_ms"] == 2000


def test_latency_rejects_wrong_event_and_clock_regression():
    with pytest.raises(ValueError, match="trigger event"):
        processing_latency(event(), decision(event_id="wrong"))
    with pytest.raises(ValidationError, match="noncausal"):
        processing_latency(event(), decision(started_at=T - timedelta(seconds=1)))
