"""Foundation contracts only; these tests do not claim trading/risk behavior works."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from eventlens.contracts import (
    AgentContext,
    AgentDecision,
    Event,
    MarketSnapshot,
    MemorySnapshot,
    PortfolioSnapshot,
    TargetPortfolio,
    TargetPosition,
    Thesis,
    ThesisChange,
)

T = datetime(2026, 1, 1, tzinfo=UTC)


def sample_event(**changes):
    values = dict(
        event_id="e1",
        source="fixture",
        source_type="news",
        source_reference="fixture:1",
        source_timestamp=T,
        received_timestamp=T,
        raw_text="Synthetic supply disruption.",
    )
    return Event.model_validate(values | changes)


def sample_thesis(**changes):
    values = dict(
        thesis_id="t1",
        revision=1,
        status="active",
        narrative="Supply may be reduced.",
        instruments=["OIL"],
        supporting_event_ids=["e1"],
        invalidation_conditions=["Verified supply restoration"],
        confidence=0.5,
        created_at=T,
        updated_at=T,
        review_at=T + timedelta(hours=1),
    )
    return Thesis.model_validate(values | changes)


def test_timestamps_normalize_to_utc():
    event = sample_event(source_timestamp="2026-01-01T08:00:00+08:00")
    assert event.source_timestamp == T
    assert event.source_timestamp.tzinfo == UTC


@pytest.mark.parametrize(
    "changes",
    [
        {"received_timestamp": datetime(2026, 1, 1)},
        {"received_timestamp": T - timedelta(seconds=1)},
        {"source_type": "unknown"},
        {"raw_text": ""},
        {"buy": True},
    ],
)
def test_reject_invalid_or_unexpected_event_fields(changes):
    with pytest.raises(ValidationError):
        sample_event(**changes)


def test_thesis_revision_contract():
    assert ThesisChange(expected_revision=0, thesis=sample_thesis(), reason="new evidence")
    with pytest.raises(ValidationError):
        ThesisChange(expected_revision=1, thesis=sample_thesis(), reason="stale revision")
    with pytest.raises(ValidationError):
        sample_thesis(confidence=1.1)


def test_memory_rejects_future_and_duplicate_revisions():
    with pytest.raises(ValidationError):
        MemorySnapshot(memory_version=1, as_of=T - timedelta(seconds=1), theses=(sample_thesis(),))
    with pytest.raises(ValidationError):
        MemorySnapshot(memory_version=1, as_of=T, theses=(sample_thesis(), sample_thesis()))


def test_target_is_exposure_not_an_order():
    position = TargetPosition(instrument="OIL", weight=Decimal("-0.1"), thesis_ids=("t1",))
    target = TargetPortfolio(
        target_id="p1",
        based_on_snapshot_id="s1",
        positions=(position,),
        rationale="Synthetic exposure proposal",
    )
    assert target.positions[0].weight == Decimal("-0.1")
    with pytest.raises(ValidationError):
        TargetPortfolio(
            target_id="p1",
            based_on_snapshot_id="s1",
            positions=(position, position),
            rationale="duplicate",
        )


def test_context_rejects_future_information():
    portfolio = PortfolioSnapshot(snapshot_id="s1", as_of=T, base_currency="USD", equity=10000)
    with pytest.raises(ValidationError):
        AgentContext(
            event=sample_event(),
            memory=MemorySnapshot(memory_version=0, as_of=T),
            portfolio=portfolio,
            market=MarketSnapshot(snapshot_id="m1", as_of=T, quotes=()),
            as_of=T - timedelta(seconds=1),
        )


def test_no_target_means_no_portfolio_change_and_decision_expires():
    fields = dict(
        decision_id="d1",
        event_id="e1",
        based_on_memory_version=0,
        based_on_snapshot_id="s1",
        provider="fake",
        model_version="fake-1",
        prompt_version="p1",
        started_at=T,
        completed_at=T,
        valid_until=T + timedelta(minutes=1),
        rationale="No action justified",
    )
    assert AgentDecision.model_validate(fields).target is None
    with pytest.raises(ValidationError):
        AgentDecision.model_validate(fields | {"valid_until": T})


def test_contracts_are_immutable():
    event = sample_event()
    with pytest.raises(ValidationError):
        event.raw_text = "overwrite evidence"
