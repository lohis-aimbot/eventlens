"""Proposal/context boundaries, pinned rules and no execution authorization."""

import copy
import json
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from eventlens.decision_cli import ProposalInput
from eventlens.research.decision_protocol import (
    DecisionReplayProtocol,
    ResearchPositionProposal,
    check_proposal,
)
from eventlens.research.research_replay import ENGINE_VERSION, CostSensitivityConfig


def payload():
    return json.loads(Path("examples/synthetic-position-proposal.json").read_text())


@pytest.mark.parametrize("direction,quantity", [("long", 1000), ("short", -1000), ("flat", 0)])
def test_direction_maps_to_fixed_research_units_without_execution(direction, quantity):
    data = payload()
    data["proposal"]["target_direction"] = direction
    parsed = ProposalInput.model_validate(data)
    result = check_proposal(parsed.proposal, parsed.context)
    assert result.proposed_quantity_eur == quantity
    assert result.execution_authorized is False
    assert result.validation_scope == "schema_and_supplied_context_only"
    assert result == check_proposal(parsed.proposal, parsed.context)


@pytest.mark.parametrize(
    "field,value",
    [
        ("units_eur", 100000),
        ("instrument", "GBP/USD"),
        ("order", {"side": "buy"}),
        ("risk_approved", True),
        ("completed_at", "2025-01-29T19:00:00Z"),
        ("horizon_seconds", 30),
    ],
)
def test_model_cannot_supply_operator_controls_or_authoritative_metadata(field, value):
    body = payload()["proposal"]
    body[field] = value
    with pytest.raises(ValidationError):
        ResearchPositionProposal.model_validate(body)


@pytest.mark.parametrize("value", [True, "0.9", float("nan"), 1.1])
def test_confidence_requires_a_finite_numeric_score(value):
    body = payload()["proposal"]
    body["confidence"] = value
    with pytest.raises(ValidationError):
        ResearchPositionProposal.model_validate(body)


def test_unsubstantiated_or_duplicate_evidence_and_old_thesis_revision_fail():
    parsed = ProposalInput.model_validate(payload())
    for body, message in [
        (
            {**payload()["proposal"], "evidence_quotes": ["Tomorrow EUR/USD definitely rises."]},
            "absent",
        ),
        (
            {
                **payload()["proposal"],
                "thesis_references": [{"thesis_id": "invented", "revision": 1}],
            },
            "Unknown",
        ),
        (
            {
                **payload()["proposal"],
                "thesis_references": [{"thesis_id": "synthetic-fed-thesis", "revision": 2}],
            },
            "revision",
        ),
    ]:
        with pytest.raises(ValueError, match=message):
            check_proposal(ResearchPositionProposal.model_validate(body), parsed.context)
    body = payload()["proposal"]
    body["evidence_quotes"] *= 2
    with pytest.raises(ValidationError, match="Duplicate"):
        ResearchPositionProposal.model_validate(body)


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "invalidated"),
        ("review_at", "2025-01-29T19:00:04Z"),
        ("instruments", ["GBP/USD"]),
    ],
)
def test_nonflat_requires_active_current_eligible_thesis(field, value):
    data = payload()
    data["context"]["memory"]["theses"][0][field] = value
    parsed = ProposalInput.model_validate(data)
    with pytest.raises(ValueError, match="inactive, expired or unrelated"):
        check_proposal(parsed.proposal, parsed.context)
    data["proposal"]["target_direction"] = "flat"
    parsed = ProposalInput.model_validate(data)
    assert check_proposal(parsed.proposal, parsed.context).proposed_quantity_eur == 0


def test_future_context_and_naive_clock_are_rejected_even_after_unchecked_copy():
    data = payload()
    data["context"]["market"]["quotes"][0]["received_at"] = "2025-01-29T19:00:05Z"
    with pytest.raises(ValidationError, match="future"):
        ProposalInput.model_validate(data)
    data = payload()
    data["context"]["as_of"] = "2025-01-29T19:00:04"
    with pytest.raises(ValidationError):
        ProposalInput.model_validate(data)
    parsed = ProposalInput.model_validate(payload())
    wrong_market = parsed.context.market.model_copy(
        update={"as_of": parsed.context.as_of.replace(year=2026)}
    )
    copied = parsed.context.model_copy(update={"market": wrong_market})
    with pytest.raises(ValidationError, match="future"):
        check_proposal(parsed.proposal, copied)


def test_stale_quote_and_nonflat_or_pending_portfolio_are_rejected():
    for mutation, message in [
        ("stale", "stale"),
        ("missing", "Missing"),
        ("position", "flat USD"),
        ("pending", "pending orders"),
    ]:
        data = payload()
        if mutation == "stale":
            data["context"]["market"]["quotes"][0]["received_at"] = "2025-01-29T19:00:01Z"
        elif mutation == "missing":
            data["context"]["market"]["quotes"] = []
        elif mutation == "position":
            data["context"]["portfolio"]["positions"][0]["quantity"] = "1000"
        else:
            data["context"]["portfolio"]["pending_order_ids"] = ["pending-research-order"]
        parsed = ProposalInput.model_validate(data)
        with pytest.raises(ValueError, match=message):
            check_proposal(parsed.proposal, parsed.context)


def test_flat_is_intentional_abstention_not_parse_failure_or_implicit_close():
    data = payload()
    data["proposal"]["target_direction"] = "flat"
    data["proposal"]["thesis_references"] = []
    data["proposal"]["invalidation_conditions"] = []
    parsed = ProposalInput.model_validate(data)
    assert check_proposal(parsed.proposal, parsed.context).proposed_quantity_eur == 0
    del data["proposal"]["target_direction"]
    with pytest.raises(ValidationError):
        ProposalInput.model_validate(data)


def test_exported_schema_and_protocol_match_code_and_forbid_silent_tuning():
    protocol = DecisionReplayProtocol()
    artifact = json.loads(Path("configs/decision-protocol-v1.json").read_text())
    assert artifact == {
        "protocol_id": protocol.protocol_id,
        "rules": protocol.model_dump(mode="json"),
    }
    assert (
        json.loads(Path("examples/position-proposal.schema.json").read_text())
        == ResearchPositionProposal.model_json_schema()
    )
    assert protocol.engine_version == ENGINE_VERSION
    costs = CostSensitivityConfig.model_validate_json(
        Path("configs/cost-sensitivity.json").read_bytes()
    )
    assert protocol.cost_profiles == costs.profiles
    changed = copy.deepcopy(artifact["rules"])
    changed["latency_scenarios"][0]["ingest_ms"] = 0
    with pytest.raises(ValidationError, match="new protocol version"):
        DecisionReplayProtocol.model_validate(changed)
    with pytest.raises(ValidationError):
        DecisionReplayProtocol(horizon_seconds=30)
    parsed = ProposalInput.model_validate(payload())
    copied = protocol.model_copy(update={"units_eur": Decimal("100000")})
    with pytest.raises(ValidationError, match="new protocol version"):
        check_proposal(parsed.proposal, parsed.context, copied)


def test_historical_capture_time_is_not_silently_relabelled_as_event_availability():
    data = payload()
    data["context"]["event"]["received_timestamp"] = "2026-10-14T12:00:00Z"
    with pytest.raises(ValidationError, match="future"):
        ProposalInput.model_validate(data)


def test_changed_context_changes_identity_without_changing_fixed_position_size():
    first = ProposalInput.model_validate(payload())
    modified = payload()
    modified["context"]["market"]["quotes"][0]["bid"] = "1.0999"
    second = ProposalInput.model_validate(modified)
    a = check_proposal(first.proposal, first.context)
    b = check_proposal(second.proposal, second.context)
    assert a.context_id != b.context_id and a.proposal_id != b.proposal_id
    assert a.proposed_quantity_eur == b.proposed_quantity_eur == -1000
