"""Research proposal boundary and pinned experiment rules; no model or execution.

Checks link model text to supplied context. They are not source verification,
calibrated forecasts, a full hard-risk engine or permission to place an order.
"""

import hashlib
from datetime import timedelta
from decimal import Decimal
from typing import Annotated, Literal, Protocol, Self

from pydantic import AfterValidator, Field, model_validator

from ..contracts import AgentContext, Contract
from .research_replay import DEFAULT_LATENCIES, CostScenario, LatencyScenario


def _meaningful(value: str) -> str:
    if not value.strip():
        raise ValueError("Text must contain non-whitespace content")
    return value  # Preserve exact evidence text, including its whitespace.


Text = Annotated[str, Field(strict=True, min_length=1), AfterValidator(_meaningful)]
Direction = Literal["long", "short", "flat"]


class ThesisReference(Contract):
    thesis_id: Text
    revision: int = Field(strict=True, ge=1)


class ResearchPositionProposal(Contract):
    """Only these fields may come from the future LLM response."""

    target_direction: Direction
    thesis_references: tuple[ThesisReference, ...]
    evidence_quotes: tuple[Text, ...] = Field(min_length=1, max_length=5)
    rationale: Text
    confidence: float = Field(strict=True, ge=0, le=1)
    invalidation_conditions: tuple[Text, ...]

    @model_validator(mode="after")
    def coherent(self) -> Self:
        ids = [reference.thesis_id for reference in self.thesis_references]
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate thesis reference")
        if len(self.evidence_quotes) != len(set(self.evidence_quotes)):
            raise ValueError("Duplicate evidence quote")
        if self.target_direction != "flat" and (
            not self.thesis_references or not self.invalidation_conditions
        ):
            raise ValueError(
                "Non-flat proposal needs thesis references and invalidation conditions"
            )
        return self


PINNED_COSTS = tuple(
    CostScenario(name=name, commission_bps="0.5", slippage_bps="0.25", spread_floor_pips=floor)
    for name, floor in (
        ("floor_0", "0"),
        ("floor_0_2", "0.2"),
        ("floor_0_5", "0.5"),
        ("floor_1", "1"),
        ("floor_2", "2"),
    )
)


class DecisionReplayProtocol(Contract):
    """A v1 experiment may not tune these fields after seeing its decisions."""

    protocol_version: Literal["eurusd-fomc-decision-v1"] = "eurusd-fomc-decision-v1"
    mode: Literal["offline_research_only"] = "offline_research_only"
    instrument: Literal["EUR/USD"] = "EUR/USD"
    catalog_version: Literal["fomc-2025-v1"] = "fomc-2025-v1"
    engine_version: Literal["research-ledger-v2"] = "research-ledger-v2"
    initial_cash_usd: Decimal = Decimal("10000")
    units_eur: Decimal = Decimal("1000")
    starting_position: Literal["flat"] = "flat"
    horizon_seconds: Literal[300] = 300
    input_cutoff_rule: Literal["portfolio_reasoning_start"] = "portfolio_reasoning_start"
    entry_rule: Literal["first_observed_quote_after_decision_and_order_ready"] = (
        "first_observed_quote_after_decision_and_order_ready"
    )
    exit_rule: Literal["signal_time_plus_300s_with_order_and_execution_delay"] = (
        "signal_time_plus_300s_with_order_and_execution_delay"
    )
    intraperiod_management: Literal["none"] = "none"
    max_quote_age_ms: Literal[2000] = 2000
    max_quote_wait_ms: Literal[2000] = 2000
    max_path_gap_ms: Literal[3000] = 3000
    primary_latency: Literal["poll_plus_reasoning_assumed"] = "poll_plus_reasoning_assumed"
    primary_cost: Literal["floor_1"] = "floor_1"
    latency_scenarios: tuple[LatencyScenario, ...] = DEFAULT_LATENCIES
    cost_profiles: tuple[CostScenario, ...] = PINNED_COSTS
    failure_rule: Literal["rejected_proposal_not_flat_prediction"] = (
        "rejected_proposal_not_flat_prediction"
    )
    baseline_directions: tuple[Direction, ...] = ("flat", "long", "short")

    @model_validator(mode="after")
    def pinned(self) -> Self:
        if (
            self.initial_cash_usd != Decimal("10000")
            or self.units_eur != Decimal("1000")
            or self.latency_scenarios != DEFAULT_LATENCIES
            or self.cost_profiles != PINNED_COSTS
            or self.baseline_directions != ("flat", "long", "short")
        ):
            raise ValueError("Changing pinned rules requires a new protocol version")
        return self

    @property
    def protocol_id(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


class ResearchProposalAgent(Protocol):
    async def propose(self, context: AgentContext) -> ResearchPositionProposal:
        """Future provider adapter; no implementation or model call in this stage."""
        ...


class ResearchProposalCheck(Contract):
    validation_scope: Literal["schema_and_supplied_context_only"] = (
        "schema_and_supplied_context_only"
    )
    execution_authorized: Literal[False] = False
    proposal_id: Text
    protocol_id: Text
    context_id: Text
    instrument: Literal["EUR/USD"] = "EUR/USD"
    proposed_quantity_eur: Decimal
    proposal: ResearchPositionProposal


def check_proposal(
    proposal: ResearchPositionProposal,
    context: AgentContext,
    protocol: DecisionReplayProtocol = DecisionReplayProtocol(),
) -> ResearchProposalCheck:
    """Pure validation. Do not commit memory, invoke AI or simulate a fill."""
    # Revalidate serialized inputs: model_copy(update=...) skips Pydantic checks.
    proposal = ResearchPositionProposal.model_validate_json(proposal.model_dump_json())
    context = AgentContext.model_validate_json(context.model_dump_json())
    protocol = DecisionReplayProtocol.model_validate_json(protocol.model_dump_json())
    portfolio = context.portfolio
    if (
        portfolio.base_currency != "USD"
        or portfolio.equity != protocol.initial_cash_usd
        or portfolio.pending_order_ids
        or any(p.instrument != protocol.instrument or p.quantity != 0 for p in portfolio.positions)
    ):
        raise ValueError(
            "Initial protocol requires a flat USD research portfolio with no pending orders"
        )
    if context.event.source_type != "macro":
        raise ValueError("Initial protocol only accepts supplied macro-event context")
    quote = next((q for q in context.market.quotes if q.instrument == protocol.instrument), None)
    if quote is None or context.as_of - quote.received_at > timedelta(
        milliseconds=protocol.max_quote_age_ms
    ):
        raise ValueError("Missing or stale EUR/USD context quote")
    if any(text not in context.event.raw_text for text in proposal.evidence_quotes):
        raise ValueError("Proposal quote is absent from the current event")
    theses = {thesis.thesis_id: thesis for thesis in context.memory.theses}
    for reference in proposal.thesis_references:
        thesis = theses.get(reference.thesis_id)
        if thesis is None or thesis.revision != reference.revision:
            raise ValueError("Unknown thesis or mismatched revision")
        # Flat may cite invalidated/resolved theses as its reason for abstention.
        if proposal.target_direction != "flat" and (
            thesis.status != "active"
            or thesis.review_at <= context.as_of
            or protocol.instrument not in thesis.instruments
        ):
            raise ValueError(
                "Non-flat proposal references an inactive, expired or unrelated thesis"
            )
    context_id = hashlib.sha256(context.model_dump_json().encode()).hexdigest()
    proposal_id = hashlib.sha256(
        (protocol.protocol_id + context_id + proposal.model_dump_json()).encode()
    ).hexdigest()
    signed_units = {"flat": Decimal(0), "long": protocol.units_eur, "short": -protocol.units_eur}
    return ResearchProposalCheck(
        proposal_id=proposal_id,
        protocol_id=protocol.protocol_id,
        context_id=context_id,
        proposed_quantity_eur=signed_units[proposal.target_direction],
        proposal=proposal,
    )
