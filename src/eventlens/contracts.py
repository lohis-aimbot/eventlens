"""Immutable boundary objects. LLM output is a proposal, never an executed position."""

from datetime import UTC
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, model_validator

Timestamp = Annotated[AwareDatetime, AfterValidator(lambda value: value.astimezone(UTC))]
Identifier = Annotated[str, Field(min_length=1)]
Score = Annotated[float, Field(ge=0, le=1)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Event(Contract):
    """Source metadata must be assigned/verified by adapters, not trusted from source text."""

    event_id: Identifier
    source: Identifier
    source_type: Literal["news", "social", "macro", "geopolitical", "company"]
    source_reference: Identifier
    source_timestamp: Timestamp
    received_timestamp: Timestamp
    raw_text: str = Field(min_length=1)
    author: str | None = None

    @model_validator(mode="after")
    def validate_time(self) -> Self:
        if self.received_timestamp < self.source_timestamp:
            raise ValueError("Receipt cannot precede source time; quarantine clock anomalies")
        return self


class FilterResult(Contract):
    event_id: Identifier
    outcome: Literal["accept", "ignore", "quarantine"]
    reason: str = Field(min_length=1)
    checked_at: Timestamp
    filter_version: Identifier


class Thesis(Contract):
    """A versioned belief, independent of whether any trade was permitted or filled."""

    thesis_id: Identifier
    revision: int = Field(ge=1)
    status: Literal["active", "invalidated", "resolved"]
    narrative: str = Field(min_length=1)
    instruments: tuple[Identifier, ...] = Field(min_length=1)
    supporting_event_ids: tuple[Identifier, ...] = ()
    contradicting_event_ids: tuple[Identifier, ...] = ()
    invalidation_conditions: tuple[str, ...] = Field(min_length=1)
    confidence: Score
    created_at: Timestamp
    updated_at: Timestamp
    review_at: Timestamp

    @model_validator(mode="after")
    def validate_time(self) -> Self:
        if not self.created_at <= self.updated_at <= self.review_at:
            raise ValueError("Thesis times must be created <= updated <= review")
        return self


class ThesisChange(Contract):
    """Create with expected_revision=0; update with a compare-and-swap revision."""

    expected_revision: int = Field(ge=0)
    thesis: Thesis
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def next_revision(self) -> Self:
        if self.thesis.revision != self.expected_revision + 1:
            raise ValueError("A thesis change must advance exactly one revision")
        return self


class MemorySnapshot(Contract):
    memory_version: int = Field(ge=0)
    as_of: Timestamp
    theses: tuple[Thesis, ...] = ()

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        ids = [thesis.thesis_id for thesis in self.theses]
        if len(ids) != len(set(ids)):
            raise ValueError("Only one revision per thesis in a snapshot")
        if any(thesis.updated_at > self.as_of for thesis in self.theses):
            raise ValueError("A memory snapshot cannot contain future revisions")
        return self


class Position(Contract):
    """Observed quantity from a shadow ledger or broker; never supplied by the LLM."""

    instrument: Identifier
    quantity: Decimal


class PortfolioSnapshot(Contract):
    snapshot_id: Identifier
    as_of: Timestamp
    base_currency: Identifier
    equity: Decimal = Field(gt=0)
    positions: tuple[Position, ...] = ()
    # Outstanding orders must be included before computing any new execution delta.
    pending_order_ids: tuple[Identifier, ...] = ()


class Quote(Contract):
    instrument: Identifier
    received_at: Timestamp
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def valid_spread(self) -> Self:
        if self.ask < self.bid:
            raise ValueError("Ask must not be below bid")
        return self


class MarketSnapshot(Contract):
    snapshot_id: Identifier
    as_of: Timestamp
    quotes: tuple[Quote, ...]

    @model_validator(mode="after")
    def valid_quotes(self) -> Self:
        if any(quote.received_at > self.as_of for quote in self.quotes):
            raise ValueError("Market snapshot cannot contain future quotes")
        instruments = [quote.instrument for quote in self.quotes]
        if len(instruments) != len(set(instruments)):
            raise ValueError("Duplicate instrument quote")
        return self


class AgentContext(Contract):
    event: Event
    memory: MemorySnapshot
    portfolio: PortfolioSnapshot
    market: MarketSnapshot
    as_of: Timestamp

    @model_validator(mode="after")
    def no_future_context(self) -> Self:
        if (
            max(
                self.event.received_timestamp,
                self.memory.as_of,
                self.portfolio.as_of,
                self.market.as_of,
            )
            > self.as_of
        ):
            raise ValueError("Agent context cannot contain information from the future")
        return self


class TargetPosition(Contract):
    """Signed fraction of account equity; e.g. -0.1 means 10% short notional."""

    instrument: Identifier
    weight: Decimal
    thesis_ids: tuple[Identifier, ...] = Field(min_length=1)


class TargetPortfolio(Contract):
    """Complete desired state. Held assets to exit must appear with zero weight."""

    target_id: Identifier
    based_on_snapshot_id: Identifier
    positions: tuple[TargetPosition, ...]
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def unique_assets(self) -> Self:
        assets = [position.instrument for position in self.positions]
        if len(assets) != len(set(assets)):
            raise ValueError("An instrument can appear only once in a target")
        return self


class AgentDecision(Contract):
    decision_id: Identifier
    event_id: Identifier
    based_on_memory_version: int = Field(ge=0)
    based_on_snapshot_id: Identifier
    provider: Identifier
    model_version: Identifier
    prompt_version: Identifier
    started_at: Timestamp
    completed_at: Timestamp
    valid_until: Timestamp
    thesis_changes: tuple[ThesisChange, ...] = ()
    # None means no portfolio adjustment, not liquidation.
    target: TargetPortfolio | None = None
    rationale: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        if not self.started_at <= self.completed_at < self.valid_until:
            raise ValueError("Decision must complete before expiry")
        if self.target and self.target.based_on_snapshot_id != self.based_on_snapshot_id:
            raise ValueError("Decision and target must refer to the same portfolio snapshot")
        ids = [change.thesis.thesis_id for change in self.thesis_changes]
        if len(ids) != len(set(ids)):
            raise ValueError("Only one change per thesis per decision")
        return self


class RiskAssessment(Contract):
    """Produced by trusted deterministic code, not deserialized from an LLM response."""

    assessment_id: Identifier
    decision_id: Identifier
    target_id: Identifier
    checked_snapshot_id: Identifier
    policy_version: Identifier
    checked_at: Timestamp
    valid_until: Timestamp
    outcome: Literal["approve", "reject"]
    reasons: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_time(self) -> Self:
        if self.valid_until <= self.checked_at:
            raise ValueError("Risk assessment must expire after its check")
        return self


class ExecutionReceipt(Contract):
    """Future shadow adapter receipt; no live execution mode in this foundation."""

    execution_id: Identifier
    decision_id: Identifier
    target_id: Identifier
    assessment_id: Identifier
    mode: Literal["shadow"] = "shadow"
    status: Literal["recorded", "rejected"]
    recorded_at: Timestamp
    reason: str


class AuditRecord(Contract):
    audit_id: Identifier
    occurred_at: Timestamp
    component: Literal["collector", "filter", "agent", "memory", "risk", "execution", "runtime"]
    event_id: Identifier | None = None
    decision_id: Identifier | None = None
    outcome: str
    detail: str
