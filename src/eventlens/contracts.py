"""Immutable boundary objects. LLM output is a proposal, never an executed position."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal, Self

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, model_validator

Timestamp = Annotated[AwareDatetime, AfterValidator(lambda value: value.astimezone(UTC))]
Identifier = Annotated[str, Field(min_length=1)]
Score = Annotated[float, Field(ge=0, le=1)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class MacroSemantics(Contract):
    """Explicit separation of actual, consensus, preliminary, previous, and revised macro values."""

    actual: str | None = None
    consensus: str | None = None
    preliminary: str | None = None
    previous: str | None = None
    revised_previous: str | None = None
    reported_forecast: str | None = None
    semantics_status: Literal[
        "clean",
        "preliminary_as_forecast_or_previous",
        "preliminary_equality_needs_verification",
        "missing_consensus",
        "cross_source_conflict",
        "ambiguous",
    ] = "clean"
    surprise_status: Literal["confirmed", "unknown", "ambiguous"] = "unknown"
    surprise_direction: Literal["above_consensus", "below_consensus", "inline", "unknown"] = (
        "unknown"
    )
    detail: str | None = None

    @property
    def allows_confirmed_shock_risk(self) -> bool:
        return self.semantics_status == "clean" and self.surprise_status == "confirmed"


class Event(Contract):
    """Source metadata must be assigned/verified by adapters, not trusted from source text."""

    event_id: Identifier
    source: Identifier
    source_type: Literal["news", "social", "macro", "geopolitical", "company", "internal_review"]
    source_reference: Identifier
    source_timestamp: Timestamp
    received_timestamp: Timestamp
    raw_text: str = Field(min_length=1)
    author: str | None = None
    scheduled_at: Timestamp | None = None
    publication_at: Timestamp | None = None
    receipt_at: Timestamp | None = None
    available_at: Timestamp | None = None
    timestamp_source: Literal[
        "scheduled",
        "official_publication",
        "publisher_display",
        "modified",
        "receipt_only",
        "unknown",
    ] = "unknown"
    publication_time_status: Literal["verified", "estimated", "unknown"] = "unknown"
    poll_gap_seconds: float | None = Field(default=None, ge=0)
    is_delayed_catchup: bool = False
    macro_semantics: MacroSemantics | None = None

    @model_validator(mode="after")
    def validate_time(self) -> Self:
        if self.received_timestamp < self.source_timestamp:
            raise ValueError("Receipt cannot precede source time; quarantine clock anomalies")
        eff_receipt = self.receipt_at or self.received_timestamp
        eff_available = self.available_at or self.received_timestamp
        if eff_available < eff_receipt:
            raise ValueError("Availability time cannot precede receipt time")
        if self.publication_at is not None:
            if self.publication_at > eff_receipt:
                raise ValueError("External publication_at cannot exceed receipt time")
            if self.publication_at > self.received_timestamp:
                raise ValueError("External publication_at cannot exceed received_timestamp")
        if (
            self.scheduled_at is not None
            and self.publication_at is not None
            and self.timestamp_source == "scheduled"
            and self.publication_time_status == "unknown"
        ):
            raise ValueError("Unknown publication time must not be disguised as scheduled_at")
        return self

    @property
    def publication_lag_seconds(self) -> float | None:
        if self.publication_at is None or self.publication_time_status == "unknown":
            return None
        eff_rx = self.receipt_at or self.received_timestamp
        return max(0.0, (eff_rx - self.publication_at).total_seconds())

    @property
    def schedule_lag_seconds(self) -> float | None:
        if self.scheduled_at is None:
            return None
        eff_rx = self.receipt_at or self.received_timestamp
        return (eff_rx - self.scheduled_at).total_seconds()


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
    unrealized_pnl_usd: Decimal = Decimal("0")
    realized_pnl_usd: Decimal = Decimal("0")
    total_fees_usd: Decimal = Decimal("0")
    daily_pnl_usd: Decimal = Decimal("0")
    daily_baseline_equity_usd: Decimal | None = None
    daily_loss_limit_triggered: bool = False
    daily_baseline_valid: bool = True
    daily_baseline_reason: str = "confirmed"
    pending_exit_reason: str | None = None
    valuation_valid: bool = True
    valuation_reason: str = "flat_no_quote_needed"


TRUSTED_COMMISSION_ZERO_SPREAD_PROVIDERS: frozenset[str] = frozenset(
    {"ctrader_algo", "ctrader_openapi"}
)
TRUSTED_COMMISSION_TYPES: frozenset[str] = frozenset({"UsdPerMillionUsdVolume"})


QuoteBasis = Literal["live_poll", "synthetic_fixture", "historical_bar_replay"]


class Quote(Contract):
    instrument: Identifier
    source_at: Timestamp | None = None
    bridge_at: Timestamp | None = None
    received_at: Timestamp
    available_at: Timestamp | None = None
    source_provider: Identifier | None = None
    basis: QuoteBasis | None = None
    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)
    commission: Decimal | None = Field(default=None, ge=0)
    commission_type: Identifier | None = None
    bar_interval_seconds: int | None = Field(default=None, gt=0)
    synthetic_spread_applied: bool = False
    simulated_receipt: bool = False

    @model_validator(mode="after")
    def valid_spread(self) -> Self:
        if self.ask < self.bid:
            raise ValueError("Ask must not be below bid")
        if (
            self.source_at is not None
            and self.bridge_at is not None
            and self.source_at > self.bridge_at
        ):
            raise ValueError("Quote source_at cannot exceed bridge_at")
        if self.source_at is not None and self.source_at > self.received_at:
            raise ValueError("Quote source_at cannot exceed received_at")
        if self.bridge_at is not None and self.bridge_at > self.received_at:
            raise ValueError("Quote bridge_at cannot exceed received_at")
        if self.available_at is not None and self.received_at > self.available_at:
            raise ValueError("Quote received_at cannot exceed available_at")
        if self.basis == "live_poll" and (
            self.bar_interval_seconds is not None
            or self.synthetic_spread_applied
            or self.simulated_receipt
        ):
            raise ValueError(
                "Historical bar, synthetic spread, or simulated receipt quotes cannot claim basis='live_poll'"
            )
        return self

    @property
    def effective_source_at(self) -> datetime:
        return self.source_at if self.source_at is not None else self.received_at

    @property
    def receipt_lag_ms(self) -> float | None:
        if self.source_at is None:
            return None
        return (self.received_at - self.source_at).total_seconds() * 1000.0

    @property
    def has_verified_provenance(self) -> bool:
        return (
            self.source_at is not None
            and self.bridge_at is not None
            and self.available_at is not None
            and self.source_provider is not None
            and self.basis == "live_poll"
            and self.bar_interval_seconds is None
            and not self.synthetic_spread_applied
            and not self.simulated_receipt
        )

    @property
    def has_verified_commission(self) -> bool:
        return (
            self.has_verified_provenance
            and self.source_provider in TRUSTED_COMMISSION_ZERO_SPREAD_PROVIDERS
            and self.commission is not None
            and self.commission > Decimal("0")
            and self.commission_type in TRUSTED_COMMISSION_TYPES
        )


def _window_direction(change_pips: Decimal | None) -> Literal["up", "down", "flat", "missing"]:
    if change_pips is None:
        return "missing"
    if change_pips > Decimal("0"):
        return "up"
    if change_pips < Decimal("0"):
        return "down"
    return "flat"


class PriceTrajectorySummary(Contract):
    """Causal recent price excursion summary computed strictly from published quotes <= as_of."""

    instrument: Identifier = "EUR/USD"
    as_of: Timestamp
    current_mid: Decimal = Field(gt=0)
    change_1m_pips: Decimal | None = None
    change_5m_pips: Decimal | None = None
    change_15m_pips: Decimal | None = None
    change_30m_pips: Decimal | None = None
    window_15m_high_mid: Decimal | None = Field(default=None, gt=0)
    window_15m_low_mid: Decimal | None = Field(default=None, gt=0)
    window_30m_high_mid: Decimal | None = Field(default=None, gt=0)
    window_30m_low_mid: Decimal | None = Field(default=None, gt=0)
    sample_count_30m: int = Field(default=0, ge=0)
    max_gap_30m_seconds: float | None = Field(default=None, ge=0)
    direction_1m: Literal["up", "down", "flat", "missing"] = "missing"
    direction_5m: Literal["up", "down", "flat", "missing"] = "missing"
    direction_15m: Literal["up", "down", "flat", "missing"] = "missing"
    direction_30m: Literal["up", "down", "flat", "missing"] = "missing"
    short_window_coverage_status: Literal["complete", "partial", "missing"] = "missing"
    coverage_ratio_30m: float | None = Field(default=None, ge=0, le=1)
    quote_freshness_ms: int | None = Field(default=None, ge=0)
    evidence_id: str | None = None

    @model_validator(mode="after")
    def _populate_window_directions_and_coverage(self) -> Self:
        d1 = _window_direction(self.change_1m_pips)
        d5 = _window_direction(self.change_5m_pips)
        d15 = _window_direction(self.change_15m_pips)
        d30 = _window_direction(self.change_30m_pips)
        object.__setattr__(self, "direction_1m", d1)
        object.__setattr__(self, "direction_5m", d5)
        object.__setattr__(self, "direction_15m", d15)
        object.__setattr__(self, "direction_30m", d30)
        present_short = sum(
            1
            for x in (self.change_1m_pips, self.change_5m_pips, self.change_15m_pips)
            if x is not None
        )
        cov_status: Literal["complete", "partial", "missing"] = (
            "complete" if present_short == 3 else ("missing" if present_short == 0 else "partial")
        )
        object.__setattr__(self, "short_window_coverage_status", cov_status)
        if self.evidence_id is None:
            object.__setattr__(
                self,
                "evidence_id",
                f"traj:{self.instrument}:{self.as_of.isoformat()}",
            )
        return self


class MarketSnapshot(Contract):
    snapshot_id: Identifier
    as_of: Timestamp
    quotes: tuple[Quote, ...]
    price_trajectory: PriceTrajectorySummary | None = None

    @model_validator(mode="after")
    def valid_quotes(self) -> Self:
        for quote in self.quotes:
            if quote.received_at > self.as_of:
                raise ValueError("Market snapshot cannot contain future quotes")
            if quote.source_at is not None and quote.source_at > self.as_of:
                raise ValueError("Market snapshot cannot contain future quote source_at")
            if quote.available_at is not None and quote.available_at > self.as_of:
                raise ValueError("Market snapshot cannot contain future quote available_at")
        if self.price_trajectory is not None and self.price_trajectory.as_of > self.as_of:
            raise ValueError("Market snapshot cannot contain future price trajectory")
        instruments = [quote.instrument for quote in self.quotes]
        if len(instruments) != len(set(instruments)):
            raise ValueError("Duplicate instrument quote")
        return self


class ReviewConditionEvaluation(Contract):
    """Explicit classification of a thesis condition as executable price rule vs. non-executable prose."""

    raw_condition: str = Field(min_length=1)
    executable: bool
    status: Literal["executable_price_rule", "unexecutable_natural_language"]
    triggered: bool = False
    detail: str = Field(min_length=1)


class ReviewContext(Contract):
    """Traceable internal re-evaluation metadata; never masquerades as a fresh news arrival."""

    is_internal_review: Literal[True] = True
    review_id: Identifier
    trigger_kind: Literal[
        "scheduled_review_at",
        "structured_price_condition",
        "price_breakout_excursion",
        "periodic_review",
    ]
    trigger_detail: str = Field(min_length=1)
    triggered_at: Timestamp
    thesis_id: Identifier
    thesis_revision: int = Field(ge=1)
    origin_event_id: Identifier
    origin_source: Identifier
    origin_source_timestamp: Timestamp | None = None
    origin_received_timestamp: Timestamp
    origin_event_age_seconds: float = Field(ge=0)
    condition_evaluations: tuple[ReviewConditionEvaluation, ...] = ()


class AgentContext(Contract):
    event: Event
    memory: MemorySnapshot
    portfolio: PortfolioSnapshot
    market: MarketSnapshot
    as_of: Timestamp
    review_context: ReviewContext | None = None

    @model_validator(mode="after")
    def no_future_context(self) -> Self:
        latest_input = max(
            self.event.received_timestamp,
            self.memory.as_of,
            self.portfolio.as_of,
            self.market.as_of,
        )
        if self.review_context is not None:
            latest_input = max(latest_input, self.review_context.triggered_at)
        if latest_input > self.as_of:
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
    portfolio_action: Literal["IGNORE_EVENT", "HOLD", "SET_TARGET", "CLOSE"] | None = None
    rationale: str = Field(min_length=1)
    strategy_hypothesis: Literal[
        "trend_following", "countertrend", "fundamental_breakout", "unspecified"
    ] = "unspecified"
    cited_evidence_ids: tuple[Identifier, ...] = ()
    evidence_verification_status: Literal[
        "verified",
        "contradicted_price_direction",
        "missing_short_window_coverage",
        "unverified_numeric_claim",
        "not_applicable",
    ] = "not_applicable"

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
    approved_quantity_eur: Decimal | None = None

    @model_validator(mode="after")
    def validate_time(self) -> Self:
        if self.valid_until <= self.checked_at:
            raise ValueError("Risk assessment must expire after its check")
        return self


class ExecutionReceipt(Contract):
    """Execution adapter receipt; supports simulated shadow and Demo-only cTrader CLI bridge."""

    execution_id: Identifier
    decision_id: Identifier
    target_id: Identifier
    assessment_id: Identifier
    mode: Literal["shadow", "ctrader_cli_demo"] = "shadow"
    status: Literal["recorded", "rejected"]
    recorded_at: Timestamp
    reason: str


class AuditRecord(Contract):
    audit_id: Identifier
    occurred_at: Timestamp
    component: Literal["collector", "filter", "agent", "memory", "risk", "execution", "runtime"]
    event_id: Identifier | None = None
    decision_id: Identifier | None = None
    thesis_ids: tuple[Identifier, ...] = ()
    target_id: Identifier | None = None
    assessment_id: Identifier | None = None
    execution_id: Identifier | None = None
    fill_id: Identifier | None = None
    model_version: str | None = None
    prompt_version: str | None = None
    policy_version: str | None = None
    cost_model_version: str | None = None
    outcome: str
    detail: str
