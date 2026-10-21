"""Hard risk gate interface and deterministic policy engine.

Limits are controlled by code and operator configuration, never by the LLM.
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import uuid4

from pydantic import Field

from ..contracts import (
    AgentDecision,
    Contract,
    Identifier,
    MarketSnapshot,
    PortfolioSnapshot,
    RiskAssessment,
    Thesis,
)

EURUSD_PIP = Decimal("0.0001")


class HardRiskEngine(Protocol):
    async def assess(
        self, decision: AgentDecision, current: PortfolioSnapshot, market: MarketSnapshot
    ) -> RiskAssessment:
        """Fail closed on missing/stale inputs, limit breaches or kill-switch activation.

        Enforce gross/net/per-asset exposure, loss limits, liquidity/spread,
        instrument eligibility and request rate independently of model rationale.
        Initial policy is approve/reject only; do not silently rewrite target exposure.
        Risk policy, quote freshness and account state must be checked again at dispatch.
        """
        ...


class RiskPolicy(Contract):
    policy_version: Identifier = "hard-risk-v1"
    allowed_instruments: tuple[Identifier, ...] = ("EUR/USD",)
    base_currency: Identifier = "USD"
    max_position_weight: Decimal = Field(default=Decimal("0.25"), gt=0, le=1)
    max_gross_weight: Decimal = Field(default=Decimal("0.25"), gt=0, le=1)
    min_equity_usd: Decimal = Field(default=Decimal("9500"), gt=0)
    max_quote_age_ms: int = Field(default=2000, gt=0)
    max_spread_pips: Decimal = Field(default=Decimal("2.0"), gt=0)
    allow_zero_spread: bool = True
    assessment_ttl_seconds: int = Field(default=30, gt=0)
    kill_switch_active: bool = False


class DeterministicHardRiskEngine:
    """Approve or reject a proposed target portfolio without rewriting model exposure."""

    def __init__(
        self,
        policy: RiskPolicy = RiskPolicy(),
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.policy = RiskPolicy.model_validate_json(policy.model_dump_json())
        self.clock = clock

    async def assess(
        self,
        decision: AgentDecision,
        current: PortfolioSnapshot,
        market: MarketSnapshot,
        *,
        prior_theses: tuple[Thesis, ...] = (),
    ) -> RiskAssessment:
        decision = AgentDecision.model_validate_json(decision.model_dump_json())
        current = PortfolioSnapshot.model_validate_json(current.model_dump_json())
        market = MarketSnapshot.model_validate_json(market.model_dump_json())
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Hard risk clock must be timezone-aware")
        checked_at = now.astimezone(UTC)
        reasons: list[str] = []

        if self.policy.kill_switch_active:
            reasons.append("kill_switch_active")

        target = decision.target
        if target is None:
            reasons.append("missing_target_portfolio")
            return self._build_assessment(
                decision_id=decision.decision_id,
                target_id="none",
                snapshot_id=current.snapshot_id,
                checked_at=checked_at,
                reasons=reasons,
            )

        if checked_at < decision.completed_at or checked_at >= decision.valid_until:
            reasons.append("decision_expired")

        if (
            decision.based_on_snapshot_id != current.snapshot_id
            or target.based_on_snapshot_id != current.snapshot_id
        ):
            reasons.append("snapshot_mismatch")

        if current.pending_order_ids:
            reasons.append("pending_orders_outstanding")

        if current.base_currency != self.policy.base_currency:
            reasons.append("unsupported_base_currency")

        is_pure_exit = bool(target.positions) and all(
            pos.weight == Decimal("0") for pos in target.positions
        )
        if not is_pure_exit and current.equity < self.policy.min_equity_usd:
            reasons.append("equity_below_minimum_floor")

        allowed = set(self.policy.allowed_instruments)
        target_by_instrument = {pos.instrument: pos for pos in target.positions}
        for held in current.positions:
            if held.quantity != 0 and held.instrument not in target_by_instrument:
                reasons.append("omitted_held_instrument")
                break

        gross_weight = Decimal("0")
        effective_theses: dict[str, Thesis] = {t.thesis_id: t for t in prior_theses}
        for change in decision.thesis_changes:
            effective_theses[change.thesis.thesis_id] = change.thesis

        quotes_by_instrument = {q.instrument: q for q in market.quotes}
        max_age = timedelta(milliseconds=self.policy.max_quote_age_ms)
        max_spread = self.policy.max_spread_pips * EURUSD_PIP

        for pos in target.positions:
            gross_weight += abs(pos.weight)
            if pos.instrument not in allowed:
                reasons.append("ineligible_instrument")
            if abs(pos.weight) > self.policy.max_position_weight:
                reasons.append("per_asset_weight_limit_exceeded")
            if pos.weight != Decimal("0"):
                for tid in pos.thesis_ids:
                    thesis = effective_theses.get(tid)
                    if (
                        thesis is None
                        or thesis.status != "active"
                        or thesis.review_at <= checked_at
                        or pos.instrument not in thesis.instruments
                    ):
                        reasons.append("missing_active_thesis_backing")
                        break
            quote = quotes_by_instrument.get(pos.instrument)
            if quote is None:
                reasons.append("missing_market_quote")
            else:
                if (
                    checked_at < quote.received_at
                    or checked_at - quote.received_at > max_age
                    or market.as_of - quote.received_at > max_age
                ):
                    reasons.append("stale_market_quote")
                spread = quote.ask - quote.bid
                if not self.policy.allow_zero_spread and spread == Decimal("0"):
                    reasons.append("zero_spread_rejected")
                if spread > max_spread:
                    reasons.append("spread_limit_exceeded")

        if gross_weight > self.policy.max_gross_weight:
            reasons.append("gross_weight_limit_exceeded")

        deduped = list(dict.fromkeys(reasons))
        return self._build_assessment(
            decision_id=decision.decision_id,
            target_id=target.target_id,
            snapshot_id=current.snapshot_id,
            checked_at=checked_at,
            reasons=deduped,
        )

    def _build_assessment(
        self,
        *,
        decision_id: str,
        target_id: str,
        snapshot_id: str,
        checked_at: datetime,
        reasons: list[str],
    ) -> RiskAssessment:
        approved = len(reasons) == 0
        return RiskAssessment(
            assessment_id=str(uuid4()),
            decision_id=decision_id,
            target_id=target_id,
            checked_snapshot_id=snapshot_id,
            policy_version=self.policy.policy_version,
            checked_at=checked_at,
            valid_until=checked_at + timedelta(seconds=self.policy.assessment_ttl_seconds),
            outcome="approve" if approved else "reject",
            reasons=("all_hard_risk_checks_passed",) if approved else tuple(reasons),
        )
