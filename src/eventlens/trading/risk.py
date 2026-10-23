"""Hard risk gate interface and deterministic policy engine.

Limits are controlled by code and operator configuration, never by the LLM.
"""

from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import uuid4
from zoneinfo import ZoneInfo

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
from ..quotes import QuoteQualityPolicy, validate_market_quote
from .portfolio import (
    ExposureChangeKind,
    classify_exposure_change,
    quantize_target_quantity_eur,
)

EURUSD_PIP = Decimal("0.0001")


def trading_day_window(
    ts: datetime,
    *,
    tz_name: str = "America/New_York",
    close_hour: int = 16,
    close_minute: int = 55,
    rollover_hour: int = 17,
    rollover_minute: int = 0,
) -> tuple[str, datetime, datetime, datetime]:
    """Return (session_day_iso, session_start_utc, daily_close_cutoff_utc, session_rollover_utc)."""
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("Trading day calculation requires timezone-aware timestamp")
    zone = ZoneInfo(tz_name)
    local_dt = ts.astimezone(zone)
    rollover_t = time(rollover_hour, rollover_minute)
    if local_dt.time() >= rollover_t:
        session_date = local_dt.date() + timedelta(days=1)
    else:
        session_date = local_dt.date()
    prev_date = session_date - timedelta(days=1)
    start_local = datetime.combine(prev_date, rollover_t, tzinfo=zone)
    close_local = datetime.combine(session_date, time(close_hour, close_minute), tzinfo=zone)
    end_local = datetime.combine(session_date, rollover_t, tzinfo=zone)
    if close_local > end_local:
        close_local -= timedelta(days=1)
    return (
        session_date.isoformat(),
        start_local.astimezone(UTC),
        close_local.astimezone(UTC),
        end_local.astimezone(UTC),
    )


def is_in_daily_close_window(
    ts: datetime,
    *,
    tz_name: str = "America/New_York",
    close_hour: int = 16,
    close_minute: int = 55,
    rollover_hour: int = 17,
    rollover_minute: int = 0,
) -> bool:
    """True when current timestamp is at or after the session daily close cutoff before rollover."""
    _, _, close_utc, rollover_utc = trading_day_window(
        ts,
        tz_name=tz_name,
        close_hour=close_hour,
        close_minute=close_minute,
        rollover_hour=rollover_hour,
        rollover_minute=rollover_minute,
    )
    ts_utc = ts.astimezone(UTC)
    return close_utc <= ts_utc < rollover_utc


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
    policy_version: Identifier = "hard-risk-v2"
    allowed_instruments: tuple[Identifier, ...] = ("EUR/USD",)
    base_currency: Identifier = "USD"
    leverage: Decimal = Field(default=Decimal("1"), gt=0, le=1000)
    min_volume_eur: Decimal = Field(default=Decimal("1"), gt=0)
    max_volume_eur: Decimal = Field(default=Decimal("10000000"), gt=0)
    volume_step_eur: Decimal = Field(default=Decimal("1"), gt=0)
    max_position_weight: Decimal = Field(default=Decimal("0.25"), gt=0, le=1)
    max_gross_weight: Decimal = Field(default=Decimal("0.25"), gt=0, le=1)
    min_equity_usd: Decimal = Field(default=Decimal("9500"), gt=0)
    max_daily_loss_usd: Decimal = Field(default=Decimal("200"), gt=0)
    max_quote_age_ms: int = Field(default=2000, gt=0)
    max_spread_pips: Decimal = Field(default=Decimal("2.0"), gt=0)
    allow_zero_spread: bool = False
    require_verified_quote_provenance: bool = False
    assessment_ttl_seconds: int = Field(default=30, gt=0)
    trading_timezone: str = "America/New_York"
    daily_close_hour: int = Field(default=16, ge=0, le=23)
    daily_close_minute: int = Field(default=55, ge=0, le=59)
    rollover_hour: int = Field(default=17, ge=0, le=23)
    rollover_minute: int = Field(default=0, ge=0, le=59)
    kill_switch_active: bool = False

    @classmethod
    def pepperstone_razor_200(
        cls,
        *,
        kill_switch_active: bool = False,
        max_quote_age_ms: int = 2000,
    ) -> "RiskPolicy":
        """Pepperstone Razor $200 USD Demo account standard: 200x leverage, 0.01..100 lots, controlled risk."""
        return cls(
            policy_version="pepperstone-razor-200-v1",
            leverage=Decimal("200"),
            min_volume_eur=Decimal("1000"),
            max_volume_eur=Decimal("10000000"),
            volume_step_eur=Decimal("1000"),
            max_position_weight=Decimal("0.25"),
            max_gross_weight=Decimal("0.25"),
            min_equity_usd=Decimal("180.00"),
            max_daily_loss_usd=Decimal("8.00"),
            max_quote_age_ms=max_quote_age_ms,
            max_spread_pips=Decimal("2.0"),
            kill_switch_active=kill_switch_active,
        )

    def quantize_target_quantity_eur(
        self,
        current_equity: Decimal,
        mid_price: Decimal,
        target_weight: Decimal,
    ) -> Decimal:
        return quantize_target_quantity_eur(
            current_equity,
            mid_price,
            target_weight,
            leverage=self.leverage,
            volume_step_eur=self.volume_step_eur,
            min_volume_eur=self.min_volume_eur,
            max_volume_eur=self.max_volume_eur,
            max_position_weight=self.max_position_weight,
        )

    def classify_exposure_change(
        self,
        current_qty: Decimal,
        current_equity: Decimal,
        mid_price: Decimal | None,
        target_weight: Decimal,
    ) -> ExposureChangeKind:
        return classify_exposure_change(
            current_qty,
            current_equity,
            mid_price,
            target_weight,
            leverage=self.leverage,
            volume_step_eur=self.volume_step_eur,
            min_volume_eur=self.min_volume_eur,
            max_volume_eur=self.max_volume_eur,
            max_position_weight=self.max_position_weight,
        )

    def quote_quality_policy(self, instrument: str = "EUR/USD") -> QuoteQualityPolicy:
        return QuoteQualityPolicy(
            instrument=instrument,
            max_quote_age_ms=self.max_quote_age_ms,
            max_spread_pips=self.max_spread_pips,
            allow_zero_spread=self.allow_zero_spread,
            require_verified_provenance=self.require_verified_quote_provenance,
        )


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

        allowed = set(self.policy.allowed_instruments)
        held_by_instrument = {
            held.instrument: held.quantity for held in current.positions if held.quantity != 0
        }
        target_by_instrument = {pos.instrument: pos for pos in target.positions}
        for held_inst in held_by_instrument:
            if held_inst not in target_by_instrument:
                reasons.append("omitted_held_instrument")
                break

        gross_weight = Decimal("0")
        effective_theses: dict[str, Thesis] = {t.thesis_id: t for t in prior_theses}
        for change in decision.thesis_changes:
            effective_theses[change.thesis.thesis_id] = change.thesis

        quotes_by_instrument = {q.instrument: q for q in market.quotes}
        increases_risk = False
        has_reversal = False

        for pos in target.positions:
            gross_weight += abs(pos.weight)
            if pos.instrument not in allowed:
                reasons.append("ineligible_instrument")
            if abs(pos.weight) > self.policy.max_position_weight:
                reasons.append("per_asset_weight_limit_exceeded")

            quote_check = validate_market_quote(
                market,
                as_of=checked_at,
                policy=self.policy.quote_quality_policy(pos.instrument),
            )
            if not quote_check.valid:
                reasons.append(quote_check.reason)

            quote = quotes_by_instrument.get(pos.instrument)
            mid_price = (quote.bid + quote.ask) / Decimal("2") if quote is not None else None
            current_qty = held_by_instrument.get(pos.instrument, Decimal("0"))
            change_kind = self.policy.classify_exposure_change(
                current_qty, current.equity, mid_price, pos.weight
            )
            if change_kind in ("risk_increasing", "reversal_risk_increasing"):
                increases_risk = True
            if change_kind == "reversal_risk_increasing":
                has_reversal = True

            if (
                pos.weight != Decimal("0")
                and mid_price is not None
                and mid_price > Decimal("0")
                and current.equity > Decimal("0")
            ):
                desired_qty = self.policy.quantize_target_quantity_eur(
                    current.equity, mid_price, pos.weight
                )
                if abs(desired_qty) < self.policy.min_volume_eur:
                    reasons.append("below_minimum_trade_volume")
                elif abs(desired_qty) > self.policy.max_volume_eur:
                    reasons.append("exceeds_maximum_trade_volume")

            if pos.weight == Decimal("0") and current_qty == Decimal("0"):
                reasons.append("close_target_without_open_position")

            for tid in pos.thesis_ids:
                thesis = effective_theses.get(tid)
                if thesis is None or pos.instrument not in thesis.instruments:
                    reasons.append("missing_active_thesis_backing")
                    break
                if pos.weight != Decimal("0") and (
                    thesis.status != "active" or thesis.review_at <= checked_at
                ):
                    reasons.append("missing_active_thesis_backing")
                    break

        if not target.positions:
            reasons.append("empty_target_positions")

        # Risk-increasing actions (open, add, or reversal) are blocked by kill switch,
        # daily loss limits, minimum equity floor, unreliable position valuation,
        # pending protective exits, and daily close cutoff window.
        # Purely risk-reducing actions (CLOSE or partial reduction) remain permitted.
        if increases_risk:
            if self.policy.kill_switch_active:
                reasons.append(
                    "reversal_blocked_by_kill_switch" if has_reversal else "kill_switch_active"
                )
            if held_by_instrument and not current.valuation_valid:
                reasons.append("unreliable_position_valuation")
            if not current.daily_baseline_valid:
                reasons.append("daily_baseline_unconfirmed")
            if current.pending_exit_reason is not None:
                reasons.append("protective_exit_pending")
            if (
                current.daily_loss_limit_triggered
                or current.daily_pnl_usd <= -self.policy.max_daily_loss_usd
            ):
                reasons.append(
                    "reversal_blocked_by_daily_loss_limit"
                    if has_reversal
                    else "daily_loss_limit_exceeded"
                )
            if current.equity < self.policy.min_equity_usd:
                reasons.append("equity_below_minimum_floor")
            if is_in_daily_close_window(
                checked_at,
                tz_name=self.policy.trading_timezone,
                close_hour=self.policy.daily_close_hour,
                close_minute=self.policy.daily_close_minute,
                rollover_hour=self.policy.rollover_hour,
                rollover_minute=self.policy.rollover_minute,
            ):
                reasons.append("daily_close_cutoff_active")

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
