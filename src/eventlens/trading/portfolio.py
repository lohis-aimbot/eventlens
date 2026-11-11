"""Observed account state is separate from memory and desired exposure."""

from datetime import datetime
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Literal, Protocol

from pydantic import Field

from ..contracts import Contract, PortfolioSnapshot, TargetPortfolio, Thesis

LOT_UNITS_EUR = Decimal("100000")
EURUSD_PIP_VALUE_PER_EUR = Decimal("0.0001")

ExposureChangeKind = Literal[
    "no_change",
    "risk_reducing",
    "risk_increasing",
    "reversal_risk_increasing",
]


class ExAnteTradeRiskMetrics(Contract):
    """Ex-ante notional, margin, and q-dependent stop/cost risk estimate (never a guaranteed fill cap)."""

    quantity_eur: Decimal
    notional_usd: Decimal = Field(ge=0)
    effective_leverage: Decimal = Field(ge=0)
    margin_used_usd: Decimal = Field(ge=0)
    margin_utilization_pct: Decimal = Field(ge=0)
    stop_loss_pips: Decimal = Field(ge=0)
    stop_distance_loss_usd: Decimal = Field(ge=0)
    spread_cost_usd: Decimal = Field(ge=0)
    estimated_slippage_usd: Decimal = Field(ge=0)
    estimated_commission_usd: Decimal = Field(ge=0)
    fixed_fees_usd: Decimal = Field(ge=0)
    ex_ante_total_risk_usd: Decimal = Field(ge=0)
    risk_budget_usd: Decimal | None = Field(default=None, ge=0)
    within_risk_budget: bool = True
    ex_ante_estimate_only: Literal[True] = True


def compute_ex_ante_trade_risk(
    *,
    quantity_eur: Decimal,
    current_equity: Decimal,
    mid_price: Decimal,
    leverage: Decimal = Decimal("1"),
    stop_loss_pips: Decimal = Decimal("0"),
    spread_pips: Decimal = Decimal("0"),
    estimated_slippage_bps: Decimal = Decimal("0.25"),
    estimated_commission_round_trip_per_million_usd: Decimal = Decimal("6.0"),
    fixed_fees_usd: Decimal = Decimal("0"),
    risk_budget_usd: Decimal | None = None,
) -> ExAnteTradeRiskMetrics:
    """Compute q-dependent ex-ante stop-out loss plus spread, slippage, commission, and fixed fees.

    Note: This is strictly an ex-ante estimate under assumed stop execution and cost parameters.
    Quote outages, liquidity gaps, and stop slippage can cause realized loss to exceed this figure.
    """
    abs_q = abs(quantity_eur)
    notional_usd = abs_q * max(mid_price, Decimal("0"))
    eff_lev = notional_usd / current_equity if current_equity > Decimal("0") else Decimal("0")
    margin_usd = notional_usd / leverage if leverage > Decimal("0") else notional_usd
    margin_pct = margin_usd / current_equity if current_equity > Decimal("0") else Decimal("0")

    stop_loss_usd = abs_q * max(stop_loss_pips, Decimal("0")) * EURUSD_PIP_VALUE_PER_EUR
    spread_usd = abs_q * max(spread_pips, Decimal("0")) * EURUSD_PIP_VALUE_PER_EUR
    slippage_usd = (
        Decimal("2") * notional_usd * (max(estimated_slippage_bps, Decimal("0")) / Decimal("10000"))
    )
    commission_usd = notional_usd * (
        max(estimated_commission_round_trip_per_million_usd, Decimal("0")) / Decimal("1000000")
    )
    fixed_usd = max(fixed_fees_usd, Decimal("0")) if abs_q > Decimal("0") else Decimal("0")
    total_risk_usd = stop_loss_usd + spread_usd + slippage_usd + commission_usd + fixed_usd
    within_budget = risk_budget_usd is None or total_risk_usd <= risk_budget_usd
    return ExAnteTradeRiskMetrics(
        quantity_eur=quantity_eur,
        notional_usd=notional_usd,
        effective_leverage=eff_lev,
        margin_used_usd=margin_usd,
        margin_utilization_pct=margin_pct,
        stop_loss_pips=max(stop_loss_pips, Decimal("0")),
        stop_distance_loss_usd=stop_loss_usd,
        spread_cost_usd=spread_usd,
        estimated_slippage_usd=slippage_usd,
        estimated_commission_usd=commission_usd,
        fixed_fees_usd=fixed_usd,
        ex_ante_total_risk_usd=total_risk_usd,
        risk_budget_usd=risk_budget_usd,
        within_risk_budget=within_budget,
    )


def quantize_risk_budget_quantity_eur(
    *,
    current_equity: Decimal,
    mid_price: Decimal,
    risk_budget_usd: Decimal,
    stop_loss_pips: Decimal,
    direction_sign: int = 1,
    leverage: Decimal = Decimal("200"),
    volume_step_eur: Decimal = Decimal("1000"),
    min_volume_eur: Decimal = Decimal("1000"),
    max_volume_eur: Decimal = Decimal("10000000"),
    max_position_weight: Decimal = Decimal("0.25"),
    max_margin_utilization_pct: Decimal | None = None,
    spread_pips: Decimal = Decimal("0"),
    estimated_slippage_bps: Decimal = Decimal("0.25"),
    estimated_commission_round_trip_per_million_usd: Decimal = Decimal("6.0"),
    fixed_fees_usd: Decimal = Decimal("0"),
) -> Decimal:
    """Solve q*stop + spread(q) + slippage(q) + commission(q) + fixed_fees <= risk_budget, ROUND_DOWN to step."""
    if (
        direction_sign == 0
        or current_equity <= Decimal("0")
        or mid_price <= Decimal("0")
        or stop_loss_pips <= Decimal("0")
        or risk_budget_usd <= max(fixed_fees_usd, Decimal("0"))
    ):
        return Decimal("0")
    per_unit_risk = (
        stop_loss_pips + max(spread_pips, Decimal("0"))
    ) * EURUSD_PIP_VALUE_PER_EUR + mid_price * (
        Decimal("2") * max(estimated_slippage_bps, Decimal("0")) / Decimal("10000")
        + max(estimated_commission_round_trip_per_million_usd, Decimal("0")) / Decimal("1000000")
    )
    if per_unit_risk <= Decimal("0"):
        return Decimal("0")
    avail_budget = risk_budget_usd - max(fixed_fees_usd, Decimal("0"))
    budget_steps = (avail_budget / per_unit_risk / volume_step_eur).to_integral_value(
        rounding=ROUND_DOWN
    )
    buying_power_usd = current_equity * leverage
    notional_cap_steps = (
        (buying_power_usd * max_position_weight) / mid_price / volume_step_eur
    ).to_integral_value(rounding=ROUND_DOWN)
    chosen_steps = min(budget_steps, notional_cap_steps)
    if max_margin_utilization_pct is not None and max_margin_utilization_pct > Decimal("0"):
        margin_cap_steps = (
            (current_equity * max_margin_utilization_pct * leverage) / mid_price / volume_step_eur
        ).to_integral_value(rounding=ROUND_DOWN)
        chosen_steps = min(chosen_steps, margin_cap_steps)

    q_abs = min(max_volume_eur, chosen_steps * volume_step_eur)
    if q_abs < min_volume_eur:
        return Decimal("0")
    recheck = compute_ex_ante_trade_risk(
        quantity_eur=q_abs,
        current_equity=current_equity,
        mid_price=mid_price,
        leverage=leverage,
        stop_loss_pips=stop_loss_pips,
        spread_pips=spread_pips,
        estimated_slippage_bps=estimated_slippage_bps,
        estimated_commission_round_trip_per_million_usd=estimated_commission_round_trip_per_million_usd,
        fixed_fees_usd=fixed_fees_usd,
        risk_budget_usd=risk_budget_usd,
    )
    while not recheck.within_risk_budget and q_abs >= min_volume_eur:
        q_abs -= volume_step_eur
        if q_abs < min_volume_eur:
            return Decimal("0")
        recheck = compute_ex_ante_trade_risk(
            quantity_eur=q_abs,
            current_equity=current_equity,
            mid_price=mid_price,
            leverage=leverage,
            stop_loss_pips=stop_loss_pips,
            spread_pips=spread_pips,
            estimated_slippage_bps=estimated_slippage_bps,
            estimated_commission_round_trip_per_million_usd=estimated_commission_round_trip_per_million_usd,
            fixed_fees_usd=fixed_fees_usd,
            risk_budget_usd=risk_budget_usd,
        )
    return q_abs if direction_sign > 0 else -q_abs


def quantize_target_quantity_eur(
    current_equity: Decimal,
    mid_price: Decimal,
    target_weight: Decimal,
    *,
    leverage: Decimal = Decimal("1"),
    volume_step_eur: Decimal = Decimal("1"),
    min_volume_eur: Decimal = Decimal("1"),
    max_volume_eur: Decimal = Decimal("10000000"),
    max_position_weight: Decimal = Decimal("1"),
) -> Decimal:
    """Convert target portfolio weight into EUR units respecting account leverage and lot step/bounds.

    - In legacy unleveraged 1-EUR unit mode (`leverage == 1` and `volume_step_eur == 1` and `min_volume_eur == 1`),
      uses `ROUND_DOWN` to the nearest 1 EUR unit.
    - In leveraged broker lot mode (e.g. Pepperstone Razor `200x` leverage, `0.01 lot = 1,000 EUR` step,
      `min = 1,000 EUR`, `max = 10,000,000 EUR`), scales notional by `current_equity * leverage * target_weight`,
      quantizes to the nearest `volume_step_eur` (`ROUND_HALF_UP`) to maximize capital efficiency on valid
      signals, clamps non-zero signals to at least `min_volume_eur` when permitted by `max_position_weight`,
      and strictly caps size at `min(max_volume_eur, floor(buying_power * max_position_weight / mid_price))`
      so lot rounding never breaches hard risk limits.
    """
    if target_weight == Decimal("0") or mid_price <= Decimal("0") or current_equity <= Decimal("0"):
        return Decimal("0")
    if (
        leverage == Decimal("1")
        and volume_step_eur == Decimal("1")
        and min_volume_eur == Decimal("1")
    ):
        raw = ((current_equity * target_weight) / mid_price).quantize(
            Decimal("1"), rounding=ROUND_DOWN
        )
        if abs(raw) > max_volume_eur:
            return max_volume_eur if raw > Decimal("0") else -max_volume_eur
        return raw

    buying_power_usd = current_equity * leverage
    raw_abs_eur = (buying_power_usd * abs(target_weight)) / mid_price
    max_risk_steps = (
        (buying_power_usd * max_position_weight) / mid_price / volume_step_eur
    ).to_integral_value(rounding=ROUND_DOWN)
    hard_cap_eur = min(max_volume_eur, max_risk_steps * volume_step_eur)
    if hard_cap_eur < min_volume_eur:
        return Decimal("0")

    steps = (raw_abs_eur / volume_step_eur).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    sized_eur = steps * volume_step_eur
    if sized_eur < min_volume_eur:
        sized_eur = min_volume_eur
    sized_eur = min(sized_eur, hard_cap_eur)
    if sized_eur < min_volume_eur:
        return Decimal("0")
    return sized_eur if target_weight > Decimal("0") else -sized_eur


def classify_exposure_change(
    current_qty: Decimal,
    current_equity: Decimal,
    mid_price: Decimal | None,
    target_weight: Decimal,
    *,
    leverage: Decimal = Decimal("1"),
    volume_step_eur: Decimal = Decimal("1"),
    min_volume_eur: Decimal = Decimal("1"),
    max_volume_eur: Decimal = Decimal("10000000"),
    max_position_weight: Decimal = Decimal("1"),
) -> ExposureChangeKind:
    """Classify whether a target weight reduces risk, increases risk, or reverses exposure."""
    if target_weight == Decimal("0"):
        return "no_change" if current_qty == Decimal("0") else "risk_reducing"
    if current_qty == Decimal("0"):
        return "risk_increasing"
    if (current_qty > Decimal("0") and target_weight < Decimal("0")) or (
        current_qty < Decimal("0") and target_weight > Decimal("0")
    ):
        return "reversal_risk_increasing"
    if mid_price is None or mid_price <= Decimal("0") or current_equity <= Decimal("0"):
        return "risk_increasing"
    desired_qty = quantize_target_quantity_eur(
        current_equity,
        mid_price,
        target_weight,
        leverage=leverage,
        volume_step_eur=volume_step_eur,
        min_volume_eur=min_volume_eur,
        max_volume_eur=max_volume_eur,
        max_position_weight=max_position_weight,
    )
    if abs(desired_qty) < abs(current_qty):
        return "risk_reducing"
    if desired_qty == current_qty:
        return "no_change"
    return "risk_increasing"


class PortfolioState(Protocol):
    async def snapshot(self) -> PortfolioSnapshot:
        """Read observed positions and outstanding orders from an authoritative ledger."""
        ...


class TargetValidator(Protocol):
    def validate(self, target: TargetPortfolio, current: PortfolioSnapshot) -> None:
        """Check snapshot identity, known instruments, thesis links and target completeness.

        Raise on omitted held instruments, ambiguity or stale account state.
        Missing instruments are never silently interpreted as a close instruction.
        """
        ...


class DeterministicTargetValidator:
    """Validate target exposure against current portfolio state and eligible instruments."""

    def __init__(
        self,
        *,
        allowed_instruments: frozenset[str] = frozenset({"EUR/USD"}),
        max_abs_weight: Decimal = Decimal("1"),
    ) -> None:
        if not allowed_instruments or max_abs_weight <= 0 or max_abs_weight > Decimal("1"):
            raise ValueError("Allowed instruments and a valid max weight in (0, 1] are required")
        self.allowed_instruments = allowed_instruments
        self.max_abs_weight = max_abs_weight

    def validate(
        self,
        target: TargetPortfolio,
        current: PortfolioSnapshot,
        *,
        theses: dict[str, Thesis] | None = None,
        as_of: datetime | None = None,
        portfolio_action: str | None = None,
        allow_pending_risk_reduction: bool = False,
    ) -> None:
        target = TargetPortfolio.model_validate_json(target.model_dump_json())
        current = PortfolioSnapshot.model_validate_json(current.model_dump_json())
        if target.based_on_snapshot_id != current.snapshot_id:
            raise ValueError("Target is based on a stale or mismatched portfolio snapshot")
        has_pending = bool(current.pending_order_ids)
        if has_pending and not allow_pending_risk_reduction:
            raise ValueError("Cannot validate target while pending orders are outstanding")
        held_by_instrument = {
            held.instrument: held.quantity for held in current.positions if held.quantity != 0
        }
        target_by_instrument = {pos.instrument: pos for pos in target.positions}
        for held_inst in held_by_instrument:
            if held_inst not in target_by_instrument:
                raise ValueError(
                    f"Held instrument {held_inst} omitted from target; explicit zero weight required to close"
                )
        if portfolio_action in ("IGNORE_EVENT", "HOLD"):
            raise ValueError(f"Action {portfolio_action} must not produce a TargetPortfolio")
        if portfolio_action == "CLOSE":
            if not held_by_instrument and not (has_pending and allow_pending_risk_reduction):
                raise ValueError("CLOSE target requires an existing open position")
            if any(pos.weight != Decimal("0") for pos in target.positions):
                raise ValueError("CLOSE target requires zero weight for all positions")
        if portfolio_action == "SET_TARGET":
            if all(pos.weight == Decimal("0") for pos in target.positions):
                raise ValueError("SET_TARGET requires a non-zero target position weight")
        for pos in target.positions:
            if pos.instrument not in self.allowed_instruments:
                raise ValueError(f"Unsupported target instrument: {pos.instrument}")
            if abs(pos.weight) > self.max_abs_weight:
                raise ValueError(f"Target weight {pos.weight} exceeds bound {self.max_abs_weight}")
            if len(pos.thesis_ids) != len(set(pos.thesis_ids)):
                raise ValueError("Duplicate thesis ID in target position")
            if (
                pos.weight == Decimal("0")
                and pos.instrument not in held_by_instrument
                and not (has_pending and allow_pending_risk_reduction)
            ):
                raise ValueError(
                    f"Zero-weight close target for {pos.instrument} requires an open position"
                )
            if theses is not None:
                check_time = as_of or current.as_of
                for tid in pos.thesis_ids:
                    thesis = theses.get(tid)
                    if thesis is None:
                        raise ValueError(f"Target references unknown thesis ID: {tid}")
                    if pos.instrument not in thesis.instruments:
                        raise ValueError(
                            f"Target for {pos.instrument} cites unrelated thesis {tid}"
                        )
                    if pos.weight != 0 and (
                        thesis.status != "active" or thesis.review_at <= check_time
                    ):
                        raise ValueError(
                            f"Non-zero target for {pos.instrument} cites inactive, expired or unrelated thesis {tid}"
                        )
