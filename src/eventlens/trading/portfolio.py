"""Observed account state is separate from memory and desired exposure."""

from datetime import datetime
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Literal, Protocol

from ..contracts import PortfolioSnapshot, TargetPortfolio, Thesis

LOT_UNITS_EUR = Decimal("100000")

ExposureChangeKind = Literal[
    "no_change",
    "risk_reducing",
    "risk_increasing",
    "reversal_risk_increasing",
]


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
    ) -> None:
        target = TargetPortfolio.model_validate_json(target.model_dump_json())
        current = PortfolioSnapshot.model_validate_json(current.model_dump_json())
        if target.based_on_snapshot_id != current.snapshot_id:
            raise ValueError("Target is based on a stale or mismatched portfolio snapshot")
        if current.pending_order_ids:
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
            if not held_by_instrument:
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
            if pos.weight == Decimal("0") and pos.instrument not in held_by_instrument:
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
