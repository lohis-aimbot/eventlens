"""Observed account state is separate from memory and desired exposure."""

from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from typing import Literal, Protocol

from ..contracts import PortfolioSnapshot, TargetPortfolio, Thesis

ExposureChangeKind = Literal[
    "no_change",
    "risk_reducing",
    "risk_increasing",
    "reversal_risk_increasing",
]


def classify_exposure_change(
    current_qty: Decimal,
    current_equity: Decimal,
    mid_price: Decimal | None,
    target_weight: Decimal,
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
    desired_qty = ((current_equity * target_weight) / mid_price).quantize(
        Decimal("1"), rounding=ROUND_DOWN
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
