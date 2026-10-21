"""Observed account state is separate from memory and desired exposure."""

from datetime import datetime
from decimal import Decimal
from typing import Protocol

from ..contracts import PortfolioSnapshot, TargetPortfolio, Thesis


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
    ) -> None:
        target = TargetPortfolio.model_validate_json(target.model_dump_json())
        current = PortfolioSnapshot.model_validate_json(current.model_dump_json())
        if target.based_on_snapshot_id != current.snapshot_id:
            raise ValueError("Target is based on a stale or mismatched portfolio snapshot")
        if current.pending_order_ids:
            raise ValueError("Cannot validate target while pending orders are outstanding")
        target_by_instrument = {pos.instrument: pos for pos in target.positions}
        for held in current.positions:
            if held.quantity != 0 and held.instrument not in target_by_instrument:
                raise ValueError(
                    f"Held instrument {held.instrument} omitted from target; explicit zero weight required to close"
                )
        for pos in target.positions:
            if pos.instrument not in self.allowed_instruments:
                raise ValueError(f"Unsupported target instrument: {pos.instrument}")
            if abs(pos.weight) > self.max_abs_weight:
                raise ValueError(f"Target weight {pos.weight} exceeds bound {self.max_abs_weight}")
            if len(pos.thesis_ids) != len(set(pos.thesis_ids)):
                raise ValueError("Duplicate thesis ID in target position")
            if theses is not None:
                check_time = as_of or current.as_of
                for tid in pos.thesis_ids:
                    thesis = theses.get(tid)
                    if thesis is None:
                        raise ValueError(f"Target references unknown thesis ID: {tid}")
                    if pos.weight != 0 and (
                        thesis.status != "active"
                        or thesis.review_at <= check_time
                        or pos.instrument not in thesis.instruments
                    ):
                        raise ValueError(
                            f"Non-zero target for {pos.instrument} cites inactive, expired or unrelated thesis {tid}"
                        )
