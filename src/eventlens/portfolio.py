"""Observed account state is separate from memory and desired exposure."""

from typing import Protocol

from .contracts import PortfolioSnapshot, TargetPortfolio


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
