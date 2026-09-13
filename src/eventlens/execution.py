"""Shadow-only boundary; intentionally no broker SDK, orders or live adapter."""

from typing import Protocol

from .contracts import AgentDecision, ExecutionReceipt, RiskAssessment


class ShadowExecutor(Protocol):
    async def record(self, decision: AgentDecision, assessment: RiskAssessment) -> ExecutionReceipt:
        """Only accept a matching, unexpired approval issued by the trusted risk gate.

        Deduplicate by decision/target ID. A target is not a fill; execution receipts
        do not overwrite positions. A future shadow ledger must model fills separately.
        Implementations must reject changed targets or stale portfolio versions.
        """
        ...
