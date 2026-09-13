"""Hard risk gate interface. Limits are controlled by code/operator, not the agent."""

from typing import Protocol

from .contracts import AgentDecision, MarketSnapshot, PortfolioSnapshot, RiskAssessment


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
