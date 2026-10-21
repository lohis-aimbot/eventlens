"""Trading layer: single-pass agent, target validation, hard risk, and shadow ledger."""

from .agent import (
    ObservationReasoner,
    SinglePassProposal,
    SinglePassTradingAgent,
    TradingAgent,
    resolve_verbatim_quote,
)
from .execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    ShadowExecutor,
    ShadowFill,
    SQLiteShadowLedger,
)
from .portfolio import (
    DeterministicTargetValidator,
    PortfolioState,
    TargetValidator,
)
from .risk import (
    DeterministicHardRiskEngine,
    HardRiskEngine,
    RiskPolicy,
)

__all__ = [
    "DeterministicHardRiskEngine",
    "DeterministicTargetValidator",
    "HardRiskEngine",
    "ObservationReasoner",
    "PortfolioState",
    "ProtectiveExitPolicy",
    "RiskPolicy",
    "SQLiteShadowLedger",
    "ShadowCostModel",
    "ShadowExecutor",
    "ShadowFill",
    "SinglePassProposal",
    "SinglePassTradingAgent",
    "TargetValidator",
    "TradingAgent",
    "resolve_verbatim_quote",
]
