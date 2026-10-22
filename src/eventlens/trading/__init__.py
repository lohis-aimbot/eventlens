"""Trading layer: single-pass agent, target validation, hard risk, and shadow ledger."""

from .agent import (
    ObservationReasoner,
    PortfolioAction,
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
    classify_exposure_change,
)
from .risk import (
    DeterministicHardRiskEngine,
    HardRiskEngine,
    RiskPolicy,
    is_in_daily_close_window,
    trading_day_window,
)

__all__ = [
    "DeterministicHardRiskEngine",
    "DeterministicTargetValidator",
    "HardRiskEngine",
    "ObservationReasoner",
    "PortfolioAction",
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
    "classify_exposure_change",
    "is_in_daily_close_window",
    "resolve_verbatim_quote",
    "trading_day_window",
]
