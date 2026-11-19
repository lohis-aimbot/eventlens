"""Trading layer: single-pass agent, target validation, hard risk, and shadow ledger."""

from .agent import (
    ObservationReasoner,
    PortfolioAction,
    SinglePassProposal,
    SinglePassTradingAgent,
    TradingAgent,
)
from .execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    ShadowExecutor,
    ShadowFill,
    SQLiteShadowLedger,
)
from .portfolio import (
    LOT_UNITS_EUR,
    DeterministicTargetValidator,
    PortfolioState,
    TargetValidator,
    classify_exposure_change,
    quantize_target_quantity_eur,
)
from .risk import (
    DeterministicHardRiskEngine,
    HardRiskEngine,
    RiskPolicy,
    is_in_daily_close_window,
    is_in_pre_close_entry_cutoff,
    trading_day_window,
)

__all__ = [
    "DeterministicHardRiskEngine",
    "DeterministicTargetValidator",
    "HardRiskEngine",
    "LOT_UNITS_EUR",
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
    "is_in_pre_close_entry_cutoff",
    "quantize_target_quantity_eur",
    "trading_day_window",
]
