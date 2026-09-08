"""Deterministic admission limits, independently configurable from parsing."""

from datetime import datetime

from pydantic import Field

from .core import Model, Quote, Signal


class RiskConfig(Model):
    units: int = Field(default=1000, gt=0, le=100000)
    max_units: int = Field(default=10000, gt=0)
    max_spread_bps: float = Field(default=3, gt=0)
    max_quote_age_seconds: float = Field(default=30, gt=0)
    max_signal_age_seconds: float = Field(default=300, gt=0)
    stop_loss_bps: float = Field(default=20, gt=0)
    max_holding_seconds: float = Field(default=3600, gt=0)
    max_loss_usd: float = Field(default=50, gt=0)
    slippage_bps: float = Field(default=0.2, ge=0)
    commission_per_order_usd: float = Field(default=0, ge=0)
    kill_switch: bool = False


def entry_rejection(
    signal: Signal, quote: Quote, *, now: datetime, equity_change: float, config: RiskConfig
) -> str | None:
    if config.kill_switch:
        return "kill_switch"
    if equity_change <= -config.max_loss_usd:
        return "loss_limit"
    if config.units > config.max_units:
        return "position_limit"
    quote_age = (now - quote.timestamp).total_seconds()
    if not 0 <= quote_age <= config.max_quote_age_seconds:
        return "stale_or_future_quote"
    if not 0 <= (now - signal.decided_at).total_seconds() <= config.max_signal_age_seconds:
        return "stale_or_future_signal"
    if (quote.ask - quote.bid) / quote.mid * 10000 > config.max_spread_bps:
        return "spread_limit"
    return None
