"""Single-position EUR/USD replay with next-quote fills and USD P&L."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .core import Event, Model, Quote, Signal
from .risk import RiskConfig, entry_rejection
from .signals import score_event


class Fill(Model):
    timestamp: datetime
    action: Literal["open", "close"]
    side: Literal[-1, 1]
    units: int
    price: float
    reason: str
    event_id: str
    net_pnl_usd: float | None = None


class Audit(Model):
    timestamp: datetime
    event_id: str
    action: str
    reason: str


class ReplayResult(Model):
    mode: Literal["offline_paper"] = "offline_paper"
    fills: list[Fill]
    audit: list[Audit]
    net_pnl_usd: float
    closed_trades: int
    warning: str = "Synthetic/demo baseline; no inference of profitability."


@dataclass
class Position:
    side: Literal[-1, 1]
    entry: float
    opened_at: datetime
    event_id: str
    episode_id: str


def replay(
    events: list[Event], quotes: list[Quote], config: RiskConfig | None = None
) -> ReplayResult:
    config = config or RiskConfig()
    ordered = sorted(quotes, key=lambda q: q.timestamp)
    if len({q.timestamp for q in ordered}) != len(ordered):
        raise ValueError("Duplicate quote timestamps are ambiguous")
    queue = sorted(events, key=lambda e: (e.available_at, e.event_id))
    if len({e.available_at for e in queue}) != len(queue):
        raise ValueError("Simultaneous events require an aggregation policy; unsupported in MVP")
    fills: list[Fill] = []
    audit: list[Audit] = []
    position: Position | None = None
    seen: set[tuple[str, str, str]] = set()
    processed: set[str] = set()
    realized = 0.0
    index = 0

    def exit_price(quote: Quote, side: int) -> float:
        return (quote.bid if side == 1 else quote.ask) * (1 - side * config.slippage_bps / 10000)

    def close(quote: Quote, reason: str) -> None:
        nonlocal position, realized
        assert position is not None
        price = exit_price(quote, position.side)
        pnl = (price - position.entry) * position.side * config.units
        realized += pnl - config.commission_per_order_usd
        fills.append(
            Fill(
                timestamp=quote.timestamp,
                action="close",
                side=position.side,
                units=config.units,
                price=price,
                reason=reason,
                event_id=position.event_id,
                net_pnl_usd=pnl - 2 * config.commission_per_order_usd,
            )
        )
        position = None

    for quote_index, quote in enumerate(ordered):
        # Information at the exact quote timestamp cannot fill at that quote.
        pending: dict[str, Signal] = {}
        while index < len(queue) and queue[index].available_at < quote.timestamp:
            event = queue[index]
            index += 1
            key = (event.record.episode_id, event.record.kind, event.record.content_hash)
            if event.event_id in processed or key in seen:
                audit.append(
                    Audit(
                        timestamp=quote.timestamp,
                        event_id=event.event_id,
                        action="skip",
                        reason="duplicate_content",
                    )
                )
                continue
            processed.add(event.event_id)
            seen.add(key)
            signal = score_event(event, now=event.available_at)
            previous = pending.get(signal.episode_id)
            if previous:
                audit.append(
                    Audit(
                        timestamp=quote.timestamp,
                        event_id=previous.event_id,
                        action="skip",
                        reason="superseded_before_fill",
                    )
                )
            pending[signal.episode_id] = signal

        exited = False
        if position is not None:
            mark = exit_price(quote, position.side)
            change_bps = (mark / position.entry - 1) * position.side * 10000
            equity_change = realized + (mark - position.entry) * position.side * config.units
            reason = None
            if config.kill_switch:
                reason = "kill_switch"
            elif change_bps <= -config.stop_loss_bps:
                reason = "stop_loss"
            elif equity_change <= -config.max_loss_usd:
                reason = "loss_limit"
            elif (
                quote.timestamp - position.opened_at
            ).total_seconds() >= config.max_holding_seconds:
                reason = "holding_limit"
            if reason:
                close(quote, reason)
                exited = True

        for signal in pending.values():
            signal_age = (quote.timestamp - signal.decided_at).total_seconds()
            if signal_age > config.max_signal_age_seconds:
                action, reason = "skip", "stale_signal"
            elif signal.target == 0:
                action, reason = "abstain", "unknown_or_neutral"
            elif position is not None:
                if signal.episode_id != position.episode_id:
                    action, reason = "skip", "unrelated_episode_position_open"
                elif signal.target != position.side:
                    close(quote, "thesis_reversal")
                    exited = True
                    action, reason = "close", "thesis_reversal"
                else:
                    action, reason = "hold", "thesis_supported"
            elif exited or quote_index == len(ordered) - 1:
                action, reason = "skip", "no_reentry_or_terminal_quote"
            else:
                rejection = entry_rejection(
                    signal, quote, now=quote.timestamp, equity_change=realized, config=config
                )
                if rejection:
                    action, reason = "skip", rejection
                else:
                    side: Literal[-1, 1] = 1 if signal.target == 1 else -1
                    price = (quote.ask if side == 1 else quote.bid) * (
                        1 + side * config.slippage_bps / 10000
                    )
                    position = Position(
                        side, price, quote.timestamp, signal.event_id, signal.episode_id
                    )
                    realized -= config.commission_per_order_usd
                    fills.append(
                        Fill(
                            timestamp=quote.timestamp,
                            action="open",
                            side=side,
                            units=config.units,
                            price=price,
                            reason="demo_stance_signal",
                            event_id=signal.event_id,
                        )
                    )
                    action, reason = "open", "risk_checks_passed"
            audit.append(
                Audit(
                    timestamp=quote.timestamp,
                    event_id=signal.event_id,
                    action=action,
                    reason=reason,
                )
            )

    if position is not None:
        close(ordered[-1], "end_of_data")
    for event in queue[index:]:
        audit.append(
            Audit(
                timestamp=event.available_at,
                event_id=event.event_id,
                action="skip",
                reason="no_later_quote",
            )
        )
    return ReplayResult(
        fills=fills,
        audit=audit,
        net_pnl_usd=realized,
        closed_trades=sum(f.action == "close" for f in fills),
    )
