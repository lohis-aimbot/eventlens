"""Descriptive event returns, not causal effects or executable backtest returns."""

from datetime import datetime, timedelta
from statistics import mean

from .core import Event, Model, Quote, require_aware


class Observation(Model):
    event_id: str
    entry_at: datetime
    exit_at: datetime
    return_bps: float


def event_study(
    events: list[Event],
    quotes: list[Quote],
    *,
    as_of: datetime,
    horizon: timedelta = timedelta(minutes=15),
    tolerance: timedelta = timedelta(minutes=1),
) -> list[Observation]:
    require_aware(as_of)
    if horizon <= timedelta(0) or tolerance < timedelta(0):
        raise ValueError("positive horizon and nonnegative tolerance required")
    available = sorted((q for q in quotes if q.timestamp <= as_of), key=lambda q: q.timestamp)
    result: list[Observation] = []
    for event in events:
        entry = next(
            (
                q
                for q in available
                if event.available_at <= q.timestamp <= event.available_at + tolerance
            ),
            None,
        )
        if entry is None:
            continue
        end = entry.timestamp + horizon
        exit_quote = next((q for q in available if end <= q.timestamp <= end + tolerance), None)
        if exit_quote is not None:
            result.append(
                Observation(
                    event_id=event.event_id,
                    entry_at=entry.timestamp,
                    exit_at=exit_quote.timestamp,
                    return_bps=(exit_quote.mid / entry.mid - 1) * 10000,
                )
            )
    return result


def similar_events(
    target: Event, candidates: list[Event], *, as_of: datetime, limit: int = 5
) -> list[Event]:
    require_aware(as_of)
    if limit < 1:
        raise ValueError("limit must be positive")
    eligible = [
        e
        for e in candidates
        if e.available_at < min(as_of, target.available_at)
        and e.event_id != target.event_id
        and e.record.episode_id != target.record.episode_id
        and e.record.kind == target.record.kind
        and e.extraction.stance == target.extraction.stance
    ]
    return sorted(eligible, key=lambda e: e.available_at, reverse=True)[:limit]


def summarize(observations: list[Observation]) -> dict[str, float | int | None]:
    return {
        "sample_count": len(observations),
        "mean_mid_return_bps": mean(o.return_bps for o in observations) if observations else None,
    }
