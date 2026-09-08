"""Explicit unvalidated baseline: hawkish USD maps to short EUR/USD."""

from datetime import datetime

from .core import Event, Signal, Stance


def score_event(event: Event, *, now: datetime) -> Signal:
    if now < event.available_at:
        raise ValueError("Cannot score information from the future")
    target = (
        -1
        if event.extraction.stance == Stance.HAWKISH
        else (1 if event.extraction.stance == Stance.DOVISH else 0)
    )
    return Signal(
        event_id=event.event_id,
        episode_id=event.record.episode_id,
        decided_at=now,
        target=target,
        score=float(target),
        reason="Uncalibrated stance baseline; zero means abstain, not an exit instruction.",
    )
