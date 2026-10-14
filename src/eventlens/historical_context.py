"""Explicit hypothetical replay inputs, separate from live AgentContext and storage.

Only model_input is intended for a future model call. The audit envelope retains
archive receipt/capture times without relabelling them as historical delivery.
"""

import hashlib
from datetime import timedelta
from typing import Literal, Self

from pydantic import Field, model_validator

from .contracts import Contract, Event, Identifier, PortfolioSnapshot, Thesis, Timestamp
from .decision_protocol import DecisionReplayProtocol
from .research_data import StudyEvent
from .research_replay import ReplayObservation, ResearchWindow


def _digest(value: Contract) -> str:
    return hashlib.sha256(value.model_dump_json().encode()).hexdigest()


class ReplayThesisRevision(Contract):
    """Run-local memory only; thesis clocks and commit time are hypothetical.

    recorded_at is the actual local generation/recording time. This is not an
    adapter for importing existing live SQLite revisions into the past.
    """

    run_id: Identifier
    latency_scenario: Identifier
    basis: Literal["generated_for_replay"] = "generated_for_replay"
    thesis: Thesis
    replay_committed_at: Timestamp
    recorded_at: Timestamp

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if not self.thesis.updated_at <= self.replay_committed_at <= self.recorded_at:
            raise ValueError("Revision update, replay commit and actual recording are noncausal")
        return self


class HistoricalContextInput(Contract):
    run_id: Identifier
    current_event_id: Identifier
    archived_events: tuple[Event, ...] = Field(min_length=1)
    window: ResearchWindow
    latency_scenario: Identifier
    portfolio_reasoning_start: Timestamp
    # Observation time is not proof of when a live feed would have delivered it.
    assumed_market_delay_ms: int = Field(strict=True, ge=0)
    memory_history: tuple[ReplayThesisRevision, ...] = ()


class ReplayClock(Contract):
    basis: Literal["hypothetical_replay_assumptions"] = "hypothetical_replay_assumptions"
    latency_scenario: Identifier
    source_time: Timestamp
    event_available_at: Timestamp
    portfolio_reasoning_start: Timestamp
    reasoning_deadline: Timestamp
    assumed_market_delay_ms: int = Field(strict=True, ge=0)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if not (
            self.source_time
            <= self.event_available_at
            <= self.portfolio_reasoning_start
            <= self.reasoning_deadline
        ):
            raise ValueError("Replay cutoff must fit the pinned reasoning budget")
        return self


class ReplayEventView(Contract):
    event_id: Identifier
    source: Identifier
    source_type: Literal["macro"] = "macro"
    source_reference: Identifier
    source_timestamp: Timestamp
    assumed_available_at: Timestamp
    raw_text: str = Field(min_length=1)
    author: str | None = None


class ReplayQuoteView(Contract):
    instrument: Literal["EUR/USD"] = "EUR/USD"
    observation: ReplayObservation
    assumed_available_at: Timestamp


class ReplayModelInput(Contract):
    """Allowlisted model view: no full windows, labels, archive or API clocks."""

    context_version: Literal["historical-context-v1"] = "historical-context-v1"
    mode: Literal["offline_research_only"] = "offline_research_only"
    run_id: Identifier
    as_of: Timestamp
    event: ReplayEventView
    theses: tuple[Thesis, ...]
    selected_revision_count: int = Field(ge=0)
    portfolio: PortfolioSnapshot
    quote: ReplayQuoteView

    @model_validator(mode="after")
    def no_future(self) -> Self:
        if max(
            self.event.source_timestamp,
            self.event.assumed_available_at,
            self.quote.observation.timestamp,
            self.quote.assumed_available_at,
            self.portfolio.as_of,
        ) > self.as_of or any(t.updated_at > self.as_of for t in self.theses):
            raise ValueError("Replay model input contains future information")
        if len({t.thesis_id for t in self.theses}) != len(self.theses):
            raise ValueError("Replay snapshot contains duplicate theses")
        return self


class ReplayProvenance(Contract):
    protocol_id: Identifier
    market_data_id: Identifier
    market_captured_at: Timestamp
    market_provider: Literal["synthetic", "ctrader_demo"]
    market_data_kind: Literal["synthetic", "demo_checkpoints", "demo_sampled_snapshots"]
    original_events: tuple[Event, ...]
    selected_memory_records: tuple[ReplayThesisRevision, ...]


class HistoricalReplayContext(Contract):
    execution_authorized: Literal[False] = False
    model_input: ReplayModelInput
    clock: ReplayClock
    provenance: ReplayProvenance

    @property
    def model_input_id(self) -> str:
        # Future observations may change the audit data ID, never this input ID.
        return _digest(self.model_input)


def build_historical_context(
    payload: HistoricalContextInput,
    protocol: DecisionReplayProtocol = DecisionReplayProtocol(),
) -> HistoricalReplayContext:
    """Pure construction; unknown/stale/missing evidence fails before a model call."""
    payload = HistoricalContextInput.model_validate_json(payload.model_dump_json())
    protocol = DecisionReplayProtocol.model_validate_json(protocol.model_dump_json())
    scenario = next(
        (s for s in protocol.latency_scenarios if s.name == payload.latency_scenario), None
    )
    if scenario is None:
        raise ValueError("Unknown pinned latency scenario")
    events = {event.event_id: event for event in payload.archived_events}
    if len(events) != len(payload.archived_events):
        raise ValueError("Duplicate archived event ID")
    event = events.get(payload.current_event_id)
    if event is None:
        raise ValueError("Current event is absent from archived evidence")
    for item in events.values():
        if item.source_type != "macro":
            raise ValueError("Historical FOMC context requires macro events")
        StudyEvent(
            event_id=item.event_id,
            source_time=item.source_timestamp,
            source_reference=item.source_reference,
        )
    window = payload.window
    if (window.event_id, window.source_reference, window.source_time) != (
        event.event_id,
        event.source_reference,
        event.source_timestamp,
    ):
        raise ValueError("Market window does not match the current archived event")
    availability = {
        key: value.source_timestamp + timedelta(milliseconds=scenario.ingest_ms)
        for key, value in events.items()
    }
    cutoff = payload.portfolio_reasoning_start
    clock = ReplayClock(
        latency_scenario=scenario.name,
        source_time=event.source_timestamp,
        event_available_at=availability[event.event_id],
        portfolio_reasoning_start=cutoff,
        reasoning_deadline=availability[event.event_id]
        + timedelta(milliseconds=scenario.reasoning_ms),
        assumed_market_delay_ms=payload.assumed_market_delay_ms,
    )
    delay = timedelta(milliseconds=payload.assumed_market_delay_ms)
    quote = next((q for q in reversed(window.observations) if q.timestamp + delay <= cutoff), None)
    if quote is None:
        raise ValueError("No market observation available by the replay cutoff")
    if cutoff - quote.timestamp > timedelta(milliseconds=protocol.max_quote_age_ms):
        raise ValueError("Stale market observation at replay cutoff; no forward fill")

    # Validate complete run-local chains, then select by COMMIT time, not update time.
    ordered = sorted(
        payload.memory_history,
        key=lambda r: (r.replay_committed_at, r.thesis.thesis_id, r.thesis.revision),
    )
    previous: dict[str, ReplayThesisRevision] = {}
    selected: dict[str, ReplayThesisRevision] = {}
    count = 0
    for record in ordered:
        thesis = record.thesis
        if record.run_id != payload.run_id:
            raise ValueError("Memory belongs to a different replay run")
        if record.latency_scenario != scenario.name:
            raise ValueError("Memory belongs to a different latency scenario")
        old = previous.get(thesis.thesis_id)
        if thesis.revision != (old.thesis.revision + 1 if old else 1):
            raise ValueError("Memory revisions must form a complete unique chain")
        if old and (
            thesis.created_at != old.thesis.created_at
            or thesis.updated_at <= old.thesis.updated_at
            or record.replay_committed_at <= old.replay_committed_at
            or record.recorded_at < old.recorded_at
        ):
            raise ValueError("Memory revisions have inconsistent clocks or creation time")
        evidence = thesis.supporting_event_ids + thesis.contradicting_event_ids
        if not evidence or len(evidence) != len(set(evidence)):
            raise ValueError("Replay thesis requires distinct evidence references")
        if any(key not in events for key in evidence):
            raise ValueError("Thesis references unknown archived evidence")
        if any(availability[key] > thesis.updated_at for key in evidence):
            raise ValueError("Thesis revision uses evidence unavailable at update time")
        if not old and thesis.created_at < min(availability[key] for key in evidence):
            raise ValueError("Thesis creation precedes all its evidence")
        if any(events[key].received_timestamp > record.recorded_at for key in evidence):
            raise ValueError("Actual memory recording precedes archive receipt")
        previous[thesis.thesis_id] = record
        if record.replay_committed_at <= cutoff:
            selected[thesis.thesis_id] = record
            count += 1
    records = tuple(selected[key] for key in sorted(selected))
    included_ids = {event.event_id}
    for record in records:
        included_ids.update(record.thesis.supporting_event_ids)
        included_ids.update(record.thesis.contradicting_event_ids)
    model_input = ReplayModelInput(
        run_id=payload.run_id,
        as_of=cutoff,
        event=ReplayEventView(
            event_id=event.event_id,
            source=event.source,
            source_reference=event.source_reference,
            source_timestamp=event.source_timestamp,
            assumed_available_at=availability[event.event_id],
            raw_text=event.raw_text,
            author=event.author,
        ),
        theses=tuple(record.thesis for record in records),
        selected_revision_count=count,
        portfolio=PortfolioSnapshot(
            snapshot_id=f"{payload.run_id}:{event.event_id}:initial-flat",
            as_of=cutoff,
            base_currency="USD",
            equity=protocol.initial_cash_usd,
        ),
        quote=ReplayQuoteView(observation=quote, assumed_available_at=quote.timestamp + delay),
    )
    return HistoricalReplayContext(
        model_input=model_input,
        clock=clock,
        provenance=ReplayProvenance(
            protocol_id=protocol.protocol_id,
            market_data_id=window.data_id,
            market_captured_at=window.captured_at,
            market_provider=window.provider,
            market_data_kind=window.data_kind,
            original_events=tuple(events[key] for key in sorted(included_ids)),
            selected_memory_records=records,
        ),
    )
