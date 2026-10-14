"""Observed local processing time; publisher age is not news transport latency."""

from typing import Literal, Self

from pydantic import computed_field, model_validator

from .contracts import AgentDecision, Contract, Event, Identifier, Timestamp


class ProcessingLatency(Contract):
    event_id: Identifier
    decision_id: Identifier
    provider: Identifier
    model_version: Identifier
    basis: Literal["observed_process_timestamps"] = "observed_process_timestamps"
    source_time: Timestamp
    received_time: Timestamp
    reasoning_started: Timestamp
    reasoning_completed: Timestamp

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if (
            not self.source_time
            <= self.received_time
            <= self.reasoning_started
            <= self.reasoning_completed
        ):
            raise ValueError("Processing clocks are noncausal")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def source_age_at_receipt_ms(self) -> int:
        return int((self.received_time - self.source_time).total_seconds() * 1000)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def receipt_to_start_ms(self) -> int:
        return int((self.reasoning_started - self.received_time).total_seconds() * 1000)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def reasoning_ms(self) -> int:
        return int((self.reasoning_completed - self.reasoning_started).total_seconds() * 1000)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def receipt_to_completion_ms(self) -> int:
        return int((self.reasoning_completed - self.received_time).total_seconds() * 1000)


def processing_latency(event: Event, decision: AgentDecision) -> ProcessingLatency:
    if event.event_id != decision.event_id:
        raise ValueError("Latency trace must refer to the trigger event")
    return ProcessingLatency(
        event_id=event.event_id,
        decision_id=decision.decision_id,
        provider=decision.provider,
        model_version=decision.model_version,
        source_time=event.source_timestamp,
        received_time=event.received_timestamp,
        reasoning_started=decision.started_at,
        reasoning_completed=decision.completed_at,
    )
