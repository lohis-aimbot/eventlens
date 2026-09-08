"""Validated, timezone-aware contracts shared by all modules."""

from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Stance(str, Enum):
    HAWKISH = "hawkish"
    DOVISH = "dovish"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"


class SourceRecord(Model):
    source: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    published_at: AwareDatetime
    received_at: AwareDatetime
    text: str = Field(min_length=1)
    url: str | None = None
    kind: Literal["fomc_statement", "fomc_followup", "other"] = "fomc_statement"
    episode_id: str = Field(min_length=1)
    revision: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def chronology(self) -> Self:
        if self.received_at < self.published_at:
            raise ValueError("received_at must not precede published_at")
        return self

    @property
    def record_id(self) -> str:
        return sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def content_hash(self) -> str:
        return sha256(" ".join(self.text.lower().split()).encode()).hexdigest()


class Extraction(Model):
    stance: Stance
    evidence: tuple[str, ...] = ()
    rationale: str


class Event(Model):
    schema_version: Literal["1"] = "1"
    event_id: str
    record: SourceRecord
    available_at: AwareDatetime
    parser_version: str
    extraction: Extraction
    entity: Literal["US.FOMC"] = "US.FOMC"
    instrument: Literal["EURUSD"] = "EURUSD"
    surprise: float | None = None

    @model_validator(mode="after")
    def chronology(self) -> Self:
        if self.available_at < self.record.received_at:
            raise ValueError("available_at must include receipt and parsing latency")
        return self


class Quote(Model):
    timestamp: AwareDatetime
    instrument: Literal["EURUSD"] = "EURUSD"
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)

    @model_validator(mode="after")
    def spread_valid(self) -> Self:
        if self.ask < self.bid:
            raise ValueError("ask must be >= bid")
        return self

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


class Signal(Model):
    event_id: str
    episode_id: str
    decided_at: AwareDatetime
    target: Literal[-1, 0, 1]
    score: float = Field(ge=-1, le=1)
    reason: str


def require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
