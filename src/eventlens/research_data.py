"""Research inputs and a predeclared sample; adapters do not infer live receipt."""

import gzip
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Self
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from .contracts import Contract, Identifier, Timestamp
from .replay_labels import ReplayRun, parse_probe_log
from .research_replay import ReplayObservation, ResearchWindow


class StudyEvent(Contract):
    event_id: Identifier
    source_time: Timestamp
    source_reference: Identifier

    @model_validator(mode="after")
    def official_statement(self) -> Self:
        local = self.source_time.astimezone(ZoneInfo("America/New_York"))
        date = local.strftime("%Y%m%d")
        expected = f"https://www.federalreserve.gov/newsevents/pressreleases/monetary{date}a.htm"
        if self.source_reference != expected or (
            local.hour,
            local.minute,
            local.second,
            local.microsecond,
        ) != (
            14,
            0,
            0,
            0,
        ):
            raise ValueError("Catalog must use the verified 14:00 New York statement release")
        return self


class ResearchCatalog(Contract):
    version: Identifier
    scope: Identifier
    events: tuple[StudyEvent, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_ordered_events(self) -> Self:
        if len({event.event_id for event in self.events}) != len(self.events):
            raise ValueError("Duplicate catalog event")
        if any(
            left.source_time >= right.source_time
            for left, right in zip(self.events, self.events[1:])
        ):
            raise ValueError("Catalog source times must be unique and chronological")
        return self


def read_window(path: Path) -> ResearchWindow:
    payload = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
    return ResearchWindow.model_validate_json(payload)


def save_window(window: ResearchWindow, path: Path) -> None:
    payload = window.model_dump_json().encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(payload, mtime=0) if path.suffix == ".gz" else payload)


def read_legacy_checkpoints(path: Path) -> ResearchWindow:
    """Keep old observations usable but explicitly mark the path as sparse."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    run = ReplayRun.model_validate(payload["run"])
    local = run.source_time.astimezone(ZoneInfo("America/New_York"))
    reference = (
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary"
        + local.strftime("%Y%m%d")
        + "a.htm"
    )
    quotes: dict[datetime, ReplayObservation] = {}
    for point in run.checkpoints:
        quote = ReplayObservation(timestamp=point.quote_time, bid=point.bid, ask=point.ask)
        if quote.timestamp in quotes and quote != quotes[quote.timestamp]:
            raise ValueError("Conflicting checkpoint quotes at the same timestamp")
        quotes[quote.timestamp] = quote
    return ResearchWindow(
        event_id=run.event_id,
        source_reference=reference,
        source_time=run.source_time,
        captured_at=datetime.now(UTC),
        provider="ctrader_demo",
        data_kind="demo_checkpoints",
        instrument=run.instrument,
        observations=tuple(quotes[key] for key in sorted(quotes)),
    )


def parse_probe_window(path: Path, event: StudyEvent) -> ResearchWindow:
    # Reuse established Demo, symbol, focus and zero-error validation.
    parse_probe_log(
        path, event_id=event.event_id, instrument="EUR/USD", source_time=event.source_time
    )
    interval: int | None = None
    expected_count: int | None = None
    quotes: list[ReplayObservation] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if "EVENTLENS_PROBE|" not in line:
            continue
        kind, *parts = line.split("EVENTLENS_PROBE|", 1)[1].split("|")
        fields = dict(part.split("=", 1) for part in parts)
        if kind == "start":
            focus = datetime.fromisoformat(fields.get("focus_utc", "").replace("Z", "+00:00"))
            if focus.tzinfo is None or focus != event.source_time:
                raise ValueError("Probe start focus does not match catalog source time")
            interval = int(fields.get("sample_interval_ms", "0"))
        elif kind == "summary":
            expected_count = int(fields.get("window_samples", "0"))
        elif kind == "quote":
            quotes.append(
                ReplayObservation(
                    timestamp=fields["time_utc"], bid=fields["bid"], ask=fields["ask"]
                )
            )
    if interval is None or interval <= 0 or not quotes or len(quotes) != expected_count:
        raise ValueError("Complete sampled window required; enable probe sampling before replay")
    start, end = event.source_time - timedelta(minutes=2), event.source_time + timedelta(minutes=35)
    if any(not start <= quote.timestamp <= end for quote in quotes):
        raise ValueError("Probe observation outside declared event window")
    return ResearchWindow(
        event_id=event.event_id,
        source_reference=event.source_reference,
        source_time=event.source_time,
        captured_at=datetime.now(UTC),
        provider="ctrader_demo",
        data_kind="demo_sampled_snapshots",
        sample_interval_ms=interval,
        observations=tuple(quotes),
    )
