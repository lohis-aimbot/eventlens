"""Inspect cTrader historical tick response pages offline; never connects or trades."""

import argparse
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from pydantic import AwareDatetime, TypeAdapter

from .ctrader_ticks import CTraderTickPage, decode_page
from .market_quality import inspect_ticks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="Local JSON with available_time and Bid/Ask pages")
    parser.add_argument("--max-side-age-ms", type=int, default=2000)
    args = parser.parse_args()
    if args.max_side_age_ms <= 0:
        parser.error("--max-side-age-ms must be positive")
    payload = json.loads(args.path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {
        "event_source_time",
        "available_time",
        "availability_basis",
        "pages",
    }:
        parser.error("Expected event_source_time, available_time, availability_basis and pages")
    pages = tuple(CTraderTickPage.model_validate(page) for page in payload["pages"])
    if len(pages) != 2 or {page.side for page in pages} != {"bid", "ask"}:
        parser.error("Provide exactly one Bid page and one Ask page")
    first = pages[0]
    if any(
        (page.instrument, page.request_start, page.request_end, page.digits)
        != (first.instrument, first.request_start, first.request_end, first.digits)
        for page in pages
    ):
        parser.error("Both pages must describe the same instrument and window")
    available_time: datetime = TypeAdapter(AwareDatetime).validate_python(payload["available_time"])
    event_source_time: datetime = TypeAdapter(AwareDatetime).validate_python(
        payload["event_source_time"]
    )
    ticks = tuple(tick for page in pages for tick in decode_page(page))
    report = inspect_ticks(
        ticks,
        instrument=first.instrument,
        window_start=first.request_start,
        window_end=first.request_end,
        event_source_time=event_source_time,
        available_time=available_time,
        availability_basis=payload["availability_basis"],
        complete=not any(page.has_more for page in pages),
        max_side_age=timedelta(milliseconds=args.max_side_age_ms),
        pip_size=Decimal("0.0001"),
    )
    print(report.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
