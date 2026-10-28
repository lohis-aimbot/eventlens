"""Capture source evidence only: no LLM, market connection, order or trading decision."""

import argparse
import asyncio
import json
import logging
import math
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .observer import CaptureBatch, CaptureSource, Kind, ObserverStore
from .sources.english_tier1 import (
    EURUSDEventFilter,
    Tier1EnglishSource,
    build_macro_schedule,
    format_macro_schedule_table,
)
from .sources.wallstreetcn import (
    CaptureError,
    WallStreetCNSource,
    filter_store_revisions,
    parse_capture,
)

logger = logging.getLogger(__name__)


async def capture(store: ObserverStore, args: argparse.Namespace) -> bool:
    resolved_source = args.source
    if resolved_source is None:
        resolved_source = (
            "wallstreetcn"
            if (args.date is not None or args.channel is not None)
            else "tier1_english"
        )
    failed = False
    if resolved_source == "tier1_english":
        tier1 = Tier1EnglishSource()
        kind_filter: Kind | None = None if args.kind == "both" else args.kind

        def _emit_stream_result(res: dict[str, Any]) -> None:
            nonlocal failed
            failed |= res.get("status") != "ok"
            print(json.dumps(res, ensure_ascii=False), flush=True)

        with store.exclusive():
            for poll in range(args.polls):
                await tier1.poll_due_streams(
                    store,
                    kind=kind_filter,
                    limit=args.limit,
                    day=args.date,
                    force=True,
                    on_stream_recorded=_emit_stream_result,
                )
                if poll + 1 < args.polls:
                    await asyncio.sleep(args.interval)
        return failed

    source: CaptureSource = WallStreetCNSource()
    channel = args.channel or "global"
    kinds: tuple[Kind, ...] = ("news", "calendar") if args.kind == "both" else (args.kind,)
    with store.exclusive():
        for poll in range(args.polls):
            for kind in kinds:
                try:
                    batch = await source.fetch(
                        kind, channel=channel, limit=args.limit, day=args.date
                    )
                except CaptureError as exc:
                    stream = f"{kind}/{channel if kind == 'news' else args.date or 'today'}"
                    store.failure(source.provider, stream, str(exc))
                    result: dict[str, Any] = {
                        "stream": stream,
                        "status": "fetch_failed",
                        "error": str(exc),
                    }
                    failed = True
                else:
                    result = store.record(batch)
                    failed |= result["status"] != "ok"
                    logger.info(
                        "observer_capture stream=%s counts=%s", batch.stream, result["counts"]
                    )
                print(json.dumps(result, ensure_ascii=False), flush=True)
            if poll + 1 < args.polls:
                await asyncio.sleep(args.interval)
    return failed


def demo(store: ObserverStore, path: Path) -> bool:
    fixture = json.loads(path.read_bytes())
    received = datetime.fromisoformat(fixture["received_at"])
    day = date.fromisoformat(fixture["calendar_day"])
    failed = False
    with store.exclusive():
        for kind in ("news", "calendar"):
            payload = json.dumps(fixture[kind], ensure_ascii=False).encode()
            records, rejected, error = parse_capture(
                payload, kind=kind, received_at=received, day=day
            )
            batch = CaptureBatch(
                provider="wallstreetcn",
                stream=f"{kind}/synthetic",
                kind=kind,
                basis="synthetic_fixture",
                request_url="synthetic://observer-fixture",
                request_started_at=received,
                received_at=received,
                elapsed_ms=0,
                attempts=1,
                payload=payload,
                records=records,
                rejected=rejected,
                parse_error=error,
            )
            result = store.record(batch)
            failed |= result["status"] != "ok"
            print(json.dumps(result, ensure_ascii=False))
    return failed


def main() -> None:
    if sys.argv[1:2] == ["quotes"]:
        from .quotes_cli import main as quotes_main

        quotes_main(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, help="Dedicated observer SQLite database")
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    poll = commands.add_parser("capture", help="Bounded public HTTP capture; no model or orders")
    poll.add_argument(
        "--source",
        choices=("tier1_english", "wallstreetcn"),
        default=None,
        help="Capture source stack (defaults to tier1_english)",
    )
    poll.add_argument("--kind", choices=("news", "calendar", "both"), default="both")
    poll.add_argument("--channel", choices=("global", "forex", "commodity"), default=None)
    poll.add_argument("--limit", type=int, default=35)
    poll.add_argument("--date", type=date.fromisoformat, help="Calendar date in Asia/Shanghai")
    poll.add_argument("--polls", type=int, default=1, help="1–100 bounded polling cycles")
    poll.add_argument("--interval", type=float, default=30, help="Seconds between cycles (min 10)")

    sched = commands.add_parser(
        "schedule",
        help="Read-only key macro window schedule (BJT + UTC) with explicit MISSING consensus markers",
    )
    sched.add_argument(
        "--currencies",
        type=str,
        default="USD,EUR",
        help="Comma-separated currency codes (e.g. USD,EUR or ALL)",
    )
    sched.add_argument(
        "--min-impact",
        choices=("high", "medium", "low"),
        default="medium",
        help="Minimum calendar impact tier to display (default: medium)",
    )
    sched.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="Specific calendar date in Asia/Shanghai (YYYY-MM-DD)",
    )
    sched.add_argument(
        "--hours",
        type=float,
        default=36.0,
        help="Hours ahead from now when --date is omitted (default: 36)",
    )
    sched.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="Output format (default: table)",
    )
    sched.add_argument(
        "--offline",
        action="store_true",
        help="Read schedule strictly from existing observer database without network calls",
    )

    example = commands.add_parser("demo", help="Offline invented inputs, tagged synthetic")
    example.add_argument("path", type=Path)
    commands.add_parser("report", help="Read-only capture report; no network calls")
    history = commands.add_parser("history", help="Read-only source revision history")
    history.add_argument("kind", choices=("news", "calendar"))
    history.add_argument("source_id")
    history.add_argument("--provider", type=str, default=None, help="Optional provider filter")
    history.add_argument("--basis", choices=("live_poll", "synthetic_fixture"), default="live_poll")
    history.add_argument(
        "--as-of", type=datetime.fromisoformat, help="Timezone-aware storage cutoff"
    )
    filtered = commands.add_parser("filter", help="Read-only EUR/USD event filter; no LLM calls")
    filtered.add_argument("--kind", choices=("news", "calendar", "both"), default="both")
    filtered.add_argument(
        "--basis", choices=("live_poll", "synthetic_fixture"), default="live_poll"
    )
    filtered.add_argument(
        "--as-of", type=datetime.fromisoformat, help="Timezone-aware storage cutoff"
    )
    filtered.add_argument("--limit", type=int, default=100)
    quotes = commands.add_parser(
        "quotes", add_help=False, help="Local Demo quote watch/report/alignment"
    )
    quotes.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command == "quotes":
        if args.database is not None or args.verbose:
            parser.error("Put quote options after 'quotes'; the news database is separate")
        from .quotes_cli import main as quotes_main

        quotes_main(args.arguments)
        return
    if args.command == "capture" and (
        not 1 <= args.polls <= 100
        or not 1 <= args.limit <= 100
        or not math.isfinite(args.interval)
        or args.interval < 10
    ):
        parser.error("Require 1–100 polls/items and a finite interval of at least 10 seconds")
    if args.command == "schedule" and (not math.isfinite(args.hours) or args.hours <= 0):
        parser.error("--hours must be a positive finite number")

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    database = args.database or Path(
        "data/observer-demo.sqlite" if args.command == "demo" else "data/observer.sqlite"
    )
    try:
        if args.command == "schedule":
            opt_store: ObserverStore | None = None
            if args.offline or database.is_file():
                opt_store = ObserverStore(database, readonly=True)
            try:
                ccys = [c.strip() for c in args.currencies.split(",") if c.strip()]
                sched_data = asyncio.run(
                    build_macro_schedule(
                        observer_store=opt_store,
                        currencies=ccys,
                        min_impact=args.min_impact,
                        target_date_bjt=args.date,
                        hours_ahead=args.hours,
                        offline_only=args.offline,
                    )
                )
                if args.format == "json":
                    print(json.dumps(sched_data, ensure_ascii=False, indent=2))
                else:
                    print(format_macro_schedule_table(sched_data))
            finally:
                if opt_store is not None:
                    opt_store.close()
            return

        store = ObserverStore(database, readonly=args.command in {"report", "history", "filter"})
        try:
            if args.command == "capture":
                failed = asyncio.run(capture(store, args))
                if failed:
                    parser.exit(1, "Capture retained failures/rejections; inspect the report.\n")
            elif args.command == "demo":
                if demo(store, args.path):
                    parser.exit(1, "Demo retained failures/rejections; inspect the report.\n")
            elif args.command == "history":
                print(
                    json.dumps(
                        [
                            item.model_dump(mode="json")
                            for item in store.history(
                                args.kind,
                                args.source_id,
                                provider=args.provider,
                                basis=args.basis,
                                as_of=args.as_of,
                            )
                        ],
                        ensure_ascii=False,
                        indent=2,
                    )
                )
            elif args.command == "filter":
                kind_arg: Kind | None = None if args.kind == "both" else args.kind
                print(
                    json.dumps(
                        filter_store_revisions(
                            store,
                            kind=kind_arg,
                            basis=args.basis,
                            as_of=args.as_of,
                            limit=args.limit,
                            event_filter=EURUSDEventFilter(),
                        ),
                        ensure_ascii=False,
                        indent=2,
                    )
                )
            else:
                print(json.dumps(store.report(), ensure_ascii=False, indent=2))
        finally:
            store.close()
    except (ValueError, RuntimeError, OSError, sqlite3.Error, CaptureError) as exc:
        parser.exit(1, f"Observer stopped: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
