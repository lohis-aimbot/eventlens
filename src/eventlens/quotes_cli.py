"""Bounded, local Demo quote observation and read-only news alignment. No orders."""

import argparse
import json
import logging
import math
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Sequence

from .observer import ObserverStore
from .quotes import QuoteStore, align_news
from .sources.ctrader_demo import DemoQuoteCollector, DirectoryTail, now_utc

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="eventlens observe quotes", description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/live-quotes.sqlite"))
    commands = parser.add_subparsers(dest="command", required=True)
    watch = commands.add_parser("watch", help="Start before the Demo bridge; new files only")
    watch.add_argument("directory", type=Path, help="EventLensQuoteBridge sandbox directory")
    watch.add_argument("--seconds", type=float, default=180, help="Bounded watch duration, 10–600s")
    commands.add_parser("report", help="Read-only stored quote summary")
    align = commands.add_parser("align", help="Retrospective observable-input audit; no signal")
    align.add_argument("source_id")
    align.add_argument("--events", type=Path, default=Path("data/observer.sqlite"))
    align.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    args = parser.parse_args(argv)
    if args.command == "watch" and (
        not math.isfinite(args.seconds) or not 10 <= args.seconds <= 600
    ):
        parser.error("Watch duration must be finite and between 10 and 600 seconds")
    try:
        store = QuoteStore(args.database, readonly=args.command != "watch")
        try:
            if args.command == "report":
                print(json.dumps(store.report(), indent=2))
            elif args.command == "align":
                events = ObserverStore(args.events, readonly=True)
                try:
                    print(
                        json.dumps(
                            align_news(events, store, args.source_id, args.as_of),
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                finally:
                    events.close()
            else:
                with store.exclusive():
                    tail = DirectoryTail(args.directory)
                    collector = DemoQuoteCollector(store, now_utc())
                    deadline = time.monotonic() + args.seconds
                    counts = {"start": 0, "quote": 0, "stop": 0, "rejected": 0}
                    print(
                        json.dumps(
                            {
                                "status": "watching",
                                "execution_authorized": False,
                                "seconds": args.seconds,
                            }
                        ),
                        flush=True,
                    )
                    while time.monotonic() < deadline:
                        for raw in tail.poll():
                            outcome = collector.accept(raw, now_utc())
                            counts[outcome] += 1
                            if outcome in {"start", "stop", "rejected"}:
                                log = logger.warning if outcome == "rejected" else logger.info
                                log("demo_quote_bridge outcome=%s", outcome)
                        if collector.stopped:
                            break
                        time.sleep(0.05)
                    print(
                        json.dumps(
                            {
                                "status": "observed_closed"
                                if counts["quote"] and collector.stopped
                                else "observed_unclosed"
                                if counts["quote"]
                                else "no_quotes",
                                "counts": counts,
                                "report": store.report(),
                            }
                        ),
                        flush=True,
                    )
                    if counts["rejected"] or not counts["quote"] or not collector.stopped:
                        parser.exit(1, "Quote watch needs inspection; no orders are possible.\n")
        finally:
            store.close()
    except (ValueError, RuntimeError, OSError, sqlite3.Error) as exc:
        parser.exit(1, f"Quote observation stopped: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
