"""Collect official Fed FOMC statements; model calls are opt-in and memory-only."""

import argparse
import asyncio
import json
import logging
from dataclasses import asdict
from pathlib import Path

from .deepseek import DeepSeekClient
from .fed import FedInbox, FedMonetarySource, FedWatcher, FeedError, SourceRejected, WatcherBusy
from .sqlite_memory import SQLiteThesisMemory


async def run(database: Path, *, reason: bool, watch: bool, interval: float) -> None:
    memory = SQLiteThesisMemory(database)
    watcher = FedWatcher(
        FedMonetarySource(),
        memory,
        FedInbox(database),
        model=DeepSeekClient.from_environment() if reason else None,
    )
    while True:
        try:
            result = await watcher.poll_once()
        except WatcherBusy:
            raise
        except (FeedError, SourceRejected) as exc:
            logging.warning("fed_poll_failed error_type=%s", type(exc).__name__)
            if not watch:
                raise
            print(json.dumps({"error": type(exc).__name__, "detail": str(exc)}), flush=True)
        else:
            print(json.dumps(asdict(result)), flush=True)
        if not watch:
            break
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument(
        "--reason", action="store_true", help="Call DeepSeek for new pending events"
    )
    parser.add_argument("--watch", action="store_true", help="Poll continuously; default is once")
    parser.add_argument("--interval", type=float, default=60, help="Watch polling seconds (min 30)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if args.database == Path("data/memory-demo.sqlite"):
        parser.error("Choose a dedicated database, not the synthetic memory demo")
    if args.watch and args.interval < 30:
        parser.error("Watch interval must be at least 30 seconds")
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    try:
        asyncio.run(
            run(args.database, reason=args.reason, watch=args.watch, interval=args.interval)
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
