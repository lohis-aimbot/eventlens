"""Chronological FOMC research plan, opt-in run/resume and read-only report."""

import argparse
import asyncio
import gzip
import json
import logging
import os
from pathlib import Path

from .deepseek import DeepSeekClient, ModelCallError
from .series_replay import SeriesPlan, SeriesRunner, SeriesStore, freeze_series


def read_plan(path: Path) -> SeriesPlan:
    data = path.read_bytes()
    return SeriesPlan.model_validate_json(gzip.decompress(data) if path.suffix == ".gz" else data)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/replay-series.sqlite"))
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("path", type=Path)
    run = commands.add_parser("run")
    run.add_argument("path", type=Path)
    run.add_argument("--call-model", action="store_true")
    run.add_argument(
        "--max-events", type=int, help="Stop cleanly between episodes; resume same plan"
    )
    report = commands.add_parser("report")
    report.add_argument("run_id")
    abandon = commands.add_parser(
        "abandon-interrupted", help="Explicitly record unknown case; NO retry"
    )
    abandon.add_argument("run_id")
    abandon.add_argument("event_id")
    abandon.add_argument("--acknowledge-unknown-billing", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    if args.command == "plan":
        print(freeze_series(read_plan(args.path)).model_dump_json(indent=2))
        return
    if args.command == "run" and not args.call_model:
        parser.error("run/resume requires --call-model; completed episodes are not called again")
    if args.command == "abandon-interrupted" and not args.acknowledge_unknown_billing:
        parser.error(
            "Explicit --acknowledge-unknown-billing required; the case will not be retried"
        )
    store = SeriesStore(args.database, readonly=args.command == "report")
    try:
        if args.command == "run":
            # The series runner journals every attempt; inner retries must stay zero.
            key = os.environ.get("DEEPSEEK_API_KEY")
            if not key:
                raise ModelCallError("Set DEEPSEEK_API_KEY locally; never paste or commit it")
            client = DeepSeekClient(key, max_retries=0)
            result = asyncio.run(
                SeriesRunner(store, client).run(read_plan(args.path), max_events=args.max_events)
            )
        elif args.command == "abandon-interrupted":
            store.abandon_interrupted(args.run_id, args.event_id)
            result = store.report(args.run_id)
        else:
            result = store.report(args.run_id)
        print(json.dumps(result, indent=2))
    finally:
        store.close()


if __name__ == "__main__":
    main()
