"""Offline CLI. No broker credentials or network access required."""

import argparse
import json
import os
from datetime import timedelta
from pathlib import Path

from .backtesting import replay
from .core import Event
from .ingestion import JsonlSource, read_quotes
from .parsing import RuleParser
from .research import event_study, similar_events, summarize
from .risk import RiskConfig
from .storage import EventStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("schema", help="Print the standard event JSON schema")
    run = sub.add_parser("demo", help="Run local FOMC fixtures; no live trading")
    run.add_argument("--events", type=Path, default=Path("examples/fomc.jsonl"))
    run.add_argument("--quotes", type=Path, default=Path("examples/eurusd.jsonl"))
    run.add_argument(
        "--database-url", default=os.environ.get("EVENTLENS_DATABASE_URL", "sqlite:///eventlens.db")
    )
    run.add_argument("--latency-seconds", type=float, default=2)
    run.add_argument("--risk-config", type=Path)
    args = parser.parse_args()
    if args.command == "schema":
        print(json.dumps(Event.model_json_schema(), indent=2))
        return
    if args.latency_seconds < 0:
        parser.error("latency must be nonnegative")
    rule_parser = RuleParser()
    parsed = [
        rule_parser.parse(
            record, available_at=record.received_at + timedelta(seconds=args.latency_seconds)
        )
        for record in JsonlSource(args.events).read()
    ]
    quotes = read_quotes(args.quotes)
    if not quotes:
        parser.error("at least one quote required")
    config = (
        RiskConfig.model_validate_json(args.risk_config.read_text())
        if args.risk_config
        else RiskConfig()
    )
    store = EventStore(args.database_url)
    try:
        inserted = sum(store.put(event) for event in parsed)
        # Replay only this input, not unrelated prior database runs.
        result = replay(parsed, quotes, config)
        observations = event_study(parsed, quotes, as_of=max(q.timestamp for q in quotes))
        historical = (
            []
            if not parsed
            else similar_events(
                parsed[-1], store.list(as_of=parsed[-1].available_at), as_of=parsed[-1].available_at
            )
        )
        print(
            json.dumps(
                {
                    "dataset": "user-supplied; bundled example is synthetic",
                    "events_parsed": len(parsed),
                    "new_events_stored": inserted,
                    "historical_analogue_ids": [e.event_id for e in historical],
                    "event_study": summarize(observations),
                    "replay": result.model_dump(mode="json"),
                },
                indent=2,
            )
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()
