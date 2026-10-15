"""Offline, read-only cTrader Demo checkpoint labeling CLI."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from .research.replay_labels import label_replay, parse_probe_log


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path, help="Local cTrader backtest log text")
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--instrument", default="EUR/USD")
    parser.add_argument(
        "--source-time", required=True, help="Verified publication time, ISO 8601 UTC"
    )
    args = parser.parse_args()
    run = parse_probe_log(
        args.log,
        event_id=args.event_id,
        instrument=args.instrument,
        source_time=datetime.fromisoformat(args.source_time.replace("Z", "+00:00")),
    )
    labels = label_replay(run)
    print(
        json.dumps(
            {
                "run": run.model_dump(mode="json"),
                "labels": [label.model_dump(mode="json") for label in labels],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
