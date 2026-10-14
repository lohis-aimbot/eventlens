"""Offline research accounting. No model calls, brokerage connection or real orders."""

import argparse
import json
from pathlib import Path

from .research_data import (
    ResearchCatalog,
    parse_probe_window,
    read_legacy_checkpoints,
    read_window,
    save_window,
)
from .research_replay import accounting_grid


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("window", "legacy", "probe"):
        command = commands.add_parser(name)
        command.add_argument("path", type=Path)
        command.add_argument("--output", type=Path, help="Optional local JSON report path")
        if name == "probe":
            command.add_argument("--catalog", type=Path, default=Path("configs/fomc-2025.json"))
            command.add_argument("--event-id", required=True)
            command.add_argument("--save-window", type=Path, help="Optional .json.gz evidence path")
    args = parser.parse_args()
    if args.command == "probe":
        catalog = ResearchCatalog.model_validate_json(args.catalog.read_bytes())
        event = next((item for item in catalog.events if item.event_id == args.event_id), None)
        if event is None:
            parser.error("Event ID is absent from the predeclared catalog")
        window = parse_probe_window(args.path, event)
        if args.save_window is not None:
            save_window(window, args.save_window)
    elif args.command == "legacy":
        window = read_legacy_checkpoints(args.path)
    else:
        window = read_window(args.path)
    results = accounting_grid(window)
    report = {
        "scope": "accounting_diagnostics_only",
        "data_id": window.data_id,
        "data": window.model_dump(mode="json", exclude={"observations"}),
        "observation_count": len(window.observations),
        "status_counts": {
            status: sum(item.status == status for item in results)
            for status in sorted({item.status for item in results})
        },
        "results": [item.model_dump(mode="json") for item in results],
    }
    output = json.dumps(report, indent=2)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
