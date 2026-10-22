"""Grouped existing commands. No collector, model or broker starts by default."""

import argparse
import importlib
import sys
from collections.abc import Sequence

_RESEARCH = {
    "accounting": ("research_cli", "eventlens-research"),
    "market": ("market_cli", "eventlens-market"),
    "labels": ("replay_cli", "eventlens-replay"),
    "decision": ("decision_cli", "eventlens-decision"),
    "infer": ("inference_cli", "eventlens-infer"),
    "series": ("series_cli", "eventlens-series"),
}
_SOURCES = {"fed": ("fed_cli", "eventlens-fed")}


def _select(
    argv: Sequence[str], *, prog: str, description: str, choices: Sequence[str]
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("command", choices=choices)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _select(
        sys.argv[1:] if argv is None else argv,
        prog="eventlens",
        description=__doc__ or "EventLens commands",
        choices=("memory", "source", "research", "observe", "shadow"),
    )
    if args.command in {"memory", "observe", "shadow"}:
        module, legacy_name = {
            "memory": ("memory_cli", "eventlens-memory"),
            "observe": ("observer_cli", "eventlens-observe"),
            "shadow": ("shadow_cli", "eventlens-shadow"),
        }[args.command]
        remaining = args.arguments
    else:
        routes = _SOURCES if args.command == "source" else _RESEARCH
        selected = _select(
            args.arguments,
            prog=f"eventlens {args.command}",
            description="Existing commands; model calls retain their original opt-in gates.",
            choices=tuple(routes),
        )
        module, legacy_name = routes[selected.command]
        remaining = selected.arguments
    # Reuse existing argparse entrypoints, including their original safety checks.
    previous = sys.argv
    try:
        sys.argv = [legacy_name, *remaining]
        importlib.import_module(f"eventlens.{module}").main()
    finally:
        sys.argv = previous


if __name__ == "__main__":
    main()
