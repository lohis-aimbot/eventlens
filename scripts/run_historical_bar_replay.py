"""Offline historical minute-bar replay provenance & arrival-time sensitivity auditor.

Produces an explicit provenance, lookahead, and arrival-sensitivity report from a pinned
replay assumptions JSON without invoking paid LLM APIs, fetching live feeds, or labeling
synthetic bar quotes as `ctrader_algo` / `live_poll`.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from eventlens.replay_provenance import evaluate_bar_replay_assumptions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/fomc_2026_10_28_replay_assumptions.json"),
        help="Path to historical bar replay assumptions JSON",
    )
    args = parser.parse_args(argv)
    spec = json.loads(args.config.read_text(encoding="utf-8"))
    report = evaluate_bar_replay_assumptions(spec)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
