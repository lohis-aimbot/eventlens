"""Reproduce the bounded FOMC quote audit from local, ignored evidence only.

Run from the repository root with its installed package. No API/model/broker
calls, quote interpolation or strategy selection. Output contains aggregates,
hashes and clock metadata, never raw quotes or account identifiers.
"""

import argparse
import json
from collections import Counter
from datetime import timedelta
from decimal import Decimal
from math import ceil
from pathlib import Path
from statistics import median
from typing import Any

from eventlens.research_data import ResearchCatalog, StudyEvent, read_window
from eventlens.research_replay import ResearchWindow, accounting_grid


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def milliseconds(delta: timedelta) -> int:
    return delta // timedelta(milliseconds=1)


def profile_window(event: StudyEvent, window: ResearchWindow) -> dict[str, Any]:
    require(
        (window.event_id, window.source_time, window.source_reference)
        == (event.event_id, event.source_time, event.source_reference),
        "Window does not match the predeclared event",
    )
    require(
        window.provider == "ctrader_demo"
        and window.data_kind == "demo_sampled_snapshots"
        and window.sample_interval_ms == 1000,
        "Audit requires actual one-second Demo snapshots",
    )
    quotes = window.observations
    start, end = event.source_time - timedelta(minutes=2), event.source_time + timedelta(minutes=35)
    require(all(start <= q.timestamp <= end for q in quotes), "Observation outside bounded window")
    # 2,220 half-open one-second buckets. The exact +35m boundary is reported
    # separately, not counted as a fictitious missing full second.
    buckets = [milliseconds(q.timestamp - start) // 1000 for q in quotes if q.timestamp < end]
    require(len(buckets) == len(set(buckets)), "Multiple snapshots in one sample bucket")
    expected_buckets = milliseconds(end - start) // 1000
    gaps = [milliseconds(b.timestamp - a.timestamp) for a, b in zip(quotes, quotes[1:])]
    spreads = sorted((q.ask - q.bid) / Decimal("0.0001") for q in quotes)
    results = accounting_grid(window)
    require(len(results) == 54, "Unexpected accounting grid size")
    closed = [r for r in results if r.status == "closed"]
    for result in closed:
        entry, exit_fill = result.fills
        require(
            result.timeline.earliest_entry_fill <= entry.timestamp < result.timeline.exit_order_time
            and result.timeline.earliest_exit_fill <= exit_fill.timestamp,
            "Noncausal simulated fill",
        )
        require(
            milliseconds(entry.timestamp - result.timeline.earliest_entry_fill)
            <= result.policy.max_quote_wait_ms
            and milliseconds(exit_fill.timestamp - result.timeline.earliest_exit_fill)
            <= result.policy.max_quote_wait_ms,
            "Simulated quote exceeds wait limit",
        )
        units = result.policy.units_eur * (1 if result.direction == "long" else -1)
        independent_pnl = units * (exit_fill.price - entry.price) - (
            entry.commission_usd + exit_fill.commission_usd
        )
        require(
            result.closed_net_pnl_usd == independent_pnl
            and result.cash_usd == result.policy.initial_cash_usd + independent_pnl
            and result.liquidation_equity_usd == result.cash_usd
            and result.ending_units_eur == 0,
            "Closed-case cash or position invariant failed",
        )
    paths = {}
    for horizon in (300, 1800):
        subset = [r for r in results if r.direction != "flat" and r.horizon_seconds == horizon]
        paths[str(horizon)] = {
            "case_count": len(subset),
            "quality_counts": dict(sorted(Counter(r.path_quality for r in subset).items())),
            "max_gap_ms": max((r.max_observed_gap_ms or 0 for r in subset), default=0),
        }
    return {
        "event_id": event.event_id,
        "source_time": event.source_time.isoformat(),
        "source_reference": event.source_reference,
        "captured_at": window.captured_at.isoformat(),
        "data_id": window.data_id,
        "snapshot_count": len(quotes),
        "first_time": quotes[0].timestamp.isoformat(),
        "last_time": quotes[-1].timestamp.isoformat(),
        "leading_gap_ms": milliseconds(quotes[0].timestamp - start),
        "trailing_gap_ms": milliseconds(end - quotes[-1].timestamp),
        "expected_full_second_buckets": expected_buckets,
        "occupied_buckets": len(buckets),
        "missing_buckets": expected_buckets - len(buckets),
        "bucket_coverage_fraction": len(buckets) / expected_buckets,
        "boundary_snapshots": sum(q.timestamp == end for q in quotes),
        "max_gap_ms": max(gaps, default=0),
        "gaps_over_3s": sum(gap > 3000 for gap in gaps),
        "spread_pips": {
            "minimum": str(spreads[0]),
            "median": str(median(spreads)),
            "p95_nearest_rank": str(spreads[ceil(len(spreads) * 0.95) - 1]),
            "maximum": str(spreads[-1]),
            "zero_count": sum(s == 0 for s in spreads),
        },
        "status_counts": dict(sorted(Counter(r.status for r in results).items())),
        "causal_fill_and_cash_checks_passed": len(closed),
        "paths": paths,
    }


def audit_catalog(catalog_path: Path, data_dir: Path) -> dict[str, Any]:
    catalog = ResearchCatalog.model_validate_json(catalog_path.read_bytes())
    rows = []
    missing = []
    for event in catalog.events:
        date = event.source_time.strftime("%Y-%m-%d")
        path = data_dir / f"fomc-{date}-window.json.gz"
        if not path.exists():
            missing.append(event.event_id)
            continue
        row = profile_window(event, read_window(path))
        row["gzip_bytes"] = path.stat().st_size
        rows.append(row)
    return {
        "audit_version": "fomc-window-quality-v1",
        "scope": "Demo snapshot coverage and simulated accounting; no strategy alpha",
        "catalog_version": catalog.version,
        "catalog_events": len(catalog.events),
        "collected_events": len(rows),
        "missing_events": missing,
        "total_snapshots": sum(row["snapshot_count"] for row in rows),
        "total_gzip_bytes": sum(row["gzip_bytes"] for row in rows),
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=Path("configs/fomc-2025.json"))
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = json.dumps(audit_catalog(args.catalog, args.data_dir), indent=2)
    if args.output is None:
        print(output)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
