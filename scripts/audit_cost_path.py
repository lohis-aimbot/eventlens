"""Offline spread-floor sensitivities and one explicitly selected dense-path check.

All floor profiles and stage delays are assumptions. No strategy directions are
chosen and no overall profitable-trade count is reported. Raw quotes are not
changed; this script prints only aggregate costs and provenance.
"""

import argparse
import hashlib
import json
from collections import Counter
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any, Literal

from eventlens.research_data import ProbeDiagnostics, ResearchCatalog, read_window
from eventlens.research_replay import (
    DEFAULT_LATENCIES,
    ENGINE_VERSION,
    CostSensitivityConfig,
    replay_roundtrip,
)


def audit_costs(catalog_path: Path, config_path: Path, data_dir: Path) -> dict[str, Any]:
    catalog = ResearchCatalog.model_validate_json(catalog_path.read_bytes())
    config = CostSensitivityConfig.model_validate_json(config_path.read_bytes())
    rows = []
    missing = []
    directions: tuple[Literal["long", "short"], ...] = ("long", "short")
    for event in catalog.events:
        path = data_dir / f"fomc-{event.source_time:%Y-%m-%d}-window.json.gz"
        if not path.exists():
            missing.append(event.event_id)
            continue
        window = read_window(path)
        if (window.event_id, window.source_time, window.source_reference) != (
            event.event_id,
            event.source_time,
            event.source_reference,
        ) or window.provider != "ctrader_demo":
            raise ValueError("Cost evidence does not match the declared Demo event")
        cases = [
            (direction, horizon, latency)
            for latency in DEFAULT_LATENCIES
            for horizon in (300, 1800)
            for direction in directions
        ]
        baseline = [
            replay_roundtrip(
                window,
                direction=direction,
                horizon_seconds=horizon,
                latency=latency,
                costs=config.profiles[0],
            )
            for direction, horizon, latency in cases
        ]
        previous_pnl: dict[int, Decimal] = {}
        profiles = []
        for cost in config.profiles:
            results = [
                replay_roundtrip(
                    window,
                    direction=direction,
                    horizon_seconds=horizon,
                    latency=latency,
                    costs=cost,
                )
                for direction, horizon, latency in cases
            ]
            extras = []
            for index, (base, result) in enumerate(zip(baseline, results)):
                if base.status != "closed" or result.status != "closed":
                    continue
                if result.closed_net_pnl_usd is None or base.closed_net_pnl_usd is None:
                    raise ValueError("Closed result is missing PnL")
                if index in previous_pnl and result.closed_net_pnl_usd > previous_pnl[index]:
                    raise ValueError("Increasing spread floor improved same-case net PnL")
                previous_pnl[index] = result.closed_net_pnl_usd
                extras.append(base.closed_net_pnl_usd - result.closed_net_pnl_usd)
            profiles.append(
                {
                    "cost": cost.model_dump(mode="json"),
                    "status_counts": dict(sorted(Counter(r.status for r in results).items())),
                    "closed_baseline_comparisons": len(extras),
                    "median_extra_cost_usd": str(median(extras)) if extras else None,
                    "max_extra_cost_usd": str(max(extras)) if extras else None,
                }
            )
        rows.append(
            {
                "event_id": event.event_id,
                "data_id": window.data_id,
                "independent_events": 1,
                "benchmark_cases_per_profile": len(cases),
                "profiles": profiles,
            }
        )
    return {
        "scope": "Cost sensitivity of fixed accounting benchmarks, not strategy alpha",
        "engine_version": ENGINE_VERSION,
        "config": config.model_dump(mode="json"),
        "config_id": hashlib.sha256(config.model_dump_json().encode()).hexdigest(),
        "catalog_version": catalog.version,
        "missing_events": missing,
        "rows": rows,
    }


def compare_march_path(data_dir: Path) -> dict[str, Any]:
    coarse = read_window(data_dir / "fomc-2025-03-19-window.json.gz")
    dense = read_window(data_dir / "fomc-2025-03-19-window-100ms.json.gz")
    diagnostics = ProbeDiagnostics.model_validate_json(
        (data_dir / "fomc-2025-03-19-100ms-diagnostics.json").read_bytes()
    )
    if (
        (coarse.event_id, coarse.source_time, coarse.source_reference)
        != (dense.event_id, dense.source_time, dense.source_reference)
        or coarse.sample_interval_ms != 1000
        or dense.sample_interval_ms != 100
        or coarse.provider != "ctrader_demo"
        or dense.provider != "ctrader_demo"
        or dense.data_id != diagnostics.data_id
        or dense.event_id != diagnostics.event_id
    ):
        raise ValueError("Dense-path evidence identities, provider or intervals differ")
    subset = set(coarse.observations) <= set(dense.observations)
    if not subset:
        raise ValueError("Dense replay does not reproduce old one-second observations")
    dense_gap = max(
        (
            (b.timestamp - a.timestamp) // timedelta(milliseconds=1)
            for a, b in zip(dense.observations, dense.observations[1:])
        ),
        default=0,
    )
    coverage = diagnostics.tick_coverage
    if (
        coverage.valid_ticks != len(dense.observations)
        or coverage.max_tick_gap_ms != dense_gap
        or coverage.first_utc != dense.observations[0].timestamp
        or coverage.last_utc != dense.observations[-1].timestamp
        or coverage.zero_spread_ticks != sum(q.bid == q.ask for q in dense.observations)
    ):
        raise ValueError("Dense snapshots do not reconcile to all valid replay ticks")
    return {
        "selection": "March chosen for the largest pre-existing sampled gap; a diagnostic, not another independent event",
        "coarse_data_id": coarse.data_id,
        "dense_data_id": dense.data_id,
        "coarse_snapshots": len(coarse.observations),
        "dense_snapshots": len(dense.observations),
        "old_quotes_subset_of_dense": subset,
        "all_valid_tick_counters_reconciled": True,
        "dense_max_gap_ms": dense_gap,
        "dense_gzip_bytes": (data_dir / "fomc-2025-03-19-window-100ms.json.gz").stat().st_size,
        "probe_diagnostics": diagnostics.model_dump(mode="json"),
        "remaining_limit": "All ticks delivered to this Demo backtest probe are counted; independent feed completeness and executable costs are unverified",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--catalog", type=Path, default=Path("configs/fomc-2025.json"))
    parser.add_argument("--cost-config", type=Path, default=Path("configs/cost-sensitivity.json"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = audit_costs(args.catalog, args.cost_config, args.data_dir)
    report["march_dense_check"] = compare_march_path(args.data_dir)
    output = json.dumps(report, indent=2)
    if args.output is None:
        print(output)
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
