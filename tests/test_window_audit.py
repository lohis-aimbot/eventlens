"""Audit denominator, incomplete data and evidence identity checks."""

import runpy
from datetime import timedelta
from pathlib import Path

import pytest

from eventlens.research_data import ResearchCatalog
from eventlens.research_replay import ReplayObservation, ResearchWindow

AUDIT = runpy.run_path(str(Path(__file__).parents[1] / "scripts/audit_fomc_windows.py"))
CATALOG_PATH = Path("configs/fomc-2025.json")
EVENT = ResearchCatalog.model_validate_json(CATALOG_PATH.read_bytes()).events[0]


def fixture_window(*offset_ms: int) -> ResearchWindow:
    # Invented quotes for contract tests, never included in the real-data audit.
    return ResearchWindow(
        event_id=EVENT.event_id,
        source_time=EVENT.source_time,
        source_reference=EVENT.source_reference,
        captured_at=EVENT.source_time + timedelta(days=1),
        provider="ctrader_demo",
        data_kind="demo_sampled_snapshots",
        sample_interval_ms=1000,
        observations=tuple(
            ReplayObservation(
                timestamp=EVENT.source_time + timedelta(milliseconds=offset), bid="1", ask="1.0001"
            )
            for offset in offset_ms
        ),
    )


def test_full_second_denominator_excludes_zero_duration_boundary():
    row = AUDIT["profile_window"](EVENT, fixture_window(-119500, -118500, 2100000))
    assert row["snapshot_count"] == 3 and row["boundary_snapshots"] == 1
    assert row["expected_full_second_buckets"] == 2220
    assert row["occupied_buckets"] == 2 and row["missing_buckets"] == 2218
    assert row["status_counts"] == {"entry_unavailable": 36, "flat": 18}
    assert row["causal_fill_and_cash_checks_passed"] == 0


def test_duplicate_bucket_and_wrong_event_fail_instead_of_counting_coverage():
    with pytest.raises(ValueError, match="Multiple snapshots"):
        AUDIT["profile_window"](EVENT, fixture_window(-119500, -119400))
    with pytest.raises(ValueError, match="predeclared event"):
        AUDIT["profile_window"](
            EVENT, fixture_window(0).model_copy(update={"event_id": "different_event"})
        )


def test_missing_files_are_listed_without_substituting_synthetic_data(tmp_path):
    result = AUDIT["audit_catalog"](CATALOG_PATH, tmp_path)
    assert result["catalog_events"] == len(result["missing_events"]) == 8
    assert result["collected_events"] == result["total_snapshots"] == 0
    assert result["rows"] == []
