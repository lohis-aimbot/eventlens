"""Diagnostic evidence cannot silently select a fee rate or claim full ticks."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from eventlens.research_data import ResearchCatalog, read_probe_diagnostics
from eventlens.research_replay import CostSensitivityConfig

EVENT = ResearchCatalog.model_validate_json(Path("configs/fomc-2025.json").read_bytes()).events[2]


def fixture_log(tmp_path):
    path = tmp_path / "probe.log"
    path.write_text(
        "EVENTLENS_PROBE|start|symbol=EURUSD|mode=demo_backtest_only|focus_utc=2025-05-07T18:00:00Z|orders=none|sample_interval_ms=100\n"
        "EVENTLENS_PROBE|symbol_cost|symbol=EURUSD|commission=3|commission_type=UsdPerMillionUsdVolume|min_commission=0|min_commission_type=QuoteAsset|min_commission_asset=USD|lot_size=100000|pip_size=0.0001|digits=5|basis=demo_backtest_symbol_metadata\n"
        "EVENTLENS_PROBE|quote|time_utc=2025-05-07T18:00:01Z|bid=1|ask=1\n"
        "EVENTLENS_PROBE|quote|time_utc=2025-05-07T18:00:02Z|bid=1|ask=1.0001\n"
        "EVENTLENS_PROBE|window_quality|valid_ticks=2|zero_spread_ticks=1|max_tick_gap_ms=1000|first_utc=2025-05-07T18:00:01Z|last_utc=2025-05-07T18:00:02Z\n"
        "EVENTLENS_PROBE|summary|invalid_quotes=0|crossed_quotes=0|backward_times=0|window_samples=2\n"
        "EVENTLENS_PROBE|checkpoint|focus_utc=2025-05-07T18:00:00Z|offset_seconds=1|quote_utc=none|bid=none|ask=none\n"
    )
    return path


def test_metadata_basis_and_all_tick_counters_match_observations(tmp_path):
    result = read_probe_diagnostics(fixture_log(tmp_path), EVENT)
    assert result.symbol_cost.basis == "demo_backtest_symbol_metadata"
    assert result.symbol_cost.commission == 3
    assert result.tick_coverage.valid_ticks == 2 and result.tick_coverage.zero_spread_ticks == 1


@pytest.mark.parametrize(
    "old,new,match",
    [
        ("zero_spread_ticks=1", "zero_spread_ticks=2", "conflict"),
        (
            "commission_type=UsdPerMillionUsdVolume",
            "commission_type=UnknownUnits",
            "commission_type",
        ),
        ("valid_ticks=2", "valid_ticks=1", "Sample count"),
    ],
)
def test_unknown_units_and_false_tick_completeness_are_rejected(tmp_path, old, new, match):
    path = fixture_log(tmp_path)
    path.write_text(path.read_text().replace(old, new))
    with pytest.raises(ValueError, match=match):
        read_probe_diagnostics(path, EVENT)


def test_duplicate_diagnostics_and_mixed_cost_profiles_are_rejected(tmp_path):
    path = fixture_log(tmp_path)
    marker = next(line for line in path.read_text().splitlines() if "|symbol_cost|" in line)
    path.write_text(path.read_text() + marker + "\n")
    with pytest.raises(ValueError, match="Duplicate"):
        read_probe_diagnostics(path, EVENT)
    config = CostSensitivityConfig.model_validate_json(
        Path("configs/cost-sensitivity.json").read_bytes()
    )
    changed = config.model_dump(mode="json")
    changed["profiles"][1]["commission_bps"] = "1"
    with pytest.raises(ValidationError, match="identical fees"):
        CostSensitivityConfig.model_validate(changed)
