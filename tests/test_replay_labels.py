"""Synthetic checkpoints verify timing and label arithmetic, not market performance."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from eventlens.replay_labels import ReplayCheckpoint, ReplayRun, label_replay, parse_probe_log

T = datetime(2025, 6, 18, 18, tzinfo=UTC)


def checkpoint(offset: int, bid: str, ask: str, *, lag_ms: int = 100) -> ReplayCheckpoint:
    return ReplayCheckpoint(
        offset_seconds=offset,
        quote_time=T + timedelta(seconds=offset, milliseconds=lag_ms),
        bid=bid,
        ask=ask,
    )


def run(*points: ReplayCheckpoint) -> ReplayRun:
    return ReplayRun(
        event_id="fed-2025-06-18",
        instrument="EUR/USD",
        source_time=T,
        focus_time=T,
        checkpoints=points,
    )


def test_delayed_entry_and_spread_only_label():
    result = label_replay(
        run(checkpoint(5, "1.10000", "1.10020"), checkpoint(65, "1.10100", "1.10120")),
        delays_seconds=(5,),
        horizons_seconds=(60,),
    )[0]
    assert result.status == "available"
    assert result.available_time == T + timedelta(seconds=5)
    assert result.entry_quote_time == T + timedelta(seconds=5, milliseconds=100)
    assert result.exit_quote_time == T + timedelta(seconds=65, milliseconds=100)
    assert result.mid_return == Decimal("1.10110") / Decimal("1.10010") - 1
    assert result.spread_only_long_return == Decimal("1.10100") / Decimal("1.10020") - 1
    assert result.spread_only_short_return == Decimal("1.10000") / Decimal("1.10120") - 1


def test_no_source_time_entry_and_missing_horizon_is_explicit():
    result = label_replay(
        run(checkpoint(1, "1.10000", "1.10020")),
        delays_seconds=(1,),
        horizons_seconds=(60,),
    )[0]
    assert result.status == "missing_checkpoint"
    assert result.mid_return is None
    assert result.available_time > result.event_source_time


def test_reject_future_or_late_checkpoint():
    with pytest.raises(ValidationError, match="precedes its target"):
        run(
            ReplayCheckpoint(
                offset_seconds=5,
                quote_time=T + timedelta(seconds=4),
                bid="1.10000",
                ask="1.10020",
            )
        )
    result = label_replay(
        run(checkpoint(5, "1.10000", "1.10020", lag_ms=3000), checkpoint(65, "1.10100", "1.10120")),
        delays_seconds=(5,),
        horizons_seconds=(60,),
    )[0]
    assert result.status == "late_quote"
    assert result.mid_return is None


def test_source_time_and_utc_guards():
    with pytest.raises(ValidationError, match="Focus must equal"):
        ReplayRun(
            event_id="x",
            instrument="EUR/USD",
            source_time=T,
            focus_time=T + timedelta(seconds=1),
            checkpoints=(),
        )
    with pytest.raises(ValidationError):
        ReplayRun(
            event_id="x",
            instrument="EUR/USD",
            source_time=datetime(2025, 6, 18, 18),
            focus_time=T,
            checkpoints=(),
        )
    with pytest.raises(ValidationError, match="Duplicate checkpoint"):
        run(checkpoint(1, "1.1", "1.2"), checkpoint(1, "1.1", "1.2"))


def test_parse_probe_log_and_label(tmp_path):
    log = tmp_path / "probe.log"
    log.write_text(
        "[info] EVENTLENS_PROBE|start|symbol=EURUSD|mode=demo_backtest_only"
        "|focus_utc=2025-06-18T18:00:00.0000000Z|orders=none\n"
        "[info] EVENTLENS_PROBE|summary|symbol=EURUSD|ticks=100"
        "|invalid_quotes=0|crossed_quotes=0|backward_times=0\n"
        "[info] EVENTLENS_PROBE|checkpoint|focus_utc=2025-06-18T18:00:00.0000000Z"
        "|offset_seconds=5|quote_utc=2025-06-18T18:00:05.1000000Z|bid=1.1|ask=1.1002\n"
        "[info] EVENTLENS_PROBE|checkpoint|focus_utc=2025-06-18T18:00:00.0000000Z"
        "|offset_seconds=65|quote_utc=2025-06-18T18:01:05.1000000Z|bid=1.101|ask=1.1012\n",
        encoding="utf-8",
    )
    parsed = parse_probe_log(log, event_id="fed-2025-06-18", instrument="EUR/USD", source_time=T)
    assert (
        label_replay(parsed, delays_seconds=(5,), horizons_seconds=(60,))[0].status == "available"
    )


def test_parse_rejects_wrong_event_time_and_invalid_probe(tmp_path):
    log = tmp_path / "bad.log"
    log.write_text(
        "EVENTLENS_PROBE|start|symbol=EURUSD|mode=demo_backtest_only|orders=none\n"
        "EVENTLENS_PROBE|summary|invalid_quotes=0|crossed_quotes=1|backward_times=0\n"
        "EVENTLENS_PROBE|checkpoint|focus_utc=2024-09-18T18:00:00Z"
        "|offset_seconds=1|quote_utc=2024-09-18T18:00:01Z|bid=1.1|ask=1.2\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="invalid probe summary"):
        parse_probe_log(log, event_id="x", instrument="EUR/USD", source_time=T)
    log.write_text(log.read_text().replace("crossed_quotes=1", "crossed_quotes=0"))
    with pytest.raises(ValidationError, match="Focus must equal"):
        parse_probe_log(log, event_id="x", instrument="EUR/USD", source_time=T)
