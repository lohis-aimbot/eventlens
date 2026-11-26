"""Tests for Stage A Research Lab, Chart Lab Web UI, Colab notebook, and export CLI."""

from __future__ import annotations

import ast
import csv
import gzip
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from eventlens.quotes import QuoteStore
from eventlens.research_lab import (
    aggregate_quote_bars,
    apply_event_cluster_dedup,
    build_chart_lab_payload,
    classify_fx_session,
    detect_optimal_entries,
    export_research_bundle,
    extract_attributed_events,
    score_decision_net_r,
    simulate_exit_templates,
    split_chronological_oos,
)
from eventlens.shadow_cli import main as shadow_main


def _make_sample_quotes_db(db_path: Path, *, start: datetime, minutes: int = 320) -> Path:
    store = QuoteStore(db_path)
    rows: list[tuple[str, int, str, str, str, str, str, str, str, str]] = []
    # Build a clean upward impulse from t=60m (+18 pips over 40m with <= 2p drawdown)
    # then range/whipsaw after t=180m.
    for i in range(minutes):
        ts = start + timedelta(minutes=i)
        if 60 <= i < 100:
            mid = 1.06000 + (i - 59) * 0.000045  # +18 pips steady climb
        elif 100 <= i < 140:
            mid = 1.06180 - (i - 100) * 0.00001
        elif 180 <= i < 210:
            mid = 1.06140 + (0.00120 if i % 2 == 0 else -0.00120)  # two-way +/-12p whipsaw trap
        else:
            mid = 1.06000 + (0.00002 if i % 2 == 0 else -0.00001)
        bid = round(mid - 0.00001, 5)
        ask = round(mid + 0.00001, 5)
        iso = ts.isoformat()
        payload = json.dumps(
            {
                "version": 1,
                "kind": "quote",
                "session_id": "s1",
                "sequence": i + 1,
                "symbol": "EURUSD",
                "source_at": iso,
                "bridge_at": iso,
                "bid": bid,
                "ask": ask,
            }
        )
        rows.append(("s1", i + 1, "EURUSD", "live_poll", iso, iso, iso, iso, iso, payload))
    with store.db:
        store.db.execute(
            "INSERT INTO quote_sessions (session_id, basis, start, received_at, stop) VALUES (?, ?, ?, ?, NULL)",
            ("s1", "live_poll", start.isoformat(), start.isoformat()),
        )
        store.db.executemany(
            """
            INSERT INTO quote_observations
            (session_id, sequence, symbol, basis, source_at, bridge_at, received_at, recorded_at, available_at, raw_line, quote)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?)
            """,
            rows,
        )
    store.close()
    return db_path


def _make_sample_observer_and_shadow_db(
    obs_db: Path,
    shadow_db: Path,
    *,
    event_dt: datetime,
    secret_url_leak: str = "https://investinglive.com/news/fed-surprise/?secret=sk-test1234567890abcdef",
) -> tuple[Path, Path]:
    o_con = sqlite3.connect(str(obs_db))
    o_con.executescript(
        """
        CREATE TABLE observer_revisions (
            provider TEXT NOT NULL, basis TEXT NOT NULL, kind TEXT NOT NULL,
            source_id TEXT NOT NULL, revision INTEGER NOT NULL, content_hash TEXT NOT NULL,
            received_at TEXT NOT NULL, recorded_at TEXT NOT NULL, record TEXT NOT NULL,
            PRIMARY KEY (provider, basis, kind, source_id, revision)
        );
        CREATE TABLE observer_enrichments (
            enrichment_id TEXT PRIMARY KEY, poll_id TEXT, provider TEXT NOT NULL,
            basis TEXT NOT NULL, source_id TEXT NOT NULL, source_url TEXT NOT NULL,
            rss_fingerprint TEXT, payload_hash TEXT NOT NULL, extracted_text TEXT NOT NULL,
            request_started_at TEXT NOT NULL, fetched_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
            available_at TEXT, metadata TEXT NOT NULL
        );
        """
    )
    pub_dt = (event_dt - timedelta(minutes=8)).isoformat()
    rec_json = json.dumps(
        {
            "source_id": "fed-surprise-1",
            "kind": "news",
            "source_url": secret_url_leak,
            "title": "Fed flags cooler inflation trajectory",
            "text": "Full release from /Users/tester/private/notes.txt",
            "original_published_at": pub_dt,
            "importance": "high",
        }
    )
    with o_con:
        o_con.execute(
            "INSERT INTO observer_revisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "investinglive",
                "live_poll",
                "news",
                "fed-surprise-1",
                1,
                "hash1",
                event_dt.isoformat(),
                event_dt.isoformat(),
                rec_json,
            ),
        )
    o_con.close()

    s_con = sqlite3.connect(str(shadow_db))
    s_con.executescript(
        """
        CREATE TABLE runtime_events (
            event_id TEXT PRIMARY KEY, source_reference TEXT NOT NULL,
            state TEXT NOT NULL, reason TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT NOT NULL, updated_at TEXT NOT NULL, received_at TEXT NOT NULL,
            context_json TEXT, decision_id TEXT, decision_json TEXT,
            memory_version INTEGER, assessment_id TEXT, assessment_json TEXT,
            execution_id TEXT, receipt_json TEXT,
            input_tokens INTEGER NOT NULL DEFAULT 0, output_tokens INTEGER NOT NULL DEFAULT 0,
            receipt_to_decision_ms INTEGER, receipt_to_fill_ms INTEGER
        );
        """
    )
    dec_json = json.dumps(
        {
            "portfolio_action": "SET_TARGET",
            "target": {"positions": [{"instrument": "EUR/USD", "weight": "0.50"}]},
        }
    )
    with s_con:
        s_con.execute(
            """
            INSERT INTO runtime_events (
                event_id, source_reference, state, reason, attempts,
                first_seen_at, updated_at, received_at, decision_id, decision_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "investinglive:news:fed-surprise-1:r1",
                secret_url_leak,
                "approved",
                "approved",
                1,
                event_dt.isoformat(),
                event_dt.isoformat(),
                event_dt.isoformat(),
                "dec-1",
                dec_json,
            ),
        )
    s_con.close()
    return obs_db, shadow_db


def test_fx_session_classification_utc_and_dst() -> None:
    # Winter (Nov): London is UTC+0 (08:00-16:30), NY AM overlap 13:00-17:00 UTC, Late NY 17:00-22:00 UTC
    assert classify_fx_session(datetime(2026, 11, 18, 3, 0, tzinfo=UTC)) == "asian"
    assert classify_fx_session(datetime(2026, 11, 18, 9, 30, tzinfo=UTC)) == "london"
    assert classify_fx_session(datetime(2026, 11, 18, 14, 0, tzinfo=UTC)) == "overlap"
    assert classify_fx_session(datetime(2026, 11, 18, 18, 30, tzinfo=UTC)) == "late_ny"
    assert classify_fx_session(datetime(2026, 11, 18, 22, 30, tzinfo=UTC)) == "asian"


def test_aggregate_detect_exit_reward_and_attribution(tmp_path: Path) -> None:
    start = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)  # London -> Overlap
    qdb = _make_sample_quotes_db(tmp_path / "live-quotes.sqlite", start=start, minutes=320)
    obs_db, sh_db = _make_sample_observer_and_shadow_db(
        tmp_path / "observer.sqlite",
        tmp_path / "shadow-ledger.sqlite",
        event_dt=start + timedelta(minutes=60),
    )

    agg = aggregate_quote_bars(qdb)
    assert agg["total_ticks"] == 320
    assert len(agg["bars_m1"]) == 320
    assert len(agg["bars_m5"]) == 64
    assert [int(b["t"]) for b in agg["bars_m5"]] == sorted(int(b["t"]) for b in agg["bars_m5"])
    assert len({int(b["t"]) for b in agg["bars_m5"]}) == len(agg["bars_m5"])

    markers = detect_optimal_entries(agg["bars_m1"], agg["bars_m5"])
    long_markers = [m for m in markers if m["side"] == "long"]
    assert long_markers, "Expected clean +18p climb to produce at least one long optimal marker"
    assert any(m["tier"] >= 2 for m in long_markers)

    # Simulate exit templates right at the start of the clean +18p climb
    entry_ts = int((start + timedelta(minutes=60)).timestamp())
    sim_long = simulate_exit_templates(agg["bars_m1"], entry_ts, "long", atr14_pips=8.0)
    assert sim_long["available"] is True
    assert sim_long["templates"]["T1_scalp"]["net_pips"] > 10.0
    assert sim_long["templates"]["T1_scalp"]["r_net"] > 1.0

    # Reward scoring checks: active winner vs. missed winner vs. two-way whipsaw trap avoid
    sc_win = score_decision_net_r(
        action="SET_TARGET",
        side="long",
        long_eval=sim_long["templates"]["T2_mid_swing"],
        short_eval=None,
        has_qualified_setup=True,
    )
    assert sc_win["regime"] == "active_entry"
    assert sc_win["score"] is not None and sc_win["score"] > 0

    sc_miss = score_decision_net_r(
        action="HOLD",
        side=None,
        long_eval=sim_long["templates"]["T1_scalp"],
        short_eval={"available": True, "r_net": -1.0, "sl_pips": 10.0, "mae_pips": 10.0},
        has_qualified_setup=True,
    )
    assert sc_miss["regime"] == "missed_qualified_setup"
    assert sc_miss["score"] < 0

    sc_avoid = score_decision_net_r(
        action="IGNORE_EVENT",
        side=None,
        long_eval={"available": True, "r_net": -1.0, "sl_pips": 14.0, "mae_pips": 14.0},
        short_eval={"available": True, "r_net": -1.0, "sl_pips": 14.0, "mae_pips": 14.0},
        has_qualified_setup=False,
    )
    assert sc_avoid["regime"] == "avoided_two_way_trap"
    assert sc_avoid["score"] == 0.25

    sc_err = score_decision_net_r(
        action="API_ERROR",
        side=None,
        long_eval=sim_long["templates"]["T1_scalp"],
        short_eval=None,
        has_qualified_setup=True,
    )
    assert sc_err["regime"] == "api_error"
    assert sc_err["score"] is None
    assert sc_err["r_net"] is None

    # Bidirectional event attribution + strict secret redaction
    events = extract_attributed_events(
        observer_db=obs_db,
        shadow_db=sh_db,
        bars_m1=agg["bars_m1"],
        algo_markers=markers,
        include_counterfactuals=True,
    )
    assert len(events) == 1
    ev = events[0]
    assert ev["runtime_state"] == "approved"
    assert ev["portfolio_action"] == "SET_TARGET"
    assert ev["target_direction"] == "long"
    assert ev["post_15m_pips"] is not None and ev["post_15m_pips"] > 5.0
    assert "[REDACTED_API_KEY]" in ev["snippet"] or "sk-test" not in json.dumps(events)
    assert "/Users/tester" not in json.dumps(events)

    # Chronological 70/30 OOS split
    in_s, oos_s, split_ts = split_chronological_oos(agg["bars_m5"], train_ratio=0.70)
    assert len(in_s) + len(oos_s) == len(agg["bars_m5"])
    assert split_ts is not None and int(in_s[-1]["t"]) < split_ts <= int(oos_s[0]["t"])


def test_export_research_bundle_cli_and_dashboard_api(tmp_path: Path, monkeypatch) -> None:
    start = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
    qdb = _make_sample_quotes_db(tmp_path / "live-quotes.sqlite", start=start, minutes=180)
    obs_db, sh_db = _make_sample_observer_and_shadow_db(
        tmp_path / "observer.sqlite",
        tmp_path / "shadow-ledger.sqlite",
        event_dt=start + timedelta(minutes=60),
    )
    out_dir = tmp_path / "bundle"
    cl_json = tmp_path / "chart-lab.json"

    res = export_research_bundle(
        quotes_db=qdb,
        observer_db=obs_db,
        shadow_db=sh_db,
        output_dir=out_dir,
        chart_lab_output=cl_json,
    )
    assert res["ticks"] == 180
    assert cl_json.is_file()
    assert (out_dir / "bars_m1.csv.gz").is_file()
    assert (out_dir / "bars_m5.csv").is_file()
    assert (out_dir / "events.json").is_file()
    assert (out_dir / "manifest.json").is_file()

    # Verify gzip M1 CSV readable & valid
    with gzip.open(out_dir / "bars_m1.csv.gz", "rt", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
        assert len(rows) == 180

    # Verify CLI invocation runs cleanly
    cli_out = tmp_path / "cli-chart-lab.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "eventlens-shadow",
            "export-research",
            "--shadow-db",
            str(sh_db),
            "--observer-db",
            str(obs_db),
            "--quotes-db",
            str(qdb),
            "--chart-lab-output",
            str(cli_out),
        ],
    )
    shadow_main()
    parsed = json.loads(cli_out.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == "chart-lab-v1"
    assert "sk-test" not in cli_out.read_text(encoding="utf-8")

    # Verify build_chart_lab_payload does not leak secrets and works cleanly for /api/chart-lab
    cl_payload = build_chart_lab_payload(quotes_db=qdb, observer_db=obs_db, shadow_db=sh_db)
    assert cl_payload["schema_version"] == "chart-lab-v1"

    from eventlens.dashboard import _open_ro, sanitize_and_verify_snapshot, serve_dashboard

    assert _open_ro(qdb) is not None
    assert "sk-test" not in sanitize_and_verify_snapshot(cl_payload, indent=None)
    assert callable(serve_dashboard)


def test_dashboard_html_and_colab_notebook_integrity() -> None:
    repo = Path(__file__).resolve().parents[1]
    html = (repo / "docs" / "dashboard" / "index.html").read_text(encoding="utf-8")
    for token in (
        "lightweight-charts",
        "tabBtnChartLab",
        "viewChartLab",
        "chart-lab.json",
        "eventlens_chart_lab_labels_v1",
        "clExportLabelsBtn",
    ):
        assert token in html, f"Missing Chart Lab UI token: {token}"

    nb_path = repo / "notebooks" / "stage_a_quant_reward_lab.ipynb"
    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    assert nb["nbformat"] == 4
    code_cells = [c for c in nb["cells"] if c.get("cell_type") == "code"]
    assert len(code_cells) >= 5
    for idx, cell in enumerate(code_cells):
        src = "".join(cell["source"])
        ast.parse(src, filename=f"cell_{idx}")


def test_apply_event_cluster_dedup_handles_entries_misses_and_errors() -> None:
    # Case 1: 4 headlines in 26s, 1 active_entry (+0.332) + 3 HOLD misses (-0.1)
    case1_in = [
        {
            "t": 1795613426,
            "title": "GDP",
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
        {
            "t": 1795613426,
            "title": "Personal Income",
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
        {
            "t": 1795613452,
            "title": "Claims",
            "score": 0.332,
            "r_net": 0.332,
            "mae_penalty": 0.0,
            "regime": "active_entry",
        },
        {
            "t": 1795613452,
            "title": "Core PCE",
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
    ]
    orig_snapshot = [dict(r) for r in case1_in]
    out1 = apply_event_cluster_dedup(case1_in, cluster_window_sec=300)
    assert case1_in == orig_snapshot  # input dicts never mutated
    assert sum(r["r_net"] for r in out1) == pytest.approx(0.332)
    assert sum(r["score"] for r in out1) == pytest.approx(0.332)
    assert [r["regime"] for r in out1].count("active_entry") == 1
    assert [r["regime"] for r in out1].count("cluster_covered_flat") == 3
    assert len({r["cluster_id"] for r in out1}) == 1

    # Case 2: 3 duplicate entries (+0.332 each) + 1 miss (-0.1) in same cluster -> scored once
    case2_in = [
        {
            "t": 1795613426,
            "title": "GDP",
            "score": 0.332,
            "r_net": 0.332,
            "mae_penalty": 0.0,
            "regime": "active_entry",
        },
        {
            "t": 1795613426,
            "title": "Income",
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
        {
            "t": 1795613452,
            "title": "Claims",
            "score": 0.332,
            "r_net": 0.332,
            "mae_penalty": 0.0,
            "regime": "active_entry",
        },
        {
            "t": 1795613452,
            "title": "Core PCE",
            "score": 0.332,
            "r_net": 0.332,
            "mae_penalty": 0.0,
            "regime": "active_entry",
        },
    ]
    out2 = apply_event_cluster_dedup(case2_in, cluster_window_sec=300)
    assert sum(r["r_net"] for r in out2) == pytest.approx(0.332)
    assert sum(r["score"] for r in out2) == pytest.approx(0.332)
    assert [r["regime"] for r in out2].count("active_entry") == 1
    assert [r["regime"] for r in out2].count("cluster_duplicate_entry") == 2
    assert [r["regime"] for r in out2].count("cluster_covered_flat") == 1

    # Case 3: All 4 misses in one cluster -> single worst penalty (-0.25)
    case3_in = [
        {
            "t": 1795613426,
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
        {
            "t": 1795613426,
            "score": -0.25,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
        {
            "t": 1795613452,
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
        {
            "t": 1795613452,
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
    ]
    out3 = apply_event_cluster_dedup(case3_in, cluster_window_sec=300)
    assert sum(r["score"] for r in out3) == pytest.approx(-0.25)
    assert [r["regime"] for r in out3].count("cluster_dedup_miss") == 3

    # Case 4: Distinct clusters >300s apart + api_error preservation
    case4_in = [
        {
            "t": 1000,
            "score": 0.341,
            "r_net": 0.341,
            "mae_penalty": 0.0,
            "regime": "active_entry",
        },
        {"t": 1020, "score": None, "r_net": None, "mae_penalty": 0.0, "regime": "api_error"},
        {
            "t": 1400,
            "score": -0.1,
            "r_net": 0.0,
            "mae_penalty": 0.0,
            "regime": "missed_qualified_setup",
        },
    ]
    out4 = apply_event_cluster_dedup(case4_in, cluster_window_sec=300)
    assert out4[0]["cluster_id"] == 0 and out4[1]["cluster_id"] == 0
    assert out4[2]["cluster_id"] == 1
    assert out4[1]["regime"] == "api_error" and out4[1]["score"] is None
    assert out4[2]["score"] == pytest.approx(-0.1)
