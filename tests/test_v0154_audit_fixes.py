"""Audit reproduction & verification tests for v0.15.4 fixes on baseline bfbe8f0.

Covers:
1. Idempotent crash recovery for simultaneous event bundles (no regrouping, no duplicate LLM calls or orders/fills; covers post-commit crash, partial-marking crash, restart, late sibling, and member revision).
2. Non-regressing full-text enrichment for ECB/Fed releases with persisted raw HTML payload evidence, fetch/availability timestamps, preserved upstream revisions, and strict anti-lookahead historical replay.
3. Independent per-stream persistence/publication and non-blocking event consumption when a slow source hangs.
4. Separation of pre-LLM market-quote waiting (within explicit event validity window -> expired) from model retry budget (timeouts / invalid outputs).
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from eventlens.contracts import Event, MarketSnapshot, Quote
from eventlens.deepseek import ModelResponse
from eventlens.observer import CaptureBatch, MacroValues, ObserverStore, SourceRecord
from eventlens.runtime import IntradayEventRuntime, ShadowRunner, SQLiteRuntimeStore
from eventlens.sources.english_tier1 import (
    ECB_PRESS_RSS_URL,
    FED_MONETARY_RSS_URL,
    FOREXFACTORY_CALENDAR_URL,
    INVESTINGLIVE_RSS_URL,
    US_BEA_RSS_URL,
    EURUSDEventFilter,
    Tier1EnglishSource,
    Tier1StreamSpec,
)
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import SinglePassTradingAgent
from eventlens.trading.execution import ProtectiveExitPolicy, SQLiteShadowLedger
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine


def _valid_market(ts: datetime) -> MarketSnapshot:
    q = Quote(
        instrument="EUR/USD",
        source_at=ts,
        received_at=ts,
        available_at=ts,
        source_provider="ctrader_algo",
        basis="live_poll",
        bid=Decimal("1.11400"),
        ask=Decimal("1.11402"),
    )
    return MarketSnapshot(snapshot_id=f"mkt:{ts.isoformat()}", as_of=ts, quotes=(q,))


def _empty_market(ts: datetime) -> MarketSnapshot:
    return MarketSnapshot(snapshot_id=f"mkt-empty:{ts.isoformat()}", as_of=ts, quotes=())


def _stale_market(ts: datetime, *, age_seconds: float = 5.0) -> MarketSnapshot:
    old = ts - timedelta(seconds=age_seconds)
    q = Quote(
        instrument="EUR/USD",
        source_at=old,
        received_at=old,
        available_at=old,
        source_provider="ctrader_algo",
        basis="live_poll",
        bid=Decimal("1.11400"),
        ask=Decimal("1.11402"),
    )
    return MarketSnapshot(snapshot_id=f"mkt-stale:{ts.isoformat()}", as_of=ts, quotes=(q,))


def _calendar_record(
    source_id: str,
    title: str,
    *,
    scheduled_at: datetime,
    actual: str,
    forecast: str | None = "3.0%",
    previous: str = "2.0%",
    importance: int = 3,
) -> SourceRecord:
    return SourceRecord(
        source_id=source_id,
        kind="calendar",
        source_url="https://www.forexfactory.com/calendar",
        title=title,
        text=f"{title} released at {actual}",
        scheduled_at=scheduled_at,
        importance=importance,
        macro=MacroValues(
            actual=actual,
            reported_forecast=forecast,
            previous=previous,
            country="USD",
        ),
    )


class _RecordingBundleModelClient:
    provider = "deepseek"
    model = "deepseek-chat"

    def __init__(self, *, quote_chooser: Any = None) -> None:
        self.calls = 0
        self.user_prompts: list[str] = []
        self._quote_chooser = quote_chooser

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        self.calls += 1
        self.user_prompts.append(user)
        ctx = json.loads(user)
        raw_text: str = ctx["current_event_verbatim_text"]
        if self._quote_chooser is not None:
            ev_quote = self._quote_chooser(raw_text)
        else:
            first_non_empty = [
                line.strip()
                for line in raw_text.splitlines()
                if line.strip() and not line.strip().startswith(("[", "---", "Structured Values"))
            ][0]
            ev_quote = first_non_empty[:120]
        payload = {
            "memory_action": "create",
            "thesis_id": None,
            "status": "active",
            "narrative": f"USD macro impulse #{self.calls} weighs on EUR/USD",
            "instruments": ["EUR/USD"],
            "invalidation_conditions": ["EUR/USD breaks above 1.1180"],
            "confidence": 0.74,
            "evidence_quote": ev_quote,
            "evidence_role": "support",
            "portfolio_action": "SET_TARGET",
            "target_direction": "short",
            "target_weight": "-0.20",
            "target_thesis_ids": ["__current__"],
            "rationale": "Macro release beat supports tactical short EUR/USD",
        }
        return ModelResponse(
            content=json.dumps(payload),
            input_tokens=600,
            output_tokens=140,
            model_id="deepseek-chat",
        )


def _record_calendar_batch(
    store: ObserverStore,
    *,
    received_at: datetime,
    records: tuple[SourceRecord, ...],
    tag: str,
) -> None:
    store.record(
        CaptureBatch(
            provider="forexfactory",
            stream="calendar/live",
            kind="calendar",
            request_url=FOREXFACTORY_CALENDAR_URL,
            request_started_at=received_at,
            received_at=received_at,
            elapsed_ms=2.0,
            attempts=1,
            payload=json.dumps({"tag": tag}).encode("utf-8"),
            records=records,
        )
    )


# ---------------------------------------------------------------------------
# 1. Idempotent Crash Recovery for Event Bundling
# ---------------------------------------------------------------------------


def test_bundle_crash_after_partial_member_marking_recovers_without_regrouping_or_duplicate_orders_fills(
    tmp_path: Path,
) -> None:
    """Crash after marking only 1 of 3 members 'bundled' must NOT regroup remaining 2 into a new bundle."""
    t_sched = datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)
    now = datetime(2026, 10, 29, 12, 30, 3, tzinfo=UTC)

    obs_db = tmp_path / "obs_partial.sqlite"
    shd_db = tmp_path / "shd_partial.sqlite"
    mem_db = tmp_path / "mem_partial.sqlite"

    obs = ObserverStore(obs_db, clock=lambda: now)
    _record_calendar_batch(
        obs, received_at=t_sched - timedelta(minutes=5), records=(), tag="baseline"
    )
    recs = (
        _calendar_record("ff:gdp@1", "USD Advance GDP q/q", scheduled_at=t_sched, actual="3.5%"),
        _calendar_record("ff:pce@1", "USD Core PCE m/m", scheduled_at=t_sched, actual="0.4%"),
        _calendar_record("ff:hpi@1", "USD FHFA HPI m/m", scheduled_at=t_sched, actual="0.5%"),
    )
    _record_calendar_batch(obs, received_at=now, records=recs, tag="release")

    client = _RecordingBundleModelClient()
    mem1 = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    store1 = SQLiteRuntimeStore(shd_db)
    ledger1 = SQLiteShadowLedger(shd_db, clock=lambda: now)
    rt1 = IntradayEventRuntime(
        memory=mem1,
        portfolio_state=ledger1,
        market_provider=lambda ts: _valid_market(ts),
        agent=SinglePassTradingAgent(client, clock=lambda: now),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger1,
        state_store=store1,
        event_filter=EURUSDEventFilter(clock=lambda: now),
        clock=lambda: now,
    )
    runner1 = ShadowRunner(
        runtime=rt1,
        ledger=ledger1,
        observer_store=obs,
        allow_baseline_revisions=False,
        bundle_simultaneous=True,
        clock=lambda: now,
    )

    # Simulate a crash right after ONLY 1 member is marked "bundled" (or force partial state if atomic)
    orig_upsert = store1.upsert_event_state
    bundled_writes = 0

    def crash_on_second_bundled(*args: Any, **kwargs: Any) -> None:
        nonlocal bundled_writes
        if kwargs.get("state") == "bundled":
            bundled_writes += 1
            if bundled_writes == 2:
                raise RuntimeError("simulated_crash_after_marking_first_member")
        orig_upsert(*args, **kwargs)

    store1.upsert_event_state = crash_on_second_bundled  # type: ignore[method-assign]
    try:
        try:
            asyncio.run(runner1.step_events())
        except RuntimeError as exc:
            assert "simulated_crash_after_marking_first_member" in str(exc)
        else:
            # Even if constituent marking was atomic inside SQLite, explicitly simulate a partial
            # crash state where 2 of 3 constituents are NOT marked 'bundled' in runtime_events!
            with store1._lock, store1.db:
                store1.db.execute(
                    """DELETE FROM runtime_events
                    WHERE event_id IN ('forexfactory:calendar:ff:pce@1:r1', 'forexfactory:calendar:ff:hpi@1:r1')"""
                )
        assert client.calls == 1
    finally:
        ledger1.close()
        store1.close()

    # Simulate process restart on the exact same databases
    now = now + timedelta(seconds=2)
    mem2 = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    store2 = SQLiteRuntimeStore(shd_db)
    ledger2 = SQLiteShadowLedger(shd_db, clock=lambda: now)
    rt2 = IntradayEventRuntime(
        memory=mem2,
        portfolio_state=ledger2,
        market_provider=lambda ts: _valid_market(ts),
        agent=SinglePassTradingAgent(client, clock=lambda: now),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger2,
        state_store=store2,
        event_filter=EURUSDEventFilter(clock=lambda: now),
        clock=lambda: now,
    )
    runner2 = ShadowRunner(
        runtime=rt2,
        ledger=ledger2,
        observer_store=obs,
        allow_baseline_revisions=False,
        bundle_simultaneous=True,
        clock=lambda: now,
    )
    try:
        asyncio.run(rt2.recover_incomplete())
        asyncio.run(runner2.step_events())
        ledger2.fill_pending(_valid_market(now))
        asyncio.run(runner2.step_events())

        # Must NEVER call LLM a second time or create duplicate orders/fills!
        assert client.calls == 1
        rep = ledger2.report(market=_valid_market(now))
        assert rep["receipt_count"] == 1
        assert sum(rep["order_status_counts"].values()) == 1
        assert rep["fill_count"] == 1

        summary = store2.summary_report()
        assert summary["model_calls"] == 1
        assert summary["state_counts"].get("bundled") == 3
        bundle_rows = [e for e in summary["events"] if e["event_id"].startswith("bundle:")]
        assert len(bundle_rows) == 1
    finally:
        obs.close()
        ledger2.close()
        store2.close()


def test_bundle_crash_after_decision_commit_and_late_or_revised_members_recover_cleanly(
    tmp_path: Path,
) -> None:
    """Covers:
    1) Crash immediately after memory.commit(decision) (before shadow record & before constituent marking),
    2) Restart + late-arriving 4th sibling at the same scheduled_at BEFORE step_events runs,
    3) Revision r2 of an already-bundled member (r1 never re-judged, r2 judged once).
    """
    t_sched = datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)
    now = datetime(2026, 10, 29, 12, 30, 2, tzinfo=UTC)

    obs_db = tmp_path / "obs_commit_crash.sqlite"
    shd_db = tmp_path / "shd_commit_crash.sqlite"
    mem_db = tmp_path / "mem_commit_crash.sqlite"

    obs = ObserverStore(obs_db, clock=lambda: now)
    _record_calendar_batch(
        obs, received_at=t_sched - timedelta(minutes=5), records=(), tag="baseline"
    )
    r_gdp_v1 = _calendar_record(
        "ff:gdp@2", "USD Advance GDP q/q", scheduled_at=t_sched, actual="3.5%"
    )
    r_pce_v1 = _calendar_record("ff:pce@2", "USD Core PCE m/m", scheduled_at=t_sched, actual="0.4%")
    _record_calendar_batch(obs, received_at=now, records=(r_gdp_v1, r_pce_v1), tag="poll1")

    client = _RecordingBundleModelClient()
    mem1 = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    store1 = SQLiteRuntimeStore(shd_db)
    ledger1 = SQLiteShadowLedger(shd_db, clock=lambda: now)

    class _CrashOnceShadowWrapper:
        def __init__(self, inner: SQLiteShadowLedger) -> None:
            self._inner = inner
            self.crashed = False

        async def record(self, decision: Any, assessment: Any) -> Any:
            if not self.crashed:
                self.crashed = True
                raise RuntimeError("simulated_crash_after_decision_commit_before_shadow")
            return await self._inner.record(decision, assessment)

    crash_exec = _CrashOnceShadowWrapper(ledger1)
    rt1 = IntradayEventRuntime(
        memory=mem1,
        portfolio_state=ledger1,
        market_provider=lambda ts: _valid_market(ts),
        agent=SinglePassTradingAgent(client, clock=lambda: now),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=crash_exec,  # type: ignore[arg-type]
        state_store=store1,
        event_filter=EURUSDEventFilter(clock=lambda: now),
        clock=lambda: now,
    )
    runner1 = ShadowRunner(
        runtime=rt1,
        ledger=ledger1,
        observer_store=obs,
        allow_baseline_revisions=False,
        bundle_simultaneous=True,
        clock=lambda: now,
    )

    with pytest.raises(RuntimeError, match="simulated_crash_after_decision_commit_before_shadow"):
        asyncio.run(runner1.step_events())
    assert client.calls == 1
    assert sum(ledger1.report(market=_valid_market(now))["order_status_counts"].values()) == 0
    ledger1.close()
    store1.close()

    # Before restart finishes scanning, a LATE sibling (FHFA HPI at the SAME scheduled_at!) arrives in Poll 2!
    now = now + timedelta(seconds=4)
    r_hpi_late = _calendar_record(
        "ff:hpi@2", "USD FHFA HPI m/m", scheduled_at=t_sched, actual="0.6%"
    )
    _record_calendar_batch(
        obs, received_at=now, records=(r_gdp_v1, r_pce_v1, r_hpi_late), tag="poll2_late_sibling"
    )

    # Restart process! Run step_events() directly (and recover_incomplete):
    # Original bundle (gdp+pce) MUST recover from committed decision WITHOUT re-calling LLM (0 extra calls for gdp/pce)
    # and record 1 order/fill, while latecomer hpi@2 is evaluated ONCE on its own (never regrouping gdp+pce+hpi)!
    mem2 = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    store2 = SQLiteRuntimeStore(shd_db)
    ledger2 = SQLiteShadowLedger(shd_db, clock=lambda: now)
    rt2 = IntradayEventRuntime(
        memory=mem2,
        portfolio_state=ledger2,
        market_provider=lambda ts: _valid_market(ts),
        agent=SinglePassTradingAgent(client, clock=lambda: now),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger2,
        state_store=store2,
        event_filter=EURUSDEventFilter(clock=lambda: now),
        clock=lambda: now,
    )
    runner2 = ShadowRunner(
        runtime=rt2,
        ledger=ledger2,
        observer_store=obs,
        allow_baseline_revisions=False,
        bundle_simultaneous=True,
        clock=lambda: now,
    )
    try:
        # Even if step_events runs before recover_incomplete (or after), gdp:r1 & pce:r1 must NOT regroup with hpi:r1!
        res_step = asyncio.run(runner2.step_events())
        assert len(res_step) == 2
        assert res_step[0].event_id.startswith("bundle:")
        assert res_step[0].recovered is True
        assert res_step[0].status == "shadow_recorded"
        assert res_step[1].event_id == "forexfactory:calendar:ff:hpi@2:r1"
        ledger2.fill_pending(_valid_market(now))
        # Total model calls so far: 1 (pre-crash bundle of gdp:r1+pce:r1) + 1 (late hpi:r1 alone) = 2
        assert client.calls == 2
        assert "USD Advance GDP q/q" not in client.user_prompts[1]
        assert "USD FHFA HPI m/m" in client.user_prompts[1]
        rep_mid = ledger2.report(market=_valid_market(now))
        # Recovered bundle recorded 1 order -> 1 fill (late hpi was risk-gated behind unfilled pending order; zero duplicate orders/fills)
        assert rep_mid["receipt_count"] == 1
        assert sum(rep_mid["order_status_counts"].values()) == 1
        assert rep_mid["fill_count"] == 1

        # Now Poll 3 delivers a REVISION (r2) of GDP ("3.5%" -> "3.7%"), while PCE and HPI are unchanged!
        now = now + timedelta(seconds=10)
        r_gdp_v2 = _calendar_record(
            "ff:gdp@2", "USD Advance GDP q/q", scheduled_at=t_sched, actual="3.7%"
        )
        _record_calendar_batch(
            obs, received_at=now, records=(r_gdp_v2, r_pce_v1, r_hpi_late), tag="poll3_revision"
        )
        res_rev = asyncio.run(runner2.step_events())
        assert len(res_rev) == 1
        assert res_rev[0].event_id == "forexfactory:calendar:ff:gdp@2:r2"
        assert client.calls == 3
        assert "3.7%" in client.user_prompts[2]
        assert "USD Core PCE m/m" not in client.user_prompts[2]

        # Subsequent scan is a complete no-op
        assert asyncio.run(runner2.step_events()) == ()
        assert client.calls == 3
    finally:
        obs.close()
        ledger2.close()
        store2.close()


# ---------------------------------------------------------------------------
# 2. Non-Regressing Official Full-Text Enrichment + Evidence + Anti-Lookahead
# ---------------------------------------------------------------------------

_ECB_BASE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Euro area bank lending survey</title>
    <link>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.pr261027~base.en.html</link>
    <guid>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.pr261027~base.en.html</guid>
    <pubDate>Tue, 27 Oct 2026 09:00:00 +0100</pubDate>
  </item>
</channel></rss>
"""

_ECB_LIVE_XML_V1 = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Monetary policy decisions</title>
    <link>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp261029~live1.en.html</link>
    <guid>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp261029~live1.en.html</guid>
    <pubDate>Thu, 29 Oct 2026 14:15:00 +0100</pubDate>
  </item>
  <item>
    <title>Euro area bank lending survey</title>
    <link>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.pr261027~base.en.html</link>
    <guid>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.pr261027~base.en.html</guid>
    <pubDate>Tue, 27 Oct 2026 09:00:00 +0100</pubDate>
  </item>
</channel></rss>
"""

_ECB_LIVE_XML_V2_UPDATED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Monetary policy decisions (corrected settlement date)</title>
    <link>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp261029~live1.en.html</link>
    <guid>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp261029~live1.en.html</guid>
    <pubDate>Thu, 29 Oct 2026 14:15:00 +0100</pubDate>
  </item>
  <item>
    <title>Euro area bank lending survey</title>
    <link>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.pr261027~base.en.html</link>
    <guid>https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.pr261027~base.en.html</guid>
    <pubDate>Tue, 27 Oct 2026 09:00:00 +0100</pubDate>
  </item>
</channel></rss>
"""

_ECB_ARTICLE_HTML_V1 = b"""<!doctype html><html><body><main>
  <div class="section">
    <p>The Governing Council today decided to lower the deposit facility rate by 25 basis points to 2.25%.</p>
  </div>
</main></body></html>"""

_ECB_ARTICLE_HTML_V2 = b"""<!doctype html><html><body><main>
  <div class="section">
    <p>The Governing Council today decided to lower the deposit facility rate by 25 basis points to 2.25%, effective 4 November 2026.</p>
  </div>
</main></body></html>"""

_FED_BASE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Federal Reserve issues FOMC calendar</title>
    <link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261001a.htm</link>
    <guid>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261001a.htm</guid>
    <description>Monetary Policy</description>
    <pubDate>Thu, 01 Oct 2026 14:00:00 GMT</pubDate>
  </item>
</channel></rss>"""

_FED_LIVE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Federal Reserve issues FOMC statement</title>
    <link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261029a.htm</link>
    <guid>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261029a.htm</guid>
    <description>Monetary Policy</description>
    <pubDate>Thu, 29 Oct 2026 18:00:00 GMT</pubDate>
  </item>
  <item>
    <title>Federal Reserve issues FOMC calendar</title>
    <link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261001a.htm</link>
    <guid>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261001a.htm</guid>
    <description>Monetary Policy</description>
    <pubDate>Thu, 01 Oct 2026 14:00:00 GMT</pubDate>
  </item>
</channel></rss>"""

_FED_ARTICLE_HTML = b"""<!doctype html><html><body>
  <div id="article"><p>The Committee decided to maintain the target range for the federal funds rate at 4-1/4 to 4-1/2 percent.</p></div>
</body></html>"""


def test_ecb_and_fed_enrichment_persists_raw_evidence_never_regresses_and_keeps_true_revisions(
    tmp_path: Path,
) -> None:
    """Subsequent RSS polls must NOT regress enriched ECB/Fed items back to RSS summaries;
    raw HTML payload + fetch/availability timestamps must be persisted; true upstream updates must revise."""
    ecb_rss = _ECB_BASE_XML
    ecb_html = _ECB_ARTICLE_HTML_V1
    fed_rss = _FED_BASE_XML
    html_fetches: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        url = str(req.url)
        if url == ECB_PRESS_RSS_URL:
            return httpx.Response(200, content=ecb_rss, headers={"content-type": "application/xml"})
        if url == FED_MONETARY_RSS_URL:
            return httpx.Response(200, content=fed_rss, headers={"content-type": "application/xml"})
        if url.endswith("ecb.mp261029~live1.en.html"):
            html_fetches.append(url)
            return httpx.Response(200, content=ecb_html, headers={"content-type": "text/html"})
        if url.endswith("monetary20261029a.htm"):
            html_fetches.append(url)
            return httpx.Response(
                200, content=_FED_ARTICLE_HTML, headers={"content-type": "text/html"}
            )
        return httpx.Response(404)

    now = datetime(2026, 10, 29, 18, 0, 5, tzinfo=UTC)
    store = ObserverStore(tmp_path / "enrichment.sqlite", clock=lambda: now)
    streams = (
        Tier1StreamSpec("ecb", "news/press", "news", ECB_PRESS_RSS_URL, 10.0, True),
        Tier1StreamSpec("fed", "news/monetary", "news", FED_MONETARY_RSS_URL, 10.0, True),
    )
    source = Tier1EnglishSource(
        streams=streams,
        transport=httpx.MockTransport(handler),
        clock=lambda: now,
    )
    try:
        # Poll 1: baseline
        r1 = asyncio.run(source.poll_due_streams(store, force=True))
        assert all(item["counts"]["baseline"] == 1 for item in r1)
        assert html_fetches == []

        # Poll 2: live ECB + live Fed announcements arrive -> enrich once & persist raw HTML evidence
        ecb_rss = _ECB_LIVE_XML_V1
        fed_rss = _FED_LIVE_XML
        now = now + timedelta(seconds=30)
        r2 = asyncio.run(source.poll_due_streams(store, force=True))
        assert {item["provider"]: item["counts"]["new"] for item in r2} == {"ecb": 1, "fed": 1}
        assert len(html_fetches) == 2

        # Verify raw HTML response evidence + fetch timestamp are durably stored in SQLite
        import gzip
        import hashlib

        ecb_html_sha = hashlib.sha256(_ECB_ARTICLE_HTML_V1).hexdigest()
        payload_row = store.db.execute(
            "SELECT body_gzip FROM observer_payloads WHERE sha256=?", (ecb_html_sha,)
        ).fetchone()
        assert payload_row is not None
        assert gzip.decompress(payload_row["body_gzip"]) == _ECB_ARTICLE_HTML_V1

        # Poll 3: next poll WITHOUT re-fetching HTML -> MUST NOT regress to RSS summary!
        now = now + timedelta(seconds=30)
        r3 = asyncio.run(source.poll_due_streams(store, force=True))
        assert len(html_fetches) == 2  # no redundant HTML re-fetch
        for item in r3:
            assert item["counts"]["revised"] == 0, f"Bogus regression revision in {item}"
            assert item["counts"]["repeat"] == 2

        ecb_live = [
            rev
            for rev in store.revisions(provider="ecb", as_of=now)
            if "Monetary policy decisions" in rev.record.title
        ]
        assert len(ecb_live) == 1
        assert (
            "lower the deposit facility rate by 25 basis points to 2.25%" in ecb_live[0].record.text
        )

        fed_live = [
            rev
            for rev in store.revisions(provider="fed", as_of=now)
            if "FOMC statement" in rev.record.title
        ]
        assert len(fed_live) == 1
        assert "4-1/4 to 4-1/2 percent" in fed_live[0].record.text

        # Poll 4: genuine upstream update in ECB RSS + HTML -> MUST record revision 2 while preserving revision 1!
        ecb_rss = _ECB_LIVE_XML_V2_UPDATED
        ecb_html = _ECB_ARTICLE_HTML_V2
        now = now + timedelta(seconds=30)
        r4 = {
            item["provider"]: item
            for item in asyncio.run(source.poll_due_streams(store, force=True))
        }
        assert r4["ecb"]["counts"]["revised"] == 1
        assert r4["fed"]["counts"]["revised"] == 0

        ecb_source_id = ecb_live[0].record.source_id
        hist = store.history("news", ecb_source_id, provider="ecb", as_of=now)
        assert len(hist) == 2
        assert "effective 4 November 2026" not in hist[0].record.text
        assert "effective 4 November 2026" in hist[1].record.text

        # Both enrichment fetches (V1 and V2) retain timestamps + raw HTML response bytes
        enr_hist = store.enrichments(provider="ecb", source_id=ecb_source_id, as_of=now)
        assert len(enr_hist) == 2
        assert store.enrichment_payload(enr_hist[0].enrichment_id) == _ECB_ARTICLE_HTML_V1
        assert store.enrichment_payload(enr_hist[1].enrichment_id) == _ECB_ARTICLE_HTML_V2
        assert enr_hist[0].fetched_at <= enr_hist[0].available_at <= enr_hist[1].available_at
    finally:
        store.close()


def test_enrichment_historical_replay_never_leaks_later_fetched_body(tmp_path: Path) -> None:
    """If full text is fetched at t2 > t1 (or HTML fetch finishes after an earlier cutoff),
    replay at as_of < t2 must NEVER expose the later-fetched full text."""
    ecb_rss = _ECB_BASE_XML
    html_status = 503

    def handler(req: httpx.Request) -> httpx.Response:
        url = str(req.url)
        if url == ECB_PRESS_RSS_URL:
            return httpx.Response(200, content=ecb_rss)
        if url.endswith("ecb.mp261029~live1.en.html"):
            if html_status != 200:
                return httpx.Response(html_status, content=b"CDN warming")
            return httpx.Response(200, content=_ECB_ARTICLE_HTML_V1)
        return httpx.Response(404)

    t0 = datetime(2026, 10, 29, 13, 15, 0, tzinfo=UTC)
    now = t0
    store = ObserverStore(tmp_path / "anti_lookahead.sqlite", clock=lambda: now)
    spec = Tier1StreamSpec("ecb", "news/press", "news", ECB_PRESS_RSS_URL, 10.0, True)
    source = Tier1EnglishSource(
        streams=(spec,),
        max_attempts=1,
        transport=httpx.MockTransport(handler),
        clock=lambda: now,
    )
    try:
        # Poll 1 at t0: baseline
        asyncio.run(source.poll_due_streams(store, force=True))

        # Poll 2 at t1 = t0 + 5s: live ECB item arrives in RSS, but HTML CDN returns 503!
        ecb_rss = _ECB_LIVE_XML_V1
        t1 = t0 + timedelta(seconds=5)
        now = t1
        res_t1 = asyncio.run(source.poll_due_streams(store, force=True))
        assert res_t1[0]["counts"]["new"] == 1

        # Poll 3 at t2 = t0 + 25s: HTML is now available (200 OK) -> enriches as revision 2 at t2!
        html_status = 200
        t2 = t0 + timedelta(seconds=25)
        now = t2
        res_t2 = asyncio.run(source.poll_due_streams(store, force=True))
        assert res_t2[0]["counts"]["revised"] == 1

        # Poll 4 at t3 = t0 + 45s: stays enriched without regression
        t3 = t0 + timedelta(seconds=45)
        now = t3
        res_t3 = asyncio.run(source.poll_due_streams(store, force=True))
        assert res_t3[0]["counts"]["revised"] == 0
        assert res_t3[0]["counts"]["repeat"] == 2

        # Historical replay at t0 + 10s (BEFORE t2 HTML enrichment!) MUST NOT see full text!
        early_cutoff = t0 + timedelta(seconds=10)
        revs_early = [
            r
            for r in store.revisions(provider="ecb", as_of=early_cutoff)
            if "Monetary policy decisions" in r.record.title
        ]
        assert len(revs_early) == 1
        assert "25 basis points" not in revs_early[0].record.text

        src_id = revs_early[0].record.source_id
        hist_early = store.history("news", src_id, provider="ecb", as_of=early_cutoff)
        assert len(hist_early) == 1
        assert "25 basis points" not in hist_early[0].record.text

        # Historical replay at t2 (AFTER enrichment commit) sees revision 2 with full text
        hist_after = store.history("news", src_id, provider="ecb", as_of=t2)
        assert len(hist_after) == 2
        assert "25 basis points" in hist_after[-1].record.text
    finally:
        store.close()


# ---------------------------------------------------------------------------
# 3. Fast Sources Must Not Wait for Slow Sources
# ---------------------------------------------------------------------------

_FAST_BASE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Earlier EUR/USD desk wrap</title>
    <link>https://investinglive.com/news/base-wrap-1/</link>
    <guid>https://investinglive.com/news/base-wrap-1/</guid>
    <category>Forex</category>
    <pubDate>Tue, 27 Oct 2026 12:00:00 GMT</pubDate>
  </item>
</channel></rss>"""

_FAST_LIVE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>ECB President Lagarde signals faster eurozone rate cuts weighing on EUR/USD</title>
    <link>https://investinglive.com/news/lagarde-live-2/</link>
    <guid>https://investinglive.com/news/lagarde-live-2/</guid>
    <category>Central Banks</category>
    <pubDate>Tue, 27 Oct 2026 12:30:01 GMT</pubDate>
    <description><![CDATA[<p>Lagarde says EUR/USD inflation pressures have eased faster than projected.</p>]]></description>
  </item>
  <item>
    <title>Earlier EUR/USD desk wrap</title>
    <link>https://investinglive.com/news/base-wrap-1/</link>
    <guid>https://investinglive.com/news/base-wrap-1/</guid>
    <category>Forex</category>
    <pubDate>Tue, 27 Oct 2026 12:00:00 GMT</pubDate>
  </item>
</channel></rss>"""

_SLOW_BASE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Personal Income and Outlays, August 2026</title>
    <link>https://www.bea.gov/news/2026/pio-aug</link>
    <guid>https://www.bea.gov/news/2026/pio-aug</guid>
    <pubDate>Tue, 27 Oct 2026 11:00:00 GMT</pubDate>
  </item>
</channel></rss>"""


def test_fast_source_persisted_published_and_consumed_while_slow_source_is_hung(
    tmp_path: Path,
) -> None:
    """Deterministic async proof: while a slow stream is hung on an asyncio.Event,
    the fast stream is ALREADY persisted, published in ObserverStore, and consumed by ShadowRunner's event handler,
    while independent protection keeps running and shutdown cancels cleanly."""

    async def _scenario() -> None:
        slow_entered = asyncio.Event()
        release_slow = asyncio.Event()
        fast_consumed = asyncio.Event()
        slow_cancelled = False
        slow_in_flight = False
        hang_slow_now = False
        fast_payload = _FAST_BASE_XML

        class _ControllableAsyncTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                nonlocal slow_in_flight, slow_cancelled
                url = str(request.url)
                if url == INVESTINGLIVE_RSS_URL:
                    return httpx.Response(200, content=fast_payload)
                if url == US_BEA_RSS_URL:
                    if not hang_slow_now:
                        return httpx.Response(200, content=_SLOW_BASE_XML)
                    slow_in_flight = True
                    slow_entered.set()
                    try:
                        await release_slow.wait()
                        return httpx.Response(200, content=_SLOW_BASE_XML)
                    except asyncio.CancelledError:
                        slow_cancelled = True
                        raise
                    finally:
                        slow_in_flight = False
                return httpx.Response(404)

        obs_db = tmp_path / "fast_slow_obs.sqlite"
        shd_db = tmp_path / "fast_slow_shd.sqlite"
        mem_db = tmp_path / "fast_slow_mem.sqlite"

        obs = ObserverStore(obs_db)
        mem = SQLiteThesisMemory(mem_db, allow_targets=True)
        state_store = SQLiteRuntimeStore(shd_db)
        ledger = SQLiteShadowLedger(
            shd_db,
            exit_policy=ProtectiveExitPolicy(max_holding_seconds=600),
        )
        streams = (
            Tier1StreamSpec("investinglive", "news/rss", "news", INVESTINGLIVE_RSS_URL, 10.0),
            Tier1StreamSpec("us_bea", "news/releases", "news", US_BEA_RSS_URL, 10.0),
        )
        source = Tier1EnglishSource(
            streams=streams,
            timeout_seconds=15.0,
            max_attempts=1,
            transport=_ControllableAsyncTransport(),
        )

        # Seed baseline for both streams before live runner starts
        base_res = await source.poll_due_streams(obs, force=True)
        assert len(base_res) == 2

        # Reset rate-limit markers so runner's first live poll fetches both immediately
        source._last_polled_mono.clear()
        hang_slow_now = True
        fast_payload = _FAST_LIVE_XML

        client = _RecordingBundleModelClient(
            quote_chooser=lambda _raw: (
                "Lagarde says EUR/USD inflation pressures have eased faster than projected."
            )
        )
        orig_complete = client.complete_json

        async def _signal_complete(*, system: str, user: str) -> ModelResponse:
            resp = await orig_complete(system=system, user=user)
            fast_consumed.set()
            return resp

        client.complete_json = _signal_complete  # type: ignore[method-assign]

        rt = IntradayEventRuntime(
            memory=mem,
            portfolio_state=ledger,
            market_provider=lambda ts: _valid_market(ts),
            agent=SinglePassTradingAgent(client),
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(),
            shadow_executor=ledger,
            state_store=state_store,
            event_filter=EURUSDEventFilter(),
        )
        runner = ShadowRunner(
            runtime=rt,
            ledger=ledger,
            observer_store=obs,
            news_source=source,
            poll_news=True,
            allow_baseline_revisions=False,
            news_poll_interval_seconds=10.0,
            event_scan_interval_seconds=0.05,
            quote_interval_seconds=0.05,
            protection_interval_seconds=0.05,
            strict_window_coverage=False,
        )
        try:
            run_task = asyncio.create_task(runner.run(duration_seconds=5.0))

            # 1. Confirm slow stream is actively hung in flight
            await asyncio.wait_for(slow_entered.wait(), timeout=1.0)
            assert slow_in_flight is True

            # 2. While slow stream is STILL hung, fast stream MUST be persisted, published, AND consumed by agent!
            await asyncio.wait_for(fast_consumed.wait(), timeout=0.8)
            assert slow_in_flight is True  # slow source STILL has not returned!

            # Wait briefly for post-LLM shadow recording of the fast event
            deadline = asyncio.get_running_loop().time() + 0.5
            while state_store.summary_report()["state_counts"].get("shadow_recorded", 0) == 0:
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.02)

            assert slow_in_flight is True
            fast_revs = [
                r for r in obs.revisions(provider="investinglive") if "Lagarde" in r.record.title
            ]
            assert len(fast_revs) == 1
            assert client.calls == 1
            assert runner._protection_checks_total >= 1

            # 3. Stop runner while slow stream is still hung -> must cancel slow stream cleanly & exit promptly
            state_store.request_stop(datetime.now(UTC))
            summary = await asyncio.wait_for(run_task, timeout=1.0)
            assert slow_cancelled is True
            assert (
                summary["worker_shutdown_verification"]["event_worker_stopped_before_drain"] is True
            )
        finally:
            if "run_task" in locals() and not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
            obs.close()
            ledger.close()
            state_store.close()

    asyncio.run(_scenario())


def test_poll_due_streams_publishes_fast_stream_independently_and_preserves_limits_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """poll_due_streams must persist/publish fast stream into ObserverStore BEFORE slow stream completes,
    while preserving min_poll_seconds rate limiting and max_attempts retry cap."""

    real_sleep = asyncio.sleep

    async def _fast_retry_sleep(seconds: float) -> None:
        await real_sleep(0.0 if seconds >= 0.5 else seconds)

    monkeypatch.setattr(asyncio, "sleep", _fast_retry_sleep)

    async def _scenario() -> None:
        slow_started = asyncio.Event()
        release_slow = asyncio.Event()
        bea_attempts = 0
        fail_bea = False

        class _Transport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                nonlocal bea_attempts
                url = str(request.url)
                if url == INVESTINGLIVE_RSS_URL:
                    return httpx.Response(200, content=_FAST_LIVE_XML)
                if url == US_BEA_RSS_URL:
                    bea_attempts += 1
                    if fail_bea:
                        return httpx.Response(503, content=b"overload")
                    slow_started.set()
                    await release_slow.wait()
                    return httpx.Response(200, content=_SLOW_BASE_XML)
                return httpx.Response(404)

        store = ObserverStore(tmp_path / "indep_poll.sqlite")
        streams = (
            Tier1StreamSpec("investinglive", "news/rss", "news", INVESTINGLIVE_RSS_URL, 30.0),
            Tier1StreamSpec("us_bea", "news/releases", "news", US_BEA_RSS_URL, 30.0),
        )
        source = Tier1EnglishSource(
            streams=streams,
            timeout_seconds=5.0,
            max_attempts=2,
            transport=_Transport(),
        )
        try:
            poll_task = asyncio.create_task(source.poll_due_streams(store, force=True))
            await asyncio.wait_for(slow_started.wait(), timeout=0.5)

            # While US BEA is STILL blocked, investinglive must ALREADY be committed & published!
            deadline = asyncio.get_running_loop().time() + 0.5
            while not store.revisions(provider="investinglive"):
                if asyncio.get_running_loop().time() >= deadline:
                    pytest.fail(
                        "Fast stream was NOT persisted/published while slow stream was pending!"
                    )
                await asyncio.sleep(0.01)

            assert not poll_task.done()
            release_slow.set()
            res = await asyncio.wait_for(poll_task, timeout=0.5)
            assert len(res) == 2

            # Immediate non-forced call respects min_poll_seconds=30.0
            assert await source.poll_due_streams(store, force=False) == []

            # Transient 503 respects max_attempts=2 cap and records observer_failures
            fail_bea = True
            bea_attempts = 0
            res_fail = await source.poll_due_streams(store, force=True)
            by_prov = {item["provider"]: item for item in res_fail}
            assert by_prov["investinglive"]["status"] == "ok"
            assert by_prov["us_bea"]["status"] == "fetch_failed"
            assert bea_attempts == 2
            assert store.report()["counts"]["failures"] == 1
        finally:
            release_slow.set()
            store.close()

    asyncio.run(_scenario())


# ---------------------------------------------------------------------------
# 4. Separate Quote Waiting from Model Retry Budget
# ---------------------------------------------------------------------------


def test_brief_quote_outage_waits_without_burning_retries_and_judges_when_quote_recovers(
    tmp_path: Path,
) -> None:
    """Missing/stale quotes must NOT consume model retry attempts;
    when valid quote returns within event validity window, event is judged normally."""
    t0 = datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)
    now = t0
    quote_healthy = False

    obs_db = tmp_path / "qw_obs.sqlite"
    shd_db = tmp_path / "qw_shd.sqlite"
    mem_db = tmp_path / "qw_mem.sqlite"

    obs = ObserverStore(obs_db, clock=lambda: now)
    _record_calendar_batch(obs, received_at=t0 - timedelta(minutes=5), records=(), tag="baseline")
    rec = _calendar_record("ff:cpi@1", "USD Core CPI m/m", scheduled_at=t0, actual="0.4%")
    _record_calendar_batch(obs, received_at=t0, records=(rec,), tag="live_cpi")
    ev_id = "forexfactory:calendar:ff:cpi@1:r1"

    client = _RecordingBundleModelClient()
    mem = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    store = SQLiteRuntimeStore(shd_db)
    ledger = SQLiteShadowLedger(shd_db, clock=lambda: now)

    def market_fn(ts: datetime) -> MarketSnapshot:
        return _valid_market(ts) if quote_healthy else _stale_market(ts, age_seconds=5.0)

    rt = IntradayEventRuntime(
        memory=mem,
        portfolio_state=ledger,
        market_provider=market_fn,
        agent=SinglePassTradingAgent(
            client,
            max_quote_age=timedelta(seconds=2),  # strict 2.0s threshold preserved!
            clock=lambda: now,
        ),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger,
        state_store=store,
        event_filter=EURUSDEventFilter(clock=lambda: now),
        clock=lambda: now,
    )
    runner = ShadowRunner(
        runtime=rt,
        ledger=ledger,
        observer_store=obs,
        allow_baseline_revisions=False,
        max_reasoning_attempts=2,  # Only 2 model retries allowed!
        clock=lambda: now,
    )
    try:
        # 4 consecutive scans over 4 seconds while quotes are stale (> max_reasoning_attempts=2!)
        for offset in (0, 1, 2, 3):
            now = t0 + timedelta(seconds=offset)
            asyncio.run(runner.step_events())
            row = store.get_event_state(ev_id)
            assert row is not None
            assert int(row["attempts"]) == 0, "Quote outage must NOT consume model retry attempts!"
            assert client.calls == 0

        # At t0 + 5s (within event validity window), valid fresh quote arrives -> judged normally!
        quote_healthy = True
        now = t0 + timedelta(seconds=5)
        res = asyncio.run(runner.step_events())
        assert len(res) == 1
        assert res[0].status == "shadow_recorded"
        assert client.calls == 1
        row_ok = store.get_event_state(ev_id)
        assert row_ok is not None and int(row_ok["attempts"]) == 1
    finally:
        obs.close()
        ledger.close()
        store.close()


def test_prolonged_quote_outage_expires_event_and_model_errors_still_count_separately(
    tmp_path: Path,
) -> None:
    """Covers:
    1) Quote outage lasting past event validity window -> recorded as 'expired' and NEVER judged even after quotes recover;
    2) Event already waiting on quote whose next scan happens AFTER validity window when quotes already recovered -> STILL 'expired', never judged;
    3) Actual model errors / invalid outputs DO increment attempts and cap at max_reasoning_attempts."""
    t0 = datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)
    now = t0
    quote_healthy = False

    obs_db = tmp_path / "exp_obs.sqlite"
    shd_db = tmp_path / "exp_shd.sqlite"
    mem_db = tmp_path / "exp_mem.sqlite"

    obs = ObserverStore(obs_db, clock=lambda: now)
    _record_calendar_batch(obs, received_at=t0 - timedelta(minutes=5), records=(), tag="baseline")
    rec1 = _calendar_record("ff:nfp@1", "USD Non-Farm Payrolls", scheduled_at=t0, actual="250K")
    _record_calendar_batch(obs, received_at=t0, records=(rec1,), tag="nfp1")
    ev1_id = "forexfactory:calendar:ff:nfp@1:r1"

    client = _RecordingBundleModelClient()
    mem = SQLiteThesisMemory(mem_db, clock=lambda: now, allow_targets=True)
    store = SQLiteRuntimeStore(shd_db)
    ledger = SQLiteShadowLedger(shd_db, clock=lambda: now)

    rt = IntradayEventRuntime(
        memory=mem,
        portfolio_state=ledger,
        market_provider=lambda ts: _valid_market(ts) if quote_healthy else _empty_market(ts),
        agent=SinglePassTradingAgent(
            client,
            max_quote_age=timedelta(seconds=2),
            clock=lambda: now,
        ),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger,
        state_store=store,
        event_filter=EURUSDEventFilter(clock=lambda: now),
        clock=lambda: now,
    )
    runner = ShadowRunner(
        runtime=rt,
        ledger=ledger,
        observer_store=obs,
        allow_baseline_revisions=False,
        max_reasoning_attempts=2,
        clock=lambda: now,
    )
    try:
        # Case 1: Outage persists past 60s validity window -> transitions to 'expired'
        now = t0 + timedelta(seconds=2)
        asyncio.run(runner.step_events())
        now = t0 + timedelta(seconds=65)
        res_exp = asyncio.run(runner.step_events())
        assert len(res_exp) == 1
        assert res_exp[0].status == "expired"
        row1 = store.get_event_state(ev1_id)
        assert row1 is not None
        assert row1["state"] == "expired"
        assert int(row1["attempts"]) == 0
        assert client.calls == 0

        # Quotes recover at t0 + 70s -> expired event MUST NOT be judged!
        quote_healthy = True
        now = t0 + timedelta(seconds=70)
        assert asyncio.run(runner.step_events()) == ()
        assert client.calls == 0

        # Case 2: Event 2 waits during brief outage at t2 + 1s, then next scan only runs at t2 + 65s AFTER quotes recovered!
        t2 = t0 + timedelta(seconds=100)
        now = t2
        quote_healthy = False
        rec2 = _calendar_record("ff:ism@2", "USD ISM Services PMI", scheduled_at=t2, actual="54.2")
        _record_calendar_batch(obs, received_at=t2, records=(rec1, rec2), tag="ism2")
        ev2_id = "forexfactory:calendar:ff:ism@2:r1"

        now = t2 + timedelta(seconds=1)
        asyncio.run(runner.step_events())  # enters quote-waiting state
        quote_healthy = True
        now = t2 + timedelta(seconds=65)  # past validity window when quotes return!
        res_exp2 = asyncio.run(runner.step_events())
        assert len(res_exp2) == 1
        assert res_exp2[0].status == "expired"
        row2 = store.get_event_state(ev2_id)
        assert row2 is not None and row2["state"] == "expired"
        assert client.calls == 0

        # Case 3: With valid quotes, invalid model JSON output & model timeout DO increment attempts separately!
        class _BadOutputModelClient:
            provider = "deepseek"
            model = "deepseek-chat"
            calls = 0

            async def complete_json(self, *, system: str, user: str) -> ModelResponse:
                self.calls += 1
                return ModelResponse(
                    content='{"not_valid_schema": true}',
                    input_tokens=100,
                    output_tokens=10,
                    model_id="deepseek-chat",
                )

        bad_client = _BadOutputModelClient()
        rt.agent = SinglePassTradingAgent(bad_client, clock=lambda: now)
        t3 = t2 + timedelta(seconds=100)
        now = t3
        ev3 = Event(
            event_id="manual:bad-json:1",
            source="investinglive_news",
            source_type="news",
            source_reference="https://investinglive.com/news/test-bad-json/",
            source_timestamp=t3,
            received_timestamp=t3,
            raw_text="ECB signals EUR/USD policy shift.",
        )
        r_bad1 = asyncio.run(rt.run_cycle(ev3))
        r_bad2 = asyncio.run(rt.run_cycle(ev3))
        assert r_bad1.status == "reasoning_failed"
        assert r_bad2.status == "reasoning_failed"
        row3 = store.get_event_state(ev3.event_id)
        assert row3 is not None and int(row3["attempts"]) == 2
        assert bad_client.calls == 2
    finally:
        obs.close()
        ledger.close()
        store.close()


def test_bundled_events_waiting_on_quotes_keep_stable_membership_and_finalize_on_expiry(
    tmp_path: Path,
) -> None:
    """Cross-feature invariant (Item 1 + Item 4):
    A simultaneous bundle waiting on quotes neither burns model retries nor regroups with latecomers,
    and if the quote outage outlasts max_quote_wait_seconds, the bundle expires and marks all bound members 'bundled'."""
    t0 = datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)
    now = t0

    obs = ObserverStore(tmp_path / "bq_obs.sqlite", clock=lambda: now)
    shd_db = tmp_path / "bq_shd.sqlite"
    mem = SQLiteThesisMemory(tmp_path / "bq_mem.sqlite", clock=lambda: now, allow_targets=True)
    store = SQLiteRuntimeStore(shd_db)
    ledger = SQLiteShadowLedger(shd_db, clock=lambda: now)
    client = _RecordingBundleModelClient()

    _record_calendar_batch(obs, received_at=t0 - timedelta(minutes=5), records=(), tag="base")
    r_a = _calendar_record("ff:a@1", "USD Retail Sales m/m", scheduled_at=t0, actual="0.8%")
    r_b = _calendar_record("ff:b@1", "USD Core Retail Sales m/m", scheduled_at=t0, actual="0.6%")
    _record_calendar_batch(obs, received_at=t0, records=(r_a, r_b), tag="pair")

    rt = IntradayEventRuntime(
        memory=mem,
        portfolio_state=ledger,
        market_provider=lambda ts: _empty_market(ts),
        agent=SinglePassTradingAgent(client, clock=lambda: now),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: now),
        shadow_executor=ledger,
        state_store=store,
        event_filter=EURUSDEventFilter(clock=lambda: now),
        clock=lambda: now,
    )
    runner = ShadowRunner(
        runtime=rt,
        ledger=ledger,
        observer_store=obs,
        allow_baseline_revisions=False,
        max_reasoning_attempts=2,
        max_quote_wait_seconds=20.0,
        clock=lambda: now,
    )
    try:
        # 3 scans during quote outage (> max_reasoning_attempts=2)
        for sec in (1, 5, 10):
            now = t0 + timedelta(seconds=sec)
            res = asyncio.run(runner.step_events())
            assert len(res) == 1
            assert res[0].status == "awaiting_market_quote"
            assert client.calls == 0

        # Outage exceeds 20s validity window -> bundle transitions to 'expired' and members finalize as 'bundled'
        now = t0 + timedelta(seconds=25)
        res_exp = asyncio.run(runner.step_events())
        assert len(res_exp) == 1
        assert res_exp[0].status == "expired"
        assert client.calls == 0

        rep = store.summary_report()
        assert rep["state_counts"].get("expired") == 1
        assert rep["state_counts"].get("bundled") == 2
        assert asyncio.run(runner.step_events()) == ()
    finally:
        obs.close()
        ledger.close()
        store.close()
