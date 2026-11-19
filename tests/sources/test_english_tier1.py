"""Tests for Tier-1 English FX/macro sources, simultaneous event bundling, and schedule CLI."""

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from eventlens.contracts import MarketSnapshot, Quote
from eventlens.deepseek import ModelResponse
from eventlens.observer import CaptureBatch, MacroValues, ObserverStore, SourceRecord
from eventlens.runtime import IntradayEventRuntime, ShadowRunner, SQLiteRuntimeStore
from eventlens.sources.english_tier1 import (
    ECB_PRESS_RSS_URL,
    FOREXFACTORY_CALENDAR_URL,
    INVESTINGLIVE_RSS_URL,
    US_BEA_RSS_URL,
    EURUSDEventFilter,
    Tier1EnglishSource,
    Tier1StreamSpec,
    build_macro_schedule,
    bundle_simultaneous_events,
    format_macro_schedule_table,
    parse_forexfactory_calendar,
    parse_rss_capture,
)
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import SinglePassTradingAgent
from eventlens.trading.execution import SQLiteShadowLedger
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine

NOW = datetime(2026, 10, 29, 12, 31, 0, tzinfo=UTC)

INVESTINGLIVE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>investingLive</title>
    <item>
      <title>US Q3 advance GDP 3.5% vs 3.2% expected</title>
      <link>https://investinglive.com/news/us-q3-advance-gdp-20261029/</link>
      <guid>https://investinglive.com/news/us-q3-advance-gdp-20261029/</guid>
      <pubDate>Thu, 29 Oct 2026 12:30:12 GMT</pubDate>
      <category>News</category>
      <category>Forex</category>
      <description><![CDATA[<ul><li>Prior was 1.5%</li><li>Consumer spending rose 3.7%</li></ul>]]></description>
    </item>
    <item>
      <title>US September core PCE price index 0.4% m/m vs 0.3% expected</title>
      <link>https://investinglive.com/news/us-sep-core-pce-20261029/</link>
      <guid>https://investinglive.com/news/us-sep-core-pce-20261029/</guid>
      <pubDate>Thu, 29 Oct 2026 12:30:28 GMT</pubDate>
      <category>Central Banks</category>
      <description><![CDATA[<ul><li>Core PCE y/y 2.9% vs 2.7% expected</li></ul>]]></description>
    </item>
    <item>
      <title>EURUSD trades near 100-hour moving average ahead of ECB</title>
      <link>https://investinglive.com/technical-analysis/eurusd-100ma-20261029/</link>
      <guid>https://investinglive.com/technical-analysis/eurusd-100ma-20261029/</guid>
      <pubDate>Thu, 29 Oct 2026 12:15:00 GMT</pubDate>
      <category>Technical Analysis</category>
      <description><![CDATA[<p>Pure chart levels.</p>]]></description>
    </item>
    <item>
      <title>Focused local software firm used new cloud stack</title>
      <link>https://investinglive.com/stocks/focused-software-firm-20261029/</link>
      <guid>https://investinglive.com/stocks/focused-software-firm-20261029/</guid>
      <pubDate>Thu, 29 Oct 2026 12:10:00 GMT</pubDate>
      <category>News</category>
      <description><![CDATA[<p>Feared supply issues eased.</p>]]></description>
    </item>
  </channel>
</rss>
"""

ECB_RSS_INITIAL = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>October 2026 euro area bank lending survey</title>
      <link>https://www.ecb.europa.eu//press/pr/date/2026/html/ecb.pr261027~506ef03145.en.html</link>
      <guid>https://www.ecb.europa.eu//press/pr/date/2026/html/ecb.pr261027~506ef03145.en.html</guid>
      <pubDate>Tue, 27 Oct 2026 01:00:00 +0100</pubDate>
    </item>
  </channel>
</rss>
"""

ECB_RSS_WITH_NEW_DECISION = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Monetary policy decisions</title>
      <link>https://www.ecb.europa.eu//press/pr/date/2026/html/ecb.mp261029~abc12345.en.html</link>
      <guid>https://www.ecb.europa.eu//press/pr/date/2026/html/ecb.mp261029~abc12345.en.html</guid>
      <pubDate>Thu, 29 Oct 2026 13:15:00 +0100</pubDate>
    </item>
    <item>
      <title>October 2026 euro area bank lending survey</title>
      <link>https://www.ecb.europa.eu//press/pr/date/2026/html/ecb.pr261027~506ef03145.en.html</link>
      <guid>https://www.ecb.europa.eu//press/pr/date/2026/html/ecb.pr261027~506ef03145.en.html</guid>
      <pubDate>Tue, 27 Oct 2026 01:00:00 +0100</pubDate>
    </item>
  </channel>
</rss>
"""

ECB_DECISION_HTML = b"""<!doctype html>
<html><body>
<header>Navigation menu</header>
<main>
  <div class="title"><h1>Monetary policy decisions</h1></div>
  <div class="section">
    <ul>
      <li>Governing Council lowers deposit facility rate by 25 basis points to 2.25%</li>
      <li>Disinflation process remains well on track across the euro area</li>
    </ul>
    <p>The Governing Council today decided to lower the three key ECB interest rates by 25 basis points.</p>
  </div>
  <div>\nCONTACT\nEuropean Central Bank Press Office</div>
</main>
</body></html>
"""

BEA_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Gross Domestic Product, Third Quarter 2026 (Advance Estimate)</title>
      <link>https://www.bea.gov/news/2026/gross-domestic-product-3q26-advance</link>
      <guid>https://www.bea.gov/news/2026/gross-domestic-product-3q26-advance</guid>
      <description><![CDATA[<p>Real gross domestic product increased at an annual rate of 3.5 percent in Q3 2026.</p>]]></description>
      <pubDate>Thu, 29 Oct 2026 08:30:00 EDT</pubDate>
    </item>
  </channel>
</rss>
"""

FOREXFACTORY_JSON = json.dumps(
    [
        {
            "title": "Advance GDP q/q",
            "country": "USD",
            "date": "2026-10-29T08:30:00-04:00",
            "impact": "High",
            "forecast": "3.2%",
            "previous": "1.5%",
        },
        {
            "title": "Core PCE Price Index m/m",
            "country": "USD",
            "date": "2026-10-29T08:30:00-04:00",
            "impact": "High",
            "forecast": "0.3%",
            "previous": "0.2%",
        },
        {
            "title": "FHFA House Price Index m/m",
            "country": "USD",
            "date": "2026-10-29T08:30:00-04:00",
            "impact": "Medium",
            "forecast": "",
            "previous": "0.3%",
        },
        {
            "title": "Monetary Policy Statement",
            "country": "EUR",
            "date": "2026-10-29T09:15:00-04:00",
            "impact": "High",
            "forecast": "",
            "previous": "",
        },
        {
            "title": "BOJ Core CPI y/y",
            "country": "JPY",
            "date": "2026-10-29T01:00:00-04:00",
            "impact": "Low",
            "forecast": "1.8%",
            "previous": "1.8%",
        },
    ]
).encode("utf-8")


def _fresh_market(ts: datetime) -> MarketSnapshot:
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


def test_investinglive_and_bea_rss_parsing_and_word_boundary_filtering(tmp_path: Path) -> None:
    records, rejected, err = parse_rss_capture(
        INVESTINGLIVE_XML,
        provider="investinglive",
        stream="news/rss",
        received_at=NOW,
    )
    assert err is None and not rejected
    assert len(records) == 4
    # Bullet extraction preserved
    assert "- Consumer spending rose 3.7%" in records[0].text
    # Technical analysis scored 0
    assert records[2].importance == 0

    bea_records, bea_rej, bea_err = parse_rss_capture(
        BEA_XML,
        provider="us_bea",
        stream="news/releases",
        received_at=NOW,
    )
    assert bea_err is None and not bea_rej
    assert bea_records[0].importance == 4
    assert bea_records[0].publisher_display_at == datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)

    store = ObserverStore(tmp_path / "obs.sqlite", clock=lambda: NOW)
    store.record(
        CaptureBatch(
            provider="investinglive",
            stream="news/rss",
            kind="news",
            request_url=INVESTINGLIVE_RSS_URL,
            request_started_at=NOW,
            received_at=NOW,
            elapsed_ms=5.0,
            attempts=1,
            payload=INVESTINGLIVE_XML,
            records=records,
        )
    )
    store.record(
        CaptureBatch(
            provider="us_bea",
            stream="news/releases",
            kind="news",
            request_url=US_BEA_RSS_URL,
            request_started_at=NOW,
            received_at=NOW,
            elapsed_ms=5.0,
            attempts=1,
            payload=BEA_XML,
            records=bea_records,
        )
    )

    revs = store.revisions(as_of=NOW, limit=50)
    assert len(revs) == 5  # multi-provider query works across investinglive + us_bea

    flt = EURUSDEventFilter(clock=lambda: NOW)
    outcomes = {}
    for rev in revs:
        res, ev = flt.evaluate_revision(rev, checked_at=NOW)
        outcomes[rev.record.title] = (res.outcome, res.reason, ev.source_type if ev else None)

    assert outcomes["US Q3 advance GDP 3.5% vs 3.2% expected"] == (
        "accept",
        "eligible_investinglive_news",
        "news",
    )
    assert outcomes["US September core PCE price index 0.4% m/m vs 0.3% expected"] == (
        "accept",
        "eligible_investinglive_news",
        "news",
    )
    assert outcomes["EURUSD trades near 100-hour moving average ahead of ECB"] == (
        "ignore",
        "technical_or_non_macro_category",
        None,
    )
    # "Focused" / "used" / "Feared" must NOT trigger false positive on fed/usd/eur
    assert outcomes["Focused local software firm used new cloud stack"] == (
        "ignore",
        "no_eurusd_macro_relevance",
        None,
    )
    assert outcomes["Gross Domestic Product, Third Quarter 2026 (Advance Estimate)"] == (
        "accept",
        "eligible_us_bea_news",
        "macro",
    )
    store.close()


def test_ecb_baseline_skips_article_fetch_and_live_arrival_enriches_main_html(
    tmp_path: Path,
) -> None:
    requests_seen: list[str] = []
    poll_step = 0

    def handler(req: httpx.Request) -> httpx.Response:
        url = str(req.url)
        requests_seen.append(url)
        if url == ECB_PRESS_RSS_URL:
            return httpx.Response(
                200, content=ECB_RSS_INITIAL if poll_step == 0 else ECB_RSS_WITH_NEW_DECISION
            )
        if url.endswith("ecb.mp261029~abc12345.en.html"):
            return httpx.Response(200, content=ECB_DECISION_HTML)
        return httpx.Response(404)

    t0 = datetime(2026, 10, 29, 13, 14, 0, tzinfo=UTC)
    clock_now = t0
    store = ObserverStore(tmp_path / "ecb.sqlite", clock=lambda: clock_now)
    spec = Tier1StreamSpec(
        provider="ecb",
        stream="news/press",
        kind="news",
        url=ECB_PRESS_RSS_URL,
        min_poll_seconds=10.0,
        enrich_live_articles=True,
    )
    source = Tier1EnglishSource(
        streams=(spec,),
        transport=httpx.MockTransport(handler),
        clock=lambda: clock_now,
    )

    # Poll 1: initial baseline -> must NOT fetch .en.html article
    res1 = asyncio.run(source.poll_due_streams(store, force=True))
    assert res1[0]["counts"]["baseline"] == 1
    assert requests_seen == [ECB_PRESS_RSS_URL]

    # Poll 2: live ECB Monetary Policy Decisions arrives -> fetches ONLY the new .en.html body!
    poll_step = 1
    clock_now = t0 + timedelta(minutes=2)
    res2 = asyncio.run(source.poll_due_streams(store, force=True))
    assert res2[0]["counts"]["new"] == 1
    assert any(u.endswith("ecb.mp261029~abc12345.en.html") for u in requests_seen)
    assert not any(u.endswith("ecb.pr261027~506ef03145.en.html") for u in requests_seen)

    revs = store.revisions(provider="ecb", as_of=clock_now)
    live_rev = [r for r in revs if r.record.title == "Monetary policy decisions"][0]
    assert "lowers deposit facility rate by 25 basis points to 2.25%" in live_rev.record.text
    assert "European Central Bank Press Office" not in live_rev.record.text
    store.close()


def test_forexfactory_calendar_preserves_missing_consensus_never_copies_previous() -> None:
    records, rejected, err = parse_forexfactory_calendar(FOREXFACTORY_JSON, received_at=NOW)
    assert err is None and not rejected
    assert len(records) == 5
    fhfa = [r for r in records if "FHFA" in r.title][0]
    assert fhfa.macro is not None
    assert fhfa.macro.reported_forecast is None
    assert fhfa.macro.previous == "0.3%"
    assert "Consensus Forecast: MISSING | Previous: 0.3%" in fhfa.text


def test_investinglive_simultaneous_news_burst_bundles_cleanly(tmp_path: Path) -> None:
    records, _, _ = parse_rss_capture(
        INVESTINGLIVE_XML,
        provider="investinglive",
        stream="news/rss",
        received_at=NOW,
    )
    store = ObserverStore(tmp_path / "burst.sqlite", clock=lambda: NOW)
    store.record(
        CaptureBatch(
            provider="investinglive",
            stream="news/rss",
            kind="news",
            request_url=INVESTINGLIVE_RSS_URL,
            request_started_at=NOW,
            received_at=NOW,
            elapsed_ms=3.0,
            attempts=1,
            payload=INVESTINGLIVE_XML,
            records=records,
        )
    )
    flt = EURUSDEventFilter(clock=lambda: NOW)
    accepted = []
    for rev in store.revisions(as_of=NOW):
        f_res, ev = flt.evaluate_revision(rev, checked_at=NOW)
        if ev is not None:
            accepted.append((rev, ev))
    assert len(accepted) == 2
    bundles = bundle_simultaneous_events(accepted, news_bundle_window_seconds=60.0)
    assert len(bundles) == 1
    assert bundles[0].is_bundled is True
    assert "US Q3 advance GDP 3.5% vs 3.2% expected" in bundles[0].event.raw_text
    assert (
        "US September core PCE price index 0.4% m/m vs 0.3% expected" in bundles[0].event.raw_text
    )
    store.close()


def test_simultaneous_release_bundling_invokes_agent_once_and_marks_constituents_bundled(
    tmp_path: Path,
) -> None:
    t_sched = datetime(2026, 10, 29, 12, 30, 0, tzinfo=UTC)
    t_recv = datetime(2026, 10, 29, 12, 30, 3, tzinfo=UTC)
    store = ObserverStore(tmp_path / "bundle_obs.sqlite", clock=lambda: t_recv)

    # First record an empty baseline so the simultaneous release arrives as live "new"
    store.record(
        CaptureBatch(
            provider="forexfactory",
            stream="calendar/live",
            kind="calendar",
            request_url=FOREXFACTORY_CALENDAR_URL,
            request_started_at=t_sched - timedelta(minutes=5),
            received_at=t_sched - timedelta(minutes=5),
            elapsed_ms=2.0,
            attempts=1,
            payload=b"[]",
            records=(),
        )
    )

    sim_records = (
        SourceRecord(
            source_id="ff:usd-advance-gdp@1793277000",
            kind="calendar",
            source_url="https://www.forexfactory.com/calendar",
            title="USD Advance GDP q/q",
            text="USD Advance GDP q/q released at 3.5%",
            scheduled_at=t_sched,
            importance=3,
            macro=MacroValues(
                actual="3.5%",
                reported_forecast="3.2%",
                previous="1.5%",
                country="USD",
            ),
        ),
        SourceRecord(
            source_id="ff:usd-core-pce@1793277000",
            kind="calendar",
            source_url="https://www.forexfactory.com/calendar",
            title="USD Core PCE Price Index m/m",
            text="USD Core PCE Price Index m/m released at 0.4%",
            scheduled_at=t_sched,
            importance=3,
            macro=MacroValues(
                actual="0.4%",
                reported_forecast="0.3%",
                previous="0.2%",
                country="USD",
            ),
        ),
        SourceRecord(
            source_id="ff:usd-fhfa-hpi@1793277000",
            kind="calendar",
            source_url="https://www.forexfactory.com/calendar",
            title="USD FHFA House Price Index m/m",
            text="USD FHFA House Price Index m/m released at 0.3%",
            scheduled_at=t_sched,
            importance=2,
            macro=MacroValues(
                actual="0.3%",
                reported_forecast=None,
                previous="0.3%",
                country="USD",
            ),
        ),
    )
    store.record(
        CaptureBatch(
            provider="forexfactory",
            stream="calendar/live",
            kind="calendar",
            request_url=FOREXFACTORY_CALENDAR_URL,
            request_started_at=t_recv,
            received_at=t_recv,
            elapsed_ms=4.0,
            attempts=1,
            payload=b"[1,2,3]",
            records=sim_records,
        )
    )

    seen_user_prompts: list[str] = []

    class FakeBundleModelClient:
        provider = "deepseek"
        model = "deepseek-chat"

        async def complete_json(self, *, system: str, user: str) -> ModelResponse:
            seen_user_prompts.append(user)
            assert "Consensus Forecast: MISSING (no consensus forecast reported)" in user
            payload = {
                "portfolio_action": "SET_TARGET",
                "target_direction": "short",
                "target_weight": "-0.10",
                "strategy_hypothesis": "fundamental_breakout",
                "market_bias": "bearish_eur",
                "market_state_note": "Hot US GDP and Core PCE reinforce hawkish Fed pricing",
                "watch_condition": None,
                "rationale": "Simultaneous hot US GDP and Core PCE beat consensus",
            }
            return ModelResponse(
                content=json.dumps(payload),
                input_tokens=850,
                output_tokens=160,
                model_id="deepseek-chat",
            )

    shadow_db = tmp_path / "bundle_shadow.sqlite"
    mem_db = tmp_path / "bundle_mem.sqlite"
    memory = SQLiteThesisMemory(mem_db, clock=lambda: t_recv, allow_targets=True)
    state_store = SQLiteRuntimeStore(shadow_db)
    ledger = SQLiteShadowLedger(shadow_db, clock=lambda: t_recv)
    agent = SinglePassTradingAgent(FakeBundleModelClient(), clock=lambda: t_recv)
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: _fresh_market(ts),
        agent=agent,
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: t_recv),
        shadow_executor=ledger,
        state_store=state_store,
        event_filter=EURUSDEventFilter(clock=lambda: t_recv),
        clock=lambda: t_recv,
    )
    runner = ShadowRunner(
        runtime=runtime,
        ledger=ledger,
        observer_store=store,
        allow_baseline_revisions=False,
        bundle_simultaneous=True,
        clock=lambda: t_recv,
    )
    try:
        res_first = asyncio.run(runner.step_events())
        assert len(res_first) == 1
        assert len(seen_user_prompts) == 1
        assert res_first[0].event_id.startswith("bundle:forexfactory:")
        assert res_first[0].status == "shadow_recorded"

        # Second scan must NOT re-process any constituent or bundle!
        res_second = asyncio.run(runner.step_events())
        assert res_second == ()
        assert len(seen_user_prompts) == 1

        rep = state_store.summary_report()
        assert rep["model_calls"] == 1
        assert rep["total_input_tokens"] == 850
        assert rep["state_counts"]["bundled"] == 3
        assert rep["state_counts"]["shadow_recorded"] == 1
    finally:
        store.close()
        ledger.close()
        state_store.close()


def test_observe_schedule_cli_groups_windows_and_marks_missing_consensus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:

    ref_now = datetime(2026, 10, 29, 0, 0, 0, tzinfo=UTC)

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=FOREXFACTORY_JSON)

    mock_src = Tier1EnglishSource(
        transport=httpx.MockTransport(handler),
        clock=lambda: ref_now,
    )
    sched = asyncio.run(
        build_macro_schedule(
            source=mock_src,
            currencies=("USD", "EUR"),
            min_impact="medium",
            target_date_bjt=date(2026, 10, 29),
            now=ref_now,
        )
    )
    assert sched["execution_authorized"] is False
    assert sched["directional_forecast_included"] is False
    assert sched["window_count"] == 2
    assert sched["windows"][0]["scheduled_bjt"] == "2026-10-29 20:30"
    assert sched["windows"][0]["bundled_count"] == 3
    fhfa_row = [i for i in sched["windows"][0]["indicators"] if "FHFA" in i["event"]][0]
    assert fhfa_row["consensus_forecast"] == "MISSING"
    assert fhfa_row["previous"] == "0.3%"

    table_str = format_macro_schedule_table(sched)
    assert "BUNDLED HIGH WINDOW (3 simultaneous releases)" in table_str
    assert "MISSING" in table_str


def test_eurusd_filter_v2_blocks_wraps_non_g2_and_preserves_g2_anchors(tmp_path: Path) -> None:
    """EURUSDEventFilter v2 rejects roundups/expiries and non-G2/single-stock noise unless a G2 macro anchor is present."""
    store = ObserverStore(tmp_path / "filter_v2.sqlite", clock=lambda: NOW)
    samples = [
        (
            "wrap-1",
            "investingLive Americas FX news wrap 18 Nov: USD mixed in quiet trade",
            "US dollar hovered near weekly highs while stocks consolidated.",
            3,
        ),
        (
            "exp-1",
            "FX option expiries for 19 November NY cut",
            "EUR/USD 1.0950 (1.2B), USD/JPY 155.00 (800M).",
            2,
        ),
        (
            "pboc-1",
            "PBOC sets USD/CNY reference rate for today at 7.1932",
            "China central bank injects 100B yuan via 7-day reverse repos (USD benchmark).",
            2,
        ),
        (
            "aud-1",
            "Australia Q3 wage price index +0.8% vs +0.8% q/q expected",
            "RBA remains patient on rates while USD trades steady.",
            2,
        ),
        (
            "ipo-1",
            "Anthropic eyes $60B IPO as Nvidia and Bitcoin rally",
            "Tech shares surge with US Treasury yields flat.",
            2,
        ),
        (
            "fed-1",
            "Fed's Waller: inflation sticky, no rush to cut rates",
            "US Treasury yields climb as market trims December Fed cut odds.",
            3,
        ),
        (
            "cross-1",
            "BOJ caution and Fed's Waller lift US dollar against EUR/USD",
            "Transatlantic yield divergence pushes EUR/USD lower.",
            3,
        ),
    ]
    store.record(
        CaptureBatch(
            provider="investinglive",
            stream="news/live_feed",
            kind="news",
            request_url="https://investinglive.com/live-feed/",
            request_started_at=NOW,
            received_at=NOW,
            elapsed_ms=5.0,
            attempts=1,
            payload=b"<html/>",
            records=tuple(
                SourceRecord(
                    source_id=sid,
                    kind="news",
                    source_url=f"https://investinglive.com/news/{sid}/",
                    title=title,
                    text=body,
                    publisher_display_at=NOW - timedelta(seconds=10),
                    importance=imp,
                )
                for sid, title, body, imp in samples
            ),
        )
    )
    flt = EURUSDEventFilter(clock=lambda: NOW)
    by_id: dict[str, str] = {}
    for rev in store.revisions(as_of=NOW, limit=50):
        res, _ = flt.evaluate_revision(rev, checked_at=NOW)
        by_id[rev.record.source_id] = f"{res.outcome}:{res.reason}"

    assert by_id["wrap-1"] == "ignore:roundup_or_schedule_preview_noise"
    assert by_id["exp-1"] == "ignore:roundup_or_schedule_preview_noise"
    assert by_id["pboc-1"] == "ignore:non_g2_or_single_stock_noise"
    assert by_id["aud-1"] == "ignore:non_g2_or_single_stock_noise"
    assert by_id["ipo-1"] == "ignore:non_g2_or_single_stock_noise"
    assert by_id["fed-1"] == "accept:eligible_investinglive_news"
    assert by_id["cross-1"] == "accept:eligible_investinglive_news"
    store.close()


def test_shadow_runner_news_revision_cooldown_and_empty_delta_suppression(tmp_path: Path) -> None:
    """Rapid investinglive r2+ edits within 300s and zero-text-delta edits are filtered; calendar r2 and >300s cumulative r3 pass."""
    t_now = [datetime(2026, 11, 18, 12, 0, 0, tzinfo=UTC)]
    store = ObserverStore(tmp_path / "cd_obs.sqlite", clock=lambda: t_now[0])
    shadow_db = tmp_path / "cd_shadow.sqlite"
    mem_db = tmp_path / "cd_mem.sqlite"

    # Baseline poll so subsequent events are live arrivals
    store.record(
        CaptureBatch(
            provider="investinglive",
            stream="news/live_feed",
            kind="news",
            request_url=INVESTINGLIVE_RSS_URL,
            request_started_at=t_now[0],
            received_at=t_now[0],
            elapsed_ms=2.0,
            attempts=1,
            payload=b"<html/>",
            records=(),
        )
    )
    store.record(
        CaptureBatch(
            provider="forexfactory",
            stream="calendar/thisweek",
            kind="calendar",
            request_url=FOREXFACTORY_CALENDAR_URL,
            request_started_at=t_now[0],
            received_at=t_now[0],
            elapsed_ms=2.0,
            attempts=1,
            payload=b"[]",
            records=(),
        )
    )

    seen_prompts: list[str] = []

    class _RecordingClient:
        provider = "deepseek"
        model = "deepseek-chat"

        async def complete_json(self, *, system: str, user: str) -> ModelResponse:
            seen_prompts.append(user)
            return ModelResponse(
                content=json.dumps(
                    {
                        "portfolio_action": "HOLD",
                        "target_direction": "flat",
                        "target_weight": "0",
                        "strategy_hypothesis": "unspecified",
                        "market_bias": "neutral",
                        "market_state_note": "Holding steady.",
                        "watch_condition": None,
                        "rationale": "Acknowledge release.",
                    }
                ),
                input_tokens=100,
                output_tokens=40,
                model_id="deepseek-chat",
            )

    def _rec_news(text: str, ts: datetime) -> SourceRecord:
        return SourceRecord(
            source_id="waller-qa",
            kind="news",
            source_url="https://investinglive.com/centralbank/waller-qa/",
            title="Fed's Waller speaks on policy path",
            text=text,
            publisher_display_at=t_now[0],
            modified_at=ts,
            importance=3,
        )

    memory = SQLiteThesisMemory(mem_db, clock=lambda: t_now[0], allow_targets=True)
    state_store = SQLiteRuntimeStore(shadow_db)
    ledger = SQLiteShadowLedger(shadow_db, clock=lambda: t_now[0])
    runtime = IntradayEventRuntime(
        memory=memory,
        portfolio_state=ledger,
        market_provider=lambda ts: _fresh_market(ts),
        agent=SinglePassTradingAgent(_RecordingClient(), clock=lambda: t_now[0]),
        target_validator=DeterministicTargetValidator(),
        risk_engine=DeterministicHardRiskEngine(clock=lambda: t_now[0]),
        shadow_executor=ledger,
        state_store=state_store,
        event_filter=EURUSDEventFilter(clock=lambda: t_now[0]),
        clock=lambda: t_now[0],
    )
    runner = ShadowRunner(
        runtime=runtime,
        ledger=ledger,
        observer_store=store,
        allow_baseline_revisions=False,
        bundle_simultaneous=False,
        news_revision_cooldown_seconds=300.0,
        clock=lambda: t_now[0],
    )
    runner._run_started_at = t_now[0] + timedelta(seconds=1)

    try:
        # 1. Live r1 news arrives -> evaluated
        t_now[0] += timedelta(seconds=10)
        store.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                request_url=INVESTINGLIVE_RSS_URL,
                request_started_at=t_now[0],
                received_at=t_now[0],
                elapsed_ms=2.0,
                attempts=1,
                payload=b"r1",
                records=(_rec_news("Inflation progress has stalled.", t_now[0]),),
            )
        )
        assert len(asyncio.run(runner.step_events())) == 1
        assert len(seen_prompts) == 1

        # 2. r2 with identical text (modified_at changed, no new text lines) -> revision_no_substantive_text_delta
        t_now[0] += timedelta(minutes=7)
        store.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                request_url=INVESTINGLIVE_RSS_URL,
                request_started_at=t_now[0],
                received_at=t_now[0],
                elapsed_ms=2.0,
                attempts=1,
                payload=b"r2",
                records=(_rec_news("Inflation progress has stalled.\n", t_now[0]),),
            )
        )
        assert asyncio.run(runner.step_events()) == ()
        row_r2 = state_store.get_event_state("investinglive:news:waller-qa:r2")
        assert row_r2 is not None
        assert row_r2["state"] == "filtered_out"
        assert row_r2["reason"] == "revision_no_substantive_text_delta"

        # 3. r3 adds new Q&A bullet only 60s later (remember r2 was empty-delta so baseline is r1 at t+10s,
        # wait -> let's test <300s explicitly with a fresh rapid article waller-qa-2!)
        t_rapid = t_now[0]
        store.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                request_url=INVESTINGLIVE_RSS_URL,
                request_started_at=t_rapid,
                received_at=t_rapid,
                elapsed_ms=2.0,
                attempts=1,
                payload=b"q2-r1",
                records=(
                    SourceRecord(
                        source_id="waller-qa-2",
                        kind="news",
                        source_url="https://investinglive.com/centralbank/waller-qa-2/",
                        title="Fed's Waller Q&A opening",
                        text="Rates may hold steady.",
                        publisher_display_at=t_rapid,
                        modified_at=t_rapid,
                        importance=3,
                    ),
                ),
            )
        )
        assert len(asyncio.run(runner.step_events())) == 1
        assert len(seen_prompts) == 2

        t_now[0] = t_rapid + timedelta(seconds=60)
        store.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                request_url=INVESTINGLIVE_RSS_URL,
                request_started_at=t_now[0],
                received_at=t_now[0],
                elapsed_ms=2.0,
                attempts=1,
                payload=b"q2-r2",
                records=(
                    SourceRecord(
                        source_id="waller-qa-2",
                        kind="news",
                        source_url="https://investinglive.com/centralbank/waller-qa-2/",
                        title="Fed's Waller Q&A opening",
                        text="Rates may hold steady.\nCore PCE risk stays elevated.",
                        publisher_display_at=t_rapid,
                        modified_at=t_now[0],
                        importance=3,
                    ),
                ),
            )
        )
        assert asyncio.run(runner.step_events()) == ()
        row_q2_r2 = state_store.get_event_state("investinglive:news:waller-qa-2:r2")
        assert row_q2_r2 is not None
        assert row_q2_r2["state"] == "filtered_out"
        assert row_q2_r2["reason"] == "rapid_revision_cooldown"

        # 4. q2-r3 arrives 360s after r1 with another line -> passes cooldown and gets cumulative delta vs r1!
        t_now[0] = t_rapid + timedelta(seconds=360)
        store.record(
            CaptureBatch(
                provider="investinglive",
                stream="news/live_feed",
                kind="news",
                request_url=INVESTINGLIVE_RSS_URL,
                request_started_at=t_now[0],
                received_at=t_now[0],
                elapsed_ms=2.0,
                attempts=1,
                payload=b"q2-r3",
                records=(
                    SourceRecord(
                        source_id="waller-qa-2",
                        kind="news",
                        source_url="https://investinglive.com/centralbank/waller-qa-2/",
                        title="Fed's Waller Q&A opening",
                        text="Rates may hold steady.\nCore PCE risk stays elevated.\nPayroll growth remains resilient.",
                        publisher_display_at=t_rapid,
                        modified_at=t_now[0],
                        importance=3,
                    ),
                ),
            )
        )
        res_r3 = asyncio.run(runner.step_events())
        assert len(res_r3) == 1
        assert "Core PCE risk stays elevated." in seen_prompts[-1]
        assert "Payroll growth remains resilient." in seen_prompts[-1]

        # 5. Calendar r1 -> r2 only 30s later MUST NOT be suppressed by news cooldown
        t_cal = t_now[0]
        cal_sched = t_cal - timedelta(seconds=10)
        store.record(
            CaptureBatch(
                provider="forexfactory",
                stream="calendar/thisweek",
                kind="calendar",
                request_url=FOREXFACTORY_CALENDAR_URL,
                request_started_at=t_cal,
                received_at=t_cal,
                elapsed_ms=2.0,
                attempts=1,
                payload=b"cal1",
                records=(
                    SourceRecord(
                        source_id="ff-us-cpi",
                        kind="calendar",
                        source_url=FOREXFACTORY_CALENDAR_URL,
                        title="US Core CPI m/m",
                        text="Actual 0.4% vs 0.3%",
                        scheduled_at=cal_sched,
                        importance=3,
                        macro=MacroValues(
                            country="USD", actual="0.4%", reported_forecast="0.3%", previous="0.3%"
                        ),
                    ),
                ),
            )
        )
        assert len(asyncio.run(runner.step_events())) == 1
        t_now[0] = t_cal + timedelta(seconds=30)
        store.record(
            CaptureBatch(
                provider="forexfactory",
                stream="calendar/thisweek",
                kind="calendar",
                request_url=FOREXFACTORY_CALENDAR_URL,
                request_started_at=t_now[0],
                received_at=t_now[0],
                elapsed_ms=2.0,
                attempts=1,
                payload=b"cal2",
                records=(
                    SourceRecord(
                        source_id="ff-us-cpi",
                        kind="calendar",
                        source_url=FOREXFACTORY_CALENDAR_URL,
                        title="US Core CPI m/m",
                        text="Actual 0.5% vs 0.3% (corrected)",
                        scheduled_at=cal_sched,
                        importance=3,
                        macro=MacroValues(
                            country="USD", actual="0.5%", reported_forecast="0.3%", previous="0.3%"
                        ),
                    ),
                ),
            )
        )
        assert len(asyncio.run(runner.step_events())) == 1
    finally:
        store.close()
        ledger.close()
        state_store.close()
