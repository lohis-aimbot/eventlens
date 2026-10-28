"""Tier-1 English FX & official macro sources (investingLive, ECB, Fed, US BEA, ForexFactory).

All adapters are bounded, read-only public HTTP/RSS/JSON collectors with zero startup side effects.
"""

import asyncio
import hashlib
import json
import logging
import math
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import httpx
from pydantic import ValidationError

from ..contracts import Event, FilterResult
from ..observer import (
    CaptureBatch,
    Kind,
    MacroValues,
    ObserverStore,
    RejectedRecord,
    SourceRecord,
    StoredRevision,
)
from .wallstreetcn import (
    CaptureError,
    WallStreetCNFilter,
    revision_event_id,
    revision_to_event,
)

logger = logging.getLogger(__name__)

BJT = ZoneInfo("Asia/Shanghai")
MAX_BYTES = 1_000_000
MAX_ARTICLE_CHARS = 3_800
MAX_BUNDLE_RAW_CHARS = 14_000
TIER1_FILTER_VERSION = "tier1-eurusd-filter-v1"
USER_AGENT = "EventLensResearch/0.2 (+https://github.com/lohis-aimbot/eventlens; shadow-only)"

INVESTINGLIVE_RSS_URL = "https://investinglive.com/feed"
ECB_PRESS_RSS_URL = "https://www.ecb.europa.eu/rss/press.html"
ECB_STATPRESS_RSS_URL = "https://www.ecb.europa.eu/rss/statpress.html"
FED_MONETARY_RSS_URL = "https://www.federalreserve.gov/feeds/press_monetary.xml"
FED_SPEECHES_RSS_URL = "https://www.federalreserve.gov/feeds/speeches.xml"
FED_ALL_PRESS_RSS_URL = "https://www.federalreserve.gov/feeds/press_all.xml"
US_BEA_RSS_URL = "https://apps.bea.gov/rss/rss.xml"
FOREXFACTORY_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

_TZ_OFFSETS: dict[str, timezone] = {
    "UTC": UTC,
    "GMT": UTC,
    "Z": UTC,
    "EDT": timezone(timedelta(hours=-4)),
    "EST": timezone(timedelta(hours=-5)),
    "CDT": timezone(timedelta(hours=-5)),
    "CST": timezone(timedelta(hours=-6)),
    "CET": timezone(timedelta(hours=1)),
    "CEST": timezone(timedelta(hours=2)),
}

_IMPACT_SCORES: dict[str, int] = {
    "high": 3,
    "medium": 2,
    "low": 1,
    "holiday": 0,
    "non-economic": 0,
}

_TECHNICAL_CATEGORIES = frozenset(
    {
        "technical analysis",
        "education",
        "cryptocurrency",
        "crypto",
        "brokers",
        "sponsored",
    }
)

_HIGH_DESK_CATEGORIES = frozenset(
    {
        "central banks",
    }
)

_MACRO_DESK_CATEGORIES = frozenset(
    {
        "forex",
        "news",
        "economic indicators",
        "economy",
        "commodities",
        "bonds",
    }
)

_FED_LOW_PRIORITY_CATEGORIES = frozenset(
    {
        "enforcement actions",
        "orders on banking applications",
    }
)

# Word-boundary regex for EUR/USD & G2 macro relevance in English headlines/wires
_EURUSD_EN_PATTERN = re.compile(
    r"\b("
    r"fed|fomc|federal\s+reserve|powell|warsh|waller|bowman|williams|jefferson|kugler|goolsbee|kashkari|bostic|daly|barkin|musalem|hammack|"
    r"ecb|european\s+central\s+bank|lagarde|guindos|lane|schnabel|cipollone|elderson|bundesbank|nagel|villeroy|knot|rehn|holzmann|panetta|"
    r"eurusd|eur/usd|euro|eurozone|euro\s+area|german|germany|french|france|ifo|zew|destatis|eurostat|"
    r"usd|u\.s\.|us\s+dollar|greenback|dxy|treasury|treasuries|yield|yields|"
    r"gdp|pce|cpi|ppi|inflation|deflation|payroll|payrolls|nfp|jobless|unemployment|employment|jolts|adp|"
    r"ism|pmi|retail\s+sales|durable\s+goods|factory\s+orders|trade\s+balance|trade\s+deficit|"
    r"consumer\s+confidence|consumer\s+sentiment|michigan|housing\s+starts|building\s+permits|"
    r"rate\s+decision|interest\s+rate|rate\s+cut|rate\s+hike|basis\s+points|monetary\s+policy|bank\s+lending\s+survey|"
    r"tariff|tariffs|white\s+house|fiscal|debt\s+ceiling|bea"
    r")\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Tier1StreamSpec:
    provider: str
    stream: str
    kind: Kind
    url: str
    min_poll_seconds: float = 15.0
    enrich_live_articles: bool = False


DEFAULT_TIER1_STREAMS: tuple[Tier1StreamSpec, ...] = (
    Tier1StreamSpec(
        provider="investinglive",
        stream="news/rss",
        kind="news",
        url=INVESTINGLIVE_RSS_URL,
        min_poll_seconds=15.0,
    ),
    Tier1StreamSpec(
        provider="ecb",
        stream="news/press",
        kind="news",
        url=ECB_PRESS_RSS_URL,
        min_poll_seconds=15.0,
        enrich_live_articles=True,
    ),
    Tier1StreamSpec(
        provider="fed",
        stream="news/monetary",
        kind="news",
        url=FED_MONETARY_RSS_URL,
        min_poll_seconds=15.0,
        enrich_live_articles=True,
    ),
    Tier1StreamSpec(
        provider="us_bea",
        stream="news/releases",
        kind="news",
        url=US_BEA_RSS_URL,
        min_poll_seconds=20.0,
    ),
    Tier1StreamSpec(
        provider="ecb",
        stream="news/statpress",
        kind="news",
        url=ECB_STATPRESS_RSS_URL,
        min_poll_seconds=180.0,
        enrich_live_articles=True,
    ),
    Tier1StreamSpec(
        provider="fed",
        stream="news/speeches",
        kind="news",
        url=FED_SPEECHES_RSS_URL,
        min_poll_seconds=180.0,
        enrich_live_articles=True,
    ),
    Tier1StreamSpec(
        provider="forexfactory",
        stream="calendar/thisweek",
        kind="calendar",
        url=FOREXFACTORY_CALENDAR_URL,
        min_poll_seconds=300.0,
    ),
)


class _StructuredHTMLTextParser(HTMLParser):
    """Extract clean readable text with bullet preservation and zero script/style leakage."""

    _BLOCK_TAGS = frozenset(
        {"p", "div", "br", "tr", "h1", "h2", "h3", "h4", "section", "article", "blockquote"}
    )
    _SKIP_TAGS = frozenset({"script", "style", "noscript", "svg", "nav", "footer"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lower = tag.lower()
        if lower in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth > 0:
            return
        if lower == "li":
            self._parts.append("\n- ")
        elif lower in self._BLOCK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        lower = tag.lower()
        if lower in self._SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if self._skip_depth == 0 and lower in (self._BLOCK_TAGS | {"li", "ul", "ol"}):
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0 and data:
            self._parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self._parts)
        lines: list[str] = []
        for raw_line in raw.splitlines():
            cleaned = " ".join(raw_line.split()).strip()
            if not cleaned or cleaned == "-":
                continue
            lines.append(cleaned)
        return "\n".join(lines).strip()


def html_to_plain_text(raw_html: str) -> str:
    if not raw_html or not raw_html.strip():
        return ""
    parser = _StructuredHTMLTextParser()
    parser.feed(unescape(raw_html))
    parser.close()
    return parser.get_text()


def extract_ecb_article_text(html_doc: str, *, max_chars: int = MAX_ARTICLE_CHARS) -> str:
    lower = html_doc.lower()
    start = lower.find("<main")
    end = lower.find("</main>", start) if start != -1 else -1
    fragment = html_doc[start : end + 7] if (start != -1 and end != -1) else html_doc
    text = html_to_plain_text(fragment)
    # Trim standard ECB press contact footer if present
    for marker in (
        "\nCONTACT\n",
        "\nMEDIA CONTACTS\n",
        "\nEuropean Central Bank\nDirectorate General",
    ):
        idx = text.find(marker)
        if idx > 120:
            text = text[:idx].strip()
            break
    return text[:max_chars].strip()


def extract_fed_article_text(html_doc: str, *, max_chars: int = MAX_ARTICLE_CHARS) -> str:
    match = re.search(r'<div[^>]+id=["\']article["\'][^>]*>', html_doc, re.IGNORECASE)
    fragment = html_doc[match.start() :] if match is not None else html_doc
    for end_marker in ('<div class="clear">', '<div id="lastUpdate">', '<div class="footer">'):
        idx = fragment.find(end_marker)
        if idx > 100:
            fragment = fragment[:idx]
            break
    text = html_to_plain_text(fragment)
    return text[:max_chars].strip()


def _parse_feed_datetime(value: str) -> datetime:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError("Empty timestamp")
    try:
        dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        if dt.tzinfo is not None and dt.utcoffset() is not None:
            return dt.astimezone(UTC)
    except ValueError:
        pass

    # Check trailing named timezone token if stdlib parsedate leaves tzinfo=None
    parts = cleaned.rsplit(" ", 1)
    tz_override = _TZ_OFFSETS.get(parts[1].upper()) if len(parts) == 2 else None
    parsed = parsedate_to_datetime(cleaned)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        if tz_override is None:
            raise ValueError(f"Timestamp lacks a recognized timezone: {cleaned!r}")
        parsed = parsed.replace(tzinfo=tz_override)
    return parsed.astimezone(UTC)


def _normalize_url(raw_url: str, *, expected_hosts: frozenset[str] | None = None) -> str:
    cleaned = raw_url.strip()
    if (
        expected_hosts is not None
        and "://" not in cleaned
        and any(cleaned.lower().startswith(f"{h}/") for h in expected_hosts)
    ):
        cleaned = f"https://{cleaned}"
    parts = urlsplit(cleaned)
    if parts.scheme not in {"https", "http"} or not parts.netloc:
        raise ValueError(f"Invalid source URL: {cleaned!r}")
    host = parts.netloc.lower()
    if expected_hosts is not None and host not in expected_hosts:
        raise ValueError(f"Unexpected source URL host: {host!r}")
    norm_path = re.sub(r"/{2,}", "/", parts.path or "/")
    return urlunsplit(("https", host, norm_path, parts.query, ""))


def _stable_source_id(prefix: str, raw_key: str) -> str:
    cleaned = raw_key.strip()
    if not cleaned:
        raise ValueError("Missing source identifier")
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", urlsplit(cleaned).path or cleaned).strip("-")
    digest = hashlib.sha256(cleaned.encode("utf-8")).hexdigest()[:12]
    if slug:
        return f"{prefix}:{slug[-64:]}:{digest}"
    return f"{prefix}:{digest}"


def _clean_opt_str(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise ValueError("Nonfinite numeric macro value")
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        raise ValueError("Unexpected macro value type")
    return text if text else None


def _child_text(elem: ElementTree.Element, local_names: tuple[str, ...]) -> str:
    for child in elem:
        tag = child.tag.split("}")[-1]
        if tag in local_names and child.text and child.text.strip():
            return child.text.strip()
    return ""


def _child_texts(elem: ElementTree.Element, local_name: str) -> tuple[str, ...]:
    out: list[str] = []
    for child in elem:
        tag = child.tag.split("}")[-1]
        if tag == local_name and child.text and child.text.strip():
            out.append(child.text.strip())
    return tuple(out)


def _score_rss_item(
    provider: str,
    stream: str,
    title: str,
    body: str,
    categories: tuple[str, ...],
) -> int:
    lower_cats = {c.lower() for c in categories}
    lower_title = title.lower()
    if provider == "investinglive":
        if lower_cats and lower_cats.issubset(_TECHNICAL_CATEGORIES):
            return 0
        if "technical analysis" in lower_cats and "central banks" not in lower_cats:
            return 0
        if lower_cats & _HIGH_DESK_CATEGORIES:
            return 3
        if lower_cats & _MACRO_DESK_CATEGORIES:
            return 2
        if _EURUSD_EN_PATTERN.search(f"{title}\n{body}"):
            return 2
        return 1
    if provider == "ecb":
        if any(
            k in lower_title
            for k in (
                "monetary policy decisions",
                "bank lending survey",
                "press conference",
                "consumer expectations survey",
                "lagarde",
            )
        ):
            return 4
        return 3
    if provider == "fed":
        if lower_cats & _FED_LOW_PRIORITY_CATEGORIES:
            return 1
        if "enforcement action" in lower_title or "approval of application" in lower_title:
            return 1
        if stream == "news/monetary" or "fomc" in lower_title:
            return 4
        if stream == "news/speeches":
            return 3
        return 2
    if provider == "us_bea":
        if any(k in lower_title for k in ("gross domestic product", "personal income and outlays")):
            return 4
        if "international trade" in lower_title:
            return 3
        return 2
    return 2


def parse_rss_capture(
    payload: bytes,
    *,
    provider: str,
    stream: str,
    received_at: datetime,
    expected_hosts: frozenset[str] | None = None,
    article_bodies: dict[str, str] | None = None,
    limit: int = 40,
) -> tuple[tuple[SourceRecord, ...], tuple[RejectedRecord, ...], str | None]:
    """Parse RSS 2.0 / RDF 1.0 XML into validated SourceRecords without failing the whole batch on one bad item."""
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("Receipt requires a timezone")
    received_utc = received_at.astimezone(UTC)
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        return (), (), f"invalid_xml:{exc}"

    items = root.findall(".//item")
    if not items:
        items = [el for el in root.iter() if el.tag.split("}")[-1] in {"item", "entry"}]
    if not items and root.tag.split("}")[-1] not in {"rss", "RDF", "feed"}:
        return (), (), f"unexpected_xml_root:{root.tag}"

    records: list[SourceRecord] = []
    rejected: list[RejectedRecord] = []
    seen_ids: set[str] = set()
    bodies = article_bodies or {}

    for idx, item in enumerate(items[:limit]):
        try:
            title = html_to_plain_text(_child_text(item, ("title",)))
            if not title:
                raise ValueError("missing_title")
            raw_link = _child_text(item, ("link",))
            if not raw_link:
                for ch in item:
                    if ch.tag.split("}")[-1] == "link" and ch.attrib.get("href"):
                        raw_link = str(ch.attrib["href"])
                        break
            url = _normalize_url(raw_link, expected_hosts=expected_hosts)
            raw_guid = _child_text(item, ("guid", "id")) or url
            source_id = _stable_source_id(provider, raw_guid)
            if source_id in seen_ids:
                continue
            seen_ids.add(source_id)

            raw_pub = _child_text(item, ("pubDate", "date", "published", "updated"))
            displayed: datetime | None = None
            if raw_pub:
                parsed_pub = _parse_feed_datetime(raw_pub)
                if parsed_pub <= received_utc:
                    displayed = parsed_pub
                elif parsed_pub <= received_utc + timedelta(seconds=120):
                    displayed = received_utc
                else:
                    raise ValueError("publisher_display_after_receipt")

            categories = _child_texts(item, "category")
            raw_desc = _child_text(item, ("encoded", "description", "summary", "content"))
            desc_plain = html_to_plain_text(raw_desc)
            enriched = bodies.get(source_id, "").strip()

            header_tags = f"[{provider.upper()}]"
            if categories:
                header_tags += f" [Categories: {', '.join(categories[:5])}]"
            body_sections = [f"{header_tags} {title}"]
            if enriched:
                body_sections.append(enriched)
            elif desc_plain and desc_plain.lower() != title.lower():
                body_sections.append(desc_plain)
            body_sections.append(f"Source URL: {url}")
            full_text = "\n".join(body_sections).strip()

            importance = _score_rss_item(provider, stream, title, full_text, categories)
            records.append(
                SourceRecord(
                    source_id=source_id,
                    kind="news",
                    source_url=url,
                    title=title,
                    text=full_text,
                    publisher_display_at=displayed,
                    importance=importance,
                )
            )
        except (ValueError, ValidationError) as exc:
            rejected.append(RejectedRecord(index=idx, reason=str(exc)))
    return tuple(records), tuple(rejected), None


def parse_forexfactory_calendar(
    payload: bytes,
    *,
    received_at: datetime,
    day: date | None = None,
) -> tuple[tuple[SourceRecord, ...], tuple[RejectedRecord, ...], str | None]:
    """Parse ForexFactory weekly JSON schedule with explicit MISSING consensus markers."""
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("Receipt requires a timezone")
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return (), (), f"invalid_json:{exc}"
    if not isinstance(decoded, list):
        return (), (), "expected_json_array"

    records: list[SourceRecord] = []
    rejected: list[RejectedRecord] = []
    seen_ids: dict[str, int] = {}

    for idx, item in enumerate(decoded):
        try:
            if not isinstance(item, dict):
                raise ValueError("expected_item_object")
            title = _clean_opt_str(item.get("title"))
            country = _clean_opt_str(item.get("country"))
            raw_date = _clean_opt_str(item.get("date"))
            if not title or not country or not raw_date:
                raise ValueError("missing_required_calendar_fields")
            scheduled = _parse_feed_datetime(raw_date)
            if day is not None and scheduled.astimezone(BJT).date() != day:
                continue

            impact_raw = (_clean_opt_str(item.get("impact")) or "Low").strip()
            importance = _IMPACT_SCORES.get(impact_raw.lower(), 1)
            actual = _clean_opt_str(item.get("actual"))
            forecast = _clean_opt_str(item.get("forecast"))
            previous = _clean_opt_str(item.get("previous"))

            slug = re.sub(r"[^a-z0-9]+", "-", f"{country.lower()}-{title.lower()}").strip("-")[:56]
            base_id = f"ff:{slug}@{int(scheduled.timestamp())}"
            count = seen_ids.get(base_id, 0) + 1
            seen_ids[base_id] = count
            source_id = base_id if count == 1 else f"{base_id}#{count}"

            macro = MacroValues(
                actual=actual,
                reported_forecast=forecast,
                previous=previous,
                country=country.upper(),
            )
            bjt_str = scheduled.astimezone(BJT).strftime("%Y-%m-%d %H:%M BJT")
            utc_str = scheduled.strftime("%Y-%m-%d %H:%M UTC")
            text = (
                f"[ForexFactory Macro Schedule] {country.upper()} — {title}\n"
                f"Scheduled: {bjt_str} ({utc_str}) | Impact: {impact_raw}\n"
                f"Actual: {actual if actual is not None else 'UNRELEASED'} | "
                f"Consensus Forecast: {forecast if forecast is not None else 'MISSING'} | "
                f"Previous: {previous if previous is not None else 'MISSING'}\n"
                + json.dumps(macro.model_dump(), ensure_ascii=False, sort_keys=True)
            )
            records.append(
                SourceRecord(
                    source_id=source_id,
                    kind="calendar",
                    source_url="https://www.forexfactory.com/calendar",
                    title=f"{country.upper()} {title}",
                    text=text,
                    scheduled_at=scheduled,
                    importance=importance,
                    macro=macro,
                )
            )
        except (ValueError, ValidationError) as exc:
            rejected.append(RejectedRecord(index=idx, reason=str(exc)))
    return tuple(records), tuple(rejected), None


_EXPECTED_HOSTS_BY_PROVIDER: dict[str, frozenset[str]] = {
    "investinglive": frozenset({"investinglive.com", "www.investinglive.com"}),
    "ecb": frozenset({"www.ecb.europa.eu", "ecb.europa.eu"}),
    "fed": frozenset({"www.federalreserve.gov", "federalreserve.gov"}),
    "us_bea": frozenset({"www.bea.gov", "bea.gov", "apps.bea.gov"}),
}


class Tier1EnglishSource:
    """Multi-stream Tier-1 English collector for investingLive, ECB, Fed, US BEA, and ForexFactory."""

    provider = "tier1_english"

    def __init__(
        self,
        *,
        streams: Sequence[Tier1StreamSpec] = DEFAULT_TIER1_STREAMS,
        timeout_seconds: float = 10.0,
        max_attempts: int = 2,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.streams = tuple(streams)
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.transport = transport
        self.clock = clock
        self._last_polled_mono: dict[tuple[str, str], float] = {}

    async def _get_bytes(
        self, url: str
    ) -> tuple[bytes, dict[str, str], datetime, datetime, float, int]:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/xml, application/json, text/xml, text/html;q=0.8, */*;q=0.5",
        }
        async with httpx.AsyncClient(
            transport=self.transport,
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers=headers,
        ) as client:
            for attempt in range(1, self.max_attempts + 1):
                started_at = self.clock().astimezone(UTC)
                t0 = time.monotonic()
                try:
                    async with client.stream("GET", url) as resp:
                        status = resp.status_code
                        resp_headers = {
                            k.lower(): v
                            for k, v in resp.headers.items()
                            if k.lower() in {"content-type", "etag", "last-modified", "date", "age"}
                        }
                        if status == 429 or 500 <= status <= 599:
                            raise CaptureError(f"transient_http_{status}")
                        if status != 200:
                            raise CaptureError(f"unexpected_http_{status}")
                        chunks: list[bytes] = []
                        total = 0
                        async for chunk in resp.aiter_bytes():
                            total += len(chunk)
                            if total > MAX_BYTES:
                                raise CaptureError("response_too_large")
                            chunks.append(chunk)
                        elapsed_ms = max(0.0, (time.monotonic() - t0) * 1000.0)
                        received_at = self.clock().astimezone(UTC)
                        if received_at < started_at:
                            received_at = started_at
                        return (
                            b"".join(chunks),
                            resp_headers,
                            started_at,
                            received_at,
                            elapsed_ms,
                            attempt,
                        )
                except CaptureError as exc:
                    if not str(exc).startswith("transient_http_") or attempt >= self.max_attempts:
                        raise
                    await asyncio.sleep(1.0)
                except httpx.HTTPError as exc:
                    if attempt >= self.max_attempts:
                        raise CaptureError(f"http_error:{type(exc).__name__}") from exc
                    await asyncio.sleep(1.0)
        raise CaptureError("unreachable_retry_exhausted")

    async def _enrich_articles(
        self,
        spec: Tier1StreamSpec,
        records: tuple[SourceRecord, ...],
        known_ids: set[str],
    ) -> dict[str, str]:
        """Fetch full official press-release body ONLY for brand-new live ECB/Fed items."""
        bodies: dict[str, str] = {}
        new_candidates = [
            rec
            for rec in records
            if rec.source_id not in known_ids and rec.source_url.lower().endswith((".html", ".htm"))
        ][:3]
        for rec in new_candidates:
            try:
                raw_html_bytes, *_ = await self._get_bytes(rec.source_url)
                html_doc = raw_html_bytes.decode("utf-8", errors="replace")
                extracted = (
                    extract_ecb_article_text(html_doc)
                    if spec.provider == "ecb"
                    else extract_fed_article_text(html_doc)
                )
                if extracted:
                    bodies[rec.source_id] = extracted
            except Exception as exc:
                logger.debug("article_enrichment_skipped url=%s err=%s", rec.source_url, exc)
        return bodies

    async def fetch_stream(
        self,
        spec: Tier1StreamSpec,
        *,
        day: date | None = None,
        limit: int = 40,
        has_baseline: bool = False,
        known_ids: set[str] | None = None,
    ) -> CaptureBatch:
        (
            payload,
            resp_headers,
            started_at,
            received_at,
            elapsed_ms,
            attempts,
        ) = await self._get_bytes(spec.url)
        if spec.kind == "calendar":
            records, rejected, error = parse_forexfactory_calendar(
                payload, received_at=received_at, day=day
            )
        else:
            expected_hosts = _EXPECTED_HOSTS_BY_PROVIDER.get(spec.provider)
            records, rejected, error = parse_rss_capture(
                payload,
                provider=spec.provider,
                stream=spec.stream,
                received_at=received_at,
                expected_hosts=expected_hosts,
                limit=limit,
            )
            if (
                error is None
                and spec.enrich_live_articles
                and has_baseline
                and known_ids is not None
                and records
            ):
                bodies = await self._enrich_articles(spec, records, known_ids)
                if bodies:
                    records, rejected, error = parse_rss_capture(
                        payload,
                        provider=spec.provider,
                        stream=spec.stream,
                        received_at=self.clock().astimezone(UTC),
                        expected_hosts=expected_hosts,
                        article_bodies=bodies,
                        limit=limit,
                    )
                    received_at = self.clock().astimezone(UTC)

        return CaptureBatch(
            provider=spec.provider,
            stream=spec.stream,
            kind=spec.kind,
            basis="live_poll",
            request_url=spec.url,
            request_started_at=started_at,
            received_at=received_at,
            elapsed_ms=elapsed_ms,
            attempts=attempts,
            payload=payload,
            response_headers=resp_headers,
            records=records,
            rejected=rejected,
            parse_error=error,
        )

    async def fetch(
        self, kind: Kind, *, channel: str = "global", limit: int = 30, day: date | None = None
    ) -> CaptureBatch:
        """CaptureSource-compatible single-batch entrypoint (investinglive for news, forexfactory for calendar)."""
        if kind == "calendar":
            spec = next(
                (s for s in self.streams if s.kind == "calendar"),
                DEFAULT_TIER1_STREAMS[-1],
            )
            return await self.fetch_stream(spec, day=day, limit=limit)
        spec = next(
            (s for s in self.streams if s.provider == "investinglive"),
            self.streams[0],
        )
        return await self.fetch_stream(spec, limit=limit)

    async def poll_due_streams(
        self,
        store: ObserverStore,
        *,
        kind: Kind | None = None,
        limit: int = 35,
        day: date | None = None,
        force: bool = False,
    ) -> list[dict[str, Any]]:
        """Poll all due Tier-1 streams concurrently and record durably into ObserverStore."""
        now_mono = time.monotonic()
        due_specs: list[tuple[Tier1StreamSpec, bool, set[str]]] = []
        for spec in self.streams:
            if kind is not None and spec.kind != kind:
                continue
            key = (spec.provider, spec.stream)
            last = self._last_polled_mono.get(key)
            if not force and last is not None and (now_mono - last) < spec.min_poll_seconds:
                continue
            has_base = await asyncio.to_thread(
                store.has_stream_baseline, provider=spec.provider, stream=spec.stream
            )
            known = (
                await asyncio.to_thread(
                    store.known_source_ids, provider=spec.provider, kind=spec.kind
                )
                if (spec.enrich_live_articles and has_base)
                else set()
            )
            due_specs.append((spec, has_base, known))

        if not due_specs:
            return []

        async def _fetch_one(
            spec: Tier1StreamSpec, has_base: bool, known: set[str]
        ) -> tuple[Tier1StreamSpec, CaptureBatch | None, Exception | None]:
            try:
                batch = await self.fetch_stream(
                    spec,
                    day=day,
                    limit=limit,
                    has_baseline=has_base,
                    known_ids=known,
                )
                return spec, batch, None
            except Exception as exc:
                return spec, None, exc

        fetched = await asyncio.gather(*(_fetch_one(s, hb, kn) for s, hb, kn in due_specs))
        results: list[dict[str, Any]] = []
        completed_mono = time.monotonic()
        for spec, batch, err in fetched:
            self._last_polled_mono[(spec.provider, spec.stream)] = completed_mono
            if err is not None or batch is None:
                err_msg = str(err) if err is not None else "unknown_fetch_failure"
                await asyncio.to_thread(store.failure, spec.provider, spec.stream, err_msg)
                results.append(
                    {
                        "provider": spec.provider,
                        "stream": spec.stream,
                        "status": "fetch_failed",
                        "error": err_msg,
                    }
                )
            else:
                res = await asyncio.to_thread(store.record, batch)
                results.append(res)
        return results


class EURUSDEventFilter(WallStreetCNFilter):
    """Unified deterministic EUR/USD event filter supporting Tier-1 English sources and legacy revisions."""

    filter_version: str = TIER1_FILTER_VERSION

    def __init__(
        self,
        *,
        min_news_importance: int = 2,
        min_calendar_importance: int = 2,
        english_calendar_countries: frozenset[str] = frozenset({"USD", "EUR"}),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        super().__init__(
            min_news_importance=min_news_importance,
            min_calendar_importance=min_calendar_importance,
            clock=clock,
        )
        self.english_calendar_countries = frozenset(c.upper() for c in english_calendar_countries)

    def evaluate_revision(
        self, revision: StoredRevision, *, checked_at: datetime | None = None
    ) -> tuple[FilterResult, Event | None]:
        if revision.provider == "wallstreetcn":
            return super().evaluate_revision(revision, checked_at=checked_at)

        now = checked_at or self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Filter check clock requires a timezone")
        now = now.astimezone(UTC)
        event_id = revision_event_id(revision)

        if revision.available_at > now:
            return (
                FilterResult(
                    event_id=event_id,
                    outcome="quarantine",
                    reason="future_revision_availability",
                    checked_at=now,
                    filter_version=TIER1_FILTER_VERSION,
                ),
                None,
            )
        if event_id in self._seen:
            return (
                FilterResult(
                    event_id=event_id,
                    outcome="ignore",
                    reason="duplicate_revision",
                    checked_at=now,
                    filter_version=TIER1_FILTER_VERSION,
                ),
                None,
            )

        record = revision.record
        if record.kind == "calendar":
            if (
                record.macro is None
                or record.macro.actual is None
                or not record.macro.actual.strip()
            ):
                return (
                    FilterResult(
                        event_id=event_id,
                        outcome="ignore",
                        reason="unreleased_calendar_schedule",
                        checked_at=now,
                        filter_version=TIER1_FILTER_VERSION,
                    ),
                    None,
                )
            country = (record.macro.country or "").strip().upper()
            if country not in self.english_calendar_countries:
                return (
                    FilterResult(
                        event_id=event_id,
                        outcome="ignore",
                        reason="unrelated_calendar_country",
                        checked_at=now,
                        filter_version=TIER1_FILTER_VERSION,
                    ),
                    None,
                )
            if (record.importance or 0) < self.min_calendar_importance:
                return (
                    FilterResult(
                        event_id=event_id,
                        outcome="ignore",
                        reason="below_calendar_importance_threshold",
                        checked_at=now,
                        filter_version=TIER1_FILTER_VERSION,
                    ),
                    None,
                )
        else:
            imp = record.importance if record.importance is not None else 0
            if imp == 0:
                return (
                    FilterResult(
                        event_id=event_id,
                        outcome="ignore",
                        reason="technical_or_non_macro_category",
                        checked_at=now,
                        filter_version=TIER1_FILTER_VERSION,
                    ),
                    None,
                )
            if imp < self.min_news_importance:
                return (
                    FilterResult(
                        event_id=event_id,
                        outcome="ignore",
                        reason="below_news_importance_threshold",
                        checked_at=now,
                        filter_version=TIER1_FILTER_VERSION,
                    ),
                    None,
                )
            if revision.provider not in {"ecb", "fed", "us_bea"}:
                combined = f"{record.title}\n{record.text}"
                if not _EURUSD_EN_PATTERN.search(combined):
                    return (
                        FilterResult(
                            event_id=event_id,
                            outcome="ignore",
                            reason="no_eurusd_macro_relevance",
                            checked_at=now,
                            filter_version=TIER1_FILTER_VERSION,
                        ),
                        None,
                    )

        try:
            event = revision_to_event(revision)
            if revision.provider in {"ecb", "fed", "us_bea"}:
                event = event.model_copy(update={"source_type": "macro"})
        except ValidationError:
            return (
                FilterResult(
                    event_id=event_id,
                    outcome="quarantine",
                    reason="receipt_precedes_source_time",
                    checked_at=now,
                    filter_version=TIER1_FILTER_VERSION,
                ),
                None,
            )

        self._seen.add(event_id)
        return (
            FilterResult(
                event_id=event_id,
                outcome="accept",
                reason=f"eligible_{revision.provider}_{record.kind}",
                checked_at=now,
                filter_version=TIER1_FILTER_VERSION,
            ),
            event,
        )

    async def evaluate(self, event: Event) -> FilterResult:
        if event.source.startswith("wallstreetcn") and not event.event_id.startswith("bundle:"):
            return await super().evaluate(event)
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Filter check clock requires a timezone")
        now = now.astimezone(UTC)
        if event.received_timestamp > now:
            return FilterResult(
                event_id=event.event_id,
                outcome="quarantine",
                reason="future_event_receipt",
                checked_at=now,
                filter_version=TIER1_FILTER_VERSION,
            )
        if event.event_id in self._seen:
            return FilterResult(
                event_id=event.event_id,
                outcome="ignore",
                reason="duplicate_event",
                checked_at=now,
                filter_version=TIER1_FILTER_VERSION,
            )
        is_authoritative = any(
            event.source.startswith(prefix) for prefix in ("ecb_", "fed_", "us_bea_")
        )
        if (
            event.source_type == "news"
            and not is_authoritative
            and not event.event_id.startswith("bundle:")
            and not _EURUSD_EN_PATTERN.search(event.raw_text)
            and not any(k in event.raw_text.lower() for k in self.news_keywords)
        ):
            return FilterResult(
                event_id=event.event_id,
                outcome="ignore",
                reason="no_eurusd_macro_relevance",
                checked_at=now,
                filter_version=TIER1_FILTER_VERSION,
            )
        self._seen.add(event.event_id)
        return FilterResult(
            event_id=event.event_id,
            outcome="accept",
            reason="eligible_event",
            checked_at=now,
            filter_version=TIER1_FILTER_VERSION,
        )


@dataclass(frozen=True)
class EventBundle:
    """Either a singleton event or a composite bundle of simultaneous macro/news releases."""

    event: Event
    constituents: tuple[tuple[StoredRevision, Event], ...]

    @property
    def is_bundled(self) -> bool:
        return len(self.constituents) > 1


def _format_macro_clarity_line(rev: StoredRevision) -> str | None:
    macro = rev.record.macro
    if macro is None:
        return None
    unit = macro.unit or ""

    def _fmt(val: str | None, empty_label: str) -> str:
        if val is None or not val.strip():
            return empty_label
        return f"{val}{unit}" if unit and not val.endswith(unit) else val

    actual_s = _fmt(macro.actual, "UNRELEASED")
    fcst_s = _fmt(macro.reported_forecast, "MISSING (no consensus forecast reported)")
    prev_s = _fmt(macro.previous, "MISSING")
    rev_s = (
        f" (Revised Prior: {_fmt(macro.revised_previous, 'MISSING')})"
        if macro.revised_previous is not None
        else ""
    )
    return (
        f"Structured Values -> Actual: {actual_s} | "
        f"Consensus Forecast: {fcst_s} | Previous: {prev_s}{rev_s}"
    )


def bundle_simultaneous_events(
    accepted: Sequence[tuple[StoredRevision, Event]],
    *,
    news_bundle_window_seconds: float = 60.0,
) -> tuple[EventBundle, ...]:
    """Bundle simultaneous calendar indicators or same-provider burst headlines into single LLM calls."""
    if not accepted:
        return ()
    if news_bundle_window_seconds < 0:
        raise ValueError("news_bundle_window_seconds must be nonnegative")

    groups: list[list[tuple[StoredRevision, Event]]] = []
    calendar_index: dict[tuple[str, str, str], int] = {}
    news_index: dict[tuple[str, int], int] = {}

    for rev, ev in accepted:
        if rev.record.kind == "calendar" and rev.record.scheduled_at is not None:
            country = (rev.record.macro.country if rev.record.macro else "") or ""
            cal_key = (
                rev.provider,
                country.strip().upper(),
                rev.record.scheduled_at.astimezone(UTC).isoformat(),
            )
            if cal_key in calendar_index:
                groups[calendar_index[cal_key]].append((rev, ev))
            else:
                calendar_index[cal_key] = len(groups)
                groups.append([(rev, ev)])
        elif news_bundle_window_seconds > 0:
            bucket = int(ev.source_timestamp.timestamp() // max(1.0, news_bundle_window_seconds))
            n_key = (rev.provider, bucket)
            if n_key in news_index:
                groups[news_index[n_key]].append((rev, ev))
            else:
                news_index[n_key] = len(groups)
                groups.append([(rev, ev)])
        else:
            groups.append([(rev, ev)])

    bundles: list[EventBundle] = []
    for group in groups:
        if len(group) == 1:
            rev0, ev0 = group[0]
            clarity = _format_macro_clarity_line(rev0)
            if clarity and "Consensus Forecast:" not in ev0.raw_text:
                ev0 = ev0.model_copy(update={"raw_text": f"{ev0.raw_text}\n{clarity}"})
            bundles.append(EventBundle(event=ev0, constituents=tuple(group)))
            continue

        ordered = sorted(group, key=lambda pair: (pair[1].source_timestamp, pair[1].event_id))
        provider = ordered[0][0].provider
        sorted_ids = sorted(pair[1].event_id for pair in ordered)
        digest = hashlib.sha256("|".join(sorted_ids).encode("utf-8")).hexdigest()[:16]
        bundle_id = f"bundle:{provider}:{digest}"
        min_source_ts = min(pair[1].source_timestamp for pair in ordered)
        max_received_ts = max(pair[1].received_timestamp for pair in ordered)
        source_type = (
            "macro"
            if any(pair[1].source_type == "macro" for pair in ordered)
            else ordered[0][1].source_type
        )

        per_item_budget = max(400, (MAX_BUNDLE_RAW_CHARS - 300) // len(ordered))
        sections: list[str] = [
            f"[Simultaneous Release Bundle: {len(ordered)} items from {provider} at {min_source_ts.isoformat()}]"
        ]
        for idx, (rev_item, ev_item) in enumerate(ordered, start=1):
            snippet = ev_item.raw_text[:per_item_budget].strip()
            clarity = _format_macro_clarity_line(rev_item)
            block = f"--- [Item {idx}/{len(ordered)} | {ev_item.event_id}] ---\n{snippet}"
            if clarity and "Consensus Forecast:" not in snippet:
                block += f"\n{clarity}"
            sections.append(block)

        composite = Event(
            event_id=bundle_id,
            source=ordered[0][1].source,
            source_type=source_type,
            source_reference=ordered[0][1].source_reference,
            source_timestamp=min_source_ts,
            received_timestamp=max_received_ts,
            raw_text="\n\n".join(sections)[:MAX_BUNDLE_RAW_CHARS],
        )
        bundles.append(EventBundle(event=composite, constituents=tuple(ordered)))

    return tuple(bundles)


async def build_macro_schedule(
    *,
    source: Tier1EnglishSource | None = None,
    observer_store: ObserverStore | None = None,
    currencies: Sequence[str] = ("USD", "EUR"),
    min_impact: str = "medium",
    target_date_bjt: date | None = None,
    hours_ahead: float = 36.0,
    now: datetime | None = None,
    offline_only: bool = False,
) -> dict[str, Any]:
    """Build a read-only, non-directional key macro window schedule in BJT and UTC."""
    ref_now = (now or datetime.now(UTC)).astimezone(UTC)
    allowed_ccy = {c.strip().upper() for c in currencies if c.strip()}
    allow_all_ccy = "ALL" in allowed_ccy or "*" in allowed_ccy
    min_score = _IMPACT_SCORES.get(min_impact.lower(), 2)

    records: list[SourceRecord] = []
    if offline_only:
        if observer_store is None:
            raise ValueError("Offline schedule requires an observer database")
        revs = observer_store.revisions(kind="calendar", provider="forexfactory", limit=1000)
        latest_by_id: dict[str, SourceRecord] = {}
        for rev in revs:
            latest_by_id[rev.record.source_id] = rev.record
        records = list(latest_by_id.values())
    else:
        active_source = source or Tier1EnglishSource(clock=lambda: ref_now)
        batch = await active_source.fetch("calendar", day=target_date_bjt)
        records = list(batch.records)

    # Optional cross-fill of actuals from ObserverStore if already captured
    released_actuals: dict[tuple[str, int, str], str] = {}
    if observer_store is not None:
        for rev in observer_store.revisions(kind="calendar", limit=1000):
            rec = rev.record
            if rec.scheduled_at and rec.macro and rec.macro.actual:
                ccy = (rec.macro.country or "").upper()
                released_actuals[(ccy, int(rec.scheduled_at.timestamp()), rec.title.lower())] = (
                    rec.macro.actual
                )

    window_start = ref_now - timedelta(hours=2)
    window_end = ref_now + timedelta(hours=hours_ahead)
    filtered: list[SourceRecord] = []
    for rec in records:
        if rec.scheduled_at is None or rec.macro is None:
            continue
        sched_utc = rec.scheduled_at.astimezone(UTC)
        if target_date_bjt is not None:
            if sched_utc.astimezone(BJT).date() != target_date_bjt:
                continue
        elif not (window_start <= sched_utc <= window_end):
            continue
        ccy = (rec.macro.country or "").upper()
        if not allow_all_ccy and ccy not in allowed_ccy:
            continue
        if (rec.importance or 0) < min_score:
            continue
        filtered.append(rec)

    filtered.sort(key=lambda r: (r.scheduled_at or ref_now, -(r.importance or 0), r.title))

    # Group simultaneous timestamps into Macro Windows
    windows_map: dict[str, list[dict[str, Any]]] = {}
    window_order: list[str] = []
    impact_labels = {4: "CRITICAL", 3: "HIGH", 2: "MEDIUM", 1: "LOW", 0: "HOLIDAY"}

    for rec in filtered:
        assert rec.scheduled_at is not None and rec.macro is not None
        sched_utc = rec.scheduled_at.astimezone(UTC)
        sched_bjt = sched_utc.astimezone(BJT)
        key = sched_utc.isoformat()
        if key not in windows_map:
            windows_map[key] = []
            window_order.append(key)
        ccy = (rec.macro.country or "").upper()
        actual_val = rec.macro.actual or released_actuals.get(
            (ccy, int(sched_utc.timestamp()), rec.title.lower())
        )
        clean_title = rec.title[len(ccy) + 1 :] if rec.title.startswith(f"{ccy} ") else rec.title
        windows_map[key].append(
            {
                "source_id": rec.source_id,
                "currency": ccy,
                "event": clean_title,
                "impact": impact_labels.get(rec.importance or 0, "LOW"),
                "importance": rec.importance or 0,
                "scheduled_bjt": sched_bjt.strftime("%Y-%m-%d %H:%M"),
                "scheduled_utc": sched_utc.strftime("%Y-%m-%d %H:%M"),
                "actual": actual_val if actual_val is not None else "UNRELEASED",
                "consensus_forecast": (
                    rec.macro.reported_forecast
                    if rec.macro.reported_forecast is not None
                    else "MISSING"
                ),
                "previous": rec.macro.previous if rec.macro.previous is not None else "MISSING",
            }
        )

    windows: list[dict[str, Any]] = []
    for key in window_order:
        items = windows_map[key]
        max_imp = max(int(i["importance"]) for i in items)
        windows.append(
            {
                "scheduled_bjt": items[0]["scheduled_bjt"],
                "scheduled_utc": items[0]["scheduled_utc"],
                "window_impact": impact_labels.get(max_imp, "LOW"),
                "bundled_count": len(items),
                "currencies": sorted({str(i["currency"]) for i in items}),
                "indicators": items,
            }
        )

    return {
        "scope": "macro_schedule_only",
        "execution_authorized": False,
        "directional_forecast_included": False,
        "generated_at_bjt": ref_now.astimezone(BJT).strftime("%Y-%m-%d %H:%M:%S BJT"),
        "generated_at_utc": ref_now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "currencies": sorted(allowed_ccy),
        "min_impact": min_impact.upper(),
        "target_date_bjt": target_date_bjt.isoformat() if target_date_bjt else None,
        "hours_ahead": None if target_date_bjt else hours_ahead,
        "window_count": len(windows),
        "indicator_count": len(filtered),
        "windows": windows,
    }


def format_macro_schedule_table(schedule: dict[str, Any]) -> str:
    """Format build_macro_schedule output as a readable terminal table."""
    header = (
        f"EventLens Macro Schedule (Generated: {schedule['generated_at_bjt']} / {schedule['generated_at_utc']})\n"
        f"Filter: currencies={','.join(schedule['currencies'])} | min_impact={schedule['min_impact']} | "
        f"windows={schedule['window_count']} | indicators={schedule['indicator_count']}\n"
        f"Note: Consensus Forecast is explicitly marked MISSING when absent (never substituted with Previous)."
    )
    windows = schedule.get("windows", [])
    if not windows:
        return f"{header}\n\nNo matching macro windows in the requested range."

    lines = [header, ""]
    lines.append(
        f"{'Beijing Time (UTC+8)':<20} | {'UTC':<16} | {'CCY':<3} | {'Impact':<6} | "
        f"{'Indicator / Event':<36} | {'Actual':<10} | {'Consensus':<10} | {'Previous':<10}"
    )
    lines.append("-" * 127)
    for win in windows:
        count = int(win["bundled_count"])
        if count > 1:
            lines.append(
                f">> [{win['scheduled_bjt']} BJT] BUNDLED {win['window_impact']} WINDOW ({count} simultaneous releases)"
            )
        for item in win["indicators"]:
            ev_name = str(item["event"])
            if len(ev_name) > 36:
                ev_name = ev_name[:33] + "..."
            lines.append(
                f"{item['scheduled_bjt']:<20} | {item['scheduled_utc']:<16} | {item['currency']:<3} | "
                f"{item['impact']:<6} | {ev_name:<36} | {str(item['actual']):<10} | "
                f"{str(item['consensus_forecast']):<10} | {str(item['previous']):<10}"
            )
    return "\n".join(lines)
