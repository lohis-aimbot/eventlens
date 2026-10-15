"""Bounded read-only polling of WallStreetCN website endpoints, not a public API SLA."""

import asyncio
import json
import logging
import math
import time
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any, Callable
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from pydantic import ValidationError

from ..observer import CaptureBatch, Kind, MacroValues, RejectedRecord, SourceRecord

logger = logging.getLogger(__name__)
NEWS_URL = "https://api-one.wallstcn.com/apiv1/content/lives"
CALENDAR_URL = "https://api-one-wscn.awtmt.com/apiv1/finance/macrodatas"
MAX_BYTES = 1_000_000
SHANGHAI = ZoneInfo("Asia/Shanghai")


class CaptureError(RuntimeError):
    """Transport/status/size failure; never synthesize an event or retry forever."""


class _Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style"}:
            self.hidden += 1
        if tag in {"p", "br", "div"} and not self.hidden:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data)


def _plain(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected source text")
    parser = _Text()
    parser.feed(value)
    return "\n".join(line.strip() for line in "".join(parser.parts).splitlines() if line.strip())


def _value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("Expected a source scalar")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Nonfinite source value")
    return str(value).strip() or None


def _epoch(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("Invalid source timestamp")
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("Invalid source timestamp")
    return datetime.fromtimestamp(seconds, UTC)


def _id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("Invalid source ID")
    source_id = str(value)
    if not source_id.isascii() or not source_id.isdigit() or int(source_id) <= 0:
        raise ValueError("Invalid source ID")
    return source_id


def _importance(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Invalid importance")
    return int(value)


def _record(item: Any, kind: Kind, received: datetime, day: date | None) -> SourceRecord:
    if not isinstance(item, dict):
        raise ValueError("Expected an item object")
    source_id = _id(item.get("id"))
    title = item.get("title", "")
    if not isinstance(title, str):
        raise ValueError("Invalid title")
    if kind == "news":
        displayed = _epoch(item.get("display_time"))
        if displayed is not None and displayed > received:
            raise ValueError("Publisher display time is after receipt")
        body = item.get("content_text") or _plain(item.get("content") or "") or title
        if not isinstance(body, str):
            raise ValueError("Invalid news text")
        more = _plain(item.get("content_more") or "")
        if more and more not in body:
            body += "\n" + more
        canonical = f"https://wallstreetcn.com/livenews/{source_id}"
        url = item.get("uri") or canonical
        if (
            not isinstance(url, str)
            or url != canonical
            or urlsplit(url).netloc != "wallstreetcn.com"
        ):
            raise ValueError("Unexpected news source URL")
        return SourceRecord(
            source_id=source_id,
            kind=kind,
            source_url=url,
            title=title,
            text=body.strip(),
            publisher_display_at=displayed,
            importance=_importance(item.get("score")),
        )
    scheduled = _epoch(item.get("public_date"))
    if scheduled is None or day is None or scheduled.astimezone(SHANGHAI).date() != day:
        raise ValueError("Calendar schedule is missing or outside the requested Shanghai date")
    macro = MacroValues(
        actual=_value(item.get("actual")),
        reported_forecast=_value(item.get("forecast")),
        previous=_value(item.get("previous")),
        revised_previous=_value(item.get("revised")),
        unit=_value(item.get("unit")),
        country=_value(item.get("country")),
        period=_value(item.get("period")),
    )
    text = f"{macro.country or ''} {title}\nScheduled: {scheduled.isoformat()}\n"
    text += json.dumps(macro.model_dump(), ensure_ascii=False, sort_keys=True)
    foresight = _plain(item.get("foresight") or "")
    if foresight:
        text += "\n" + foresight
    return SourceRecord(
        # Indicator IDs may recur: one source item represents one scheduled occurrence.
        source_id=f"{source_id}@{int(scheduled.timestamp())}",
        kind=kind,
        source_url="https://wallstreetcn.com/calendar",
        title=title,
        text=text,
        scheduled_at=scheduled,
        macro=macro,
        importance=_importance(item.get("importance")),
    )


def parse_capture(
    payload: bytes,
    *,
    kind: Kind,
    received_at: datetime,
    day: date | None = None,
) -> tuple[tuple[SourceRecord, ...], tuple[RejectedRecord, ...], str | None]:
    """Keep malformed raw payloads; quarantine individual bad rows without guessing."""
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("Receipt requires a timezone")
    try:

        def no_constant(value: str) -> None:
            raise ValueError("Nonfinite JSON constant")

        envelope = json.loads(payload, parse_constant=no_constant)
        if not isinstance(envelope, dict) or envelope.get("code") != 20000:
            raise ValueError("Unexpected envelope code")
        items = envelope["data"]["items"]
        if not isinstance(items, list) or len(items) > 5000:
            raise ValueError("Unexpected item collection")
    except (ValueError, TypeError, KeyError):
        return (), (), "invalid_json_or_envelope"
    records, rejected = [], []
    seen: set[str] = set()
    for index, item in enumerate(items):
        try:
            record = _record(item, kind, received_at, day)
            if record.source_id in seen:
                raise ValueError("Duplicate source ID in response")
            seen.add(record.source_id)
        except (ValueError, TypeError, OverflowError, OSError, ValidationError) as exc:
            rejected.append(RejectedRecord(index=index, reason=str(exc)[:160]))
        else:
            records.append(record)
    return tuple(records), tuple(rejected), None


class WallStreetCNSource:
    """Website endpoint adapter; no login, account, model or order capabilities."""

    provider = "wallstreetcn"

    def __init__(
        self,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        timeout_seconds: float = 10,
        max_retries: int = 1,
        min_interval_seconds: float = 1,
    ) -> None:
        if (
            not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
            or not 0 <= max_retries <= 3
            or not math.isfinite(min_interval_seconds)
            or min_interval_seconds < 0
        ):
            raise ValueError("Invalid source HTTP settings")
        self.transport, self.clock = transport, clock
        self.timeout, self.retries, self.min_interval = (
            timeout_seconds,
            max_retries,
            min_interval_seconds,
        )
        self._last_request = 0.0

    async def fetch(
        self,
        kind: Kind,
        *,
        channel: str = "global",
        limit: int = 30,
        day: date | None = None,
    ) -> CaptureBatch:
        if kind not in {"news", "calendar"} or channel not in {"global", "forex", "commodity"}:
            raise ValueError("Unsupported source kind or channel")
        if not 1 <= limit <= 100:
            raise ValueError("News limit must be between 1 and 100")
        started, tick = self.clock(), time.monotonic()
        if started.tzinfo is None or started.utcoffset() is None:
            raise ValueError("Request clock requires a timezone")
        params: dict[str, str | int]
        if kind == "news":
            url, params, stream = (
                NEWS_URL,
                {"channel": f"{channel}-channel", "limit": limit},
                f"news/{channel}",
            )
        else:
            day = day or started.astimezone(SHANGHAI).date()
            start = datetime.combine(day, datetime.min.time(), SHANGHAI)
            url = CALENDAR_URL
            params = {
                "start": int(start.timestamp()),
                "end": int((start + timedelta(days=1)).timestamp()) - 1,
            }
            stream = f"calendar/{day.isoformat()}"
        async with httpx.AsyncClient(
            transport=self.transport,
            timeout=self.timeout,
            follow_redirects=False,
            headers={
                "User-Agent": "EventLens observer research (https://github.com/lohis-aimbot/eventlens)"
            },
        ) as client:
            for attempt in range(self.retries + 1):
                await asyncio.sleep(
                    max(0, self.min_interval - (time.monotonic() - self._last_request))
                )
                self._last_request = time.monotonic()
                retry_after: str | None = None
                try:
                    async with client.stream("GET", url, params=params) as response:
                        if response.status_code in {408, 429, 500, 502, 503, 504}:
                            retry_after = response.headers.get("retry-after")
                            raise CaptureError(f"temporary_http_{response.status_code}")
                        if response.status_code != 200:
                            raise CaptureError(f"rejected_http_{response.status_code}")
                        chunks, size = [], 0
                        async for chunk in response.aiter_bytes():
                            size += len(chunk)
                            if size > MAX_BYTES:
                                raise CaptureError("response_size_limit")
                            chunks.append(chunk)
                        payload, received = b"".join(chunks), self.clock()
                        records, rejected, error = parse_capture(
                            payload, kind=kind, received_at=received, day=day
                        )
                        headers = {
                            key: response.headers[key]
                            for key in ("date", "age", "cache-control", "etag", "last-modified")
                            if key in response.headers
                        }
                        return CaptureBatch(
                            provider="wallstreetcn",
                            stream=stream,
                            kind=kind,
                            request_url=str(response.request.url),
                            request_started_at=started,
                            received_at=received,
                            elapsed_ms=(time.monotonic() - tick) * 1000,
                            attempts=attempt + 1,
                            payload=payload,
                            response_headers=headers,
                            records=records,
                            rejected=rejected,
                            parse_error=error,
                        )
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    reason = type(exc).__name__
                except CaptureError as exc:
                    reason = str(exc)
                    if not reason.startswith("temporary_http_"):
                        raise
                logger.warning(
                    "observer_fetch_retry stream=%s attempt=%s error=%s",
                    stream,
                    attempt + 1,
                    reason,
                )
                if attempt == self.retries:
                    raise CaptureError(f"request_failed:{reason}")
                delay = float(2**attempt)
                if retry_after:
                    try:
                        requested_delay = float(retry_after)
                    except ValueError:
                        try:
                            requested_delay = (
                                parsedate_to_datetime(retry_after) - datetime.now(UTC)
                            ).total_seconds()
                        except (ValueError, TypeError):
                            raise CaptureError("invalid_retry_after") from None
                    if not math.isfinite(requested_delay):
                        raise CaptureError("invalid_retry_after")
                    delay = max(delay, requested_delay)
                    if delay > 30:
                        raise CaptureError("retry_after_exceeds_budget")
                await asyncio.sleep(delay)
        raise AssertionError("Unreachable source retry loop")
