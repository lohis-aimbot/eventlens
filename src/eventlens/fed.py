"""Official Fed FOMC RSS intake; historical feed items are baselined before inference."""

import asyncio
import fcntl
import hashlib
import logging
import re
import sqlite3
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Literal
from urllib.parse import urlsplit
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

import httpx
from pydantic import ValidationError

from .contracts import Event
from .deepseek import JSONModelClient, ModelCallError
from .memory import MemoryConflict, ThesisMemory
from .reasoning import MemoryReasoner

logger = logging.getLogger(__name__)
FEED_URL = "https://www.federalreserve.gov/feeds/press_monetary.xml"
FOMC_TITLE = "Federal Reserve issues FOMC statement"
RELEASE_PATH = re.compile(r"/newsevents/pressreleases/monetary(\d{8})[a-z]\.htm\Z")
MAX_FEED_BYTES = 1_000_000
MAX_ARTICLE_BYTES = 1_000_000


class FeedError(RuntimeError):
    """A feed fetch or document parse failed without creating an event."""


class WatcherBusy(FeedError):
    """Another local watcher owns this database; starting a second risks paid duplicates."""


class SourceRejected(ValueError):
    """A feed item or article cannot be trusted as the expected Fed source."""


@dataclass(frozen=True)
class FeedItem:
    title: str
    link: str
    guid: str
    published: str

    @property
    def quarantine_id(self) -> str:
        return (
            "fed-rejected-"
            + hashlib.sha256(f"{self.link}\n{self.guid}\n{self.published}".encode()).hexdigest()[
                :24
            ]
        )


@dataclass(frozen=True)
class ValidatedItem:
    event_id: str
    url: str
    source_timestamp: datetime


def parse_feed(payload: bytes) -> tuple[FeedItem, ...]:
    if len(payload) > MAX_FEED_BYTES or b"<!DOCTYPE" in payload.upper():
        raise FeedError("Fed feed is too large or contains a document type declaration")
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        raise FeedError("Fed feed XML is malformed") from exc
    if root.tag != "rss" or root.find("channel") is None:
        raise FeedError("Unexpected Fed feed structure")
    return tuple(
        FeedItem(
            title=(node.findtext("title") or "").strip(),
            link=(node.findtext("link") or "").strip(),
            guid=(node.findtext("guid") or "").strip(),
            published=(node.findtext("pubDate") or "").strip(),
        )
        for node in root.findall("./channel/item")
        if (node.findtext("title") or "").strip() == FOMC_TITLE
    )


def _official_release_date(value: str) -> str | None:
    try:
        url = urlsplit(value)
    except ValueError:
        return None
    matched = RELEASE_PATH.fullmatch(url.path)
    if (
        url.scheme != "https"
        or url.netloc != "www.federalreserve.gov"
        or matched is None
        or url.query
        or url.fragment
    ):
        return None
    return matched.group(1)


def validate_item(item: FeedItem) -> ValidatedItem:
    release_date = _official_release_date(item.link)
    if release_date is None or item.guid != item.link:
        raise SourceRejected("Release URL or GUID is outside the official allowlist")
    try:
        source_time = parsedate_to_datetime(item.published)
    except (TypeError, ValueError) as exc:
        raise SourceRejected("Invalid release timestamp") from exc
    if source_time.tzinfo is None or source_time.utcoffset() is None:
        raise SourceRejected("Release timestamp has no timezone")
    local_date = source_time.astimezone(ZoneInfo("America/New_York")).strftime("%Y%m%d")
    if local_date != release_date:
        raise SourceRejected("Release URL date disagrees with feed timestamp")
    event_id = "fed-fomc-" + hashlib.sha256(item.link.encode()).hexdigest()[:24]
    return ValidatedItem(event_id, item.link, source_time.astimezone(UTC))


class _ArticleText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.article_depth = 0
        self.body_depth = 0
        self.in_paragraph = False
        self.parts: list[str] = []
        self.paragraphs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "div":
            attributes = dict(attrs)
            if self.article_depth:
                self.article_depth += 1
                if self.body_depth:
                    self.body_depth += 1
                elif attributes.get("class") == "col-xs-12 col-sm-8 col-md-8":
                    self.body_depth = 1
            elif attributes.get("id") == "article":
                self.article_depth = 1
        elif tag == "p" and self.body_depth:
            self.in_paragraph = True
            self.parts = []

    def handle_data(self, data: str) -> None:
        if self.body_depth and self.in_paragraph:
            self.parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "p" and self.in_paragraph:
            text = " ".join("".join(self.parts).split())
            if text:
                self.paragraphs.append(text)
            self.in_paragraph = False
            self.parts = []
        elif tag == "div" and self.article_depth:
            if self.body_depth:
                self.body_depth -= 1
            self.article_depth -= 1


def extract_article(payload: bytes) -> str:
    if len(payload) > MAX_ARTICLE_BYTES:
        raise SourceRejected("Release page is too large")
    parser = _ArticleText()
    parser.feed(payload.decode("utf-8-sig", errors="replace"))
    text = "\n".join(parser.paragraphs)
    if len(text) < 80 or len(text) > 16_000:
        raise SourceRejected("Expected FOMC statement body was not found")
    return text


class FedMonetarySource:
    """Bounded HTTP polling of one official feed and allowlisted release pages."""

    def __init__(
        self,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 20,
        max_retries: int = 2,
        min_request_interval_seconds: float = 0.5,
    ) -> None:
        if timeout_seconds <= 0 or max_retries < 0 or min_request_interval_seconds < 0:
            raise ValueError("Invalid Fed HTTP settings")
        self.transport = transport
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.min_interval = min_request_interval_seconds
        self._last_request = 0.0

    async def fetch_items(self) -> tuple[FeedItem, ...]:
        return parse_feed(await self._get(FEED_URL, MAX_FEED_BYTES))

    async def fetch_article(self, item: ValidatedItem) -> str:
        if _official_release_date(item.url) is None:
            raise SourceRejected("Release URL is outside the official allowlist")
        return extract_article(await self._get(item.url, MAX_ARTICLE_BYTES))

    async def _get(self, url: str, maximum_bytes: int) -> bytes:
        headers = {
            "User-Agent": "EventLens/0.5 research (https://github.com/lohis-aimbot/eventlens)"
        }
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds, transport=self.transport, follow_redirects=False
        ) as client:
            for attempt in range(self.max_retries + 1):
                elapsed = time.monotonic() - self._last_request
                if elapsed < self.min_interval:
                    await asyncio.sleep(self.min_interval - elapsed)
                self._last_request = time.monotonic()
                try:
                    response = await client.get(url, headers=headers)
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    if attempt == self.max_retries:
                        raise FeedError("Fed request failed after bounded retries") from exc
                else:
                    if response.status_code in {408, 429, 500, 502, 503, 504}:
                        if attempt == self.max_retries:
                            raise FeedError(f"Fed temporary HTTP {response.status_code}")
                    elif 300 <= response.status_code < 500:
                        raise SourceRejected(
                            f"Fed HTTP {response.status_code}; redirects are not followed"
                        )
                    elif response.status_code != 200:
                        raise FeedError(
                            f"Fed HTTP {response.status_code}; redirects are not followed"
                        )
                    else:
                        if len(response.content) > maximum_bytes:
                            raise FeedError("Fed response exceeds size limit")
                        return response.content
                await asyncio.sleep(min(0.5 * 2**attempt, 2))
        raise AssertionError("Unreachable Fed retry loop")


FeedStatus = Literal["baseline", "pending", "processed", "quarantined"]


class FedInbox:
    """Small persistent cursor; raw evidence and decisions remain in ThesisMemory."""

    def __init__(
        self, database: Path, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self.database = database
        self.clock = clock
        database.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS fed_feed_state (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fed_feed_items (
                    item_id TEXT PRIMARY KEY, url TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('baseline','pending','processed','quarantined')),
                    detail TEXT NOT NULL, first_seen_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS fed_feed_items_status ON fed_feed_items(status);
            """)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.database, timeout=10)
        db.execute("PRAGMA busy_timeout=10000")
        return db

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        """One local poll/inference process per database, including across restarts."""
        lock_path = self.database.with_suffix(self.database.suffix + ".feed.lock")
        with lock_path.open("a+") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise WatcherBusy("Another Fed watcher is using this database") from exc
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def initialized(self) -> bool:
        with closing(self._connect()) as db:
            return (
                db.execute("SELECT 1 FROM fed_feed_state WHERE key='initialized'").fetchone()
                is not None
            )

    def bootstrap(self, items: tuple[ValidatedItem, ...]) -> None:
        now = self.clock().astimezone(UTC).isoformat()
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM fed_feed_state WHERE key='initialized'").fetchone():
                return
            db.executemany(
                "INSERT OR IGNORE INTO fed_feed_items VALUES (?, ?, 'baseline', '', ?, ?)",
                [(item.event_id, item.url, now, now) for item in items],
            )
            db.execute("INSERT INTO fed_feed_state VALUES ('initialized', ?)", (now,))

    def status(self, item_id: str) -> FeedStatus | None:
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT status FROM fed_feed_items WHERE item_id=?", (item_id,)
            ).fetchone()
        return row[0] if row else None

    def mark(self, item_id: str, url: str, status: FeedStatus, detail: str = "") -> None:
        now = self.clock().astimezone(UTC).isoformat()
        with closing(self._connect()) as db, db:
            db.execute(
                """INSERT INTO fed_feed_items VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(item_id) DO UPDATE SET status=excluded.status,
                   detail=excluded.detail, updated_at=excluded.updated_at""",
                (item_id, url[:512], status, detail[:200], now, now),
            )

    def pending_ids(self) -> tuple[str, ...]:
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT item_id FROM fed_feed_items WHERE status='pending' ORDER BY first_seen_at, item_id"
            ).fetchall()
        return tuple(row[0] for row in rows)


@dataclass(frozen=True)
class PollResult:
    bootstrapped: bool = False
    baseline: int = 0
    collected: int = 0
    reasoned: int = 0
    skipped: int = 0
    quarantined: int = 0
    fetch_failed: int = 0


class FedWatcher:
    def __init__(
        self,
        source: FedMonetarySource,
        memory: ThesisMemory,
        inbox: FedInbox,
        *,
        model: JSONModelClient | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.source = source
        self.memory = memory
        self.inbox = inbox
        self.model = model
        self.clock = clock

    async def poll_once(self) -> PollResult:
        with self.inbox.exclusive():
            return await self._poll_once()

    async def _poll_once(self) -> PollResult:
        raw_items = await self.source.fetch_items()
        accepted: dict[str, ValidatedItem] = {}
        quarantined = 0
        for raw in raw_items:
            try:
                item = validate_item(raw)
            except SourceRejected as exc:
                if self.inbox.status(raw.quarantine_id) != "quarantined":
                    self.inbox.mark(raw.quarantine_id, raw.link, "quarantined", str(exc))
                    quarantined += 1
                continue
            accepted[item.event_id] = item
        items = tuple(sorted(accepted.values(), key=lambda item: (item.source_timestamp, item.url)))
        if not self.inbox.initialized():
            self.inbox.bootstrap(items)
            logger.info(
                "fed_bootstrapped existing_items=%s quarantined=%s", len(items), quarantined
            )
            return PollResult(bootstrapped=True, baseline=len(items), quarantined=quarantined)
        collected = skipped = fetch_failed = 0
        for item in items:
            status = self.inbox.status(item.event_id)
            if status is not None:
                skipped += 1
                continue
            existing = await self.memory.get_event(item.event_id)
            if existing is not None:
                if existing.source_reference != item.url:
                    self.inbox.mark(
                        item.event_id, item.url, "quarantined", "Stored source mismatch"
                    )
                    quarantined += 1
                else:
                    # Recover after a crash between evidence commit and inbox update.
                    self.inbox.mark(item.event_id, item.url, "pending")
                    skipped += 1
                continue
            try:
                text = await self.source.fetch_article(item)
            except FeedError:
                logger.warning("fed_article_fetch_failed event_id=%s", item.event_id)
                fetch_failed += 1
                continue
            except SourceRejected as exc:
                self.inbox.mark(item.event_id, item.url, "quarantined", str(exc))
                quarantined += 1
                continue
            try:
                event = Event(
                    event_id=item.event_id,
                    source="Federal Reserve",
                    source_type="macro",
                    source_reference=item.url,
                    source_timestamp=item.source_timestamp,
                    received_timestamp=self.clock(),
                    raw_text=text,
                    author="Federal Open Market Committee",
                )
                await self.memory.record_event(event)
            except (ValidationError, ValueError) as exc:
                self.inbox.mark(item.event_id, item.url, "quarantined", type(exc).__name__)
                quarantined += 1
                continue
            self.inbox.mark(item.event_id, item.url, "pending")
            collected += 1
            logger.info("fed_event_collected event_id=%s", item.event_id)
        reasoned = 0
        if self.model is not None:
            reasoner = MemoryReasoner(self.model, self.memory)
            for event_id in self.inbox.pending_ids():
                pending_event = await self.memory.get_event(event_id)
                if pending_event is None:
                    self.inbox.mark(event_id, "", "quarantined", "Missing raw event")
                    quarantined += 1
                    continue
                try:
                    await reasoner.process(pending_event)
                except (ModelCallError, MemoryConflict, ValidationError, ValueError) as exc:
                    self.inbox.mark(
                        event_id, pending_event.source_reference, "quarantined", type(exc).__name__
                    )
                    quarantined += 1
                    logger.warning(
                        "fed_reasoning_quarantined event_id=%s error_type=%s",
                        event_id,
                        type(exc).__name__,
                    )
                    continue
                self.inbox.mark(event_id, pending_event.source_reference, "processed")
                reasoned += 1
        return PollResult(
            collected=collected,
            reasoned=reasoned,
            skipped=skipped,
            quarantined=quarantined,
            fetch_failed=fetch_failed,
        )
