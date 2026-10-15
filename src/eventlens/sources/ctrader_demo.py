"""Read only new JSONL files emitted by the local, no-order cTrader Demo bridge."""

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ..quotes import BridgeQuote, BridgeStart, BridgeStop, QuoteStore, utc

MAX_LINE_BYTES = 16384


class DirectoryTail:
    """Ignore preexisting files; reject truncation/replacement rather than replaying them."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory.resolve()
        self.ignored = set(self.directory.glob("quotes-*.jsonl"))
        self.files: dict[Path, tuple[int, int, bytes]] = {}

    def poll(self) -> Iterator[str]:
        for path in sorted(self.directory.glob("quotes-*.jsonl")):
            if path in self.ignored:
                continue
            stat = path.stat()
            identity, offset, buffer = self.files.get(path, (stat.st_ino, 0, b""))
            if path.is_symlink() or stat.st_ino != identity or stat.st_size < offset:
                raise ValueError(
                    "Quote file replaced, linked or truncated; restart with a new bridge run"
                )
            # Bound each read and the total number of active files; no directory-wide bulk import.
            if len(self.files) >= 4 and path not in self.files:
                raise ValueError("Too many new bridge files")
            with path.open("rb") as file:
                file.seek(offset)
                data = file.read(65536)
            offset += len(data)
            buffer += data
            lines = buffer.split(b"\n")
            buffer = lines.pop()
            if len(buffer) > MAX_LINE_BYTES or any(len(line) > MAX_LINE_BYTES for line in lines):
                raise ValueError("Bridge line exceeds size limit")
            self.files[path] = (identity, offset, buffer)
            for line in lines:
                yield line.decode("utf-8")


class DemoQuoteCollector:
    """Validate a fresh Demo handshake; old/backtest/live/unknown sessions never emit quotes."""

    def __init__(self, store: QuoteStore, watch_started_at: datetime) -> None:
        self.store, self.started = store, utc(watch_started_at)
        self.session: BridgeStart | None = None
        self.stopped = False

    def accept(self, raw: str, received: datetime) -> str:
        received = utc(received)
        try:
            if len(raw.encode()) > MAX_LINE_BYTES:
                raise ValueError("Bridge line exceeds size limit")
            obj: dict[str, Any] = json.loads(raw)
            if (
                not isinstance(obj, dict)
                or type(obj.get("version")) is not int
                or obj["version"] != 1
            ):
                raise ValueError("Invalid bridge envelope/version")
            kind = obj.get("kind")
            if kind == "start":
                start = BridgeStart.model_validate(obj)
                if self.session is not None:
                    raise ValueError("Only one fresh bridge session per watch is allowed")
                if (
                    not self.started <= start.started_at <= received
                    or received - start.started_at > timedelta(seconds=10)
                ):
                    raise ValueError("Bridge must start after the watcher, with a fresh handshake")
                self.store.start(start, received, basis="live_poll")
                self.session = start
                return "start"
            if (
                self.session is None
                or self.stopped
                or obj.get("session_id") != str(self.session.session_id)
            ):
                raise ValueError("Unknown or closed bridge session")
            if kind == "quote":
                if not isinstance(obj.get("bid"), str) or not isinstance(obj.get("ask"), str):
                    raise ValueError("Bridge prices must be decimal strings")
                quote = BridgeQuote.model_validate(obj)
                if received - quote.source_at > timedelta(seconds=5):
                    raise ValueError("Quote was already stale at Python receipt")
                self.store.record(quote, received, raw)
                return "quote"
            if kind == "stop":
                self.store.stop(BridgeStop.model_validate(obj), received)
                self.stopped = True
                return "stop"
            raise ValueError("Unknown bridge message kind")
        except (ValueError, TypeError) as exc:
            self.store.failure(received, str(exc), raw)
            return "rejected"


def now_utc() -> datetime:
    return datetime.now(UTC)
