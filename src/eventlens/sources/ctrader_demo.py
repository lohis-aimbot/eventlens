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
        candidates = sorted(self.directory.glob("quotes-*.jsonl"))
        newest_candidate = candidates[-1] if candidates else None
        for path in candidates:
            if path in self.ignored:
                continue
            stat = path.stat()
            identity, offset, buffer = self.files.get(path, (stat.st_ino, 0, b""))
            if path.is_symlink() or stat.st_ino != identity or stat.st_size < offset:
                raise ValueError(
                    "Quote file replaced, linked or truncated; restart with a new bridge run"
                )
            # Evict older drained session files superseded by newer rotated files before enforcing cap.
            if len(self.files) >= 4 and path not in self.files:
                for old_path in sorted(list(self.files.keys())):
                    if old_path == path or old_path == newest_candidate:
                        continue
                    try:
                        old_stat = old_path.stat()
                    except FileNotFoundError:
                        self.files.pop(old_path, None)
                        self.ignored.add(old_path)
                        continue
                    old_ino, old_off, old_buf = self.files[old_path]
                    if (
                        old_stat.st_ino == old_ino
                        and old_stat.st_size == old_off
                        and not old_buf
                        and old_stat.st_mtime <= stat.st_mtime
                    ):
                        self.files.pop(old_path, None)
                        self.ignored.add(old_path)
                        if len(self.files) < 4:
                            break
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
            saw_stop = False
            for line in lines:
                decoded = line.decode("utf-8")
                if '"stop"' in decoded:
                    try:
                        parsed = json.loads(decoded)
                        if isinstance(parsed, dict) and parsed.get("kind") == "stop":
                            saw_stop = True
                    except ValueError:
                        pass
                yield decoded
            if saw_stop and not buffer:
                self.files.pop(path, None)
                self.ignored.add(path)


class DemoQuoteCollector:
    """Validate a fresh Demo handshake; old/backtest/live/unknown sessions never emit quotes."""

    def __init__(
        self,
        store: QuoteStore,
        watch_started_at: datetime,
        *,
        allow_multiple_sessions: bool = False,
        allow_unclean_session_rollover: bool = False,
        max_start_handshake_age_seconds: float = 10.0,
    ) -> None:
        self.store, self.started = store, utc(watch_started_at)
        self.allow_multiple_sessions = allow_multiple_sessions
        self.allow_unclean_session_rollover = allow_unclean_session_rollover
        self.max_start_handshake_age_seconds = max_start_handshake_age_seconds
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
                for field in ("pip_size", "commission"):
                    if obj.get(field) is not None and not isinstance(obj.get(field), str):
                        raise ValueError("Bridge symbol metadata decimals must be strings")
                start = BridgeStart.model_validate(obj)
                if (
                    self.session is not None
                    and self.allow_multiple_sessions
                    and self.allow_unclean_session_rollover
                    and not self.stopped
                    and str(start.session_id) != str(self.session.session_id)
                    and start.started_at >= self.session.started_at
                ):
                    self.store.mark_session_superseded(
                        str(self.session.session_id),
                        received,
                        reason="superseded_by_new_bridge_start",
                    )
                    self.stopped = True
                if self.session is not None and (
                    not self.allow_multiple_sessions or not self.stopped
                ):
                    raise ValueError("Only one fresh bridge session per watch is allowed")
                if (
                    not self.started <= start.started_at <= received
                    or received - start.started_at
                    > timedelta(seconds=self.max_start_handshake_age_seconds)
                ):
                    raise ValueError("Bridge must start after the watcher, with a fresh handshake")
                if not self.store.has_session(str(start.session_id)):
                    self.store.start(start, received, basis="live_poll")
                self.session = start
                self.stopped = False
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
