"""Sources yield immutable raw records; they never place orders."""

from collections.abc import Iterable
from pathlib import Path
from typing import Protocol

from .core import Quote, SourceRecord


class SourceAdapter(Protocol):
    def read(self) -> Iterable[SourceRecord]: ...


class JsonlSource:
    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self) -> Iterable[SourceRecord]:
        for number, line in enumerate(self.path.read_text().splitlines(), 1):
            if line.strip():
                try:
                    yield SourceRecord.model_validate_json(line)
                except ValueError as exc:
                    raise ValueError(f"Invalid source record at line {number}: {exc}") from exc


def read_quotes(path: Path) -> list[Quote]:
    return [
        Quote.model_validate_json(line) for line in path.read_text().splitlines() if line.strip()
    ]
