"""Provider-neutral input and filtering interfaces. No source adapters implemented."""

from collections.abc import AsyncIterator
from typing import Protocol

from .contracts import Event, FilterResult


class EventSource(Protocol):
    def stream(self) -> AsyncIterator[Event]:
        """Adapters own authentication, normalization, retry, rate limits and source IDs."""
        ...


class EventFilter(Protocol):
    async def evaluate(self, event: Event) -> FilterResult:
        """Deduplicate and check source provenance; rejection never invokes the agent."""
        ...
