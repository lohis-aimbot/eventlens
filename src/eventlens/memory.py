"""Durable thesis repository contract; no database/backend is selected yet."""

from datetime import datetime
from typing import Protocol

from .contracts import AgentDecision, Event, MemorySnapshot


class MemoryConflict(Exception):
    """A concurrent update invalidated the snapshot; reload and re-evaluate."""


class ThesisMemory(Protocol):
    async def record_event(self, event: Event) -> None:
        """Persist raw evidence idempotently; conflicting payloads for an ID must fail."""
        ...

    async def snapshot(self, *, as_of: datetime) -> MemorySnapshot:
        """Return committed revisions visible at an aware UTC cutoff, not future revisions."""
        ...

    async def commit(self, decision: AgentDecision) -> int:
        """Atomically append decision + thesis revisions and return new memory version.

        Compare memory and individual thesis revisions; raise MemoryConflict if stale.
        Identical decision IDs are idempotent; ID reuse with different data must fail.
        Evidence must already exist, and earlier revisions must remain retrievable.
        This operation updates beliefs only, never actual positions.
        """
        ...
