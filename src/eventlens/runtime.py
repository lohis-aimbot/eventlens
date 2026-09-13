"""Future orchestration entry point. There is no running loop in this foundation."""

from typing import Protocol

from .contracts import AuditRecord, Event


class AuditSink(Protocol):
    async def append(self, record: AuditRecord) -> None:
        """Append structured lifecycle records; exclude credentials and secret prompts."""
        ...


class EventRuntime(Protocol):
    async def handle(self, event: Event) -> None:
        """Persist → filter → snapshot → agent → validate → commit → risk → shadow.

        Serialize portfolio decisions initially. Never resume a pre-crash decision
        blindly: reload memory, reconcile account state and recheck expiry/risk.
        Persist cycle progress and distinguish reasoning from execution status.
        """
        ...
