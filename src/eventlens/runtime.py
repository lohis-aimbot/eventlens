"""Deterministic intraday event runtime: Persist → filter → snapshot → agent → validate → commit → risk → shadow."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import uuid4

from .contracts import (
    AgentContext,
    AgentDecision,
    AuditRecord,
    Contract,
    Event,
    ExecutionReceipt,
    Identifier,
    MarketSnapshot,
    MemorySnapshot,
    RiskAssessment,
    Thesis,
)
from .memory import ThesisMemory
from .sources.events import EventFilter
from .trading.agent import TradingAgent
from .trading.execution import ShadowExecutor
from .trading.portfolio import DeterministicTargetValidator, PortfolioState
from .trading.risk import DeterministicHardRiskEngine


class AuditSink(Protocol):
    async def append(self, record: AuditRecord) -> None:
        """Append structured lifecycle records; exclude credentials and secret prompts."""
        ...


class InMemoryAuditSink:
    """In-memory audit trail for deterministic runtime verification."""

    def __init__(self) -> None:
        self.records: list[AuditRecord] = []

    async def append(self, record: AuditRecord) -> None:
        self.records.append(AuditRecord.model_validate_json(record.model_dump_json()))


class RuntimeCycleResult(Contract):
    event_id: Identifier
    status: Literal[
        "filtered_out",
        "no_target",
        "target_rejected",
        "risk_rejected",
        "shadow_recorded",
        "shadow_rejected",
    ]
    reason: str
    decision: AgentDecision | None = None
    committed_memory_version: int | None = None
    assessment: RiskAssessment | None = None
    receipt: ExecutionReceipt | None = None
    audit_records: tuple[AuditRecord, ...] = ()


class EventRuntime(Protocol):
    async def handle(self, event: Event) -> None:
        """Persist → filter → snapshot → agent → validate → commit → risk → shadow.

        Serialize portfolio decisions initially. Never resume a pre-crash decision
        blindly: reload memory, reconcile account state and recheck expiry/risk.
        Persist cycle progress and distinguish reasoning from execution status.
        """
        ...


def _effective_theses(memory: MemorySnapshot, decision: AgentDecision) -> dict[str, Thesis]:
    by_id: dict[str, Thesis] = {thesis.thesis_id: thesis for thesis in memory.theses}
    for change in decision.thesis_changes:
        by_id[change.thesis.thesis_id] = change.thesis
    return by_id


class IntradayEventRuntime:
    """Serialized intraday runtime coordinator connecting observation, memory, risk, and shadow."""

    def __init__(
        self,
        *,
        memory: ThesisMemory,
        portfolio_state: PortfolioState,
        market_provider: Callable[[datetime], MarketSnapshot],
        agent: TradingAgent,
        target_validator: DeterministicTargetValidator | None = None,
        risk_engine: DeterministicHardRiskEngine | None = None,
        shadow_executor: ShadowExecutor,
        audit_sink: AuditSink | None = None,
        event_filter: EventFilter | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.memory = memory
        self.portfolio_state = portfolio_state
        self.market_provider = market_provider
        self.agent = agent
        self.target_validator = target_validator or DeterministicTargetValidator()
        self.risk_engine = risk_engine or DeterministicHardRiskEngine(clock=clock)
        self.shadow_executor = shadow_executor
        self.audit_sink = audit_sink or InMemoryAuditSink()
        self.event_filter = event_filter
        self.clock = clock
        self._lock = asyncio.Lock()

    async def handle(self, event: Event) -> None:
        await self.run_cycle(event)

    async def run_cycle(self, event: Event) -> RuntimeCycleResult:
        async with self._lock:
            cycle_audit: list[AuditRecord] = []

            async def log(
                component: Literal[
                    "collector", "filter", "agent", "memory", "risk", "execution", "runtime"
                ],
                outcome: str,
                detail: str,
                *,
                decision_id: str | None = None,
            ) -> None:
                rec = AuditRecord(
                    audit_id=str(uuid4()),
                    occurred_at=self.clock(),
                    component=component,
                    event_id=event.event_id,
                    decision_id=decision_id,
                    outcome=outcome,
                    detail=detail,
                )
                cycle_audit.append(rec)
                await self.audit_sink.append(rec)

            await self.memory.record_event(event)
            await log("collector", "event_persisted", event.source_reference)

            if self.event_filter is not None:
                filter_result = await self.event_filter.evaluate(event)
                if filter_result.outcome != "accept":
                    await log("filter", filter_result.outcome, filter_result.reason)
                    return RuntimeCycleResult(
                        event_id=event.event_id,
                        status="filtered_out",
                        reason=filter_result.reason,
                        audit_records=tuple(cycle_audit),
                    )

            now = self.clock()
            memory_snap = await self.memory.snapshot(as_of=max(now, event.received_timestamp))
            market_snap = self.market_provider(now)
            portfolio_snap = await self.portfolio_state.snapshot()
            as_of = max(
                now,
                event.received_timestamp,
                memory_snap.as_of,
                market_snap.as_of,
                portfolio_snap.as_of,
            )
            context = AgentContext(
                event=event,
                memory=memory_snap,
                portfolio=portfolio_snap,
                market=market_snap,
                as_of=as_of,
            )
            await log(
                "runtime",
                "context_assembled",
                f"memory_v={memory_snap.memory_version} snap={portfolio_snap.snapshot_id}",
            )

            decision = await self.agent.decide(context)
            await log(
                "agent",
                "agent_decided",
                f"changes={len(decision.thesis_changes)} has_target={decision.target is not None}",
                decision_id=decision.decision_id,
            )

            if decision.target is not None:
                effective = _effective_theses(memory_snap, decision)
                try:
                    self.target_validator.validate(
                        decision.target,
                        portfolio_snap,
                        theses=effective,
                        as_of=decision.completed_at,
                    )
                except ValueError as exc:
                    reason = str(exc) or "target_validation_failed"
                    await log(
                        "runtime",
                        "target_rejected",
                        reason,
                        decision_id=decision.decision_id,
                    )
                    return RuntimeCycleResult(
                        event_id=event.event_id,
                        status="target_rejected",
                        reason=reason,
                        decision=decision,
                        audit_records=tuple(cycle_audit),
                    )
                await log(
                    "runtime",
                    "target_validated",
                    decision.target.target_id,
                    decision_id=decision.decision_id,
                )

            committed_version = await self.memory.commit(decision)
            await log(
                "memory",
                "memory_committed",
                f"memory_v={committed_version}",
                decision_id=decision.decision_id,
            )

            if decision.target is None:
                return RuntimeCycleResult(
                    event_id=event.event_id,
                    status="no_target",
                    reason="decision_has_no_target",
                    decision=decision,
                    committed_memory_version=committed_version,
                    audit_records=tuple(cycle_audit),
                )

            assessment = await self.risk_engine.assess(
                decision,
                portfolio_snap,
                market_snap,
                prior_theses=memory_snap.theses,
            )
            await log(
                "risk",
                assessment.outcome,
                ",".join(assessment.reasons),
                decision_id=decision.decision_id,
            )
            if assessment.outcome != "approve":
                return RuntimeCycleResult(
                    event_id=event.event_id,
                    status="risk_rejected",
                    reason=assessment.reasons[0],
                    decision=decision,
                    committed_memory_version=committed_version,
                    assessment=assessment,
                    audit_records=tuple(cycle_audit),
                )

            receipt = await self.shadow_executor.record(decision, assessment)
            await log(
                "execution",
                receipt.status,
                receipt.reason,
                decision_id=decision.decision_id,
            )
            return RuntimeCycleResult(
                event_id=event.event_id,
                status="shadow_recorded" if receipt.status == "recorded" else "shadow_rejected",
                reason=receipt.reason,
                decision=decision,
                committed_memory_version=committed_version,
                assessment=assessment,
                receipt=receipt,
                audit_records=tuple(cycle_audit),
            )
