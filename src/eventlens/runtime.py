"""Deterministic intraday event runtime and single-process automated Shadow loop."""

import asyncio
import inspect
import json
import logging
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Protocol
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
    PortfolioSnapshot,
    RiskAssessment,
    Thesis,
)
from .memory import ThesisMemory
from .observer import Basis, ObserverStore
from .quotes import QuoteStore, build_market_snapshot, validate_market_quote
from .sources.events import EventFilter
from .sources.wallstreetcn import CaptureError, WallStreetCNFilter, WallStreetCNSource
from .trading.agent import SinglePassTradingAgent, TradingAgent
from .trading.execution import APPLICATION_ID, ShadowExecutor, ShadowFill, SQLiteShadowLedger
from .trading.portfolio import (
    DeterministicTargetValidator,
    PortfolioState,
    classify_exposure_change,
)
from .trading.risk import DeterministicHardRiskEngine

logger = logging.getLogger(__name__)

CycleStatus = Literal[
    "filtered_out",
    "reasoning_failed",
    "no_target",
    "target_rejected",
    "risk_rejected",
    "shadow_recorded",
    "shadow_rejected",
    "recovery_expired",
    "recovery_abandoned",
]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Runtime requires timezone-aware timestamps")
    return value.astimezone(UTC)


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


class SQLiteRuntimeStore:
    """Durable event lifecycle state machine, audit log, and runner control table."""

    def __init__(self, path: Path) -> None:
        if str(path) == ":memory:":
            raise ValueError("Use a filesystem path for SQLiteRuntimeStore")
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self._lock, self.db:
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS runtime_events (
                    event_id TEXT PRIMARY KEY,
                    source_reference TEXT NOT NULL,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    context_json TEXT,
                    decision_id TEXT,
                    decision_json TEXT,
                    memory_version INTEGER,
                    assessment_id TEXT,
                    assessment_json TEXT,
                    execution_id TEXT,
                    receipt_json TEXT,
                    input_tokens INTEGER NOT NULL DEFAULT 0,
                    output_tokens INTEGER NOT NULL DEFAULT 0,
                    receipt_to_decision_ms INTEGER,
                    receipt_to_fill_ms INTEGER
                );
                CREATE TABLE IF NOT EXISTS runtime_audit_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    audit_id TEXT NOT NULL UNIQUE,
                    occurred_at TEXT NOT NULL,
                    component TEXT NOT NULL,
                    event_id TEXT,
                    decision_id TEXT,
                    outcome TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    record_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS shadow_runner_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    status TEXT NOT NULL,
                    started_at TEXT,
                    stopped_at TEXT,
                    heartbeat_at TEXT,
                    duration_seconds REAL,
                    stop_requested INTEGER NOT NULL DEFAULT 0,
                    stop_reason TEXT,
                    summary_json TEXT
                );
                """
            )
            runner_cols = {
                r["name"]
                for r in self.db.execute("PRAGMA table_info(shadow_runner_state)").fetchall()
            }
            if runner_cols and "stop_reason" not in runner_cols:
                self.db.execute("ALTER TABLE shadow_runner_state ADD COLUMN stop_reason TEXT")
            row = self.db.execute("SELECT 1 FROM shadow_runner_state WHERE id=1").fetchone()
            if row is None:
                self.db.execute(
                    "INSERT INTO shadow_runner_state VALUES (1, 'idle', NULL, NULL, NULL, NULL, 0, NULL, NULL)"
                )
            self.db.execute(f"PRAGMA application_id={APPLICATION_ID}")

    def close(self) -> None:
        with self._lock:
            self.db.close()

    def append_audit(self, record: AuditRecord) -> None:
        with self._lock, self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO runtime_audit_log
                (audit_id, occurred_at, component, event_id, decision_id, outcome, detail, record_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record.audit_id,
                    record.occurred_at.isoformat(),
                    record.component,
                    record.event_id,
                    record.decision_id,
                    record.outcome,
                    record.detail,
                    record.model_dump_json(),
                ),
            )

    def get_event_state(self, event_id: str) -> sqlite3.Row | None:
        with self._lock:
            row = self.db.execute(
                "SELECT * FROM runtime_events WHERE event_id=?", (event_id,)
            ).fetchone()
            return row if isinstance(row, sqlite3.Row) else None

    def upsert_event_state(
        self,
        *,
        event_id: str,
        source_reference: str,
        received_at: datetime,
        now: datetime,
        state: str,
        reason: str,
        increment_attempts: bool = False,
        context_json: str | None = None,
        decision_id: str | None = None,
        decision_json: str | None = None,
        memory_version: int | None = None,
        assessment_id: str | None = None,
        assessment_json: str | None = None,
        execution_id: str | None = None,
        receipt_json: str | None = None,
        input_tokens: int | None = None,
        output_tokens: int | None = None,
        receipt_to_decision_ms: int | None = None,
        receipt_to_fill_ms: int | None = None,
    ) -> None:
        with self._lock, self.db:
            existing = self.db.execute(
                "SELECT * FROM runtime_events WHERE event_id=?", (event_id,)
            ).fetchone()
            if existing is None:
                attempts = 1 if increment_attempts else 0
                self.db.execute(
                    """INSERT INTO runtime_events VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )""",
                    (
                        event_id,
                        source_reference,
                        state,
                        reason,
                        attempts,
                        now.isoformat(),
                        now.isoformat(),
                        received_at.isoformat(),
                        context_json,
                        decision_id,
                        decision_json,
                        memory_version,
                        assessment_id,
                        assessment_json,
                        execution_id,
                        receipt_json,
                        input_tokens or 0,
                        output_tokens or 0,
                        receipt_to_decision_ms,
                        receipt_to_fill_ms,
                    ),
                )
            else:
                attempts = int(existing["attempts"]) + (1 if increment_attempts else 0)
                self.db.execute(
                    """UPDATE runtime_events
                    SET state=?, reason=?, attempts=?, updated_at=?,
                        context_json=COALESCE(?, context_json),
                        decision_id=COALESCE(?, decision_id),
                        decision_json=COALESCE(?, decision_json),
                        memory_version=COALESCE(?, memory_version),
                        assessment_id=COALESCE(?, assessment_id),
                        assessment_json=COALESCE(?, assessment_json),
                        execution_id=COALESCE(?, execution_id),
                        receipt_json=COALESCE(?, receipt_json),
                        input_tokens=CASE WHEN ? IS NOT NULL THEN ? ELSE input_tokens END,
                        output_tokens=CASE WHEN ? IS NOT NULL THEN ? ELSE output_tokens END,
                        receipt_to_decision_ms=COALESCE(?, receipt_to_decision_ms),
                        receipt_to_fill_ms=COALESCE(?, receipt_to_fill_ms)
                    WHERE event_id=?""",
                    (
                        state,
                        reason,
                        attempts,
                        now.isoformat(),
                        context_json,
                        decision_id,
                        decision_json,
                        memory_version,
                        assessment_id,
                        assessment_json,
                        execution_id,
                        receipt_json,
                        input_tokens,
                        input_tokens,
                        output_tokens,
                        output_tokens,
                        receipt_to_decision_ms,
                        receipt_to_fill_ms,
                        event_id,
                    ),
                )

    def mark_fill_completed(self, fill: ShadowFill) -> None:
        if fill.execution_id is None:
            return
        with self._lock, self.db:
            row = self.db.execute(
                "SELECT * FROM runtime_events WHERE execution_id=?", (fill.execution_id,)
            ).fetchone()
            if row is None:
                return
            received_at = datetime.fromisoformat(row["received_at"])
            lag_ms = max(0, int((fill.filled_at - received_at).total_seconds() * 1000))
            self.db.execute(
                """UPDATE runtime_events
                SET state='completed', reason='shadow_target_filled', updated_at=?, receipt_to_fill_ms=?
                WHERE event_id=?""",
                (fill.filled_at.isoformat(), lag_ms, row["event_id"]),
            )

    def mark_order_cancelled(self, execution_id: str, reason: str, now: datetime) -> str | None:
        with self._lock, self.db:
            row = self.db.execute(
                "SELECT event_id FROM runtime_events WHERE execution_id=?",
                (execution_id,),
            ).fetchone()
            if row is None:
                return None
            ev_id = str(row["event_id"])
            self.db.execute(
                """UPDATE runtime_events
                SET state='shadow_rejected', reason=?, updated_at=?
                WHERE event_id=? AND state='shadow_recorded'""",
                (reason, _utc(now).isoformat(), ev_id),
            )
            return ev_id

    def incomplete_events(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.db.execute(
                    """SELECT * FROM runtime_events
                    WHERE state IN ('received', 'filtered_accepted', 'reasoning_failed', 'memory_committed', 'risk_assessed')
                    ORDER BY first_seen_at"""
                ).fetchall()
            )

    def audit_records_for_event(self, event_id: str) -> tuple[AuditRecord, ...]:
        with self._lock:
            rows = self.db.execute(
                "SELECT record_json FROM runtime_audit_log WHERE event_id=? ORDER BY seq",
                (event_id,),
            ).fetchall()
            return tuple(AuditRecord.model_validate_json(r["record_json"]) for r in rows)

    def request_stop(self, now: datetime, *, reason: str = "runner_stop_requested") -> None:
        with self._lock, self.db:
            self.db.execute(
                "UPDATE shadow_runner_state SET stop_requested=1, stop_reason=COALESCE(stop_reason, ?), heartbeat_at=? WHERE id=1",
                (reason, _utc(now).isoformat()),
            )

    def is_stop_requested(self) -> bool:
        with self._lock:
            row = self.db.execute(
                "SELECT stop_requested FROM shadow_runner_state WHERE id=1"
            ).fetchone()
            return bool(row and row["stop_requested"])

    def active_stop_reason(self, now: datetime) -> str | None:
        check_now = _utc(now)
        with self._lock:
            row = self.db.execute(
                "SELECT status, started_at, duration_seconds, stop_requested, stop_reason FROM shadow_runner_state WHERE id=1"
            ).fetchone()
            if row is None:
                return None
            if bool(row["stop_requested"]):
                return str(row["stop_reason"] or "runner_stop_requested")
            if row["status"] in ("stopping", "stopped"):
                return str(row["stop_reason"] or "runner_stopped")
            if (
                row["status"] == "running"
                and row["started_at"] is not None
                and row["duration_seconds"] is not None
            ):
                started = _utc(datetime.fromisoformat(row["started_at"]))
                if check_now >= started + timedelta(seconds=float(row["duration_seconds"])):
                    return "runner_duration_expired"
            return None

    def update_runner_state(
        self,
        *,
        status: str,
        now: datetime,
        started_at: datetime | None = None,
        stopped_at: datetime | None = None,
        duration_seconds: float | None = None,
        reset_stop: bool = False,
        stop_reason: str | None = None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self.db:
            self.db.execute(
                """UPDATE shadow_runner_state
                SET status=CASE
                        WHEN ? = 0 AND (stop_requested = 1 OR status IN ('stopping', 'stopped')) AND ? = 'running'
                        THEN status
                        ELSE ?
                    END,
                    started_at=COALESCE(?, started_at),
                    stopped_at=CASE WHEN ? = 1 THEN NULL ELSE COALESCE(?, stopped_at) END,
                    heartbeat_at=?,
                    duration_seconds=COALESCE(?, duration_seconds),
                    stop_requested=CASE WHEN ? = 1 THEN 0 ELSE stop_requested END,
                    stop_reason=CASE WHEN ? = 1 THEN NULL ELSE COALESCE(?, stop_reason) END,
                    summary_json=COALESCE(?, summary_json)
                WHERE id=1""",
                (
                    1 if reset_stop else 0,
                    status,
                    status,
                    _utc(started_at).isoformat() if started_at else None,
                    1 if reset_stop else 0,
                    _utc(stopped_at).isoformat() if stopped_at else None,
                    _utc(now).isoformat(),
                    duration_seconds,
                    1 if reset_stop else 0,
                    1 if reset_stop else 0,
                    stop_reason,
                    json.dumps(summary, ensure_ascii=False) if summary is not None else None,
                ),
            )

    def runner_status(self) -> dict[str, Any]:
        with self._lock:
            row = self.db.execute("SELECT * FROM shadow_runner_state WHERE id=1").fetchone()
            if row is None:
                return {"status": "idle"}
            data = dict(row)
            if data.get("summary_json"):
                data["summary"] = json.loads(data.pop("summary_json"))
            else:
                data.pop("summary_json", None)
                data["summary"] = None
            return data

    def summary_report(self) -> dict[str, Any]:
        with self._lock:
            events = [
                dict(r)
                for r in self.db.execute(
                    "SELECT event_id, source_reference, state, reason, attempts, first_seen_at, updated_at, received_at, decision_id, memory_version, assessment_id, execution_id, input_tokens, output_tokens, receipt_to_decision_ms, receipt_to_fill_ms FROM runtime_events ORDER BY first_seen_at"
                ).fetchall()
            ]
            state_counts: dict[str, int] = {}
            reason_counts: dict[str, int] = {}
            decision_lags: list[int] = []
            fill_lags: list[int] = []
            total_input_tokens = 0
            total_output_tokens = 0
            model_calls = 0
            for ev in events:
                state_counts[ev["state"]] = state_counts.get(ev["state"], 0) + 1
                reason_counts[ev["reason"]] = reason_counts.get(ev["reason"], 0) + 1
                total_input_tokens += int(ev["input_tokens"] or 0)
                total_output_tokens += int(ev["output_tokens"] or 0)
                if ev["decision_id"] is not None:
                    model_calls += 1
                if ev["receipt_to_decision_ms"] is not None:
                    decision_lags.append(int(ev["receipt_to_decision_ms"]))
                if ev["receipt_to_fill_ms"] is not None:
                    fill_lags.append(int(ev["receipt_to_fill_ms"]))
            audits = [
                json.loads(r["record_json"])
                for r in self.db.execute(
                    "SELECT record_json FROM runtime_audit_log ORDER BY seq"
                ).fetchall()
            ]
            return {
                "runner": self.runner_status(),
                "event_count": len(events),
                "state_counts": state_counts,
                "reason_counts": reason_counts,
                "model_calls": model_calls,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "latency_summary": {
                    "receipt_to_decision_samples": len(decision_lags),
                    "receipt_to_decision_ms": decision_lags,
                    "receipt_to_fill_samples": len(fill_lags),
                    "receipt_to_fill_ms": fill_lags,
                },
                "events": events,
                "audit_count": len(audits),
                "audit_records": audits,
            }


class RuntimeCycleResult(Contract):
    event_id: Identifier
    status: CycleStatus
    reason: str
    reused: bool = False
    recovered: bool = False
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


TERMINAL_EVENT_STATES = frozenset(
    {
        "filtered_out",
        "no_target",
        "target_rejected",
        "risk_rejected",
        "shadow_recorded",
        "shadow_rejected",
        "completed",
        "recovery_expired",
        "recovery_abandoned",
    }
)


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
        state_store: SQLiteRuntimeStore | None = None,
        event_filter: EventFilter | None = None,
        max_recovery_event_age: timedelta = timedelta(seconds=60),
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
        if state_store is not None:
            self.state_store: SQLiteRuntimeStore | None = state_store
        elif isinstance(shadow_executor, SQLiteShadowLedger):
            self.state_store = SQLiteRuntimeStore(shadow_executor.path)
        else:
            self.state_store = None
        self.event_filter = event_filter
        self.max_recovery_event_age = max_recovery_event_age
        self.clock = clock
        self._lock = asyncio.Lock()

    async def _snapshot_portfolio(self, market: MarketSnapshot | None) -> PortfolioSnapshot:
        sig = inspect.signature(self.portfolio_state.snapshot)
        if "market" in sig.parameters:
            return await self.portfolio_state.snapshot(market=market)  # type: ignore[call-arg]
        return await self.portfolio_state.snapshot()

    async def handle(self, event: Event) -> None:
        await self.run_cycle(event)

    async def run_cycle(self, event: Event) -> RuntimeCycleResult:
        async with self._lock:
            return await self._run_cycle_locked(event)

    async def _run_cycle_locked(self, event: Event) -> RuntimeCycleResult:
        now = _utc(self.clock())
        cycle_audit: list[AuditRecord] = []
        cost_version = (
            self.shadow_executor.cost_model.name
            if isinstance(self.shadow_executor, SQLiteShadowLedger)
            else None
        )

        async def log(
            component: Literal[
                "collector", "filter", "agent", "memory", "risk", "execution", "runtime"
            ],
            outcome: str,
            detail: str,
            *,
            decision: AgentDecision | None = None,
            assessment: RiskAssessment | None = None,
            receipt: ExecutionReceipt | None = None,
        ) -> None:
            thesis_ids: tuple[str, ...] = ()
            target_id: str | None = None
            if decision is not None:
                if decision.target is not None:
                    target_id = decision.target.target_id
                    t_ids: list[str] = []
                    for p in decision.target.positions:
                        t_ids.extend(p.thesis_ids)
                    thesis_ids = tuple(dict.fromkeys(t_ids))
                elif decision.thesis_changes:
                    thesis_ids = tuple(c.thesis.thesis_id for c in decision.thesis_changes)
            rec = AuditRecord(
                audit_id=str(uuid4()),
                occurred_at=_utc(self.clock()),
                component=component,
                event_id=event.event_id,
                decision_id=decision.decision_id if decision is not None else None,
                thesis_ids=thesis_ids,
                target_id=target_id,
                assessment_id=assessment.assessment_id if assessment is not None else None,
                execution_id=receipt.execution_id if receipt is not None else None,
                model_version=decision.model_version if decision is not None else None,
                prompt_version=decision.prompt_version if decision is not None else None,
                policy_version=self.risk_engine.policy.policy_version,
                cost_model_version=cost_version,
                outcome=outcome,
                detail=detail,
            )
            cycle_audit.append(rec)
            if self.state_store is not None:
                self.state_store.append_audit(rec)
            await self.audit_sink.append(rec)

        await self.memory.record_event(event)

        existing_row = (
            self.state_store.get_event_state(event.event_id)
            if self.state_store is not None
            else None
        )
        committed_decision = await self.memory.decision_for_event(event.event_id)

        # 1. If already in a terminal state in persistent store, return idempotently without re-running LLM
        if existing_row is not None and existing_row["state"] in TERMINAL_EVENT_STATES:
            status_val: CycleStatus = (
                "shadow_recorded" if existing_row["state"] == "completed" else existing_row["state"]
            )
            dec = (
                AgentDecision.model_validate_json(existing_row["decision_json"])
                if existing_row["decision_json"]
                else committed_decision
            )
            assess = (
                RiskAssessment.model_validate_json(existing_row["assessment_json"])
                if existing_row["assessment_json"]
                else None
            )
            rcpt = (
                ExecutionReceipt.model_validate_json(existing_row["receipt_json"])
                if existing_row["receipt_json"]
                else None
            )
            audits = (
                self.state_store.audit_records_for_event(event.event_id)
                if self.state_store is not None
                else ()
            )
            return RuntimeCycleResult(
                event_id=event.event_id,
                status=status_val,
                reason=existing_row["reason"],
                reused=True,
                decision=dec,
                committed_memory_version=existing_row["memory_version"],
                assessment=assess,
                receipt=rcpt,
                audit_records=audits,
            )

        # 2. Crash recovery window: decision is already committed in SQLiteThesisMemory,
        # but runtime crashed before completing risk/shadow recording!
        if committed_decision is not None:
            return await self._recover_committed_decision_locked(
                event=event,
                decision=committed_decision,
                existing_row=existing_row,
                log=log,
                cycle_audit=cycle_audit,
            )

        # 3. Record initial receipt if not yet recorded
        if existing_row is None:
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=now,
                    state="received",
                    reason="event_persisted",
                )
            await log("collector", "event_persisted", event.source_reference)

        # 4. Run filter only if not already filtered_accepted / reasoning_failed
        if existing_row is None or existing_row["state"] == "received":
            if self.event_filter is not None:
                if hasattr(self.event_filter, "unmark_seen"):
                    self.event_filter.unmark_seen(event.event_id)
                filter_result = await self.event_filter.evaluate(event)
                if filter_result.outcome != "accept":
                    if self.state_store is not None:
                        self.state_store.upsert_event_state(
                            event_id=event.event_id,
                            source_reference=event.source_reference,
                            received_at=event.received_timestamp,
                            now=_utc(self.clock()),
                            state="filtered_out",
                            reason=filter_result.reason,
                        )
                    await log("filter", filter_result.outcome, filter_result.reason)
                    return RuntimeCycleResult(
                        event_id=event.event_id,
                        status="filtered_out",
                        reason=filter_result.reason,
                        audit_records=tuple(cycle_audit),
                    )
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=_utc(self.clock()),
                    state="filtered_accepted",
                    reason="eligible_event",
                )
            await log("filter", "accept", "eligible_event")

        # 5. Assemble pre-LLM context and preserve exact snapshot seen by the model
        pre_now = _utc(self.clock())
        memory_snap = await self.memory.snapshot(as_of=max(pre_now, event.received_timestamp))
        pre_market = self.market_provider(pre_now)
        pre_portfolio = await self._snapshot_portfolio(pre_market)
        pre_as_of = max(
            pre_now,
            event.received_timestamp,
            memory_snap.as_of,
            pre_market.as_of,
            pre_portfolio.as_of,
        )
        context = AgentContext(
            event=event,
            memory=memory_snap,
            portfolio=pre_portfolio,
            market=pre_market,
            as_of=pre_as_of,
        )
        context_json = context.model_dump_json()
        if self.state_store is not None:
            self.state_store.upsert_event_state(
                event_id=event.event_id,
                source_reference=event.source_reference,
                received_at=event.received_timestamp,
                now=pre_now,
                state="filtered_accepted",
                reason="context_assembled",
                context_json=context_json,
            )
        await log(
            "runtime",
            "context_assembled",
            f"memory_v={memory_snap.memory_version} pre_snap={pre_portfolio.snapshot_id} pre_market={pre_market.snapshot_id}",
        )

        # 6. Invoke LLM agent (transient failures are recorded as reasoning_failed and remain retryable)
        input_tokens = 0
        output_tokens = 0
        try:
            if isinstance(self.agent, SinglePassTradingAgent):
                run_res = await self.agent.decide_with_usage(context)
                decision = run_res.decision
                input_tokens = run_res.input_tokens
                output_tokens = run_res.output_tokens
            else:
                decision = await self.agent.decide(context)
        except Exception as exc:
            err_reason = f"{type(exc).__name__}: {exc}"
            if self.event_filter is not None and hasattr(self.event_filter, "unmark_seen"):
                self.event_filter.unmark_seen(event.event_id)
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=_utc(self.clock()),
                    state="reasoning_failed",
                    reason=err_reason,
                    increment_attempts=True,
                )
            await log("agent", "reasoning_failed", err_reason)
            return RuntimeCycleResult(
                event_id=event.event_id,
                status="reasoning_failed",
                reason=err_reason,
                audit_records=tuple(cycle_audit),
            )

        decision_lag_ms = max(
            0, int((decision.completed_at - event.received_timestamp).total_seconds() * 1000)
        )
        await log(
            "agent",
            "agent_decided",
            f"action={decision.portfolio_action} changes={len(decision.thesis_changes)} has_target={decision.target is not None}",
            decision=decision,
        )

        # 7. Structural target check against pre-LLM portfolio before committing memory
        effective = _effective_theses(memory_snap, decision)
        if decision.target is not None:
            try:
                self.target_validator.validate(
                    decision.target,
                    pre_portfolio,
                    theses=effective,
                    as_of=decision.completed_at,
                    portfolio_action=decision.portfolio_action,
                )
            except ValueError as exc:
                reason = str(exc) or "target_validation_failed"
                if self.state_store is not None:
                    self.state_store.upsert_event_state(
                        event_id=event.event_id,
                        source_reference=event.source_reference,
                        received_at=event.received_timestamp,
                        now=_utc(self.clock()),
                        state="target_rejected",
                        reason=reason,
                        increment_attempts=True,
                        decision_id=decision.decision_id,
                        decision_json=decision.model_dump_json(),
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        receipt_to_decision_ms=decision_lag_ms,
                    )
                await log("runtime", "target_rejected", reason, decision=decision)
                return RuntimeCycleResult(
                    event_id=event.event_id,
                    status="target_rejected",
                    reason=reason,
                    decision=decision,
                    audit_records=tuple(cycle_audit),
                )

        # 8. Commit thesis & decision to SQLiteThesisMemory
        committed_version = await self.memory.commit(decision)
        if self.state_store is not None:
            self.state_store.upsert_event_state(
                event_id=event.event_id,
                source_reference=event.source_reference,
                received_at=event.received_timestamp,
                now=_utc(self.clock()),
                state="memory_committed",
                reason="memory_committed",
                increment_attempts=True,
                decision_id=decision.decision_id,
                decision_json=decision.model_dump_json(),
                memory_version=committed_version,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                receipt_to_decision_ms=decision_lag_ms,
            )
        await log(
            "memory",
            "memory_committed",
            f"memory_v={committed_version}",
            decision=decision,
        )

        # 9. Continue into post-LLM refresh, risk check, and shadow execution
        return await self._evaluate_and_record_post_commit_locked(
            event=event,
            decision=decision,
            committed_version=committed_version,
            pre_portfolio=pre_portfolio,
            prior_theses=memory_snap.theses,
            effective_theses=effective,
            log=log,
            cycle_audit=cycle_audit,
            recovered=False,
        )

    async def _recover_committed_decision_locked(
        self,
        *,
        event: Event,
        decision: AgentDecision,
        existing_row: sqlite3.Row | None,
        log: Any,
        cycle_audit: list[AuditRecord],
    ) -> RuntimeCycleResult:
        now = _utc(self.clock())
        memory_snap = await self.memory.snapshot(as_of=max(now, decision.completed_at))
        committed_version = (
            int(existing_row["memory_version"])
            if existing_row is not None and existing_row["memory_version"] is not None
            else memory_snap.memory_version
        )
        await log(
            "runtime",
            "recover_committed_decision",
            f"decision_id={decision.decision_id} memory_v={committed_version}",
            decision=decision,
        )

        # Check if shadow ledger already recorded this decision before the crash
        if isinstance(self.shadow_executor, SQLiteShadowLedger):
            with self.shadow_executor._lock:
                rcpt_row = self.shadow_executor.db.execute(
                    "SELECT * FROM shadow_receipts WHERE decision_id=?",
                    (decision.decision_id,),
                ).fetchone()
            if rcpt_row is not None:
                receipt = ExecutionReceipt.model_validate_json(rcpt_row["receipt_json"])
                assessment = RiskAssessment.model_validate_json(rcpt_row["assessment_json"])
                status_val: CycleStatus = (
                    "shadow_recorded" if receipt.status == "recorded" else "shadow_rejected"
                )
                if self.state_store is not None:
                    self.state_store.upsert_event_state(
                        event_id=event.event_id,
                        source_reference=event.source_reference,
                        received_at=event.received_timestamp,
                        now=now,
                        state=status_val,
                        reason=receipt.reason,
                        decision_id=decision.decision_id,
                        decision_json=decision.model_dump_json(),
                        memory_version=committed_version,
                        assessment_id=assessment.assessment_id,
                        assessment_json=assessment.model_dump_json(),
                        execution_id=receipt.execution_id,
                        receipt_json=receipt.model_dump_json(),
                    )
                return RuntimeCycleResult(
                    event_id=event.event_id,
                    status=status_val,
                    reason=receipt.reason,
                    reused=True,
                    recovered=True,
                    decision=decision,
                    committed_memory_version=committed_version,
                    assessment=assessment,
                    receipt=receipt,
                    audit_records=tuple(cycle_audit),
                )

        pre_portfolio: PortfolioSnapshot | None = None
        if existing_row is not None and existing_row["context_json"]:
            saved_ctx = AgentContext.model_validate_json(existing_row["context_json"])
            pre_portfolio = saved_ctx.portfolio

        effective = _effective_theses(memory_snap, decision)
        return await self._evaluate_and_record_post_commit_locked(
            event=event,
            decision=decision,
            committed_version=committed_version,
            pre_portfolio=pre_portfolio,
            prior_theses=memory_snap.theses,
            effective_theses=effective,
            log=log,
            cycle_audit=cycle_audit,
            recovered=True,
        )

    async def _evaluate_and_record_post_commit_locked(
        self,
        *,
        event: Event,
        decision: AgentDecision,
        committed_version: int,
        pre_portfolio: PortfolioSnapshot | None,
        prior_theses: tuple[Thesis, ...],
        effective_theses: dict[str, Thesis],
        log: Any,
        cycle_audit: list[AuditRecord],
        recovered: bool,
    ) -> RuntimeCycleResult:
        if decision.target is None:
            reason = (
                f"portfolio_action_{decision.portfolio_action.lower()}"
                if decision.portfolio_action
                else "decision_has_no_target"
            )
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=_utc(self.clock()),
                    state="no_target",
                    reason=reason,
                    decision_id=decision.decision_id,
                    decision_json=decision.model_dump_json(),
                    memory_version=committed_version,
                )
            return RuntimeCycleResult(
                event_id=event.event_id,
                status="no_target",
                reason=reason,
                recovered=recovered,
                decision=decision,
                committed_memory_version=committed_version,
                audit_records=tuple(cycle_audit),
            )

        # Refresh market and portfolio AFTER model completion / during recovery
        post_now = _utc(self.clock())
        post_market = self.market_provider(post_now)
        post_portfolio = await self._snapshot_portfolio(post_market)
        await log(
            "runtime",
            "post_llm_state_refreshed",
            f"post_snap={post_portfolio.snapshot_id} post_market={post_market.snapshot_id}",
            decision=decision,
        )

        # Check if stop request or runtime duration limit is active before approving/recording new risk
        active_stop = (
            self.state_store.active_stop_reason(post_now) if self.state_store is not None else None
        )
        if active_stop is None and isinstance(self.shadow_executor, SQLiteShadowLedger):
            with self.shadow_executor._lock:
                active_stop = self.shadow_executor._active_stop_reason_locked(post_now)
        if active_stop is not None:
            pos_target = next(
                (p for p in decision.target.positions if p.instrument == "EUR/USD"), None
            )
            weight = pos_target.weight if pos_target is not None else Decimal("0")
            curr_pos = next(
                (p for p in post_portfolio.positions if p.instrument == "EUR/USD"), None
            )
            curr_qty = curr_pos.quantity if curr_pos is not None else Decimal("0")
            if decision.portfolio_action == "CLOSE" or weight == Decimal("0"):
                increases_risk = False
            elif (
                curr_qty == Decimal("0")
                or (curr_qty > Decimal("0") and weight < Decimal("0"))
                or (curr_qty < Decimal("0") and weight > Decimal("0"))
            ):
                increases_risk = True
            else:
                q_eur = next((q for q in post_market.quotes if q.instrument == "EUR/USD"), None)
                mid = (
                    (q_eur.bid + q_eur.ask) / Decimal("2") if q_eur is not None else Decimal("1.0")
                )
                policy = getattr(self.risk_engine, "policy", None)
                change_kind = (
                    policy.classify_exposure_change(curr_qty, post_portfolio.equity, mid, weight)
                    if policy is not None and hasattr(policy, "classify_exposure_change")
                    else classify_exposure_change(curr_qty, post_portfolio.equity, mid, weight)
                )
                increases_risk = change_kind in ("risk_increasing", "reversal_risk_increasing")

            if increases_risk:
                stop_block_reason = f"blocked_by_{active_stop}"
                if self.state_store is not None:
                    self.state_store.upsert_event_state(
                        event_id=event.event_id,
                        source_reference=event.source_reference,
                        received_at=event.received_timestamp,
                        now=post_now,
                        state="risk_rejected",
                        reason=stop_block_reason,
                        decision_id=decision.decision_id,
                        decision_json=decision.model_dump_json(),
                        memory_version=committed_version,
                    )
                await log(
                    "risk",
                    "reject",
                    stop_block_reason,
                    decision=decision,
                )
                return RuntimeCycleResult(
                    event_id=event.event_id,
                    status="risk_rejected",
                    reason=stop_block_reason,
                    recovered=recovered,
                    decision=decision,
                    committed_memory_version=committed_version,
                    audit_records=tuple(cycle_audit),
                )

        # Check if decision expired before post-commit evaluation
        if post_now >= decision.valid_until:
            reason = (
                "committed_decision_expired_before_recovery"
                if recovered
                else "decision_expired_after_inference"
            )
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=post_now,
                    state="target_rejected",
                    reason=reason,
                    decision_id=decision.decision_id,
                    decision_json=decision.model_dump_json(),
                    memory_version=committed_version,
                )
            await log("runtime", "target_rejected", reason, decision=decision)
            return RuntimeCycleResult(
                event_id=event.event_id,
                status="target_rejected",
                reason=reason,
                recovered=recovered,
                decision=decision,
                committed_memory_version=committed_version,
                audit_records=tuple(cycle_audit),
            )

        # Check if portfolio snapshot, positions, or pending orders changed during inference / before recovery
        state_changed = (
            post_portfolio.snapshot_id != decision.based_on_snapshot_id
            or bool(post_portfolio.pending_order_ids)
            or (
                pre_portfolio is not None
                and (
                    post_portfolio.positions != pre_portfolio.positions
                    or post_portfolio.pending_order_ids != pre_portfolio.pending_order_ids
                )
            )
        )
        if state_changed:
            reason = (
                "portfolio_state_changed_before_recovery"
                if recovered
                else "portfolio_state_changed_during_inference"
            )
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=post_now,
                    state="target_rejected",
                    reason=reason,
                    decision_id=decision.decision_id,
                    decision_json=decision.model_dump_json(),
                    memory_version=committed_version,
                )
            await log(
                "runtime",
                "target_invalidated_state_change",
                f"{reason}: based_on={decision.based_on_snapshot_id} current={post_portfolio.snapshot_id}",
                decision=decision,
            )
            return RuntimeCycleResult(
                event_id=event.event_id,
                status="target_rejected",
                reason=reason,
                recovered=recovered,
                decision=decision,
                committed_memory_version=committed_version,
                audit_records=tuple(cycle_audit),
            )

        try:
            self.target_validator.validate(
                decision.target,
                post_portfolio,
                theses=effective_theses,
                as_of=post_now,
                portfolio_action=decision.portfolio_action,
            )
        except ValueError as exc:
            reason = str(exc) or "target_validation_failed"
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=post_now,
                    state="target_rejected",
                    reason=reason,
                    decision_id=decision.decision_id,
                    decision_json=decision.model_dump_json(),
                    memory_version=committed_version,
                )
            await log("runtime", "target_rejected", reason, decision=decision)
            return RuntimeCycleResult(
                event_id=event.event_id,
                status="target_rejected",
                reason=reason,
                recovered=recovered,
                decision=decision,
                committed_memory_version=committed_version,
                audit_records=tuple(cycle_audit),
            )
        await log("runtime", "target_validated", decision.target.target_id, decision=decision)

        # Assess hard risk using the refreshed post-LLM market and portfolio snapshots
        assessment = await self.risk_engine.assess(
            decision,
            post_portfolio,
            post_market,
            prior_theses=prior_theses,
        )
        if self.state_store is not None:
            self.state_store.upsert_event_state(
                event_id=event.event_id,
                source_reference=event.source_reference,
                received_at=event.received_timestamp,
                now=_utc(self.clock()),
                state="risk_assessed",
                reason=assessment.reasons[0],
                decision_id=decision.decision_id,
                decision_json=decision.model_dump_json(),
                memory_version=committed_version,
                assessment_id=assessment.assessment_id,
                assessment_json=assessment.model_dump_json(),
            )
        await log(
            "risk",
            assessment.outcome,
            ",".join(assessment.reasons),
            decision=decision,
            assessment=assessment,
        )
        if assessment.outcome != "approve":
            if self.state_store is not None:
                self.state_store.upsert_event_state(
                    event_id=event.event_id,
                    source_reference=event.source_reference,
                    received_at=event.received_timestamp,
                    now=_utc(self.clock()),
                    state="risk_rejected",
                    reason=assessment.reasons[0],
                    decision_id=decision.decision_id,
                    decision_json=decision.model_dump_json(),
                    memory_version=committed_version,
                    assessment_id=assessment.assessment_id,
                    assessment_json=assessment.model_dump_json(),
                )
            return RuntimeCycleResult(
                event_id=event.event_id,
                status="risk_rejected",
                reason=assessment.reasons[0],
                recovered=recovered,
                decision=decision,
                committed_memory_version=committed_version,
                assessment=assessment,
                audit_records=tuple(cycle_audit),
            )

        receipt = await self.shadow_executor.record(decision, assessment)
        final_state: CycleStatus = (
            "shadow_recorded" if receipt.status == "recorded" else "shadow_rejected"
        )
        if self.state_store is not None:
            self.state_store.upsert_event_state(
                event_id=event.event_id,
                source_reference=event.source_reference,
                received_at=event.received_timestamp,
                now=_utc(self.clock()),
                state=final_state,
                reason=receipt.reason,
                decision_id=decision.decision_id,
                decision_json=decision.model_dump_json(),
                memory_version=committed_version,
                assessment_id=assessment.assessment_id,
                assessment_json=assessment.model_dump_json(),
                execution_id=receipt.execution_id,
                receipt_json=receipt.model_dump_json(),
            )
        await log(
            "execution",
            receipt.status,
            receipt.reason,
            decision=decision,
            assessment=assessment,
            receipt=receipt,
        )
        return RuntimeCycleResult(
            event_id=event.event_id,
            status=final_state,
            reason=receipt.reason,
            recovered=recovered,
            decision=decision,
            committed_memory_version=committed_version,
            assessment=assessment,
            receipt=receipt,
            audit_records=tuple(cycle_audit),
        )

    async def recover_incomplete(
        self,
        *,
        max_event_age: timedelta | None = None,
        max_attempts: int = 3,
        llm_timeout_seconds: float | None = 20.0,
        stop_event: asyncio.Event | None = None,
    ) -> tuple[RuntimeCycleResult, ...]:
        """Reconcile any pre-crash incomplete events without blind replay, enforcing timeliness, retry cap, timeout, and cancellation."""
        if self.state_store is None:
            return ()
        eff_max_age = max_event_age if max_event_age is not None else self.max_recovery_event_age
        rows = self.state_store.incomplete_events()
        results: list[RuntimeCycleResult] = []

        async def _wait_for_stop() -> None:
            while True:
                if (stop_event is not None and stop_event.is_set()) or (
                    self.state_store is not None and self.state_store.is_stop_requested()
                ):
                    return
                if stop_event is not None:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=0.05)
                        return
                    except TimeoutError:
                        pass
                else:
                    await asyncio.sleep(0.05)

        for row in rows:
            if (stop_event is not None and stop_event.is_set()) or (
                self.state_store is not None and self.state_store.is_stop_requested()
            ):
                break
            ev = await self.memory.get_event(row["event_id"])
            if ev is None:
                continue

            committed_decision = await self.memory.decision_for_event(ev.event_id)
            if committed_decision is None:
                now = _utc(self.clock())
                attempts = int(row["attempts"] or 0)
                if attempts >= max_attempts:
                    self.state_store.upsert_event_state(
                        event_id=ev.event_id,
                        source_reference=ev.source_reference,
                        received_at=ev.received_timestamp,
                        now=now,
                        state="recovery_abandoned",
                        reason="recovery_max_attempts_exceeded",
                    )
                    rec = AuditRecord(
                        audit_id=str(uuid4()),
                        occurred_at=now,
                        component="runtime",
                        event_id=ev.event_id,
                        policy_version=self.risk_engine.policy.policy_version,
                        outcome="recovery_abandoned",
                        detail=f"recovery_max_attempts_exceeded: attempts={attempts} max={max_attempts}",
                    )
                    self.state_store.append_audit(rec)
                    await self.audit_sink.append(rec)
                    results.append(
                        RuntimeCycleResult(
                            event_id=ev.event_id,
                            status="recovery_abandoned",
                            reason="recovery_max_attempts_exceeded",
                            recovered=True,
                            audit_records=(rec,),
                        )
                    )
                    continue

                ev_received = _utc(ev.received_timestamp)
                ev_source = (
                    _utc(ev.source_timestamp) if ev.source_timestamp is not None else ev_received
                )
                stale_reason: str | None = None
                if ev_received > now or ev_source > now:
                    stale_reason = "recovery_event_future_timestamp"
                elif (now - ev_received) > eff_max_age or (
                    ev.source_timestamp is not None and (now - ev_source) > eff_max_age
                ):
                    stale_reason = "recovery_event_stale"

                if stale_reason is not None:
                    self.state_store.upsert_event_state(
                        event_id=ev.event_id,
                        source_reference=ev.source_reference,
                        received_at=ev.received_timestamp,
                        now=now,
                        state="recovery_expired",
                        reason=stale_reason,
                    )
                    rec = AuditRecord(
                        audit_id=str(uuid4()),
                        occurred_at=now,
                        component="runtime",
                        event_id=ev.event_id,
                        policy_version=self.risk_engine.policy.policy_version,
                        outcome="recovery_expired",
                        detail=f"{stale_reason}: received_at={ev_received.isoformat()} now={now.isoformat()}",
                    )
                    self.state_store.append_audit(rec)
                    await self.audit_sink.append(rec)
                    results.append(
                        RuntimeCycleResult(
                            event_id=ev.event_id,
                            status="recovery_expired",
                            reason=stale_reason,
                            recovered=True,
                            audit_records=(rec,),
                        )
                    )
                    continue

            cycle_task = asyncio.create_task(self.run_cycle(ev))
            stop_task = asyncio.create_task(_wait_for_stop())
            try:
                done, _ = await asyncio.wait(
                    {cycle_task, stop_task},
                    timeout=llm_timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done and not cycle_task.done():
                    cycle_task.cancel()
                    await asyncio.gather(cycle_task, return_exceptions=True)
                    break
                if not done:
                    cycle_task.cancel()
                    await asyncio.gather(cycle_task, return_exceptions=True)
                    raise TimeoutError
                res = cycle_task.result()
                results.append(res.model_copy(update={"recovered": True}))
            except TimeoutError:
                if self.event_filter is not None and hasattr(self.event_filter, "unmark_seen"):
                    self.event_filter.unmark_seen(ev.event_id)
                now_to = _utc(self.clock())
                self.state_store.upsert_event_state(
                    event_id=ev.event_id,
                    source_reference=ev.source_reference,
                    received_at=ev.received_timestamp,
                    now=now_to,
                    state="reasoning_failed",
                    reason="llm_decision_timeout",
                    increment_attempts=True,
                )
                rec = AuditRecord(
                    audit_id=str(uuid4()),
                    occurred_at=now_to,
                    component="agent",
                    event_id=ev.event_id,
                    policy_version=self.risk_engine.policy.policy_version,
                    outcome="reasoning_failed",
                    detail="llm_decision_timeout_during_recovery",
                )
                self.state_store.append_audit(rec)
                await self.audit_sink.append(rec)
                results.append(
                    RuntimeCycleResult(
                        event_id=ev.event_id,
                        status="reasoning_failed",
                        reason="llm_decision_timeout",
                        recovered=True,
                        audit_records=(rec,),
                    )
                )
            finally:
                if not stop_task.done():
                    stop_task.cancel()
                    await asyncio.gather(stop_task, return_exceptions=True)
                if not cycle_task.done():
                    cycle_task.cancel()
                    await asyncio.gather(cycle_task, return_exceptions=True)
        return tuple(results)


class ShadowRunner:
    """Single-process automated Shadow loop with independent event, quote, and protection tasks."""

    def __init__(
        self,
        *,
        runtime: IntradayEventRuntime,
        ledger: SQLiteShadowLedger,
        observer_store: ObserverStore | None = None,
        quote_store: QuoteStore | None = None,
        quote_bridge_dir: Path | None = None,
        news_source: WallStreetCNSource | None = None,
        basis: Basis = "live_poll",
        poll_news: bool = False,
        allow_baseline_revisions: bool = True,
        news_poll_interval_seconds: float = 15.0,
        event_scan_interval_seconds: float = 1.0,
        quote_interval_seconds: float = 0.5,
        protection_interval_seconds: float = 0.5,
        llm_timeout_seconds: float = 20.0,
        max_reasoning_attempts: int = 3,
        max_recovery_event_age_seconds: float = 60.0,
        exit_buffer_seconds: float = 0.0,
        exit_drain_timeout_seconds: float = 0.0,
        strict_window_coverage: bool = False,
        close_on_stop: bool = False,
        confirm_handoff: bool = False,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.runtime = runtime
        self.ledger = ledger
        self.observer_store = observer_store
        self.quote_store = quote_store
        self.quote_bridge_dir = quote_bridge_dir
        self.news_source = news_source
        self.basis: Basis = basis
        self.poll_news = poll_news
        self.allow_baseline_revisions = allow_baseline_revisions
        self.news_poll_interval_seconds = news_poll_interval_seconds
        self.event_scan_interval_seconds = event_scan_interval_seconds
        self.quote_interval_seconds = quote_interval_seconds
        self.protection_interval_seconds = protection_interval_seconds
        self.llm_timeout_seconds = llm_timeout_seconds
        self.max_reasoning_attempts = max_reasoning_attempts
        self.max_recovery_event_age_seconds = max_recovery_event_age_seconds
        self.exit_buffer_seconds = exit_buffer_seconds
        self.exit_drain_timeout_seconds = exit_drain_timeout_seconds
        self.strict_window_coverage = strict_window_coverage
        self.close_on_stop = close_on_stop
        self.confirm_handoff = confirm_handoff
        self.clock = clock
        self._stop_event = asyncio.Event()
        self._drain_stop_event = asyncio.Event()
        self._stop_reason: str | None = None
        self._run_started_at: datetime | None = None
        self._run_duration_seconds: float | None = None
        self._protection_lock = threading.Lock()
        self._protection_checks_total = 0
        self._protection_checks_valid_quote = 0
        self._protection_checks_unavailable_quote = 0
        self._protection_checks_unavailable_while_open = 0
        self._active_outage: dict[str, Any] | None = None
        self._outage_intervals: list[dict[str, Any]] = []
        self._quote_tail: Any = None
        self._quote_collector: Any = None
        self._quote_bridge_lock = threading.Lock()
        self._quote_bridge_counts: dict[str, int] = {
            "start": 0,
            "quote": 0,
            "stop": 0,
            "rejected": 0,
        }
        if self.quote_bridge_dir is not None and self.quote_store is not None:
            from .quotes import BridgeStart
            from .sources.ctrader_demo import DemoQuoteCollector, DirectoryTail

            now_init = _utc(self.clock())
            self._quote_tail = DirectoryTail(self.quote_bridge_dir)
            self._quote_collector = DemoQuoteCollector(
                self.quote_store,
                now_init,
                allow_multiple_sessions=True,
            )
            self._attach_active_bridge_session(now_init, BridgeStart)
        self.ledger.stop_reason_provider = self._active_stop_reason

    def _attach_active_bridge_session(self, now: datetime, start_cls: Any) -> None:
        if (
            self.quote_bridge_dir is None
            or self.quote_store is None
            or self._quote_tail is None
            or self._quote_collector is None
        ):
            return
        candidates = sorted(
            self.quote_bridge_dir.resolve().glob("quotes-*.jsonl"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            return
        newest = candidates[0]
        try:
            stat = newest.stat()
            raw_bytes = newest.read_bytes()
            lines = raw_bytes.split(b"\n")
            remainder = lines.pop()
            if not lines:
                return
            first_obj = json.loads(lines[0].decode("utf-8"))
            if first_obj.get("kind") != "start":
                return
            if any(json.loads(ln.decode("utf-8")).get("kind") == "stop" for ln in lines if ln):
                return
            start_item = start_cls.model_validate(first_obj)
            if now > start_item.started_at + timedelta(seconds=start_item.duration_seconds):
                return
            self._quote_tail.ignored.discard(newest)
            consumed_offset = len(raw_bytes) - len(remainder)
            self._quote_tail.files[newest] = (stat.st_ino, consumed_offset, remainder)
            self.quote_store.start(start_item, now, basis="live_poll")
            self._quote_collector.session = start_item
            self._quote_collector.stopped = False
            self._quote_bridge_counts["start"] += 1
        except Exception as exc:
            logger.warning("attach_active_bridge_session_skipped: %s", exc)

    def _active_stop_reason(self, now: datetime | None = None) -> str | None:
        check_now = _utc(now or self.clock())
        if self._stop_reason is not None:
            return self._stop_reason
        if self._stop_event.is_set():
            return "runner_stop_requested"
        required_window = float(self.ledger.exit_policy.max_holding_seconds) + float(
            self.exit_buffer_seconds
        )
        enforce_window = self.strict_window_coverage or self.exit_buffer_seconds > 0.0
        if self._run_started_at is not None and self._run_duration_seconds is not None:
            run_end = self._run_started_at + timedelta(seconds=self._run_duration_seconds)
            if check_now >= run_end:
                return "runner_duration_expired"
            if enforce_window:
                remaining_run = (run_end - check_now).total_seconds()
                if remaining_run < required_window:
                    return "insufficient_remaining_run_window_for_new_risk"
        if enforce_window and self.quote_store is not None:
            bridge_rem = self.quote_store.active_session_remaining_seconds(
                check_now, basis=self.basis
            )
            if bridge_rem is not None and bridge_rem < required_window:
                return "insufficient_quote_bridge_window_for_new_risk"
        return None

    def stop(
        self,
        *,
        reason: str = "runner_stop_requested",
        close_on_stop: bool | None = None,
        confirm_handoff: bool | None = None,
    ) -> None:
        now = _utc(self.clock())
        if close_on_stop is not None:
            self.close_on_stop = close_on_stop
        if confirm_handoff is not None:
            self.confirm_handoff = confirm_handoff
        self._stop_reason = reason
        self._stop_event.set()
        if self.runtime.state_store is not None:
            self.runtime.state_store.request_stop(now, reason=reason)
        cancelled = self.ledger.cancel_risk_increasing_pending_orders(
            now=now, reason=f"cancelled_on_{reason}"
        )
        if self.close_on_stop:
            self.ledger.request_protective_exit_on_stop(
                now=now, reason=f"protective_exit_on_{reason}"
            )
        if self.runtime.state_store is not None:
            for item in cancelled:
                ev_id = self.runtime.state_store.mark_order_cancelled(
                    item["execution_id"], item["reason"], now
                )
                rec = AuditRecord(
                    audit_id=str(uuid4()),
                    occurred_at=now,
                    component="execution",
                    event_id=ev_id,
                    decision_id=item["decision_id"],
                    target_id=item["target_id"],
                    execution_id=item["execution_id"],
                    policy_version=self.runtime.risk_engine.policy.policy_version,
                    cost_model_version=self.ledger.cost_model.name,
                    outcome="order_cancelled",
                    detail=item["reason"],
                )
                self.runtime.state_store.append_audit(rec)

    def _poll_quote_bridge_sync(self) -> None:
        if self._quote_tail is None or self._quote_collector is None:
            return
        with self._quote_bridge_lock:
            for raw in self._quote_tail.poll():
                outcome = self._quote_collector.accept(raw, _utc(self.clock()))
                self._quote_bridge_counts[outcome] = self._quote_bridge_counts.get(outcome, 0) + 1

    def _current_market(self) -> MarketSnapshot:
        self._poll_quote_bridge_sync()
        now = _utc(self.clock())
        if self.quote_store is not None:
            return build_market_snapshot(self.quote_store, now, basis=self.basis)
        return self.runtime.market_provider(now)

    async def step_quotes_and_fills(
        self, *, stop_reason: str | None = None
    ) -> tuple[tuple[ShadowFill, ...], tuple[ShadowFill, ...]]:
        """Execute one quote-driven protective exit + pending fill + mark-to-market cycle."""
        market = await asyncio.to_thread(self._current_market)
        exit_fills = await asyncio.to_thread(self.ledger.evaluate_protective_exits, market)
        target_fills = await asyncio.to_thread(
            self.ledger.fill_pending,
            market,
            risk_policy=self.runtime.risk_engine.policy,
            stop_reason=stop_reason,
        )
        if self.runtime.state_store is not None:
            for fill in (*exit_fills, *target_fills):
                self.runtime.state_store.mark_fill_completed(fill)
                rec = AuditRecord(
                    audit_id=str(uuid4()),
                    occurred_at=fill.filled_at,
                    component="execution",
                    decision_id=fill.decision_id,
                    target_id=fill.target_id,
                    execution_id=fill.execution_id,
                    fill_id=fill.fill_id,
                    policy_version=self.runtime.risk_engine.policy.policy_version,
                    cost_model_version=self.ledger.cost_model.name,
                    outcome=fill.kind,
                    detail=f"reason={fill.reason} delta={fill.delta_quantity} price={fill.fill_price}",
                )
                self.runtime.state_store.append_audit(rec)
                await self.runtime.audit_sink.append(rec)
        return exit_fills, target_fills

    def _record_protection_check(
        self, now: datetime, market: MarketSnapshot | None, *, position_open: bool
    ) -> None:
        quote_check = validate_market_quote(
            market,
            as_of=now,
            policy=self.ledger.exit_policy.quote_quality_policy("EUR/USD"),
        )
        with self._protection_lock:
            self._protection_checks_total += 1
            if quote_check.valid:
                self._protection_checks_valid_quote += 1
                if self._active_outage is not None:
                    start_dt: datetime = self._active_outage["started_dt"]
                    self._outage_intervals.append(
                        {
                            "started_at": self._active_outage["started_at"],
                            "ended_at": now.isoformat(),
                            "duration_seconds": round(
                                max(0.0, (now - start_dt).total_seconds()), 4
                            ),
                            "reason": self._active_outage["reason"],
                            "position_open": self._active_outage["position_open"],
                        }
                    )
                    self._active_outage = None
            else:
                self._protection_checks_unavailable_quote += 1
                if position_open:
                    self._protection_checks_unavailable_while_open += 1
                if self._active_outage is None:
                    self._active_outage = {
                        "started_at": now.isoformat(),
                        "started_dt": now,
                        "reason": quote_check.reason,
                        "position_open": position_open,
                    }
                elif position_open:
                    self._active_outage["position_open"] = True

    def _finalize_protection_coverage(self, stopped_at: datetime) -> dict[str, Any]:
        with self._protection_lock:
            if self._active_outage is not None:
                start_dt: datetime = self._active_outage["started_dt"]
                self._outage_intervals.append(
                    {
                        "started_at": self._active_outage["started_at"],
                        "ended_at": stopped_at.isoformat(),
                        "duration_seconds": round(
                            max(0.0, (stopped_at - start_dt).total_seconds()), 4
                        ),
                        "reason": self._active_outage["reason"],
                        "position_open": self._active_outage["position_open"],
                    }
                )
                self._active_outage = None
            max_outage = max(
                (float(item["duration_seconds"]) for item in self._outage_intervals),
                default=0.0,
            )
            return {
                "total_protection_checks": self._protection_checks_total,
                "valid_quote_checks": self._protection_checks_valid_quote,
                "unavailable_quote_checks": self._protection_checks_unavailable_quote,
                "unavailable_checks_while_position_open": self._protection_checks_unavailable_while_open,
                "outage_interval_count": len(self._outage_intervals),
                "max_outage_seconds": max_outage,
                "outage_intervals": list(self._outage_intervals),
            }

    async def step_protection(self) -> tuple[int, tuple[ShadowFill, ...]]:
        """Execute one timer-driven order expiry and protective exit check without acquiring the LLM lock."""
        now = _utc(self.clock())
        expired = await asyncio.to_thread(self.ledger.expire_pending_orders, now=now)
        market = await asyncio.to_thread(self._current_market)
        acct = await asyncio.to_thread(
            lambda: self.ledger.db.execute(
                "SELECT quantity_eur FROM shadow_account WHERE id=1"
            ).fetchone()
        )
        pos_open = acct is not None and Decimal(acct["quantity_eur"]) != Decimal("0")
        self._record_protection_check(now, market, position_open=pos_open)
        exit_fills = await asyncio.to_thread(self.ledger.evaluate_protective_exits, market)
        if self.runtime.state_store is not None:
            for fill in exit_fills:
                self.runtime.state_store.mark_fill_completed(fill)
                rec = AuditRecord(
                    audit_id=str(uuid4()),
                    occurred_at=fill.filled_at,
                    component="execution",
                    fill_id=fill.fill_id,
                    policy_version=self.runtime.risk_engine.policy.policy_version,
                    cost_model_version=self.ledger.cost_model.name,
                    outcome="protective_exit",
                    detail=f"reason={fill.reason} delta={fill.delta_quantity} price={fill.fill_price}",
                )
                self.runtime.state_store.append_audit(rec)
                await self.runtime.audit_sink.append(rec)
        return expired, exit_fills

    async def step_events(self) -> tuple[RuntimeCycleResult, ...]:
        """Scan ObserverStore for unprocessed revisions, filter, and run bounded-timeout cycles."""
        if self.observer_store is None:
            return ()
        now = _utc(self.clock())
        revisions = await asyncio.to_thread(
            self.observer_store.revisions,
            basis=self.basis,
            as_of=now,
            limit=200,
        )
        wscn_filter = (
            self.runtime.event_filter
            if isinstance(self.runtime.event_filter, WallStreetCNFilter)
            else WallStreetCNFilter(clock=self.clock)
        )
        results: list[RuntimeCycleResult] = []
        for rev in revisions:
            if self._stop_event.is_set() or (
                self.runtime.state_store is not None
                and self.runtime.state_store.is_stop_requested()
            ):
                break
            from .sources.wallstreetcn import revision_event_id

            ev_id = revision_event_id(rev)
            if self.runtime.state_store is not None:
                existing = self.runtime.state_store.get_event_state(ev_id)
                if existing is not None and (
                    existing["state"] in TERMINAL_EVENT_STATES
                    or (
                        existing["state"] == "reasoning_failed"
                        and int(existing["attempts"]) >= self.max_reasoning_attempts
                    )
                ):
                    continue
            wscn_filter.unmark_seen(ev_id)
            f_res, ev = wscn_filter.evaluate_revision(rev, checked_at=_utc(self.clock()))
            if not self.allow_baseline_revisions:
                sighting = await asyncio.to_thread(
                    self.observer_store.first_sighting_state,
                    provider=rev.provider,
                    basis=rev.basis,
                    kind=rev.record.kind,
                    source_id=rev.record.source_id,
                    revision=rev.revision,
                )
                is_pre_run = (
                    self._run_started_at is not None and rev.received_at < self._run_started_at
                )
                if sighting == "baseline" or is_pre_run:
                    if self.runtime.state_store is not None:
                        b_reason = (
                            "baseline_snapshot_not_live_arrival"
                            if ev is not None
                            else f"baseline_{f_res.reason}"
                        )
                        self.runtime.state_store.upsert_event_state(
                            event_id=ev_id,
                            source_reference=rev.record.source_url,
                            received_at=rev.received_at,
                            now=_utc(self.clock()),
                            state="filtered_out",
                            reason=b_reason,
                        )
                    continue
            if ev is None:
                if self.runtime.state_store is not None:
                    self.runtime.state_store.upsert_event_state(
                        event_id=ev_id,
                        source_reference=rev.record.source_url,
                        received_at=rev.received_at,
                        now=_utc(self.clock()),
                        state="filtered_out",
                        reason=f_res.reason,
                    )
                continue
            try:
                cycle_res = await asyncio.wait_for(
                    self.runtime.run_cycle(ev),
                    timeout=self.llm_timeout_seconds,
                )
                results.append(cycle_res)
            except TimeoutError:
                wscn_filter.unmark_seen(ev.event_id)
                if self.runtime.state_store is not None:
                    self.runtime.state_store.upsert_event_state(
                        event_id=ev.event_id,
                        source_reference=ev.source_reference,
                        received_at=ev.received_timestamp,
                        now=_utc(self.clock()),
                        state="reasoning_failed",
                        reason="llm_decision_timeout",
                        increment_attempts=True,
                    )
                    rec = AuditRecord(
                        audit_id=str(uuid4()),
                        occurred_at=_utc(self.clock()),
                        component="agent",
                        event_id=ev.event_id,
                        outcome="reasoning_failed",
                        detail="llm_decision_timeout",
                    )
                    self.runtime.state_store.append_audit(rec)
                    await self.runtime.audit_sink.append(rec)
        return tuple(results)

    async def run(self, *, duration_seconds: float) -> dict[str, Any]:
        """Run all three loops concurrently with a hard duration limit and two-phase protective drain shutdown."""
        if duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        self._stop_event = asyncio.Event()
        self._drain_stop_event = asyncio.Event()
        self._stop_reason = None
        started_at = _utc(self.clock())
        self._run_started_at = started_at
        self._run_duration_seconds = duration_seconds
        with self._protection_lock:
            self._protection_checks_total = 0
            self._protection_checks_valid_quote = 0
            self._protection_checks_unavailable_quote = 0
            self._protection_checks_unavailable_while_open = 0
            self._active_outage = None
            self._outage_intervals = []
        if self.runtime.state_store is not None:
            self.runtime.state_store.update_runner_state(
                status="running",
                now=started_at,
                started_at=started_at,
                duration_seconds=duration_seconds,
                reset_stop=True,
            )

        async def _quote_worker() -> None:
            while not self._drain_stop_event.is_set():
                if (
                    not self._stop_event.is_set()
                    and self.runtime.state_store is not None
                    and self.runtime.state_store.is_stop_requested()
                ):
                    self._stop_event.set()
                try:
                    await self.step_quotes_and_fills(stop_reason=self._stop_reason)
                except Exception as exc:
                    logger.warning("quote_worker_error: %s", exc)
                try:
                    await asyncio.wait_for(
                        self._drain_stop_event.wait(), timeout=self.quote_interval_seconds
                    )
                except TimeoutError:
                    pass

        async def _protection_worker() -> None:
            while not self._drain_stop_event.is_set():
                if (
                    not self._stop_event.is_set()
                    and self.runtime.state_store is not None
                    and self.runtime.state_store.is_stop_requested()
                ):
                    self._stop_event.set()
                try:
                    await self.step_protection()
                    if self.runtime.state_store is not None and not self._stop_event.is_set():
                        self.runtime.state_store.update_runner_state(
                            status="running", now=_utc(self.clock())
                        )
                except Exception as exc:
                    logger.warning("protection_worker_error: %s", exc)
                try:
                    await asyncio.wait_for(
                        self._drain_stop_event.wait(), timeout=self.protection_interval_seconds
                    )
                except TimeoutError:
                    pass

        async def _news_and_event_worker() -> None:
            try:
                await self.runtime.recover_incomplete(
                    max_event_age=timedelta(seconds=self.max_recovery_event_age_seconds),
                    max_attempts=self.max_reasoning_attempts,
                    llm_timeout_seconds=self.llm_timeout_seconds,
                    stop_event=self._stop_event,
                )
            except Exception as exc:
                logger.warning("recovery_worker_error: %s", exc)

            last_poll = 0.0
            loop = asyncio.get_running_loop()
            while not self._stop_event.is_set():
                if (
                    self.runtime.state_store is not None
                    and self.runtime.state_store.is_stop_requested()
                ):
                    self._stop_event.set()
                    break
                now_mono = loop.time()
                if (
                    self.poll_news
                    and self.observer_store is not None
                    and self.news_source is not None
                    and (
                        last_poll == 0.0 or now_mono - last_poll >= self.news_poll_interval_seconds
                    )
                ):
                    last_poll = now_mono
                    for kind in ("news", "calendar"):
                        if self._stop_event.is_set():
                            break
                        try:
                            batch = await self.news_source.fetch(kind, channel="global", limit=25)
                            await asyncio.to_thread(self.observer_store.record, batch)
                        except (CaptureError, Exception) as exc:
                            await asyncio.to_thread(
                                self.observer_store.failure,
                                self.news_source.provider,
                                f"{kind}/global",
                                str(exc),
                            )
                try:
                    await self.step_events()
                except Exception as exc:
                    logger.warning("event_worker_error: %s", exc)
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self.event_scan_interval_seconds
                    )
                except TimeoutError:
                    pass

        def _open_position_state() -> tuple[Decimal, str | None]:
            row = self.ledger.db.execute(
                "SELECT quantity_eur, pending_exit_reason FROM shadow_account WHERE id=1"
            ).fetchone()
            if row is None:
                return Decimal("0"), None
            return Decimal(row["quantity_eur"]), row["pending_exit_reason"]

        protection_task = asyncio.create_task(_protection_worker(), name="shadow-protection")
        quote_task = asyncio.create_task(_quote_worker(), name="shadow-quotes")
        event_task = asyncio.create_task(_news_and_event_worker(), name="shadow-events")
        timed_out = False
        shutdown_reason = "runner_stop_requested"
        event_worker_stopped_before_drain = False
        quote_worker_alive_during_drain = False
        protection_worker_alive_during_drain = False
        drain_elapsed_seconds = 0.0
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=duration_seconds)
        except TimeoutError:
            timed_out = True
            if self._stop_reason is None:
                self._stop_reason = "runner_duration_expired"
            self._stop_event.set()
        finally:
            shutdown_now = _utc(self.clock())
            if self._stop_reason is not None:
                shutdown_reason = self._stop_reason
            elif (
                self.runtime.state_store is not None
                and self.runtime.state_store.is_stop_requested()
            ):
                shutdown_reason = "runner_stop_requested"
            elif timed_out or (shutdown_now >= started_at + timedelta(seconds=duration_seconds)):
                shutdown_reason = "runner_duration_expired"
            else:
                shutdown_reason = "runner_stop_requested"

            self._stop_reason = shutdown_reason
            self._stop_event.set()
            if self.runtime.state_store is not None:
                self.runtime.state_store.update_runner_state(
                    status="stopping",
                    now=shutdown_now,
                    stop_reason=shutdown_reason,
                )

            # Phase 1: Stop new event/LLM worker first while keeping quote & protection workers running
            event_task.cancel()
            await asyncio.gather(event_task, return_exceptions=True)
            event_worker_stopped_before_drain = event_task.done()
            quote_worker_alive_during_drain = not quote_task.done()
            protection_worker_alive_during_drain = not protection_task.done()

            cancelled_orders = await asyncio.to_thread(
                self.ledger.cancel_risk_increasing_pending_orders,
                now=_utc(self.clock()),
                reason=f"cancelled_on_{shutdown_reason}",
            )
            if self.close_on_stop:
                await asyncio.to_thread(
                    self.ledger.request_protective_exit_on_stop,
                    now=_utc(self.clock()),
                    reason=f"protective_exit_on_{shutdown_reason}",
                )
            if self.runtime.state_store is not None:
                for item in cancelled_orders:
                    ev_id = self.runtime.state_store.mark_order_cancelled(
                        item["execution_id"], item["reason"], item["cancelled_at"]
                    )
                    rec = AuditRecord(
                        audit_id=str(uuid4()),
                        occurred_at=item["cancelled_at"],
                        component="execution",
                        event_id=ev_id,
                        decision_id=item["decision_id"],
                        target_id=item["target_id"],
                        execution_id=item["execution_id"],
                        policy_version=self.runtime.risk_engine.policy.policy_version,
                        cost_model_version=self.ledger.cost_model.name,
                        outcome="order_cancelled",
                        detail=item["reason"],
                    )
                    self.runtime.state_store.append_audit(rec)
                    await self.runtime.audit_sink.append(rec)

            # Immediate quote & protection evaluation pass
            await self.step_quotes_and_fills(stop_reason=shutdown_reason)
            await self.step_protection()

            # Phase 2: If an open position remains and handoff is not confirmed, drain with protection/quotes active
            open_qty, _ = await asyncio.to_thread(_open_position_state)
            if (
                open_qty != Decimal("0")
                and not self.confirm_handoff
                and self.exit_drain_timeout_seconds > 0.0
            ):
                if self.runtime.state_store is not None:
                    self.runtime.state_store.update_runner_state(
                        status="draining",
                        now=_utc(self.clock()),
                        stop_reason=shutdown_reason,
                    )
                loop = asyncio.get_running_loop()
                drain_start_mono = loop.time()
                drain_deadline_mono = drain_start_mono + float(self.exit_drain_timeout_seconds)
                poll_step = min(
                    self.quote_interval_seconds,
                    self.protection_interval_seconds,
                    0.05,
                )
                while open_qty != Decimal("0") and loop.time() < drain_deadline_mono:
                    await asyncio.sleep(max(0.01, poll_step))
                    await self.step_quotes_and_fills(stop_reason=shutdown_reason)
                    await self.step_protection()
                    open_qty, _ = await asyncio.to_thread(_open_position_state)
                drain_elapsed_seconds = round(max(0.0, loop.time() - drain_start_mono), 4)

            # Phase 3: Stop quote & protection workers after flat exit, handoff confirmation, or drain timeout
            self._drain_stop_event.set()
            for t in (protection_task, quote_task):
                t.cancel()
            await asyncio.gather(protection_task, quote_task, return_exceptions=True)

        open_qty, pending_exit = await asyncio.to_thread(_open_position_state)
        if open_qty == Decimal("0"):
            shutdown_status = "flat_completed"
            unclosed_risk = False
        elif self.confirm_handoff:
            shutdown_status = "handoff_confirmed_open_position"
            unclosed_risk = True
        else:
            shutdown_status = "unclosed_risk_requires_recovery"
            unclosed_risk = True
            await asyncio.to_thread(
                self.ledger.mark_shutdown_unclosed_risk,
                now=_utc(self.clock()),
                reason=(
                    f"exit_timeout_on_{shutdown_reason}_open_qty_{open_qty}"
                    f"_pending_{pending_exit or 'none'}"
                ),
            )

        stopped_at = _utc(self.clock())
        protection_coverage = self._finalize_protection_coverage(stopped_at)
        if self.runtime.state_store is not None:
            self.runtime.state_store.update_runner_state(
                status="stopped",
                now=stopped_at,
                stopped_at=stopped_at,
                stop_reason=shutdown_reason,
            )
        market = await asyncio.to_thread(self._current_market)
        ledger_rep = await asyncio.to_thread(self.ledger.report, market=market)
        runtime_rep = (
            self.runtime.state_store.summary_report()
            if self.runtime.state_store is not None
            else {}
        )
        summary = {
            "scope": "automated_shadow_loop_only",
            "execution_authorized": False,
            "started_at": started_at.isoformat(),
            "stopped_at": stopped_at.isoformat(),
            "duration_seconds": duration_seconds,
            "stop_reason": shutdown_reason,
            "shutdown_status": shutdown_status,
            "unclosed_risk": unclosed_risk,
            "open_quantity_eur": ledger_rep["quantity_eur"],
            "open_quantity_lots": ledger_rep["quantity_lots"],
            "pending_exit_reason": ledger_rep["pending_exit_reason"],
            "pending_order_ids": ledger_rep["snapshot"]["pending_order_ids"],
            "alerts": ledger_rep["alerts"],
            "recovery_instructions": ledger_rep["recovery_instructions"],
            "protection_coverage": protection_coverage,
            "worker_shutdown_verification": {
                "event_worker_stopped_before_drain": event_worker_stopped_before_drain,
                "quote_worker_alive_during_drain": quote_worker_alive_during_drain,
                "protection_worker_alive_during_drain": protection_worker_alive_during_drain,
                "drain_elapsed_seconds": drain_elapsed_seconds,
                "close_on_stop": self.close_on_stop,
                "confirm_handoff": self.confirm_handoff,
            },
            "observer": (
                await asyncio.to_thread(self.observer_store.report)
                if self.observer_store is not None
                else None
            ),
            "quotes": (
                await asyncio.to_thread(self.quote_store.report)
                if self.quote_store is not None
                else None
            ),
            "quote_bridge_counts": (
                dict(self._quote_bridge_counts) if self.quote_bridge_dir is not None else None
            ),
            "ledger": ledger_rep,
            "runtime": runtime_rep,
        }
        if self.runtime.state_store is not None:
            self.runtime.state_store.update_runner_state(
                status="stopped",
                now=stopped_at,
                stopped_at=stopped_at,
                summary=summary,
            )
        return summary
