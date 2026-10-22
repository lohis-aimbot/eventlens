"""Shadow-only boundary and causal SQLite shadow ledger; no broker SDK or live orders."""

import asyncio
import json
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal, Protocol, Self
from uuid import uuid4

from pydantic import Field, model_validator

from ..contracts import (
    AgentDecision,
    Contract,
    ExecutionReceipt,
    Identifier,
    MarketSnapshot,
    PortfolioSnapshot,
    Position,
    Quote,
    RiskAssessment,
    TargetPortfolio,
    Timestamp,
)
from ..quotes import QuoteQualityPolicy, validate_market_quote
from .portfolio import classify_exposure_change
from .risk import RiskPolicy, is_in_daily_close_window, trading_day_window

BPS = Decimal("10000")
EURUSD_PIP = Decimal("0.0001")
APPLICATION_ID = 0x45565348

OrderLifecycleStatus = Literal["pending", "filled", "expired", "cancelled", "rejected"]


class ShadowExecutor(Protocol):
    async def record(self, decision: AgentDecision, assessment: RiskAssessment) -> ExecutionReceipt:
        """Only accept a matching, unexpired approval issued by the trusted risk gate.

        Deduplicate by decision/target ID. A target is not a fill; execution receipts
        do not overwrite positions. A future shadow ledger must model fills separately.
        Implementations must reject changed targets or stale portfolio versions.
        """
        ...


class ShadowCostModel(Contract):
    """Transaction cost model for EUR/USD shadow accounting.

    Spread floor defaults to 0 (disabled) so bad/zero-spread quotes are never
    silently converted into valid real quotes. If spread_floor_pips > 0 is used
    for synthetic sensitivity analysis, allow_synthetic_spread_floor must be True.
    """

    name: Identifier = "pepperstone-razor-conservative-v2"
    commission_bps: Decimal = Field(default=Decimal("0.3"), ge=0, le=100)
    slippage_bps: Decimal = Field(default=Decimal("0.25"), ge=0, le=100)
    spread_floor_pips: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    allow_synthetic_spread_floor: bool = False
    fixed_fee_usd: Decimal = Field(default=Decimal("0"), ge=0)

    @model_validator(mode="after")
    def validate_synthetic_floor(self) -> Self:
        if self.spread_floor_pips > Decimal("0") and not self.allow_synthetic_spread_floor:
            raise ValueError(
                "spread_floor_pips > 0 requires allow_synthetic_spread_floor=True and marks fills as synthetic"
            )
        return self

    def execution_price(self, quote: Quote, *, buy: bool) -> tuple[Decimal, bool]:
        if quote.ask < quote.bid:
            raise ValueError("Crossed quote rejected")
        if quote.ask == quote.bid and not self.allow_synthetic_spread_floor:
            raise ValueError("Zero-spread quote cannot be filled without explicit synthetic opt-in")
        mid = (quote.bid + quote.ask) / Decimal("2")
        synthetic_used = False
        effective_ask = quote.ask
        effective_bid = quote.bid
        if self.spread_floor_pips > Decimal("0") and self.allow_synthetic_spread_floor:
            half_floor = self.spread_floor_pips * EURUSD_PIP / Decimal("2")
            if mid + half_floor > effective_ask or mid - half_floor < effective_bid:
                synthetic_used = True
            effective_ask = max(effective_ask, mid + half_floor)
            effective_bid = min(effective_bid, mid - half_floor)
        if effective_bid <= Decimal("0"):
            raise ValueError("Effective bid is nonpositive")
        raw = (
            effective_ask * (Decimal("1") + self.slippage_bps / BPS)
            if buy
            else effective_bid * (Decimal("1") - self.slippage_bps / BPS)
        )
        return raw.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP), synthetic_used

    def fee_usd(self, units: Decimal, price: Decimal) -> Decimal:
        notional_usd = abs(units) * price
        fee = notional_usd * self.commission_bps / BPS + self.fixed_fee_usd
        return fee.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class ProtectiveExitPolicy(Contract):
    """Deterministic exit and daily protection rules operating without the LLM."""

    max_holding_seconds: int = Field(default=1800, gt=0)
    max_position_loss_usd: Decimal = Field(default=Decimal("150"), gt=0)
    max_daily_loss_usd: Decimal = Field(default=Decimal("200"), gt=0)
    max_quote_age_ms: int = Field(default=2000, gt=0)
    max_spread_pips: Decimal = Field(default=Decimal("2.0"), gt=0)
    allow_zero_spread: bool = False
    require_verified_quote_provenance: bool = False
    trading_timezone: str = "America/New_York"
    daily_close_hour: int = Field(default=16, ge=0, le=23)
    daily_close_minute: int = Field(default=55, ge=0, le=59)
    rollover_hour: int = Field(default=17, ge=0, le=23)
    rollover_minute: int = Field(default=0, ge=0, le=59)
    daily_close_utc_hour: int | None = Field(default=None, ge=0, le=23)

    def quote_quality_policy(self, instrument: str = "EUR/USD") -> QuoteQualityPolicy:
        return QuoteQualityPolicy(
            instrument=instrument,
            max_quote_age_ms=self.max_quote_age_ms,
            max_spread_pips=self.max_spread_pips,
            allow_zero_spread=self.allow_zero_spread,
            require_verified_provenance=self.require_verified_quote_provenance,
        )

    def effective_window(self, ts: datetime) -> tuple[str, datetime, datetime, datetime]:
        if self.daily_close_utc_hour is not None:
            rollover_h = (self.daily_close_utc_hour + 1) % 24
            return trading_day_window(
                ts,
                tz_name="UTC",
                close_hour=self.daily_close_utc_hour,
                close_minute=0,
                rollover_hour=rollover_h,
                rollover_minute=0,
            )
        return trading_day_window(
            ts,
            tz_name=self.trading_timezone,
            close_hour=self.daily_close_hour,
            close_minute=self.daily_close_minute,
            rollover_hour=self.rollover_hour,
            rollover_minute=self.rollover_minute,
        )


class ShadowFill(Contract):
    fill_id: Identifier
    execution_id: Identifier | None = None
    decision_id: Identifier | None = None
    target_id: Identifier | None = None
    instrument: Identifier = "EUR/USD"
    kind: Literal["target_fill", "protective_exit"]
    reason: Identifier
    filled_at: Timestamp
    quote_source_at: Timestamp | None = None
    quote_received_at: Timestamp
    quote_available_at: Timestamp | None = None
    prior_quantity: Decimal
    delta_quantity: Decimal
    new_quantity: Decimal
    fill_price: Decimal = Field(gt=0)
    commission_usd: Decimal = Field(ge=0)
    realized_pnl_usd: Decimal
    equity_after_usd: Decimal = Field(gt=0)
    snapshot_id_after: Identifier
    synthetic_assumption: bool = False

    @model_validator(mode="after")
    def valid_fill(self) -> Self:
        if self.prior_quantity + self.delta_quantity != self.new_quantity:
            raise ValueError("Fill quantities do not reconcile")
        if self.filled_at < self.quote_received_at:
            raise ValueError("Fill cannot precede its market quote receipt")
        if self.quote_source_at is not None and self.filled_at < self.quote_source_at:
            raise ValueError("Fill cannot precede its market quote source timestamp")
        return self


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Shadow ledger requires timezone-aware timestamps")
    return value.astimezone(UTC)


class SQLiteShadowLedger:
    """Durable shadow execution and accounting ledger implementing PortfolioState and ShadowExecutor."""

    def __init__(
        self,
        path: Path,
        *,
        initial_cash_usd: Decimal = Decimal("10000"),
        cost_model: ShadowCostModel = ShadowCostModel(),
        exit_policy: ProtectiveExitPolicy = ProtectiveExitPolicy(),
        risk_policy: RiskPolicy = RiskPolicy(),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if str(path) == ":memory:":
            raise ValueError("Use a filesystem path for SQLiteShadowLedger")
        if initial_cash_usd <= Decimal("0"):
            raise ValueError("Initial cash must be positive")
        self.path = path.resolve()
        self.initial_cash_usd = initial_cash_usd
        self.cost_model = cost_model
        self.exit_policy = exit_policy
        self.risk_policy = risk_policy
        self.clock = clock
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        try:
            with self._lock:
                app_id = self.db.execute("PRAGMA application_id").fetchone()[0]
                tables = self.db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
                if app_id not in {0, APPLICATION_ID} or (app_id == 0 and tables):
                    raise ValueError("Not an EventLens shadow ledger database")
                self.db.execute("PRAGMA foreign_keys=ON")
                self.db.execute("PRAGMA synchronous=FULL")
                self.db.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS shadow_account (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        snapshot_id TEXT NOT NULL,
                        as_of TEXT NOT NULL,
                        cash_usd TEXT NOT NULL,
                        realized_pnl_usd TEXT NOT NULL,
                        total_fees_usd TEXT NOT NULL,
                        quantity_eur TEXT NOT NULL,
                        avg_entry_price TEXT,
                        opened_at TEXT,
                        pending_exit_reason TEXT,
                        pending_exit_since TEXT
                    );
                    CREATE TABLE IF NOT EXISTS shadow_daily_state (
                        trading_day TEXT PRIMARY KEY,
                        session_start_utc TEXT NOT NULL,
                        daily_close_utc TEXT NOT NULL,
                        session_rollover_utc TEXT NOT NULL,
                        baseline_equity_usd TEXT NOT NULL,
                        min_equity_seen_usd TEXT NOT NULL,
                        loss_limit_triggered INTEGER NOT NULL DEFAULT 0,
                        triggered_at TEXT,
                        trigger_reason TEXT
                    );
                    CREATE TABLE IF NOT EXISTS shadow_receipts (
                        execution_id TEXT PRIMARY KEY,
                        decision_id TEXT NOT NULL UNIQUE,
                        target_id TEXT NOT NULL UNIQUE,
                        assessment_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        order_status TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        terminal_at TEXT,
                        terminal_reason TEXT,
                        decision_json TEXT NOT NULL,
                        assessment_json TEXT NOT NULL,
                        receipt_json TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS shadow_fills (
                        fill_id TEXT PRIMARY KEY,
                        execution_id TEXT,
                        filled_at TEXT NOT NULL,
                        fill_json TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS shadow_faults (
                        fault_id TEXT PRIMARY KEY,
                        occurred_at TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        detail TEXT NOT NULL
                    );
                    """
                )
                self._migrate_columns_if_needed()
                self.db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                self.db.execute("PRAGMA user_version=2")
                row = self.db.execute("SELECT 1 FROM shadow_account WHERE id=1").fetchone()
                if row is None:
                    now = _utc(self.clock())
                    self.db.execute(
                        """INSERT INTO shadow_account
                        (id, snapshot_id, as_of, cash_usd, realized_pnl_usd, total_fees_usd,
                         quantity_eur, avg_entry_price, opened_at, pending_exit_reason, pending_exit_since)
                        VALUES (1, ?, ?, ?, '0.00', '0.00', '0', NULL, NULL, NULL, NULL)""",
                        ("shadow-snapshot-0", now.isoformat(), str(initial_cash_usd)),
                    )
                    self._ensure_daily_state_locked(now, initial_cash_usd)
                self.db.commit()
        except BaseException:
            self.db.close()
            raise

    def _migrate_columns_if_needed(self) -> None:
        acct_cols = {
            r["name"] for r in self.db.execute("PRAGMA table_info(shadow_account)").fetchall()
        }
        if "pending_exit_reason" not in acct_cols:
            self.db.execute("ALTER TABLE shadow_account ADD COLUMN pending_exit_reason TEXT")
        if "pending_exit_since" not in acct_cols:
            self.db.execute("ALTER TABLE shadow_account ADD COLUMN pending_exit_since TEXT")
        rcpt_cols = {
            r["name"] for r in self.db.execute("PRAGMA table_info(shadow_receipts)").fetchall()
        }
        if "order_status" not in rcpt_cols:
            self.db.execute(
                "ALTER TABLE shadow_receipts ADD COLUMN order_status TEXT NOT NULL DEFAULT 'pending'"
            )
            self.db.execute("ALTER TABLE shadow_receipts ADD COLUMN terminal_at TEXT")
            self.db.execute("ALTER TABLE shadow_receipts ADD COLUMN terminal_reason TEXT")
            if "filled" in rcpt_cols:
                self.db.execute(
                    """UPDATE shadow_receipts
                    SET order_status = CASE
                        WHEN status = 'rejected' THEN 'rejected'
                        WHEN filled = 1 THEN 'filled'
                        ELSE 'pending'
                    END"""
                )

    def close(self) -> None:
        with self._lock:
            self.db.close()

    def _record_fault_locked(self, now: datetime, stage: str, reason: str, detail: str) -> None:
        self.db.execute(
            "INSERT INTO shadow_faults VALUES (?, ?, ?, ?, ?)",
            (str(uuid4()), now.isoformat(), stage, reason, detail[:1000]),
        )

    def _ensure_daily_state_locked(
        self, now: datetime, current_equity: Decimal, *, valid_valuation: bool = True
    ) -> sqlite3.Row:
        day_id, start_utc, close_utc, rollover_utc = self.exit_policy.effective_window(now)
        row = self.db.execute(
            "SELECT * FROM shadow_daily_state WHERE trading_day=?", (day_id,)
        ).fetchone()
        if row is None:
            self.db.execute(
                """INSERT INTO shadow_daily_state
                (trading_day, session_start_utc, daily_close_utc, session_rollover_utc,
                 baseline_equity_usd, min_equity_seen_usd, loss_limit_triggered, triggered_at, trigger_reason)
                VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL)""",
                (
                    day_id,
                    start_utc.isoformat(),
                    close_utc.isoformat(),
                    rollover_utc.isoformat(),
                    str(current_equity),
                    str(current_equity),
                ),
            )
            row = self.db.execute(
                "SELECT * FROM shadow_daily_state WHERE trading_day=?", (day_id,)
            ).fetchone()
            assert isinstance(row, sqlite3.Row)
            return row

        if valid_valuation:
            baseline = Decimal(row["baseline_equity_usd"])
            min_seen = min(Decimal(row["min_equity_seen_usd"]), current_equity)
            daily_pnl = current_equity - baseline
            triggered = bool(row["loss_limit_triggered"])
            triggered_at = row["triggered_at"]
            trigger_reason = row["trigger_reason"]
            limit = min(self.exit_policy.max_daily_loss_usd, self.risk_policy.max_daily_loss_usd)
            if not triggered and daily_pnl <= -limit:
                triggered = True
                triggered_at = now.isoformat()
                trigger_reason = "daily_loss_limit_breached"
            self.db.execute(
                """UPDATE shadow_daily_state
                SET min_equity_seen_usd=?, loss_limit_triggered=?, triggered_at=?, trigger_reason=?
                WHERE trading_day=?""",
                (str(min_seen), int(triggered), triggered_at, trigger_reason, day_id),
            )
            row = self.db.execute(
                "SELECT * FROM shadow_daily_state WHERE trading_day=?", (day_id,)
            ).fetchone()
        assert isinstance(row, sqlite3.Row)
        return row

    async def snapshot(self, *, market: MarketSnapshot | None = None) -> PortfolioSnapshot:
        return await asyncio.to_thread(self.snapshot_sync, market=market)

    def snapshot_sync(self, *, market: MarketSnapshot | None = None) -> PortfolioSnapshot:
        with self._lock:
            now = _utc(self.clock())
            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            pending_rows = self.db.execute(
                "SELECT execution_id FROM shadow_receipts WHERE order_status='pending' ORDER BY rowid"
            ).fetchall()
            qty = Decimal(account["quantity_eur"])
            cash = Decimal(account["cash_usd"])
            realized = Decimal(account["realized_pnl_usd"])
            fees = Decimal(account["total_fees_usd"])
            entry = (
                Decimal(account["avg_entry_price"]) if account["avg_entry_price"] else Decimal("0")
            )
            unrealized = Decimal("0.00")
            valuation_valid = True
            valuation_reason = "flat_no_quote_needed"

            if qty != Decimal("0"):
                quote_check = validate_market_quote(
                    market,
                    as_of=now,
                    policy=self.exit_policy.quote_quality_policy("EUR/USD"),
                )
                if quote_check.valid and quote_check.quote is not None:
                    mark = quote_check.quote.bid if qty > 0 else quote_check.quote.ask
                    unrealized = (qty * (mark - entry)).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                    valuation_valid = True
                    valuation_reason = "marked_to_valid_quote"
                else:
                    valuation_valid = False
                    valuation_reason = f"unreliable_quote:{quote_check.reason}"

            equity = cash + unrealized
            with self.db:
                daily_row = self._ensure_daily_state_locked(
                    now, equity, valid_valuation=valuation_valid
                )
            baseline = Decimal(daily_row["baseline_equity_usd"])
            daily_pnl = (equity - baseline).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            positions = (
                (Position(instrument="EUR/USD", quantity=qty),) if qty != Decimal("0") else ()
            )
            acct_as_of = datetime.fromisoformat(account["as_of"])
            as_of = max(
                acct_as_of,
                market.as_of if market is not None and market.as_of <= now else acct_as_of,
            )
            return PortfolioSnapshot(
                snapshot_id=account["snapshot_id"],
                as_of=as_of,
                base_currency="USD",
                equity=equity,
                positions=positions,
                pending_order_ids=tuple(row["execution_id"] for row in pending_rows),
                unrealized_pnl_usd=unrealized,
                realized_pnl_usd=realized,
                total_fees_usd=fees,
                daily_pnl_usd=daily_pnl,
                daily_loss_limit_triggered=bool(daily_row["loss_limit_triggered"]),
                pending_exit_reason=account["pending_exit_reason"],
                valuation_valid=valuation_valid,
                valuation_reason=valuation_reason,
            )

    async def record(self, decision: AgentDecision, assessment: RiskAssessment) -> ExecutionReceipt:
        return await asyncio.to_thread(self.record_sync, decision, assessment)

    def record_sync(self, decision: AgentDecision, assessment: RiskAssessment) -> ExecutionReceipt:
        decision = AgentDecision.model_validate_json(decision.model_dump_json())
        assessment = RiskAssessment.model_validate_json(assessment.model_dump_json())
        now = _utc(self.clock())
        if decision.target is None:
            raise ValueError("ShadowExecutor requires a decision with a non-null TargetPortfolio")
        target = decision.target
        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            existing = self.db.execute(
                "SELECT decision_json, assessment_json, receipt_json FROM shadow_receipts WHERE decision_id=? OR target_id=?",
                (decision.decision_id, target.target_id),
            ).fetchone()
            if existing is not None:
                if (
                    existing["decision_json"] != decision.model_dump_json()
                    or existing["assessment_json"] != assessment.model_dump_json()
                ):
                    raise ValueError("Conflicting decision or target ID in shadow executor")
                return ExecutionReceipt.model_validate_json(existing["receipt_json"])

            self._expire_pending_orders_locked(now)
            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            pending = self.db.execute(
                "SELECT COUNT(*) FROM shadow_receipts WHERE order_status='pending'"
            ).fetchone()[0]

            rejection_reason: str | None = None
            if assessment.outcome != "approve":
                rejection_reason = "risk_assessment_not_approved"
            elif (
                assessment.decision_id != decision.decision_id
                or assessment.target_id != target.target_id
                or assessment.checked_snapshot_id != decision.based_on_snapshot_id
            ):
                rejection_reason = "assessment_decision_target_mismatch"
            elif now >= assessment.valid_until or now >= decision.valid_until:
                rejection_reason = "approval_or_decision_expired"
            elif assessment.checked_snapshot_id != account["snapshot_id"]:
                rejection_reason = "stale_portfolio_snapshot"
            elif pending > 0:
                rejection_reason = "pending_shadow_order_exists"

            status: Literal["recorded", "rejected"] = (
                "rejected" if rejection_reason is not None else "recorded"
            )
            order_status: OrderLifecycleStatus = (
                "rejected" if rejection_reason is not None else "pending"
            )
            reason = rejection_reason or "approved_target_recorded_pending_fill"
            receipt = ExecutionReceipt(
                execution_id=str(uuid4()),
                decision_id=decision.decision_id,
                target_id=target.target_id,
                assessment_id=assessment.assessment_id,
                mode="shadow",
                status=status,
                recorded_at=now,
                reason=reason,
            )
            self.db.execute(
                """INSERT INTO shadow_receipts
                (execution_id, decision_id, target_id, assessment_id, status, order_status,
                 recorded_at, terminal_at, terminal_reason, decision_json, assessment_json, receipt_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    receipt.execution_id,
                    decision.decision_id,
                    target.target_id,
                    assessment.assessment_id,
                    receipt.status,
                    order_status,
                    now.isoformat(),
                    now.isoformat() if order_status == "rejected" else None,
                    rejection_reason,
                    decision.model_dump_json(),
                    assessment.model_dump_json(),
                    receipt.model_dump_json(),
                ),
            )
            return receipt

    def expire_pending_orders(self, *, now: datetime | None = None) -> int:
        """Mark any expired pending orders as 'expired' without faking fills."""
        check_now = _utc(now or self.clock())
        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            return self._expire_pending_orders_locked(check_now)

    def _expire_pending_orders_locked(self, now: datetime) -> int:
        rows = self.db.execute(
            "SELECT * FROM shadow_receipts WHERE order_status='pending' ORDER BY rowid"
        ).fetchall()
        expired_count = 0
        for row in rows:
            decision = AgentDecision.model_validate_json(row["decision_json"])
            assessment = RiskAssessment.model_validate_json(row["assessment_json"])
            reason: str | None = None
            if now >= decision.valid_until:
                reason = "decision_expired_before_fill"
            elif now >= assessment.valid_until:
                reason = "risk_approval_expired_before_fill"
            if reason is not None:
                self.db.execute(
                    """UPDATE shadow_receipts
                    SET order_status='expired', terminal_at=?, terminal_reason=?
                    WHERE execution_id=?""",
                    (now.isoformat(), reason, row["execution_id"]),
                )
                self._record_fault_locked(
                    now,
                    "order_expiry",
                    reason,
                    f"execution_id={row['execution_id']} decision_id={decision.decision_id}",
                )
                expired_count += 1
        return expired_count

    def fill_pending(
        self,
        market: MarketSnapshot | None,
        *,
        risk_policy: RiskPolicy | None = None,
    ) -> tuple[ShadowFill, ...]:
        """Re-verify expiry, risk policy, daily limits, and quote quality before filling pending orders."""
        if market is not None:
            market = MarketSnapshot.model_validate_json(market.model_dump_json())
        now = _utc(self.clock())
        active_risk = risk_policy or self.risk_policy
        fills: list[ShadowFill] = []

        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            self._expire_pending_orders_locked(now)
            rows = self.db.execute(
                "SELECT * FROM shadow_receipts WHERE order_status='pending' ORDER BY rowid"
            ).fetchall()
            if not rows:
                return ()

            quote_check = validate_market_quote(
                market,
                as_of=now,
                policy=active_risk.quote_quality_policy("EUR/USD"),
            )
            if not quote_check.valid or quote_check.quote is None:
                self._record_fault_locked(
                    now,
                    "fill_pending_quote_rejected",
                    quote_check.reason,
                    f"pending_count={len(rows)}",
                )
                return ()

            quote = quote_check.quote
            for row in rows:
                recorded_at = datetime.fromisoformat(row["recorded_at"])
                decision = AgentDecision.model_validate_json(row["decision_json"])
                assessment = RiskAssessment.model_validate_json(row["assessment_json"])
                assert decision.target is not None
                target: TargetPortfolio = decision.target

                if now >= decision.valid_until or now >= assessment.valid_until:
                    self._expire_pending_orders_locked(now)
                    continue

                # Causal check: quote receipt and availability must not precede decision completion or receipt recording
                min_causal = max(decision.completed_at, recorded_at)
                if (
                    quote.received_at < min_causal
                    or (quote.available_at is not None and quote.available_at < min_causal)
                    or quote.effective_source_at < decision.completed_at
                ):
                    continue

                account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
                if decision.based_on_snapshot_id != account["snapshot_id"]:
                    self.db.execute(
                        """UPDATE shadow_receipts
                        SET order_status='cancelled', terminal_at=?, terminal_reason=?
                        WHERE execution_id=?""",
                        (
                            now.isoformat(),
                            "stale_portfolio_snapshot_at_fill",
                            row["execution_id"],
                        ),
                    )
                    self._record_fault_locked(
                        now,
                        "fill_pending_cancelled",
                        "stale_portfolio_snapshot_at_fill",
                        f"execution_id={row['execution_id']}",
                    )
                    continue

                pos_target = next((p for p in target.positions if p.instrument == "EUR/USD"), None)
                weight = pos_target.weight if pos_target is not None else Decimal("0")
                current_qty = Decimal(account["quantity_eur"])
                cash = Decimal(account["cash_usd"])
                entry = (
                    Decimal(account["avg_entry_price"])
                    if account["avg_entry_price"]
                    else Decimal("0")
                )
                mid = (quote.bid + quote.ask) / Decimal("2")
                mark = quote.bid if current_qty > 0 else quote.ask
                unrealized = (
                    (current_qty * (mark - entry)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    if current_qty != Decimal("0")
                    else Decimal("0.00")
                )
                equity = cash + unrealized
                daily_row = self._ensure_daily_state_locked(now, equity, valid_valuation=True)
                daily_pnl = equity - Decimal(daily_row["baseline_equity_usd"])

                change_kind = classify_exposure_change(current_qty, equity, mid, weight)
                increases_risk = change_kind in ("risk_increasing", "reversal_risk_increasing")

                reject_at_fill: str | None = None
                if increases_risk:
                    if active_risk.kill_switch_active:
                        reject_at_fill = (
                            "reversal_blocked_by_kill_switch_at_fill"
                            if change_kind == "reversal_risk_increasing"
                            else "kill_switch_active_at_fill"
                        )
                    elif account["pending_exit_reason"] is not None:
                        reject_at_fill = "protective_exit_pending_at_fill"
                    elif (
                        bool(daily_row["loss_limit_triggered"])
                        or daily_pnl <= -active_risk.max_daily_loss_usd
                    ):
                        reject_at_fill = "daily_loss_limit_exceeded_at_fill"
                    elif equity < active_risk.min_equity_usd:
                        reject_at_fill = "equity_below_minimum_floor_at_fill"
                    elif (
                        abs(weight) > active_risk.max_position_weight
                        or abs(weight) > active_risk.max_gross_weight
                    ):
                        reject_at_fill = "weight_limit_exceeded_at_fill"
                    elif is_in_daily_close_window(
                        now,
                        tz_name=active_risk.trading_timezone,
                        close_hour=active_risk.daily_close_hour,
                        close_minute=active_risk.daily_close_minute,
                        rollover_hour=active_risk.rollover_hour,
                        rollover_minute=active_risk.rollover_minute,
                    ):
                        reject_at_fill = "daily_close_cutoff_active_at_fill"

                if reject_at_fill is not None:
                    self.db.execute(
                        """UPDATE shadow_receipts
                        SET order_status='rejected', terminal_at=?, terminal_reason=?
                        WHERE execution_id=?""",
                        (now.isoformat(), reject_at_fill, row["execution_id"]),
                    )
                    self._record_fault_locked(
                        now,
                        "fill_pending_rejected",
                        reject_at_fill,
                        f"execution_id={row['execution_id']} decision_id={decision.decision_id}",
                    )
                    continue

                desired_qty = (
                    Decimal("0")
                    if weight == Decimal("0")
                    else ((equity * weight) / mid).quantize(Decimal("1"), rounding=ROUND_DOWN)
                )
                delta = desired_qty - current_qty
                if delta == Decimal("0"):
                    self.db.execute(
                        """UPDATE shadow_receipts
                        SET order_status='cancelled', terminal_at=?, terminal_reason=?
                        WHERE execution_id=?""",
                        (
                            now.isoformat(),
                            "zero_quantity_delta_at_fill",
                            row["execution_id"],
                        ),
                    )
                    continue

                fill = self._apply_delta_locked(
                    delta=delta,
                    quote=quote,
                    filled_at=max(now, quote.received_at),
                    kind="target_fill",
                    reason="recorded_shadow_target_filled",
                    execution_id=row["execution_id"],
                    decision_id=decision.decision_id,
                    target_id=target.target_id,
                )
                self.db.execute(
                    """UPDATE shadow_receipts
                    SET order_status='filled', terminal_at=?, terminal_reason=?
                    WHERE execution_id=?""",
                    (
                        fill.filled_at.isoformat(),
                        "recorded_shadow_target_filled",
                        row["execution_id"],
                    ),
                )
                fills.append(fill)
        return tuple(fills)

    def _is_daily_close_due(self, opened_at: datetime, check_ts: datetime) -> bool:
        _, _, open_close_utc, open_rollover_utc = self.exit_policy.effective_window(opened_at)
        if opened_at < open_close_utc and check_ts >= open_close_utc:
            return True
        if opened_at >= open_close_utc and check_ts >= open_rollover_utc:
            return True
        _, _, curr_close_utc, curr_rollover_utc = self.exit_policy.effective_window(check_ts)
        if curr_close_utc <= check_ts < curr_rollover_utc and opened_at < check_ts:
            return True
        return False

    def evaluate_protective_exits(
        self, market: MarketSnapshot | None = None
    ) -> tuple[ShadowFill, ...]:
        """Close open positions when deterministic timer, stop-loss, daily loss, or daily cutoff fires.

        When an exit trigger fires but no valid quote is available, persists pending_exit_reason,
        cancels risk-increasing pending orders, records a fault, and defers execution until a
        valid quote arrives. Never fabricates a fill or claims an exit completed without a quote.
        """
        if market is not None:
            market = MarketSnapshot.model_validate_json(market.model_dump_json())
        now = _utc(self.clock())
        check_ts = max(now, market.as_of) if market is not None and market.as_of <= now else now

        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            self._expire_pending_orders_locked(now)
            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            qty = Decimal(account["quantity_eur"])
            if qty == Decimal("0"):
                if account["pending_exit_reason"] is not None:
                    self.db.execute(
                        "UPDATE shadow_account SET pending_exit_reason=NULL, pending_exit_since=NULL WHERE id=1"
                    )
                return ()

            entry = Decimal(account["avg_entry_price"])
            cash = Decimal(account["cash_usd"])
            opened_at = datetime.fromisoformat(account["opened_at"])
            persisted_exit_reason: str | None = account["pending_exit_reason"]

            quote_check = validate_market_quote(
                market,
                as_of=now,
                policy=self.exit_policy.quote_quality_policy("EUR/USD"),
            )
            exit_reason: str | None = persisted_exit_reason

            if quote_check.valid and quote_check.quote is not None:
                quote = quote_check.quote
                mark = quote.bid if qty > 0 else quote.ask
                unrealized = (qty * (mark - entry)).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                equity = cash + unrealized
                daily_row = self._ensure_daily_state_locked(now, equity, valid_valuation=True)
                daily_pnl = equity - Decimal(daily_row["baseline_equity_usd"])
                if unrealized <= -self.exit_policy.max_position_loss_usd:
                    exit_reason = exit_reason or "protective_stop_loss"
                elif (
                    bool(daily_row["loss_limit_triggered"])
                    or daily_pnl <= -self.exit_policy.max_daily_loss_usd
                ):
                    exit_reason = exit_reason or "protective_daily_loss_limit"
            else:
                daily_row = self._ensure_daily_state_locked(now, cash, valid_valuation=False)
                if bool(daily_row["loss_limit_triggered"]):
                    exit_reason = exit_reason or "protective_daily_loss_limit"

            if exit_reason is None:
                if check_ts - opened_at >= timedelta(seconds=self.exit_policy.max_holding_seconds):
                    exit_reason = "protective_max_holding_horizon"
                elif self._is_daily_close_due(opened_at, check_ts):
                    exit_reason = "protective_daily_close"

            if exit_reason is None:
                if not quote_check.valid:
                    self._record_fault_locked(
                        now,
                        "protective_monitor",
                        "protective_monitor_quote_unavailable",
                        f"quote_reason={quote_check.reason} quantity_eur={qty}",
                    )
                return ()

            # Cancel any pending orders before executing or deferring protective exit
            self.db.execute(
                """UPDATE shadow_receipts
                SET order_status='cancelled', terminal_at=?, terminal_reason=?
                WHERE order_status='pending'""",
                (now.isoformat(), f"cancelled_by_{exit_reason}"),
            )

            if not quote_check.valid or quote_check.quote is None:
                pending_since = account["pending_exit_since"] or now.isoformat()
                self.db.execute(
                    """UPDATE shadow_account
                    SET pending_exit_reason=?, pending_exit_since=?
                    WHERE id=1""",
                    (exit_reason, pending_since),
                )
                self._record_fault_locked(
                    now,
                    "protective_exit",
                    "protective_exit_deferred_bad_quote",
                    f"exit_reason={exit_reason} quote_reason={quote_check.reason} quantity_eur={qty}",
                )
                return ()

            fill = self._apply_delta_locked(
                delta=-qty,
                quote=quote_check.quote,
                filled_at=max(now, quote_check.quote.received_at),
                kind="protective_exit",
                reason=exit_reason,
                execution_id=None,
                decision_id=None,
                target_id=None,
            )
            return (fill,)

    def _apply_delta_locked(
        self,
        *,
        delta: Decimal,
        quote: Quote,
        filled_at: datetime,
        kind: Literal["target_fill", "protective_exit"],
        reason: str,
        execution_id: str | None,
        decision_id: str | None,
        target_id: str | None,
    ) -> ShadowFill:
        account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
        prior_qty = Decimal(account["quantity_eur"])
        cash = Decimal(account["cash_usd"])
        realized_total = Decimal(account["realized_pnl_usd"])
        fees_total = Decimal(account["total_fees_usd"])
        prior_entry = (
            Decimal(account["avg_entry_price"]) if account["avg_entry_price"] else Decimal("0")
        )
        prior_opened = account["opened_at"]

        buy = delta > Decimal("0")
        fill_price, synthetic_used = self.cost_model.execution_price(quote, buy=buy)
        commission = self.cost_model.fee_usd(delta, fill_price)
        new_qty = prior_qty + delta
        realized_trade = Decimal("0.00")

        if prior_qty == Decimal("0"):
            new_entry: Decimal | None = fill_price
            new_opened: str | None = filled_at.isoformat()
        elif (prior_qty > Decimal("0") and delta > Decimal("0")) or (
            prior_qty < Decimal("0") and delta < Decimal("0")
        ):
            total_abs = abs(prior_qty) + abs(delta)
            weighted = (abs(prior_qty) * prior_entry + abs(delta) * fill_price) / total_abs
            new_entry = weighted.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            new_opened = prior_opened
        else:
            closed_units = min(abs(prior_qty), abs(delta))
            signed_closed = closed_units if prior_qty > Decimal("0") else -closed_units
            realized_trade = (signed_closed * (fill_price - prior_entry)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            if new_qty == Decimal("0"):
                new_entry = None
                new_opened = None
            elif (prior_qty > Decimal("0") and new_qty > Decimal("0")) or (
                prior_qty < Decimal("0") and new_qty < Decimal("0")
            ):
                new_entry = prior_entry
                new_opened = prior_opened
            else:
                # Position reversal: closed prior leg, opened remainder at fill_price
                new_entry = fill_price
                new_opened = filled_at.isoformat()

        new_cash = cash + realized_trade - commission
        new_realized = realized_total + realized_trade
        new_fees = fees_total + commission
        unrealized_after = Decimal("0.00")
        if new_qty != Decimal("0") and new_entry is not None:
            mark = quote.bid if new_qty > 0 else quote.ask
            unrealized_after = (new_qty * (mark - new_entry)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        equity_after = new_cash + unrealized_after
        self._ensure_daily_state_locked(filled_at, equity_after, valid_valuation=True)
        fill_count = self.db.execute("SELECT COUNT(*) FROM shadow_fills").fetchone()[0] + 1
        new_snapshot_id = f"shadow-snapshot-{fill_count}"

        self.db.execute(
            """
            UPDATE shadow_account
            SET snapshot_id=?, as_of=?, cash_usd=?, realized_pnl_usd=?, total_fees_usd=?,
                quantity_eur=?, avg_entry_price=?, opened_at=?,
                pending_exit_reason=CASE WHEN ? = '0' THEN NULL ELSE pending_exit_reason END,
                pending_exit_since=CASE WHEN ? = '0' THEN NULL ELSE pending_exit_since END
            WHERE id=1
            """,
            (
                new_snapshot_id,
                filled_at.isoformat(),
                str(new_cash),
                str(new_realized),
                str(new_fees),
                str(new_qty),
                str(new_entry) if new_entry is not None else None,
                new_opened,
                str(new_qty),
                str(new_qty),
            ),
        )
        fill = ShadowFill(
            fill_id=str(uuid4()),
            execution_id=execution_id,
            decision_id=decision_id,
            target_id=target_id,
            instrument="EUR/USD",
            kind=kind,
            reason=reason,
            filled_at=filled_at,
            quote_source_at=quote.source_at,
            quote_received_at=quote.received_at,
            quote_available_at=quote.available_at,
            prior_quantity=prior_qty,
            delta_quantity=delta,
            new_quantity=new_qty,
            fill_price=fill_price,
            commission_usd=commission,
            realized_pnl_usd=realized_trade,
            equity_after_usd=equity_after,
            snapshot_id_after=new_snapshot_id,
            synthetic_assumption=synthetic_used,
        )
        self.db.execute(
            "INSERT INTO shadow_fills VALUES (?, ?, ?, ?)",
            (fill.fill_id, execution_id, filled_at.isoformat(), fill.model_dump_json()),
        )
        return fill

    def report(self, *, market: MarketSnapshot | None = None) -> dict[str, Any]:
        with self._lock:
            snap = self.snapshot_sync(market=market)
            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            receipt_rows = self.db.execute(
                "SELECT * FROM shadow_receipts ORDER BY rowid"
            ).fetchall()
            receipts = []
            order_status_counts: dict[str, int] = {}
            for r in receipt_rows:
                item = json.loads(r["receipt_json"])
                item["order_status"] = r["order_status"]
                item["terminal_at"] = r["terminal_at"]
                item["terminal_reason"] = r["terminal_reason"]
                order_status_counts[r["order_status"]] = (
                    order_status_counts.get(r["order_status"], 0) + 1
                )
                receipts.append(item)
            fills = [
                json.loads(row["fill_json"])
                for row in self.db.execute("SELECT fill_json FROM shadow_fills ORDER BY rowid")
            ]
            faults = [
                dict(row) for row in self.db.execute("SELECT * FROM shadow_faults ORDER BY rowid")
            ]
            daily_states = [
                dict(row)
                for row in self.db.execute("SELECT * FROM shadow_daily_state ORDER BY trading_day")
            ]
            return {
                "scope": "shadow_ledger_only",
                "execution_authorized": False,
                "cost_model": self.cost_model.model_dump(mode="json"),
                "exit_policy": self.exit_policy.model_dump(mode="json"),
                "risk_policy": self.risk_policy.model_dump(mode="json"),
                "snapshot": snap.model_dump(mode="json"),
                "cash_usd": account["cash_usd"],
                "realized_pnl_usd": account["realized_pnl_usd"],
                "total_fees_usd": account["total_fees_usd"],
                "quantity_eur": account["quantity_eur"],
                "avg_entry_price": account["avg_entry_price"],
                "opened_at": account["opened_at"],
                "pending_exit_reason": account["pending_exit_reason"],
                "pending_exit_since": account["pending_exit_since"],
                "receipt_count": len(receipts),
                "order_status_counts": order_status_counts,
                "fill_count": len(fills),
                "fault_count": len(faults),
                "daily_states": daily_states,
                "receipts": receipts,
                "fills": fills,
                "faults": faults,
            }
