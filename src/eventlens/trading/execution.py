"""Shadow-only boundary and causal SQLite shadow ledger; no broker SDK or live orders."""

import asyncio
import json
import sqlite3
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

BPS = Decimal("10000")
EURUSD_PIP = Decimal("0.0001")
APPLICATION_ID = 0x45565348


class ShadowExecutor(Protocol):
    async def record(self, decision: AgentDecision, assessment: RiskAssessment) -> ExecutionReceipt:
        """Only accept a matching, unexpired approval issued by the trusted risk gate.

        Deduplicate by decision/target ID. A target is not a fill; execution receipts
        do not overwrite positions. A future shadow ledger must model fills separately.
        Implementations must reject changed targets or stale portfolio versions.
        """
        ...


class ShadowCostModel(Contract):
    """Conservative transaction cost model for EUR/USD shadow accounting."""

    name: Identifier = "pepperstone-razor-conservative-v1"
    commission_bps: Decimal = Field(default=Decimal("0.3"), ge=0, le=100)
    slippage_bps: Decimal = Field(default=Decimal("0.25"), ge=0, le=100)
    spread_floor_pips: Decimal = Field(default=Decimal("0.1"), ge=0, le=100)
    fixed_fee_usd: Decimal = Field(default=Decimal("0"), ge=0)

    def execution_price(self, quote: Quote, *, buy: bool) -> Decimal:
        mid = (quote.bid + quote.ask) / Decimal("2")
        half_floor = self.spread_floor_pips * EURUSD_PIP / Decimal("2")
        effective_ask = max(quote.ask, mid + half_floor)
        effective_bid = min(quote.bid, mid - half_floor)
        if effective_bid <= Decimal("0"):
            raise ValueError("Spread floor produced a nonpositive bid")
        raw = (
            effective_ask * (Decimal("1") + self.slippage_bps / BPS)
            if buy
            else effective_bid * (Decimal("1") - self.slippage_bps / BPS)
        )
        return raw.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

    def fee_usd(self, units: Decimal, price: Decimal) -> Decimal:
        notional_usd = abs(units) * price
        fee = notional_usd * self.commission_bps / BPS + self.fixed_fee_usd
        return fee.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class ProtectiveExitPolicy(Contract):
    """Deterministic exit rules operating without the LLM."""

    max_holding_seconds: int = Field(default=1800, gt=0)
    max_position_loss_usd: Decimal = Field(default=Decimal("150"), gt=0)
    max_quote_age_ms: int = Field(default=2000, gt=0)
    daily_close_utc_hour: int | None = Field(default=21, ge=0, le=23)


class ShadowFill(Contract):
    fill_id: Identifier
    execution_id: Identifier | None = None
    decision_id: Identifier | None = None
    target_id: Identifier | None = None
    instrument: Identifier = "EUR/USD"
    kind: Literal["target_fill", "protective_exit"]
    reason: Identifier
    filled_at: Timestamp
    quote_received_at: Timestamp
    prior_quantity: Decimal
    delta_quantity: Decimal
    new_quantity: Decimal
    fill_price: Decimal = Field(gt=0)
    commission_usd: Decimal = Field(ge=0)
    realized_pnl_usd: Decimal
    equity_after_usd: Decimal = Field(gt=0)
    snapshot_id_after: Identifier

    @model_validator(mode="after")
    def valid_fill(self) -> Self:
        if self.prior_quantity + self.delta_quantity != self.new_quantity:
            raise ValueError("Fill quantities do not reconcile")
        if self.filled_at < self.quote_received_at:
            raise ValueError("Fill cannot precede its market quote receipt")
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
        self.clock = clock
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        try:
            app_id = self.db.execute("PRAGMA application_id").fetchone()[0]
            tables = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
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
                    opened_at TEXT
                );
                CREATE TABLE IF NOT EXISTS shadow_receipts (
                    execution_id TEXT PRIMARY KEY,
                    decision_id TEXT NOT NULL UNIQUE,
                    target_id TEXT NOT NULL UNIQUE,
                    assessment_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    decision_json TEXT NOT NULL,
                    assessment_json TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    filled INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS shadow_fills (
                    fill_id TEXT PRIMARY KEY,
                    execution_id TEXT,
                    filled_at TEXT NOT NULL,
                    fill_json TEXT NOT NULL
                );
                """
            )
            self.db.execute(f"PRAGMA application_id={APPLICATION_ID}")
            self.db.execute("PRAGMA user_version=1")
            row = self.db.execute("SELECT 1 FROM shadow_account WHERE id=1").fetchone()
            if row is None:
                now = _utc(self.clock()).isoformat()
                self.db.execute(
                    "INSERT INTO shadow_account VALUES (1, ?, ?, ?, '0.00', '0.00', '0', NULL, NULL)",
                    ("shadow-snapshot-0", now, str(initial_cash_usd)),
                )
            self.db.commit()
        except BaseException:
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    async def snapshot(self, *, market: MarketSnapshot | None = None) -> PortfolioSnapshot:
        return await asyncio.to_thread(self.snapshot_sync, market=market)

    def snapshot_sync(self, *, market: MarketSnapshot | None = None) -> PortfolioSnapshot:
        account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
        pending_rows = self.db.execute(
            "SELECT execution_id FROM shadow_receipts WHERE status='recorded' AND filled=0 ORDER BY rowid"
        ).fetchall()
        qty = Decimal(account["quantity_eur"])
        cash = Decimal(account["cash_usd"])
        entry = Decimal(account["avg_entry_price"]) if account["avg_entry_price"] else Decimal("0")
        unrealized = Decimal("0")
        if qty != Decimal("0") and market is not None:
            quote = next((q for q in market.quotes if q.instrument == "EUR/USD"), None)
            if quote is not None:
                mark = quote.bid if qty > 0 else quote.ask
                unrealized = (qty * (mark - entry)).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
        equity = cash + unrealized
        positions = (Position(instrument="EUR/USD", quantity=qty),) if qty != Decimal("0") else ()
        as_of = max(
            datetime.fromisoformat(account["as_of"]),
            market.as_of if market is not None else datetime.fromisoformat(account["as_of"]),
        )
        return PortfolioSnapshot(
            snapshot_id=account["snapshot_id"],
            as_of=as_of,
            base_currency="USD",
            equity=equity,
            positions=positions,
            pending_order_ids=tuple(row["execution_id"] for row in pending_rows),
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
        with self.db:
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

            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            pending = self.db.execute(
                "SELECT COUNT(*) FROM shadow_receipts WHERE status='recorded' AND filled=0"
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
                "INSERT INTO shadow_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    receipt.execution_id,
                    decision.decision_id,
                    target.target_id,
                    assessment.assessment_id,
                    receipt.status,
                    now.isoformat(),
                    decision.model_dump_json(),
                    assessment.model_dump_json(),
                    receipt.model_dump_json(),
                    0 if status == "recorded" else 1,
                ),
            )
            return receipt

    def fill_pending(self, market: MarketSnapshot) -> tuple[ShadowFill, ...]:
        """Execute recorded pending shadow targets at the supplied point-in-time market quote."""
        market = MarketSnapshot.model_validate_json(market.model_dump_json())
        quote = next((q for q in market.quotes if q.instrument == "EUR/USD"), None)
        if quote is None:
            return ()
        now = _utc(self.clock())
        if now < market.as_of or market.as_of - quote.received_at > timedelta(
            milliseconds=self.exit_policy.max_quote_age_ms
        ):
            return ()
        fills: list[ShadowFill] = []
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            rows = self.db.execute(
                "SELECT * FROM shadow_receipts WHERE status='recorded' AND filled=0 ORDER BY rowid"
            ).fetchall()
            for row in rows:
                recorded_at = datetime.fromisoformat(row["recorded_at"])
                if quote.received_at < recorded_at:
                    continue
                decision = AgentDecision.model_validate_json(row["decision_json"])
                assert decision.target is not None
                target: TargetPortfolio = decision.target
                pos_target = next((p for p in target.positions if p.instrument == "EUR/USD"), None)
                weight = pos_target.weight if pos_target is not None else Decimal("0")
                account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
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
                    else Decimal("0")
                )
                equity = cash + unrealized
                desired_qty = (
                    Decimal("0")
                    if weight == Decimal("0")
                    else ((equity * weight) / mid).quantize(Decimal("1"), rounding=ROUND_DOWN)
                )
                delta = desired_qty - current_qty
                if delta == Decimal("0"):
                    self.db.execute(
                        "UPDATE shadow_receipts SET filled=1 WHERE execution_id=?",
                        (row["execution_id"],),
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
                    "UPDATE shadow_receipts SET filled=1 WHERE execution_id=?",
                    (row["execution_id"],),
                )
                fills.append(fill)
        return tuple(fills)

    def evaluate_protective_exits(self, market: MarketSnapshot) -> tuple[ShadowFill, ...]:
        """Close open positions when deterministic timer, stop-loss or daily cutoff fires."""
        market = MarketSnapshot.model_validate_json(market.model_dump_json())
        quote = next((q for q in market.quotes if q.instrument == "EUR/USD"), None)
        if quote is None:
            return ()
        now = _utc(self.clock())
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            qty = Decimal(account["quantity_eur"])
            if qty == Decimal("0"):
                return ()
            entry = Decimal(account["avg_entry_price"])
            opened_at = datetime.fromisoformat(account["opened_at"])
            mark = quote.bid if qty > 0 else quote.ask
            unrealized = (qty * (mark - entry)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            exit_reason: str | None = None
            if unrealized <= -self.exit_policy.max_position_loss_usd:
                exit_reason = "protective_stop_loss"
            elif market.as_of - opened_at >= timedelta(
                seconds=self.exit_policy.max_holding_seconds
            ):
                exit_reason = "protective_max_holding_horizon"
            elif (
                self.exit_policy.daily_close_utc_hour is not None
                and market.as_of.hour == self.exit_policy.daily_close_utc_hour
                and opened_at < market.as_of
            ):
                exit_reason = "protective_daily_close"

            if exit_reason is None:
                return ()

            # Cancel any unfilled pending orders before flattening.
            self.db.execute("UPDATE shadow_receipts SET filled=1 WHERE filled=0")
            fill = self._apply_delta_locked(
                delta=-qty,
                quote=quote,
                filled_at=max(now, quote.received_at),
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
        fill_price = self.cost_model.execution_price(quote, buy=buy)
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
        fill_count = self.db.execute("SELECT COUNT(*) FROM shadow_fills").fetchone()[0] + 1
        new_snapshot_id = f"shadow-snapshot-{fill_count}"

        self.db.execute(
            """
            UPDATE shadow_account
            SET snapshot_id=?, as_of=?, cash_usd=?, realized_pnl_usd=?, total_fees_usd=?,
                quantity_eur=?, avg_entry_price=?, opened_at=?
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
            quote_received_at=quote.received_at,
            prior_quantity=prior_qty,
            delta_quantity=delta,
            new_quantity=new_qty,
            fill_price=fill_price,
            commission_usd=commission,
            realized_pnl_usd=realized_trade,
            equity_after_usd=equity_after,
            snapshot_id_after=new_snapshot_id,
        )
        self.db.execute(
            "INSERT INTO shadow_fills VALUES (?, ?, ?, ?)",
            (fill.fill_id, execution_id, filled_at.isoformat(), fill.model_dump_json()),
        )
        return fill

    def report(self, *, market: MarketSnapshot | None = None) -> dict[str, Any]:
        snap = self.snapshot_sync(market=market)
        account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
        receipts = [
            json.loads(row["receipt_json"])
            for row in self.db.execute("SELECT receipt_json FROM shadow_receipts ORDER BY rowid")
        ]
        fills = [
            json.loads(row["fill_json"])
            for row in self.db.execute("SELECT fill_json FROM shadow_fills ORDER BY rowid")
        ]
        return {
            "scope": "shadow_ledger_only",
            "execution_authorized": False,
            "snapshot": snap.model_dump(mode="json"),
            "cash_usd": account["cash_usd"],
            "realized_pnl_usd": account["realized_pnl_usd"],
            "total_fees_usd": account["total_fees_usd"],
            "quantity_eur": account["quantity_eur"],
            "avg_entry_price": account["avg_entry_price"],
            "opened_at": account["opened_at"],
            "receipt_count": len(receipts),
            "fill_count": len(fills),
            "receipts": receipts,
            "fills": fills,
        }
