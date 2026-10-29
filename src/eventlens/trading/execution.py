"""Shadow-only boundary and causal SQLite shadow ledger; no broker SDK or live orders."""

import asyncio
import json
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
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
from .portfolio import LOT_UNITS_EUR
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

    Commission & Slippage Provenance:
    - Platform Metadata (`broker_metadata_usd_per_million`): When a quote originates from an
      allowlisted live cTrader bridge session (`source_provider == "ctrader_algo"`, `basis == "live_poll"`)
      with verified `commission > 0` and `commission_type == "UsdPerMillionUsdVolume"`, the broker
      commission is `abs(units_eur) * fill_price * (commission / 1,000,000)` USD per side
      (e.g. `3 USD / $1M USD notional = 0.30 bps` per side, or `~$3.36` per standard lot per side at `1.1200`).
    - Unverified Shadow Assumptions (`unverified_model_commission_bps` & `slippage_bps`):
      `slippage_bps = 0.25` (`0.025 pip` adverse slippage per fill) and fallback `commission_bps = 0.30`
      are conservative shadow modeling assumptions, not verified live broker fills.
    """

    name: Identifier = "pepperstone-razor-conservative-v2"
    commission_bps: Decimal = Field(default=Decimal("0.3"), ge=0, le=100)
    slippage_bps: Decimal = Field(default=Decimal("0.25"), ge=0, le=100)
    spread_floor_pips: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    allow_synthetic_spread_floor: bool = False
    allow_commission_bearing_zero_spread: bool = False
    fixed_fee_usd: Decimal = Field(default=Decimal("0"), ge=0)

    @classmethod
    def pepperstone_razor(cls) -> "ShadowCostModel":
        """Explicitly opt in to verified Pepperstone Razor cTrader commission-bearing 0.0-pip quotes."""
        return cls(allow_commission_bearing_zero_spread=True)

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
        if (
            quote.ask == quote.bid
            and not self.allow_synthetic_spread_floor
            and not (self.allow_commission_bearing_zero_spread and quote.has_verified_commission)
        ):
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

    def fee_with_source(
        self, units: Decimal, price: Decimal, *, quote: Quote | None = None
    ) -> tuple[
        Decimal,
        Literal[
            "broker_metadata_usd_per_million",
            "broker_metadata_verified_conservative_bps_floor",
            "unverified_model_commission_bps",
        ],
    ]:
        notional_usd = abs(units) * price
        bps_fee = notional_usd * self.commission_bps / BPS
        broker_fee = Decimal("0")
        has_broker_meta = (
            quote is not None and quote.has_verified_commission and quote.commission is not None
        )
        if has_broker_meta and quote is not None and quote.commission is not None:
            broker_fee = notional_usd * quote.commission / Decimal("1000000")
        fee = (max(bps_fee, broker_fee) + self.fixed_fee_usd).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        source: Literal[
            "broker_metadata_usd_per_million",
            "broker_metadata_verified_conservative_bps_floor",
            "unverified_model_commission_bps",
        ]
        if not has_broker_meta:
            source = "unverified_model_commission_bps"
        elif broker_fee >= bps_fee:
            source = "broker_metadata_usd_per_million"
        else:
            source = "broker_metadata_verified_conservative_bps_floor"
        return fee, source

    def fee_usd(self, units: Decimal, price: Decimal, *, quote: Quote | None = None) -> Decimal:
        return self.fee_with_source(units, price, quote=quote)[0]


class ProtectiveExitPolicy(Contract):
    """Deterministic exit and daily protection rules operating without the LLM."""

    max_holding_seconds: int = Field(default=1800, gt=0)
    max_position_loss_usd: Decimal = Field(default=Decimal("150"), gt=0)
    take_profit_usd: Decimal | None = Field(default=None, gt=0)
    stop_loss_pips: Decimal | None = Field(default=None, gt=0)
    take_profit_pips: Decimal | None = Field(default=None, gt=0)
    trailing_activation_pips: Decimal | None = Field(default=None, gt=0)
    trailing_distance_pips: Decimal | None = Field(default=None, gt=0)
    max_daily_loss_usd: Decimal = Field(default=Decimal("200"), gt=0)
    max_quote_age_ms: int = Field(default=2000, gt=0)
    max_spread_pips: Decimal = Field(default=Decimal("2.0"), gt=0)
    allow_zero_spread: bool = False
    allow_commission_bearing_zero_spread: bool = False
    require_verified_quote_provenance: bool = False
    trading_timezone: str = "America/New_York"
    daily_close_hour: int = Field(default=16, ge=0, le=23)
    daily_close_minute: int = Field(default=55, ge=0, le=59)
    rollover_hour: int = Field(default=17, ge=0, le=23)
    rollover_minute: int = Field(default=0, ge=0, le=59)
    daily_close_utc_hour: int | None = Field(default=None, ge=0, le=23)

    @model_validator(mode="after")
    def validate_trailing_policy(self) -> Self:
        if (self.trailing_activation_pips is None) != (self.trailing_distance_pips is None):
            raise ValueError(
                "trailing_activation_pips and trailing_distance_pips must be both set or both None"
            )
        if (
            self.trailing_activation_pips is not None
            and self.trailing_distance_pips is not None
            and self.trailing_distance_pips >= self.trailing_activation_pips
        ):
            raise ValueError(
                "trailing_distance_pips must be strictly less than trailing_activation_pips"
            )
        return self

    @classmethod
    def pepperstone_razor_200(
        cls,
        *,
        max_holding_seconds: int = 900,
        max_quote_age_ms: int = 2000,
        max_position_loss_usd: Decimal = Decimal("7.00"),
        take_profit_usd: Decimal | None = None,
        stop_loss_pips: Decimal | None = Decimal("11.0"),
        take_profit_pips: Decimal | None = Decimal("18.0"),
        trailing_activation_pips: Decimal | None = Decimal("10.0"),
        trailing_distance_pips: Decimal | None = Decimal("5.0"),
        max_daily_loss_usd: Decimal = Decimal("8.00"),
        allow_commission_bearing_zero_spread: bool = True,
    ) -> "ProtectiveExitPolicy":
        """Pepperstone Razor $200 USD Demo protection standard:
        15m horizon, position-scaled 11-pip SL ($7.00 hard USD cap), 18-pip TP,
        +10.0-pip trailing profit lock (5.0-pip pullback distance), and $8.00 daily loss cap."""
        return cls(
            max_holding_seconds=max_holding_seconds,
            max_position_loss_usd=max_position_loss_usd,
            take_profit_usd=take_profit_usd,
            stop_loss_pips=stop_loss_pips,
            take_profit_pips=take_profit_pips,
            trailing_activation_pips=trailing_activation_pips,
            trailing_distance_pips=trailing_distance_pips,
            max_daily_loss_usd=max_daily_loss_usd,
            max_quote_age_ms=max_quote_age_ms,
            max_spread_pips=Decimal("2.0"),
            allow_commission_bearing_zero_spread=allow_commission_bearing_zero_spread,
        )

    def quote_quality_policy(self, instrument: str = "EUR/USD") -> QuoteQualityPolicy:
        return QuoteQualityPolicy(
            instrument=instrument,
            max_quote_age_ms=self.max_quote_age_ms,
            max_spread_pips=self.max_spread_pips,
            allow_zero_spread=self.allow_zero_spread,
            allow_commission_bearing_zero_spread=self.allow_commission_bearing_zero_spread,
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
    raw_bid: Decimal | None = None
    raw_ask: Decimal | None = None
    fill_price: Decimal = Field(gt=0)
    commission_usd: Decimal = Field(ge=0)
    commission_source: Literal[
        "broker_metadata_usd_per_million",
        "broker_metadata_verified_conservative_bps_floor",
        "unverified_model_commission_bps",
    ] = "unverified_model_commission_bps"
    slippage_bps_applied: Decimal = Decimal("0")
    slippage_assumption_unverified: bool = True
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
        cost_model: ShadowCostModel | None = None,
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
        if cost_model is not None:
            self.cost_model = cost_model
        elif (
            risk_policy.allow_commission_bearing_zero_spread
            or exit_policy.allow_commission_bearing_zero_spread
        ):
            self.cost_model = ShadowCostModel.pepperstone_razor()
        else:
            self.cost_model = ShadowCostModel()
        self.exit_policy = exit_policy
        self.risk_policy = risk_policy
        self.clock = clock
        self.stop_reason_provider: Callable[[datetime], str | None] | None = None
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
                        peak_favorable_mark TEXT,
                        pending_exit_reason TEXT,
                        pending_exit_since TEXT,
                        last_valid_equity_usd TEXT
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
                        trigger_reason TEXT,
                        baseline_valid INTEGER NOT NULL DEFAULT 1,
                        baseline_reason TEXT NOT NULL DEFAULT 'confirmed'
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
                self._migrate_columns_if_needed()
                runner_row = self.db.execute(
                    "SELECT 1 FROM shadow_runner_state WHERE id=1"
                ).fetchone()
                if runner_row is None:
                    self.db.execute(
                        "INSERT INTO shadow_runner_state VALUES (1, 'idle', NULL, NULL, NULL, NULL, 0, NULL, NULL)"
                    )
                self.db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                self.db.execute("PRAGMA user_version=2")
                row = self.db.execute("SELECT 1 FROM shadow_account WHERE id=1").fetchone()
                if row is None:
                    now = _utc(self.clock())
                    self.db.execute(
                        """INSERT INTO shadow_account
                        (id, snapshot_id, as_of, cash_usd, realized_pnl_usd, total_fees_usd,
                         quantity_eur, avg_entry_price, opened_at, peak_favorable_mark,
                         pending_exit_reason, pending_exit_since, last_valid_equity_usd)
                        VALUES (1, ?, ?, ?, '0.00', '0.00', '0', NULL, NULL, NULL, NULL, NULL, ?)""",
                        (
                            "shadow-snapshot-0",
                            now.isoformat(),
                            str(initial_cash_usd),
                            str(initial_cash_usd),
                        ),
                    )
                    self._ensure_daily_state_locked(now, initial_cash_usd, valid_valuation=True)
                self.db.commit()
        except BaseException:
            self.db.close()
            raise

    def _migrate_columns_if_needed(self) -> None:
        acct_cols = {
            r["name"] for r in self.db.execute("PRAGMA table_info(shadow_account)").fetchall()
        }
        if "peak_favorable_mark" not in acct_cols:
            self.db.execute("ALTER TABLE shadow_account ADD COLUMN peak_favorable_mark TEXT")
        if "pending_exit_reason" not in acct_cols:
            self.db.execute("ALTER TABLE shadow_account ADD COLUMN pending_exit_reason TEXT")
        if "pending_exit_since" not in acct_cols:
            self.db.execute("ALTER TABLE shadow_account ADD COLUMN pending_exit_since TEXT")
        if "last_valid_equity_usd" not in acct_cols:
            self.db.execute("ALTER TABLE shadow_account ADD COLUMN last_valid_equity_usd TEXT")
            self.db.execute(
                "UPDATE shadow_account SET last_valid_equity_usd = cash_usd WHERE last_valid_equity_usd IS NULL"
            )
        daily_cols = {
            r["name"] for r in self.db.execute("PRAGMA table_info(shadow_daily_state)").fetchall()
        }
        if "baseline_valid" not in daily_cols:
            self.db.execute(
                "ALTER TABLE shadow_daily_state ADD COLUMN baseline_valid INTEGER NOT NULL DEFAULT 1"
            )
        if "baseline_reason" not in daily_cols:
            self.db.execute(
                "ALTER TABLE shadow_daily_state ADD COLUMN baseline_reason TEXT NOT NULL DEFAULT 'confirmed'"
            )
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
        runner_cols = {
            r["name"] for r in self.db.execute("PRAGMA table_info(shadow_runner_state)").fetchall()
        }
        if runner_cols and "stop_reason" not in runner_cols:
            self.db.execute("ALTER TABLE shadow_runner_state ADD COLUMN stop_reason TEXT")

    def close(self) -> None:
        with self._lock:
            self.db.close()

    def _record_fault_locked(self, now: datetime, stage: str, reason: str, detail: str) -> None:
        self.db.execute(
            "INSERT INTO shadow_faults VALUES (?, ?, ?, ?, ?)",
            (str(uuid4()), now.isoformat(), stage, reason, detail[:1000]),
        )

    def _active_stop_reason_locked(
        self, now: datetime, *, explicit_stop_reason: str | None = None
    ) -> str | None:
        if explicit_stop_reason is not None:
            return explicit_stop_reason
        if self.stop_reason_provider is not None:
            provided = self.stop_reason_provider(now)
            if provided is not None:
                return provided
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
            if now >= started + timedelta(seconds=float(row["duration_seconds"])):
                return "runner_duration_expired"
        return None

    def _is_order_risk_increasing_locked(
        self,
        account: sqlite3.Row,
        decision: AgentDecision,
        *,
        quote: Quote | None = None,
    ) -> bool:
        target = decision.target
        if target is None:
            return False
        pos_target = next((p for p in target.positions if p.instrument == "EUR/USD"), None)
        weight = pos_target.weight if pos_target is not None else Decimal("0")
        current_qty = Decimal(account["quantity_eur"])
        if decision.portfolio_action == "CLOSE" or weight == Decimal("0"):
            return False
        if current_qty == Decimal("0"):
            return True
        if (current_qty > Decimal("0") and weight < Decimal("0")) or (
            current_qty < Decimal("0") and weight > Decimal("0")
        ):
            return True
        entry = Decimal(account["avg_entry_price"]) if account["avg_entry_price"] else Decimal("0")
        if quote is not None:
            mid = (quote.bid + quote.ask) / Decimal("2")
            mark = quote.bid if current_qty > 0 else quote.ask
            unrealized = (current_qty * (mark - entry)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            equity = Decimal(account["cash_usd"]) + unrealized
        else:
            mid = entry if entry > Decimal("0") else Decimal("1.0")
            equity = Decimal(account["last_valid_equity_usd"] or account["cash_usd"])
        change_kind = self.risk_policy.classify_exposure_change(current_qty, equity, mid, weight)
        return change_kind in ("risk_increasing", "reversal_risk_increasing")

    def _cancel_risk_increasing_pending_locked(
        self,
        now: datetime,
        *,
        reason: str,
        quote: Quote | None = None,
    ) -> list[dict[str, Any]]:
        account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
        rows = self.db.execute(
            "SELECT * FROM shadow_receipts WHERE order_status='pending' ORDER BY rowid"
        ).fetchall()
        has_runtime_events = (
            self.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_events'"
            ).fetchone()
            is not None
        )
        cancelled: list[dict[str, Any]] = []
        for row in rows:
            decision = AgentDecision.model_validate_json(row["decision_json"])
            if self._is_order_risk_increasing_locked(account, decision, quote=quote):
                self.db.execute(
                    """UPDATE shadow_receipts
                    SET order_status='cancelled', terminal_at=?, terminal_reason=?
                    WHERE execution_id=?""",
                    (now.isoformat(), reason, row["execution_id"]),
                )
                if has_runtime_events:
                    self.db.execute(
                        """UPDATE runtime_events
                        SET state='shadow_rejected', reason=?, updated_at=?
                        WHERE execution_id=? AND state='shadow_recorded'""",
                        (reason, now.isoformat(), row["execution_id"]),
                    )
                self._record_fault_locked(
                    now,
                    "stop_cancelled_pending_order",
                    reason,
                    f"execution_id={row['execution_id']} decision_id={decision.decision_id}",
                )
                cancelled.append(
                    {
                        "execution_id": row["execution_id"],
                        "decision_id": decision.decision_id,
                        "target_id": decision.target.target_id if decision.target else None,
                        "reason": reason,
                        "cancelled_at": now,
                    }
                )
        return cancelled

    def cancel_risk_increasing_pending_orders(
        self,
        *,
        now: datetime | None = None,
        reason: str = "cancelled_on_runner_stop_requested",
    ) -> tuple[dict[str, Any], ...]:
        """Cancel all pending risk-increasing shadow orders when stop or duration limit triggers."""
        check_now = _utc(now or self.clock())
        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            return tuple(self._cancel_risk_increasing_pending_locked(check_now, reason=reason))

    def request_protective_exit_on_stop(
        self,
        *,
        now: datetime | None = None,
        reason: str = "protective_runner_stop",
    ) -> bool:
        """Persist a protective exit request on an open position so drain loop closes it on the next fresh valid quote."""
        check_now = _utc(now or self.clock())
        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            qty = Decimal(account["quantity_eur"])
            if qty == Decimal("0"):
                return False
            if account["pending_exit_reason"] is None:
                self.db.execute(
                    """UPDATE shadow_account
                    SET pending_exit_reason=?, pending_exit_since=?
                    WHERE id=1""",
                    (reason, check_now.isoformat()),
                )
            return True

    def mark_shutdown_unclosed_risk(
        self,
        *,
        now: datetime | None = None,
        reason: str = "unclosed_on_runner_shutdown",
    ) -> dict[str, Any] | None:
        """Persist pending exit state and fault when shutdown/drain ends with an open position."""
        check_now = _utc(now or self.clock())
        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
            qty = Decimal(account["quantity_eur"])
            if qty == Decimal("0"):
                return None
            exit_reason = account["pending_exit_reason"] or reason
            pending_since = account["pending_exit_since"] or check_now.isoformat()
            self.db.execute(
                """UPDATE shadow_account
                SET pending_exit_reason=?, pending_exit_since=?
                WHERE id=1""",
                (exit_reason, pending_since),
            )
            detail = (
                f"quantity_eur={qty} avg_entry_price={account['avg_entry_price']} "
                f"pending_exit_reason={exit_reason} pending_exit_since={pending_since}"
            )
            self._record_fault_locked(
                check_now,
                "shutdown_drain",
                "shutdown_exit_timeout_unclosed_risk",
                detail,
            )
            return {
                "quantity_eur": str(qty),
                "pending_exit_reason": exit_reason,
                "pending_exit_since": pending_since,
                "recorded_at": check_now.isoformat(),
            }

    def _ensure_daily_state_locked(
        self, now: datetime, current_equity: Decimal, *, valid_valuation: bool = True
    ) -> sqlite3.Row:
        day_id, start_utc, close_utc, rollover_utc = self.exit_policy.effective_window(now)
        account = self.db.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
        qty = Decimal(account["quantity_eur"]) if account is not None else Decimal("0")
        last_valid_eq = (
            Decimal(account["last_valid_equity_usd"])
            if account is not None and account["last_valid_equity_usd"] is not None
            else current_equity
        )
        prior_row = self.db.execute(
            "SELECT * FROM shadow_daily_state WHERE trading_day < ? ORDER BY trading_day DESC LIMIT 1",
            (day_id,),
        ).fetchone()
        prior_baseline = (
            Decimal(prior_row["baseline_equity_usd"]) if prior_row is not None else last_valid_eq
        )
        carried_ref = max(last_valid_eq, prior_baseline)
        limit = min(self.exit_policy.max_daily_loss_usd, self.risk_policy.max_daily_loss_usd)

        row = self.db.execute(
            "SELECT * FROM shadow_daily_state WHERE trading_day=?", (day_id,)
        ).fetchone()
        if row is None:
            if not valid_valuation:
                # Position is open across trading-day boundary without a valid quote:
                # never write raw cash balance as a confirmed equity baseline.
                provisional = carried_ref if qty != Decimal("0") else current_equity
                self.db.execute(
                    """INSERT INTO shadow_daily_state
                    (trading_day, session_start_utc, daily_close_utc, session_rollover_utc,
                     baseline_equity_usd, min_equity_seen_usd, loss_limit_triggered,
                     triggered_at, trigger_reason, baseline_valid, baseline_reason)
                    VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, 0, ?)""",
                    (
                        day_id,
                        start_utc.isoformat(),
                        close_utc.isoformat(),
                        rollover_utc.isoformat(),
                        str(provisional),
                        str(provisional),
                        "unconfirmed_open_position_without_valid_quote",
                    ),
                )
            else:
                if qty != Decimal("0"):
                    confirmed_base = max(carried_ref, current_equity)
                    base_reason = (
                        "confirmed_carried_position_conservative"
                        if carried_ref > current_equity
                        else "confirmed_marked_to_market"
                    )
                else:
                    confirmed_base = current_equity
                    base_reason = "confirmed_flat"
                min_seen = min(confirmed_base, current_equity)
                daily_pnl = current_equity - confirmed_base
                triggered = daily_pnl <= -limit
                triggered_at = now.isoformat() if triggered else None
                trigger_reason = "daily_loss_limit_breached" if triggered else None
                self.db.execute(
                    """INSERT INTO shadow_daily_state
                    (trading_day, session_start_utc, daily_close_utc, session_rollover_utc,
                     baseline_equity_usd, min_equity_seen_usd, loss_limit_triggered,
                     triggered_at, trigger_reason, baseline_valid, baseline_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)""",
                    (
                        day_id,
                        start_utc.isoformat(),
                        close_utc.isoformat(),
                        rollover_utc.isoformat(),
                        str(confirmed_base),
                        str(min_seen),
                        int(triggered),
                        triggered_at,
                        trigger_reason,
                        base_reason,
                    ),
                )
                self.db.execute(
                    "UPDATE shadow_account SET last_valid_equity_usd=? WHERE id=1",
                    (str(current_equity),),
                )
            row = self.db.execute(
                "SELECT * FROM shadow_daily_state WHERE trading_day=?", (day_id,)
            ).fetchone()
            assert isinstance(row, sqlite3.Row)
            return row

        if valid_valuation:
            was_valid = bool(row["baseline_valid"])
            if not was_valid:
                # Quote recovered after an unconfirmed cross-day rollover:
                # confirm baseline conservatively so post-recovery equity cannot mask prior losses.
                baseline = max(
                    Decimal(row["baseline_equity_usd"]),
                    carried_ref,
                    current_equity,
                )
                base_reason = "confirmed_after_quote_recovery_conservative"
            else:
                baseline = Decimal(row["baseline_equity_usd"])
                base_reason = str(row["baseline_reason"])
            min_seen = min(Decimal(row["min_equity_seen_usd"]), baseline, current_equity)
            daily_pnl = current_equity - baseline
            triggered = bool(row["loss_limit_triggered"])
            triggered_at = row["triggered_at"]
            trigger_reason = row["trigger_reason"]
            if not triggered and daily_pnl <= -limit:
                triggered = True
                triggered_at = now.isoformat()
                trigger_reason = "daily_loss_limit_breached"
            self.db.execute(
                """UPDATE shadow_daily_state
                SET baseline_equity_usd=?, min_equity_seen_usd=?, loss_limit_triggered=?,
                    triggered_at=?, trigger_reason=?, baseline_valid=1, baseline_reason=?
                WHERE trading_day=?""",
                (
                    str(baseline),
                    str(min_seen),
                    int(triggered),
                    triggered_at,
                    trigger_reason,
                    base_reason,
                    day_id,
                ),
            )
            self.db.execute(
                "UPDATE shadow_account SET last_valid_equity_usd=? WHERE id=1",
                (str(current_equity),),
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
            active_stop = self._active_stop_reason_locked(now)
            if active_stop is not None:
                with self.db:
                    self._cancel_risk_increasing_pending_locked(
                        now, reason=f"cancelled_on_{active_stop}"
                    )
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
                daily_baseline_equity_usd=baseline,
                daily_loss_limit_triggered=bool(daily_row["loss_limit_triggered"]),
                daily_baseline_valid=bool(daily_row["baseline_valid"]),
                daily_baseline_reason=str(daily_row["baseline_reason"]),
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
            active_stop = self._active_stop_reason_locked(now)
            if active_stop is not None:
                self._cancel_risk_increasing_pending_locked(
                    now, reason=f"cancelled_on_{active_stop}"
                )
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
            elif active_stop is not None and self._is_order_risk_increasing_locked(
                account, decision
            ):
                rejection_reason = f"blocked_by_{active_stop}"
            elif pending > 0:
                rejection_reason = "pending_shadow_order_exists"

            if rejection_reason is not None and rejection_reason.startswith("blocked_by_"):
                self._record_fault_locked(
                    now,
                    "record_rejected_on_stop",
                    rejection_reason,
                    f"decision_id={decision.decision_id} target_id={target.target_id}",
                )

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
        stop_reason: str | None = None,
    ) -> tuple[ShadowFill, ...]:
        """Re-verify expiry, stop state, risk policy, daily limits, and quote quality before filling pending orders."""
        if market is not None:
            market = MarketSnapshot.model_validate_json(market.model_dump_json())
        now = _utc(self.clock())
        active_risk = risk_policy or self.risk_policy
        fills: list[ShadowFill] = []

        with self._lock, self.db:
            self.db.execute("BEGIN IMMEDIATE")
            self._expire_pending_orders_locked(now)
            active_stop = self._active_stop_reason_locked(now, explicit_stop_reason=stop_reason)
            if active_stop is not None:
                self._cancel_risk_increasing_pending_locked(
                    now, reason=f"cancelled_on_{active_stop}"
                )
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

                change_kind = active_risk.classify_exposure_change(current_qty, equity, mid, weight)
                increases_risk = change_kind in ("risk_increasing", "reversal_risk_increasing")

                loop_stop = self._active_stop_reason_locked(
                    _utc(self.clock()), explicit_stop_reason=stop_reason
                )
                if increases_risk and loop_stop is not None:
                    cancel_reason = f"cancelled_on_{loop_stop}"
                    self.db.execute(
                        """UPDATE shadow_receipts
                        SET order_status='cancelled', terminal_at=?, terminal_reason=?
                        WHERE execution_id=?""",
                        (now.isoformat(), cancel_reason, row["execution_id"]),
                    )
                    self._record_fault_locked(
                        now,
                        "stop_cancelled_pending_order",
                        cancel_reason,
                        f"execution_id={row['execution_id']} decision_id={decision.decision_id}",
                    )
                    continue

                desired_qty = active_risk.quantize_target_quantity_eur(equity, mid, weight)

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
                    elif not bool(daily_row["baseline_valid"]):
                        reject_at_fill = "daily_baseline_unconfirmed_at_fill"
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
                    elif weight != Decimal("0") and (
                        abs(desired_qty) < active_risk.min_volume_eur
                        or abs(desired_qty) > active_risk.max_volume_eur
                    ):
                        reject_at_fill = "volume_limit_exceeded_at_fill"
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
                favorable_pips = (
                    (mark - entry) / EURUSD_PIP
                    if qty > Decimal("0")
                    else (entry - mark) / EURUSD_PIP
                )
                raw_prior_peak = account["peak_favorable_mark"]
                prior_peak = Decimal(raw_prior_peak) if raw_prior_peak is not None else entry
                if qty > Decimal("0"):
                    peak_mark = max(prior_peak, entry, mark)
                    peak_favorable_pips = (peak_mark - entry) / EURUSD_PIP
                else:
                    peak_mark = min(prior_peak, entry, mark)
                    peak_favorable_pips = (entry - peak_mark) / EURUSD_PIP
                giveback_pips = peak_favorable_pips - favorable_pips
                if raw_prior_peak is None or peak_mark != prior_peak:
                    self.db.execute(
                        "UPDATE shadow_account SET peak_favorable_mark=? WHERE id=1",
                        (str(peak_mark),),
                    )
                equity = cash + unrealized
                daily_row = self._ensure_daily_state_locked(now, equity, valid_valuation=True)
                daily_pnl = equity - Decimal(daily_row["baseline_equity_usd"])
                hit_usd_sl = unrealized <= -self.exit_policy.max_position_loss_usd
                hit_pip_sl = (
                    self.exit_policy.stop_loss_pips is not None
                    and favorable_pips <= -self.exit_policy.stop_loss_pips
                )
                hit_usd_tp = (
                    self.exit_policy.take_profit_usd is not None
                    and unrealized >= self.exit_policy.take_profit_usd
                )
                hit_pip_tp = (
                    self.exit_policy.take_profit_pips is not None
                    and favorable_pips >= self.exit_policy.take_profit_pips
                )
                hit_trailing = (
                    self.exit_policy.trailing_activation_pips is not None
                    and self.exit_policy.trailing_distance_pips is not None
                    and peak_favorable_pips >= self.exit_policy.trailing_activation_pips
                    and giveback_pips >= self.exit_policy.trailing_distance_pips
                )
                if hit_usd_sl or hit_pip_sl:
                    exit_reason = exit_reason or "protective_stop_loss"
                elif hit_usd_tp or hit_pip_tp:
                    exit_reason = exit_reason or "protective_take_profit"
                elif hit_trailing:
                    exit_reason = exit_reason or "protective_trailing_stop"
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
        raw_prior_peak = account["peak_favorable_mark"]
        prior_peak = Decimal(raw_prior_peak) if raw_prior_peak is not None else None

        buy = delta > Decimal("0")
        fill_price, synthetic_used = self.cost_model.execution_price(quote, buy=buy)
        commission, comm_source = self.cost_model.fee_with_source(delta, fill_price, quote=quote)
        new_qty = prior_qty + delta
        realized_trade = Decimal("0.00")

        if prior_qty == Decimal("0"):
            new_entry: Decimal | None = fill_price
            new_opened: str | None = filled_at.isoformat()
            same_direction = False
        elif (prior_qty > Decimal("0") and delta > Decimal("0")) or (
            prior_qty < Decimal("0") and delta < Decimal("0")
        ):
            total_abs = abs(prior_qty) + abs(delta)
            weighted = (abs(prior_qty) * prior_entry + abs(delta) * fill_price) / total_abs
            new_entry = weighted.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
            new_opened = prior_opened
            same_direction = True
        else:
            closed_units = min(abs(prior_qty), abs(delta))
            signed_closed = closed_units if prior_qty > Decimal("0") else -closed_units
            realized_trade = (signed_closed * (fill_price - prior_entry)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            if new_qty == Decimal("0"):
                new_entry = None
                new_opened = None
                same_direction = False
            elif (prior_qty > Decimal("0") and new_qty > Decimal("0")) or (
                prior_qty < Decimal("0") and new_qty < Decimal("0")
            ):
                new_entry = prior_entry
                new_opened = prior_opened
                same_direction = True
            else:
                # Position reversal: closed prior leg, opened remainder at fill_price
                new_entry = fill_price
                new_opened = filled_at.isoformat()
                same_direction = False

        new_cash = cash + realized_trade - commission
        new_realized = realized_total + realized_trade
        new_fees = fees_total + commission
        unrealized_after = Decimal("0.00")
        new_peak_mark: Decimal | None = None
        if new_qty != Decimal("0") and new_entry is not None:
            mark = quote.bid if new_qty > 0 else quote.ask
            unrealized_after = (new_qty * (mark - new_entry)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            if new_qty > Decimal("0"):
                new_peak_mark = (
                    max(prior_peak, new_entry, mark)
                    if (same_direction and prior_peak is not None)
                    else max(new_entry, mark)
                )
            else:
                new_peak_mark = (
                    min(prior_peak, new_entry, mark)
                    if (same_direction and prior_peak is not None)
                    else min(new_entry, mark)
                )
        equity_after = new_cash + unrealized_after
        fill_count = self.db.execute("SELECT COUNT(*) FROM shadow_fills").fetchone()[0] + 1
        new_snapshot_id = f"shadow-snapshot-{fill_count}"

        self.db.execute(
            """
            UPDATE shadow_account
            SET snapshot_id=?, as_of=?, cash_usd=?, realized_pnl_usd=?, total_fees_usd=?,
                quantity_eur=?, avg_entry_price=?, opened_at=?, peak_favorable_mark=?,
                pending_exit_reason=CASE WHEN ? = '0' THEN NULL ELSE pending_exit_reason END,
                pending_exit_since=CASE WHEN ? = '0' THEN NULL ELSE pending_exit_since END,
                last_valid_equity_usd=?
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
                str(new_peak_mark) if new_peak_mark is not None else None,
                str(new_qty),
                str(new_qty),
                str(equity_after),
            ),
        )
        self._ensure_daily_state_locked(filled_at, equity_after, valid_valuation=True)
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
            raw_bid=quote.bid,
            raw_ask=quote.ask,
            fill_price=fill_price,
            commission_usd=commission,
            commission_source=comm_source,
            slippage_bps_applied=self.cost_model.slippage_bps,
            slippage_assumption_unverified=self.cost_model.slippage_bps > Decimal("0"),
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
            qty_eur = Decimal(account["quantity_eur"])
            qty_lots = (qty_eur / LOT_UNITS_EUR).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            ref_price = (
                Decimal(account["avg_entry_price"])
                if account["avg_entry_price"]
                else Decimal("1.0")
            )
            if market is not None:
                q_eur = next((q for q in market.quotes if q.instrument == "EUR/USD"), None)
                if q_eur is not None:
                    ref_price = (q_eur.bid + q_eur.ask) / Decimal("2")
            margin_used = (
                ((abs(qty_eur) * ref_price) / self.risk_policy.leverage).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                if qty_eur != Decimal("0")
                else Decimal("0.00")
            )
            free_margin = (snap.equity - margin_used).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            unclosed_risk = qty_eur != Decimal("0")
            alerts: list[str] = []
            recovery_instructions: str | None = None
            if unclosed_risk:
                alerts.append(
                    f"UNCLOSED_SHADOW_POSITION: quantity_eur={account['quantity_eur']} "
                    f"({qty_lots} lots), avg_entry_price={account['avg_entry_price']}, "
                    f"pending_exit_reason={account['pending_exit_reason']}, "
                    f"valuation_valid={snap.valuation_valid}"
                )
                recovery_instructions = (
                    f"Start EventLensQuoteBridge in cTrader and run: "
                    f".venv/bin/eventlens shadow recover --shadow-db {self.path} "
                    f'--quotes-db <quotes.sqlite> --quote-bridge-dir "$HOME/cAlgo/Data/cBots/EventLensQuoteBridge"'
                )
            elif account["pending_exit_reason"] is not None:
                alerts.append(
                    f"PENDING_EXIT_STATE: pending_exit_reason={account['pending_exit_reason']}"
                )
            return {
                "scope": "shadow_ledger_only",
                "execution_authorized": False,
                "unclosed_risk": unclosed_risk,
                "alerts": alerts,
                "recovery_instructions": recovery_instructions,
                "cost_model": self.cost_model.model_dump(mode="json"),
                "exit_policy": self.exit_policy.model_dump(mode="json"),
                "risk_policy": self.risk_policy.model_dump(mode="json"),
                "snapshot": snap.model_dump(mode="json"),
                "cash_usd": account["cash_usd"],
                "realized_pnl_usd": account["realized_pnl_usd"],
                "total_fees_usd": account["total_fees_usd"],
                "quantity_eur": account["quantity_eur"],
                "quantity_lots": str(qty_lots),
                "leverage": str(self.risk_policy.leverage),
                "margin_used_usd": str(margin_used),
                "free_margin_usd": str(free_margin),
                "avg_entry_price": account["avg_entry_price"],
                "opened_at": account["opened_at"],
                "peak_favorable_mark": account["peak_favorable_mark"],
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
