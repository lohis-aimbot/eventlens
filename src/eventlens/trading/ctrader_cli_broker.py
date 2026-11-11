"""Testable file-bridged cTrader CLI Demo execution & reconciliation adapter.

Communicates with the headless `EventLensCliBridge` cBot (`TargetFramework=net6.0`,
compiled via `.NET 8 SDK`) running inside `ghcr.io/spotware/ctrader-console` over append-only JSONL files in a shared directory:
  - `order-intents.jsonl` (Python -> cTrader CLI Demo cBot)
  - `intent-journal.jsonl` (cTrader CLI Demo cBot internal two-stage crash journal)
  - `execution-events.jsonl` (cTrader CLI Demo cBot -> Python)
  - `quotes-<session>.jsonl` (cTrader CLI Demo cBot -> Python `DirectoryTail`)

Safety & durability invariants:
  1. Strictly Demo-only (`account_mode == "demo"`, `is_live is False`, verified
     `account_number` and `account_currency == "USD"`). Any Live account frame or
     safety refusal halts the adapter immediately.
  2. Never bypasses `DeterministicHardRiskEngine`: every `market_order` requires an
     unexpired `RiskAssessment(outcome="approve")` with `approved_quantity_eur` matching
     both `side` and `volume_units`, plus a fresh verified `reconcile_snapshot`.
  3. Never treats `"accepted"` or Shadow simulated fills as confirmed Demo positions;
     confirmed positions come strictly from broker `"partial"` / `"filled"` events or
     verified `"reconcile_snapshot"` frames carrying `broker_position_id`.
  4. Never closes a position via an opposite hedging market order; `close_position`
     strictly targets `broker_position_id` on an independent TTL channel.
  5. Uses `\n`-bounded byte offsets on JSONL streams (never advancing past partial
     trailing lines), a two-phase SQLite outbox before file append, and an explicit
     `unknown_execution` manual-unlock boundary (`acknowledge_manual_unlock`) that
     forbids automatic market-order retry when crash outcomes are ambiguous.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal, Self
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
    RiskAssessment,
    TargetPortfolio,
    Timestamp,
)
from ..quotes import EURUSD_PIP, validate_market_quote
from .execution import ProtectiveExitPolicy, ShadowFill
from .risk import RiskPolicy, is_in_daily_close_window

OrderLifecycleStatus = Literal[
    "outbox_pending",
    "submitted",
    "accepted",
    "in_flight",
    "partial",
    "filled",
    "position_closed_unpriced",
    "rejected",
    "cancelled",
    "expired",
    "unknown_execution",
    "reconciled_after_unknown",
    "duplicate_ignored",
]

PublishFaultMode = Literal[
    "none",
    "skip_file_write_after_db_commit",
    "write_half_line_after_db_commit",
    "crash_after_file_write_before_db_mark",
]

DaemonCrashMode = Literal[
    "none",
    "before_broker_call",
    "after_broker_call_before_event",
]

_TERMINAL_INTENT_STATUSES: frozenset[str] = frozenset(
    {
        "filled",
        "rejected",
        "cancelled",
        "expired",
        "reconciled_after_unknown",
    }
)

_ACTIVE_PENDING_STATUSES: tuple[str, ...] = (
    "outbox_pending",
    "submitted",
    "accepted",
    "in_flight",
    "unknown_execution",
)


class LiveAccountForbiddenError(RuntimeError):
    """Raised immediately if any cTrader CLI bridge frame reports a Live account."""


class DemoBrokerSafetyError(RuntimeError):
    """Raised when Demo execution preconditions, risk approval, or reconciliation gates fail."""


class SimulatedBridgeCrashError(RuntimeError):
    """Raised by `SimulatedCTraderCliDaemon` to deterministically test mid-flight crash windows."""


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    ts = clock()
    if ts.tzinfo is None or ts.utcoffset() is None:
        raise ValueError("Demo broker adapter clock must be timezone-aware")
    return ts.astimezone(UTC)


_INT64_MAX: int = 9_223_372_036_854_775_807


def parse_strict_int64_account_number(
    raw_value: Any,
    *,
    allow_zero: bool = False,
) -> int | None:
    """Strictly parse a 64-bit integer account number (never float/double/bool).

    Mirrors C# `EventLensCliBridge.TryParseExpectedDemoAccountNumber` (`NumberStyles.None`
    `long.TryParse`): accepts `int` or ASCII-digit `str` in `[0, Int64.MaxValue]` (when
    `allow_zero=True`) or `[1, Int64.MaxValue]` (when `allow_zero=False`). Rejects `bool`,
    `float` (`double`), negative values, non-ASCII digits, decimals, and >64-bit overflow.
    """
    if isinstance(raw_value, (bool, float)) or raw_value is None:
        return None
    if isinstance(raw_value, int):
        parsed = raw_value
    elif isinstance(raw_value, str):
        trimmed = raw_value.strip()
        if not trimmed or not trimmed.isascii() or not trimmed.isdigit():
            return None
        parsed = int(trimmed)
    else:
        return None

    min_val = 0 if allow_zero else 1
    if parsed < min_val or parsed > _INT64_MAX:
        return None
    return parsed


def _parse_iso_utc(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        dt = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None or dt.utcoffset() is None:
        return None
    return dt.astimezone(UTC)


def _parse_decimal_strict(raw: Any) -> Decimal | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        val = Decimal(str(raw).strip())
    except Exception:
        return None
    if not val.is_finite():
        return None
    return val


def _format_price_clean(val: Decimal) -> str:
    quant = val.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    txt = format(quant.normalize(), "f")
    return txt if "." in txt else f"{txt}.0"


def _format_usd_clean(val: Decimal) -> str:
    return str(val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


class DemoOrderIntent(Contract):
    version: Literal[1] = 1
    client_intent_id: Identifier
    intent_type: Literal[
        "market_order",
        "close_position",
        "modify_protection",
        "cancel_pending_order",
        "reconcile",
    ]
    symbol: Literal["EURUSD"] = "EURUSD"
    created_at: Timestamp
    expires_at: Timestamp
    decision_id: Identifier | None = None
    assessment_id: Identifier | None = None
    side: Literal["buy", "sell"] | None = None
    volume_units: int | None = Field(default=None, gt=0)
    broker_position_id: int | None = Field(default=None, gt=0)
    broker_order_id: int | None = Field(default=None, gt=0)
    stop_loss_pips: float | None = Field(default=None, gt=0)
    take_profit_pips: float | None = Field(default=None, gt=0)
    stop_loss_price: float | None = Field(default=None, gt=0)
    take_profit_price: float | None = Field(default=None, gt=0)
    protective_reason: str | None = None

    @model_validator(mode="after")
    def _validate_expiry(self) -> Self:
        if self.expires_at <= self.created_at:
            raise ValueError("DemoOrderIntent expires_at must be strictly after created_at")
        return self


class DemoBrokerPosition(Contract):
    broker_position_id: int = Field(gt=0)
    symbol: Identifier = "EURUSD"
    side: Literal["buy", "sell"]
    volume_units: int = Field(gt=0)
    entry_price: Decimal = Field(gt=0)
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    sl_confirmed: bool = False
    tp_confirmed: bool = False
    label: str = ""
    opened_at: Timestamp | None = None


class DemoReconcileResult(Contract):
    reconciled_at: Timestamp
    trigger: str
    clean: bool
    blocks_new_risk: bool
    drift_reasons: tuple[str, ...] = ()
    orphan_broker_position_ids: tuple[int, ...] = ()
    closed_at_broker_position_ids: tuple[int, ...] = ()
    unpriced_closed_position_ids: tuple[int, ...] = ()
    unconfirmed_sl_position_ids: tuple[int, ...] = ()
    closed_deal_ids_count: int = 0
    broker_balance_usd: Decimal | None = None
    broker_equity_usd: Decimal | None = None
    broker_margin_used_usd: Decimal | None = None
    open_positions: tuple[DemoBrokerPosition, ...] = ()
    bridge_session_id: str | None = None
    snapshot_event_seq: int | None = None
    snapshot_emitted_at: Timestamp | None = None
    snapshot_age_seconds: float | None = None
    account_number: int | None = None
    account_currency: str | None = None
    allow_demo_orders: bool | None = None
    manual_unlock_required: bool = False
    unknown_intent_ids: tuple[str, ...] = ()


class CTraderCliDemoBrokerAdapter:
    """Durable SQLite + file-bridged Demo execution & reconciliation adapter for `EventLensCliBridge`."""

    APPLICATION_ID = 0x45564C44  # 'EVLD'

    def __init__(
        self,
        *,
        state_db_path: Path,
        bridge_dir: Path,
        allow_orders: bool = False,
        confirm_demo_execution: bool = False,
        expected_demo_account_number: int | str | None = None,
        expected_account_currency: str = "USD",
        max_snapshot_age_seconds: float = 45.0,
        max_intent_ttl_seconds: float = 30.0,
        protective_intent_ttl_seconds: float = 60.0,
        in_flight_unknown_timeout_seconds: float = 15.0,
        require_stop_loss_confirmation: bool = True,
        default_stop_loss_pips: float | None = 11.0,
        default_take_profit_pips: float | None = 22.0,
        publish_fault_mode: PublishFaultMode = "none",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if allow_orders and not confirm_demo_execution:
            raise DemoBrokerSafetyError(
                "allow_orders=True requires explicit confirm_demo_execution=True"
            )
        if max_snapshot_age_seconds <= 0:
            raise ValueError("max_snapshot_age_seconds must be positive")
        if max_intent_ttl_seconds <= 0 or protective_intent_ttl_seconds <= 0:
            raise ValueError("Intent TTL seconds must be positive")

        parsed_expected_acct: int | None = None
        if expected_demo_account_number is not None:
            parsed_any = parse_strict_int64_account_number(
                expected_demo_account_number, allow_zero=True
            )
            if parsed_any is None:
                raise DemoBrokerSafetyError(
                    f"Invalid expected_demo_account_number={expected_demo_account_number!r}: "
                    "must be a strict 64-bit integer or ASCII digit string (never float/double)"
                )
            if allow_orders and parsed_any <= 0:
                raise DemoBrokerSafetyError(
                    "allow_orders=True requires an explicit positive integer expected_demo_account_number (> 0)"
                )
            parsed_expected_acct = parsed_any if parsed_any > 0 else None
        elif allow_orders:
            raise DemoBrokerSafetyError(
                "allow_orders=True requires an explicit positive integer expected_demo_account_number (> 0)"
            )

        self.state_db_path = state_db_path.resolve()
        self.bridge_dir = bridge_dir.resolve()
        self.bridge_dir.mkdir(parents=True, exist_ok=True)
        self.intents_path = self.bridge_dir / "order-intents.jsonl"
        self.events_path = self.bridge_dir / "execution-events.jsonl"
        self.allow_orders = allow_orders
        self.confirm_demo_execution = confirm_demo_execution
        self.expected_demo_account_number = parsed_expected_acct
        self.expected_account_currency = expected_account_currency.strip().upper() or "USD"
        self.max_snapshot_age_seconds = float(max_snapshot_age_seconds)
        self.max_intent_ttl_seconds = float(max_intent_ttl_seconds)
        self.protective_intent_ttl_seconds = float(protective_intent_ttl_seconds)
        self.in_flight_unknown_timeout_seconds = float(in_flight_unknown_timeout_seconds)
        self.require_stop_loss_confirmation = require_stop_loss_confirmation
        self.default_stop_loss_pips = default_stop_loss_pips
        self.default_take_profit_pips = default_take_profit_pips
        self.publish_fault_mode: PublishFaultMode = publish_fault_mode
        self.clock = clock
        self._lock = threading.Lock()
        self._events_offset = 0
        self._halted_reason: str | None = None
        self._manual_unlock_required = False

        self.db = sqlite3.connect(self.state_db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        with self._lock, self.db:
            # Migrate legacy single-column event_seq table if encountered on an old scratch DB
            ev_cols = {
                str(r["name"])
                for r in self.db.execute("PRAGMA table_info(demo_broker_events)").fetchall()
            }
            if ev_cols and "bridge_session_id" not in ev_cols:
                self.db.execute("DROP TABLE demo_broker_events")

            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS demo_adapter_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    events_offset INTEGER NOT NULL DEFAULT 0,
                    halted_reason TEXT,
                    manual_unlock_required INTEGER NOT NULL DEFAULT 0,
                    unlock_audit_json TEXT
                );
                INSERT OR IGNORE INTO demo_adapter_meta
                    (id, events_offset, halted_reason, manual_unlock_required, unlock_audit_json)
                VALUES (1, 0, NULL, 0, NULL);

                CREATE TABLE IF NOT EXISTS demo_broker_intents (
                    client_intent_id TEXT PRIMARY KEY,
                    intent_type TEXT NOT NULL,
                    decision_id TEXT,
                    assessment_id TEXT,
                    side TEXT,
                    status TEXT NOT NULL,
                    requested_units INTEGER,
                    filled_units INTEGER NOT NULL DEFAULT 0,
                    broker_position_id INTEGER,
                    broker_order_id INTEGER,
                    fill_price TEXT,
                    sl_confirmed INTEGER NOT NULL DEFAULT 0,
                    tp_confirmed INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT,
                    expires_at TEXT NOT NULL DEFAULT '',
                    outbox_flushed INTEGER NOT NULL DEFAULT 0,
                    intent_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS demo_broker_positions (
                    broker_position_id INTEGER PRIMARY KEY,
                    client_intent_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    initial_volume_units INTEGER NOT NULL DEFAULT 0,
                    volume_units INTEGER NOT NULL,
                    priced_closed_units INTEGER NOT NULL DEFAULT 0,
                    unpriced_closed_units INTEGER NOT NULL DEFAULT 0,
                    entry_price TEXT NOT NULL,
                    stop_loss TEXT,
                    take_profit TEXT,
                    sl_confirmed INTEGER NOT NULL DEFAULT 0,
                    tp_confirmed INTEGER NOT NULL DEFAULT 0,
                    label TEXT,
                    status TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    closed_at TEXT,
                    close_price TEXT,
                    open_commission_usd TEXT,
                    realized_net_pnl_usd TEXT,
                    commission_usd TEXT,
                    swap_usd TEXT
                );

                CREATE TABLE IF NOT EXISTS demo_broker_close_deals (
                    closing_deal_id INTEGER PRIMARY KEY,
                    broker_position_id INTEGER NOT NULL,
                    client_intent_id TEXT,
                    symbol TEXT NOT NULL,
                    position_side TEXT NOT NULL,
                    volume_units INTEGER NOT NULL,
                    entry_price TEXT,
                    closing_price TEXT NOT NULL,
                    closing_time TEXT NOT NULL,
                    gross_profit_usd TEXT NOT NULL,
                    net_profit_usd TEXT NOT NULL,
                    commissions_usd TEXT NOT NULL,
                    swap_usd TEXT NOT NULL DEFAULT '0',
                    label TEXT,
                    source TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_demo_close_deals_pos
                    ON demo_broker_close_deals(broker_position_id, closing_deal_id);

                CREATE TABLE IF NOT EXISTS demo_broker_events (
                    event_rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                    bridge_session_id TEXT NOT NULL,
                    event_seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    client_intent_id TEXT,
                    status TEXT,
                    emitted_at TEXT,
                    raw_json TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    UNIQUE(bridge_session_id, event_seq)
                );

                CREATE TABLE IF NOT EXISTS demo_emitted_fills (
                    fill_key TEXT PRIMARY KEY,
                    fill_json TEXT NOT NULL,
                    emitted_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS demo_daily_state (
                    trading_day TEXT PRIMARY KEY,
                    session_start_utc TEXT,
                    daily_close_utc TEXT,
                    session_rollover_utc TEXT,
                    baseline_equity_usd TEXT NOT NULL,
                    min_equity_seen_usd TEXT NOT NULL,
                    loss_limit_triggered INTEGER NOT NULL DEFAULT 0,
                    triggered_at TEXT,
                    trigger_reason TEXT,
                    baseline_valid INTEGER NOT NULL DEFAULT 1,
                    baseline_reason TEXT NOT NULL DEFAULT 'confirmed_from_demo_broker_snapshot',
                    provisional_has_prior_ref INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                """
            )
            meta_cols = {
                str(r["name"])
                for r in self.db.execute("PRAGMA table_info(demo_adapter_meta)").fetchall()
            }
            if "manual_unlock_required" not in meta_cols:
                self.db.execute(
                    "ALTER TABLE demo_adapter_meta ADD COLUMN manual_unlock_required INTEGER NOT NULL DEFAULT 0"
                )
            if "unlock_audit_json" not in meta_cols:
                self.db.execute("ALTER TABLE demo_adapter_meta ADD COLUMN unlock_audit_json TEXT")
            if "last_valid_equity_usd" not in meta_cols:
                self.db.execute(
                    "ALTER TABLE demo_adapter_meta ADD COLUMN last_valid_equity_usd TEXT"
                )
            if "last_valid_open_qty_eur" not in meta_cols:
                self.db.execute(
                    "ALTER TABLE demo_adapter_meta ADD COLUMN last_valid_open_qty_eur TEXT NOT NULL DEFAULT '0'"
                )
            if "last_valid_trading_day" not in meta_cols:
                self.db.execute(
                    "ALTER TABLE demo_adapter_meta ADD COLUMN last_valid_trading_day TEXT"
                )
            if "last_valid_at" not in meta_cols:
                self.db.execute("ALTER TABLE demo_adapter_meta ADD COLUMN last_valid_at TEXT")

            daily_cols = {
                str(r["name"])
                for r in self.db.execute("PRAGMA table_info(demo_daily_state)").fetchall()
            }
            for col_name, col_def in (
                ("session_start_utc", "TEXT"),
                ("daily_close_utc", "TEXT"),
                ("session_rollover_utc", "TEXT"),
                ("triggered_at", "TEXT"),
                ("trigger_reason", "TEXT"),
                ("baseline_valid", "INTEGER NOT NULL DEFAULT 1"),
                (
                    "baseline_reason",
                    "TEXT NOT NULL DEFAULT 'confirmed_from_demo_broker_snapshot'",
                ),
                ("provisional_has_prior_ref", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if col_name not in daily_cols:
                    self.db.execute(f"ALTER TABLE demo_daily_state ADD COLUMN {col_name} {col_def}")

            intent_cols = {
                str(r["name"])
                for r in self.db.execute("PRAGMA table_info(demo_broker_intents)").fetchall()
            }
            if "side" not in intent_cols:
                self.db.execute("ALTER TABLE demo_broker_intents ADD COLUMN side TEXT")
            if "expires_at" not in intent_cols:
                self.db.execute(
                    "ALTER TABLE demo_broker_intents ADD COLUMN expires_at TEXT NOT NULL DEFAULT ''"
                )
            if "outbox_flushed" not in intent_cols:
                self.db.execute(
                    "ALTER TABLE demo_broker_intents ADD COLUMN outbox_flushed INTEGER NOT NULL DEFAULT 0"
                )

            pos_cols = {
                str(r["name"])
                for r in self.db.execute("PRAGMA table_info(demo_broker_positions)").fetchall()
            }
            if "label" not in pos_cols:
                self.db.execute("ALTER TABLE demo_broker_positions ADD COLUMN label TEXT")
            if "initial_volume_units" not in pos_cols:
                self.db.execute(
                    "ALTER TABLE demo_broker_positions ADD COLUMN initial_volume_units INTEGER NOT NULL DEFAULT 0"
                )
            if "priced_closed_units" not in pos_cols:
                self.db.execute(
                    "ALTER TABLE demo_broker_positions ADD COLUMN priced_closed_units INTEGER NOT NULL DEFAULT 0"
                )
            if "unpriced_closed_units" not in pos_cols:
                self.db.execute(
                    "ALTER TABLE demo_broker_positions ADD COLUMN unpriced_closed_units INTEGER NOT NULL DEFAULT 0"
                )
            if "open_commission_usd" not in pos_cols:
                self.db.execute(
                    "ALTER TABLE demo_broker_positions ADD COLUMN open_commission_usd TEXT"
                )
            if "swap_usd" not in pos_cols:
                self.db.execute("ALTER TABLE demo_broker_positions ADD COLUMN swap_usd TEXT")
            self.db.execute(
                """UPDATE demo_broker_positions
                SET initial_volume_units = volume_units
                WHERE initial_volume_units = 0 AND volume_units > 0"""
            )

            app_id_now = int(self.db.execute("PRAGMA application_id").fetchone()[0])
            if app_id_now == 0:
                self.db.execute(f"PRAGMA application_id={self.APPLICATION_ID}")

            meta = self.db.execute("SELECT * FROM demo_adapter_meta WHERE id=1").fetchone()
            if meta is not None:
                self._events_offset = int(meta["events_offset"])
                self._halted_reason = meta["halted_reason"]
                self._manual_unlock_required = bool(meta["manual_unlock_required"])
            if self.events_path.exists() and self.events_path.stat().st_size < self._events_offset:
                self._events_offset = 0
                self.db.execute("UPDATE demo_adapter_meta SET events_offset=0 WHERE id=1")

            self._flush_outbox_locked(_utc_now(self.clock), is_startup_recovery=True)

    def close(self) -> None:
        with self._lock:
            self.db.close()

    @property
    def halted_reason(self) -> str | None:
        return self._halted_reason

    @property
    def manual_unlock_required(self) -> bool:
        return self._manual_unlock_required

    def _halt_locked(self, reason: str, *, manual_unlock_required: bool = False) -> None:
        self._halted_reason = reason
        if manual_unlock_required:
            self._manual_unlock_required = True
            self.db.execute(
                "UPDATE demo_adapter_meta SET halted_reason=?, manual_unlock_required=1 WHERE id=1",
                (reason,),
            )
        else:
            self.db.execute(
                "UPDATE demo_adapter_meta SET halted_reason=? WHERE id=1",
                (reason,),
            )

    def _ensure_orders_permitted_locked(self, *, risk_increasing: bool) -> None:
        if not self.allow_orders or not self.confirm_demo_execution:
            raise DemoBrokerSafetyError("Demo order submission is disabled on this adapter")
        if self._halted_reason is not None:
            if "live_account" in self._halted_reason:
                raise LiveAccountForbiddenError(self._halted_reason)
            if risk_increasing:
                raise DemoBrokerSafetyError(
                    f"New risk halted by adapter state: {self._halted_reason}"
                )
        if risk_increasing and self._manual_unlock_required:
            raise DemoBrokerSafetyError(
                "New risk halted: manual_unlock_required after ambiguous/unknown execution"
            )

    def _repair_and_scan_intents_file_locked(self) -> set[str]:
        """Truncate any crash-interrupted trailing half-line in `order-intents.jsonl` and return published IDs."""
        published_ids: set[str] = set()
        if not self.intents_path.exists():
            return published_ids
        raw = self.intents_path.read_bytes()
        if not raw:
            return published_ids
        if not raw.endswith(b"\n"):
            last_nl = raw.rfind(b"\n")
            valid_len = last_nl + 1 if last_nl >= 0 else 0
            with self.intents_path.open("r+b") as f:
                f.truncate(valid_len)
                f.flush()
                os.fsync(f.fileno())
            raw = raw[:valid_len]
        if not raw:
            return published_ids
        for raw_ln in raw.decode("utf-8").split("\n"):
            stripped = raw_ln.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
                cid = obj.get("client_intent_id")
                if isinstance(cid, str) and cid:
                    published_ids.add(cid)
            except ValueError:
                continue
        return published_ids

    def _stage_outbox_locked(self, intent: DemoOrderIntent, now: datetime) -> bool:
        existing = self.db.execute(
            "SELECT status FROM demo_broker_intents WHERE client_intent_id=?",
            (intent.client_intent_id,),
        ).fetchone()
        if existing is not None:
            return False
        raw_line = intent.model_dump_json()
        self.db.execute(
            """INSERT INTO demo_broker_intents (
                client_intent_id, intent_type, decision_id, assessment_id, side, status,
                requested_units, filled_units, broker_position_id, broker_order_id,
                fill_price, sl_confirmed, tp_confirmed, error_code, expires_at,
                outbox_flushed, intent_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'outbox_pending', ?, 0, ?, ?, NULL, 0, 0, NULL, ?, 0, ?, ?, ?)""",
            (
                intent.client_intent_id,
                intent.intent_type,
                intent.decision_id,
                intent.assessment_id,
                intent.side,
                intent.volume_units,
                intent.broker_position_id,
                intent.broker_order_id,
                intent.expires_at.isoformat(),
                raw_line,
                intent.created_at.isoformat(),
                now.isoformat(),
            ),
        )
        return True

    def _flush_outbox_locked(self, now: datetime, *, is_startup_recovery: bool) -> int:
        """Flush `outbox_pending` rows to `order-intents.jsonl` with crash-window deduplication & TTL gates."""
        published_ids = self._repair_and_scan_intents_file_locked()
        pending_rows = self.db.execute(
            "SELECT * FROM demo_broker_intents WHERE status='outbox_pending' ORDER BY rowid ASC"
        ).fetchall()
        flushed_count = 0
        now_iso = now.isoformat()
        for row in pending_rows:
            cid = str(row["client_intent_id"])
            itype = str(row["intent_type"])
            raw_json = str(row["intent_json"])
            exp_dt = _parse_iso_utc(row["expires_at"])

            if cid in published_ids:
                # Crash occurred AFTER complete line reached order-intents.jsonl, BEFORE DB mark
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status='submitted', outbox_flushed=1, updated_at=?
                    WHERE client_intent_id=?""",
                    (now_iso, cid),
                )
                flushed_count += 1
                continue

            if exp_dt is None or now >= exp_dt:
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status='expired', error_code='outbox_expired_before_publish', updated_at=?
                    WHERE client_intent_id=?""",
                    (now_iso, cid),
                )
                continue

            if is_startup_recovery and itype == "market_order":
                # Never replay unpublished pre-crash market_order entries across process restarts!
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status='cancelled',
                        error_code='aborted_unpublished_market_order_on_restart',
                        updated_at=?
                    WHERE client_intent_id=?""",
                    (now_iso, cid),
                )
                continue

            if (
                not is_startup_recovery
                and self.publish_fault_mode == "skip_file_write_after_db_commit"
            ):
                continue

            payload_bytes = (raw_json + "\n").encode("utf-8")
            if (
                not is_startup_recovery
                and self.publish_fault_mode == "write_half_line_after_db_commit"
            ):
                half_len = max(1, len(payload_bytes) // 2)
                with self.intents_path.open("ab") as f:
                    f.write(payload_bytes[:half_len])
                    f.flush()
                    os.fsync(f.fileno())
                continue

            with self.intents_path.open("ab") as f:
                f.write(payload_bytes)
                f.flush()
                os.fsync(f.fileno())

            if (
                not is_startup_recovery
                and self.publish_fault_mode == "crash_after_file_write_before_db_mark"
            ):
                raise RuntimeError("Simulated crash after order-intents.jsonl write before DB mark")

            self.db.execute(
                """UPDATE demo_broker_intents
                SET status='submitted', outbox_flushed=1, updated_at=?
                WHERE client_intent_id=?""",
                (now_iso, cid),
            )
            published_ids.add(cid)
            flushed_count += 1
        return flushed_count

    def flush_outbox(self) -> int:
        now = _utc_now(self.clock)
        with self._lock, self.db:
            return self._flush_outbox_locked(now, is_startup_recovery=False)

    def _validate_snapshot_dict_locked(
        self, snap: dict[str, Any] | None, *, now: datetime
    ) -> tuple[bool, tuple[str, ...], float | None]:
        if snap is None:
            return False, ("no_reconcile_snapshot_received_from_cli_bridge",), None
        if bool(snap.get("is_live")) or snap.get("account_mode") != "demo":
            self._halt_locked("live_account_forbidden_in_reconcile", manual_unlock_required=True)
            raise LiveAccountForbiddenError("Reconcile snapshot reported non-Demo account!")

        reasons: list[str] = []
        raw_acct = snap.get("account_number")
        acct_num = parse_strict_int64_account_number(raw_acct, allow_zero=False)

        if acct_num is None:
            if (
                raw_acct is None
                or (isinstance(raw_acct, int) and not isinstance(raw_acct, bool) and raw_acct == 0)
                or raw_acct == "0"
            ):
                reasons.append("missing_snapshot_account_number")
            else:
                inv_reason = f"invalid_snapshot_account_number:{raw_acct!r}"
                reasons.append(inv_reason)
                self._halt_locked(inv_reason, manual_unlock_required=True)
        elif (
            self.expected_demo_account_number is not None
            and acct_num != self.expected_demo_account_number
        ):
            mismatch_reason = (
                f"unexpected_demo_account_number:got={acct_num}"
                f":expected={self.expected_demo_account_number}"
            )
            reasons.append(mismatch_reason)
            self._halt_locked(mismatch_reason, manual_unlock_required=True)

        ccy = str(snap.get("account_currency") or "").strip().upper()
        if not ccy:
            reasons.append("missing_snapshot_account_currency")
        elif ccy != self.expected_account_currency:
            ccy_reason = (
                f"unexpected_account_currency:got={ccy}:expected={self.expected_account_currency}"
            )
            reasons.append(ccy_reason)
            self._halt_locked(ccy_reason, manual_unlock_required=True)

        emitted_dt = _parse_iso_utc(snap.get("emitted_at"))
        age_s: float | None = None
        if emitted_dt is None:
            reasons.append("missing_or_invalid_snapshot_emitted_at")
        else:
            age_s = round((now - emitted_dt).total_seconds(), 4)
            if age_s < -5.0:
                reasons.append(f"future_reconcile_snapshot_timestamp:age_s={age_s}")
            elif age_s > self.max_snapshot_age_seconds:
                reasons.append(
                    f"stale_reconcile_snapshot:age_s={age_s}>max={self.max_snapshot_age_seconds}"
                )

        return len(reasons) == 0, tuple(reasons), age_s

    def submit_market_order(
        self,
        *,
        decision: AgentDecision,
        assessment: RiskAssessment,
        side: Literal["buy", "sell"],
        volume_units: int,
        client_intent_id: str | None = None,
        stop_loss_pips: float | None = None,
        take_profit_pips: float | None = None,
        intent_ttl_seconds: float | None = None,
    ) -> ExecutionReceipt:
        """Submit a risk-approved EURUSD Demo market order intent (idempotent on `client_intent_id`)."""
        now = _utc_now(self.clock)
        if assessment.outcome != "approve":
            raise DemoBrokerSafetyError(
                f"Refusing Demo market order without approved RiskAssessment: {assessment.reasons}"
            )
        if assessment.decision_id != decision.decision_id:
            raise DemoBrokerSafetyError("RiskAssessment decision_id does not match AgentDecision")
        if decision.target is None or assessment.target_id != decision.target.target_id:
            raise DemoBrokerSafetyError("RiskAssessment target_id does not match TargetPortfolio")
        if decision.portfolio_action == "CLOSE":
            raise DemoBrokerSafetyError(
                "CLOSE decisions must use submit_close_position, never submit_market_order"
            )
        eur_target = next(
            (p for p in decision.target.positions if p.instrument == "EUR/USD"),
            None,
        )
        if eur_target is None or eur_target.weight == Decimal("0"):
            raise DemoBrokerSafetyError(
                "submit_market_order requires non-zero EUR/USD target weight in decision.target"
            )
        target_side: Literal["buy", "sell"] = "buy" if eur_target.weight > Decimal("0") else "sell"
        if side != target_side:
            raise DemoBrokerSafetyError(
                f"Order side={side!r} contradicts approved decision.target weight={eur_target.weight}"
            )
        if volume_units <= 0 or volume_units % 1000 != 0:
            raise DemoBrokerSafetyError(
                f"EURUSD Demo volume_units must be a positive multiple of 1,000 EUR, got {volume_units}"
            )
        if assessment.approved_quantity_eur is None or assessment.approved_quantity_eur == Decimal(
            "0"
        ):
            raise DemoBrokerSafetyError(
                "RiskAssessment missing non-zero approved_quantity_eur required for Demo sizing gate"
            )
        approved_total_signed = int(assessment.approved_quantity_eur)
        approved_side: Literal["buy", "sell"] = "buy" if approved_total_signed > 0 else "sell"
        if side != approved_side or abs(approved_total_signed) % 1000 != 0:
            raise DemoBrokerSafetyError(
                f"Order side={side!r} contradicts RiskAssessment.approved_quantity_eur={approved_total_signed}"
            )

        intent_id = client_intent_id or f"mkt-{decision.decision_id}"
        with self._lock:
            existing = self.db.execute(
                "SELECT * FROM demo_broker_intents WHERE client_intent_id=?",
                (intent_id,),
            ).fetchone()
            if existing is not None:
                self._ensure_orders_permitted_locked(risk_increasing=False)
                if (
                    str(existing["intent_type"]) != "market_order"
                    or str(existing["decision_id"] or "") != decision.decision_id
                    or str(existing["side"] or "") != side
                    or int(existing["requested_units"] or 0) != volume_units
                ):
                    raise DemoBrokerSafetyError(
                        f"Conflicting parameters on existing client_intent_id={intent_id}"
                    )
                if str(existing["status"]) == "outbox_pending":
                    with self.db:
                        self._flush_outbox_locked(now, is_startup_recovery=False)
                return ExecutionReceipt(
                    execution_id=intent_id,
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="recorded",
                    recorded_at=now,
                    reason="idempotent_existing_intent_reused",
                )

        if now >= assessment.valid_until or now >= decision.valid_until:
            raise DemoBrokerSafetyError("Decision or RiskAssessment expired before Demo dispatch")

        # Poll bridge stream and enforce fresh verified Demo snapshot + clean reconciliation BEFORE new risk
        rec = self.reconcile()
        now = _utc_now(self.clock)
        eff_ttl = (
            float(intent_ttl_seconds)
            if intent_ttl_seconds is not None and float(intent_ttl_seconds) > 0
            else self.max_intent_ttl_seconds
        )
        expires_at = min(
            decision.valid_until.astimezone(UTC),
            assessment.valid_until.astimezone(UTC),
            now + timedelta(seconds=eff_ttl),
        )
        if expires_at <= now:
            raise DemoBrokerSafetyError(
                "Intent UTC expiry deadline already elapsed before dispatch"
            )

        eff_sl = self.default_stop_loss_pips if stop_loss_pips is None else stop_loss_pips
        eff_tp = self.default_take_profit_pips if take_profit_pips is None else take_profit_pips
        intent = DemoOrderIntent(
            client_intent_id=intent_id,
            intent_type="market_order",
            created_at=now,
            expires_at=expires_at,
            decision_id=decision.decision_id,
            assessment_id=assessment.assessment_id,
            side=side,
            volume_units=volume_units,
            stop_loss_pips=eff_sl,
            take_profit_pips=eff_tp,
        )

        with self._lock:
            self._ensure_orders_permitted_locked(risk_increasing=True)
            if rec.drift_reasons or self._halted_reason or self._manual_unlock_required:
                raise DemoBrokerSafetyError(
                    f"New risk blocked by Demo reconciliation: {','.join(rec.drift_reasons) or self._halted_reason or 'halted'}"
                )
            if rec.allow_demo_orders is not True:
                raise DemoBrokerSafetyError(
                    "Latest broker reconcile_snapshot has allow_demo_orders=False"
                )
            if rec.blocks_new_risk:
                raise DemoBrokerSafetyError("New risk blocked by Demo reconciliation")
            open_rows = self.db.execute(
                "SELECT side, volume_units FROM demo_broker_positions WHERE status='open'"
            ).fetchall()
            curr_signed = sum(
                int(r["volume_units"]) if str(r["side"]) == "buy" else -int(r["volume_units"])
                for r in open_rows
            )
            if curr_signed != 0 and (curr_signed > 0) != (approved_total_signed > 0):
                raise DemoBrokerSafetyError(
                    "Opposing Demo position open; close existing broker_position_id before opening opposite leg"
                )
            expected_delta = approved_total_signed - curr_signed
            if (
                expected_delta == 0
                or ((expected_delta > 0) != (side == "buy"))
                or volume_units != abs(expected_delta)
            ):
                raise DemoBrokerSafetyError(
                    f"Order volume_units={volume_units} ({side}) does not match approved target delta={expected_delta} "
                    f"(approved_quantity_eur={approved_total_signed}, current_open_eur={curr_signed})"
                )

            # Stage durably in SQLite FIRST, then flush outbox to order-intents.jsonl
            with self.db:
                self._stage_outbox_locked(intent, now)
            with self.db:
                self._flush_outbox_locked(now, is_startup_recovery=False)

        return ExecutionReceipt(
            execution_id=intent_id,
            decision_id=decision.decision_id,
            target_id=decision.target.target_id,
            assessment_id=assessment.assessment_id,
            mode="ctrader_cli_demo",
            status="recorded",
            recorded_at=now,
            reason="demo_intent_submitted",
        )

    def submit_close_position(
        self,
        *,
        broker_position_id: int,
        volume_units: int | None = None,
        client_intent_id: str | None = None,
        decision_id: str | None = None,
        assessment_id: str | None = None,
        ttl_seconds: float | None = None,
        protective_reason: str | None = None,
    ) -> str:
        """Close an open Demo position strictly by `broker_position_id` on an independent TTL channel."""
        if broker_position_id <= 0:
            raise DemoBrokerSafetyError("close_position requires explicit broker_position_id > 0")
        now = _utc_now(self.clock)
        eff_ttl = (
            float(ttl_seconds)
            if ttl_seconds is not None and float(ttl_seconds) > 0
            else self.protective_intent_ttl_seconds
        )
        expires_at = now + timedelta(seconds=eff_ttl)

        with self._lock:
            if self._halted_reason is not None and "live_account" in self._halted_reason:
                raise LiveAccountForbiddenError(self._halted_reason)
            self._ensure_orders_permitted_locked(risk_increasing=False)

            if client_intent_id is not None:
                existing_by_id = self.db.execute(
                    "SELECT status FROM demo_broker_intents WHERE client_intent_id=?",
                    (client_intent_id,),
                ).fetchone()
                if existing_by_id is not None:
                    if str(existing_by_id["status"]) == "outbox_pending":
                        with self.db:
                            self._flush_outbox_locked(now, is_startup_recovery=False)
                    return client_intent_id
            else:
                # Reuse active non-expired close_position intent for the same broker_position_id
                in_flight_close = self.db.execute(
                    """SELECT client_intent_id, expires_at
                    FROM demo_broker_intents
                    WHERE intent_type='close_position'
                      AND broker_position_id=?
                      AND status IN ('outbox_pending', 'submitted', 'accepted', 'in_flight')
                    ORDER BY rowid DESC LIMIT 1""",
                    (broker_position_id,),
                ).fetchone()
                if in_flight_close is not None:
                    exp_chk = _parse_iso_utc(in_flight_close["expires_at"])
                    if exp_chk is not None and now < exp_chk:
                        return str(in_flight_close["client_intent_id"])

            pos_row = self.db.execute(
                "SELECT status, volume_units FROM demo_broker_positions WHERE broker_position_id=?",
                (broker_position_id,),
            ).fetchone()
            if pos_row is None or pos_row["status"] != "open":
                raise DemoBrokerSafetyError(
                    f"Cannot close unknown or already-closed broker_position_id={broker_position_id}"
                )

            intent_id = client_intent_id or f"close-{broker_position_id}-{uuid4().hex[:8]}"
            intent = DemoOrderIntent(
                client_intent_id=intent_id,
                intent_type="close_position",
                created_at=now,
                expires_at=expires_at,
                decision_id=decision_id,
                assessment_id=assessment_id,
                broker_position_id=broker_position_id,
                volume_units=volume_units,
                protective_reason=protective_reason,
            )
            with self.db:
                self._stage_outbox_locked(intent, now)
            with self.db:
                self._flush_outbox_locked(now, is_startup_recovery=False)
        return intent_id

    def submit_cancel_pending_order(
        self,
        *,
        broker_order_id: int,
        client_intent_id: str | None = None,
        ttl_seconds: float | None = None,
    ) -> str:
        if broker_order_id <= 0:
            raise DemoBrokerSafetyError("cancel_pending_order requires broker_order_id > 0")
        now = _utc_now(self.clock)
        eff_ttl = (
            float(ttl_seconds)
            if ttl_seconds is not None and float(ttl_seconds) > 0
            else self.protective_intent_ttl_seconds
        )
        intent_id = client_intent_id or f"cancel-{broker_order_id}-{uuid4().hex[:8]}"
        intent = DemoOrderIntent(
            client_intent_id=intent_id,
            intent_type="cancel_pending_order",
            created_at=now,
            expires_at=now + timedelta(seconds=eff_ttl),
            broker_order_id=broker_order_id,
        )
        with self._lock:
            self._ensure_orders_permitted_locked(risk_increasing=False)
            with self.db:
                self._stage_outbox_locked(intent, now)
            with self.db:
                self._flush_outbox_locked(now, is_startup_recovery=False)
        return intent_id

    def poll_events(self) -> list[dict[str, Any]]:
        """Consume complete `\n`-terminated frames from `execution-events.jsonl` without losing partial lines."""
        now = _utc_now(self.clock)
        consumed: list[dict[str, Any]] = []
        with self._lock, self.db:
            if not self.events_path.exists():
                return consumed
            size = self.events_path.stat().st_size
            if size < self._events_offset:
                self._halt_locked("execution_events_file_truncated", manual_unlock_required=True)
                return consumed
            if size == self._events_offset:
                return consumed
            with self.events_path.open("rb") as f:
                f.seek(self._events_offset)
                raw_bytes = f.read(size - self._events_offset)
            if not raw_bytes:
                return consumed

            # Advance byte offset ONLY up to the last complete '\n'-terminated byte!
            last_nl = raw_bytes.rfind(b"\n")
            if last_nl < 0:
                return consumed

            complete_len = last_nl + 1
            chunk_text = raw_bytes[:complete_len].decode("utf-8")
            self._events_offset += complete_len
            for raw_line in chunk_text.split("\n"):
                stripped = raw_line.strip()
                if not stripped:
                    continue
                ev = json.loads(stripped)
                self._apply_event_locked(ev, stripped, now)
                consumed.append(ev)
            self.db.execute(
                "UPDATE demo_adapter_meta SET events_offset=? WHERE id=1",
                (self._events_offset,),
            )
        return consumed

    def _apply_event_locked(self, ev: dict[str, Any], raw_line: str, now: datetime) -> None:
        if bool(ev.get("is_live")) or ev.get("account_mode") == "live":
            self._halt_locked(
                "live_account_forbidden_detected_in_bridge_stream",
                manual_unlock_required=True,
            )
            raise LiveAccountForbiddenError("Live account detected in cTrader CLI bridge stream!")

        kind = str(ev.get("kind", ""))
        ev_acct_raw = ev.get("account_number")
        ev_identity_error: str | None = None
        if kind != "safety_refusal":
            ev_acct = parse_strict_int64_account_number(ev_acct_raw, allow_zero=False)
            if ev_acct is None:
                ev_identity_error = f"invalid_or_missing_event_account_number:{ev_acct_raw!r}"
            elif (
                self.expected_demo_account_number is not None
                and ev_acct != self.expected_demo_account_number
            ):
                ev_identity_error = (
                    f"unexpected_demo_account_number:got={ev_acct}"
                    f":expected={self.expected_demo_account_number}"
                )
            ev_ccy = str(ev.get("account_currency") or "").strip().upper()
            if ev_identity_error is None:
                if not ev_ccy:
                    ev_identity_error = "missing_event_account_currency"
                elif ev_ccy != self.expected_account_currency:
                    ev_identity_error = (
                        f"unexpected_account_currency:got={ev_ccy}"
                        f":expected={self.expected_account_currency}"
                    )
        else:
            if ev_acct_raw is not None:
                ev_acct_sr = parse_strict_int64_account_number(ev_acct_raw, allow_zero=True)
                if ev_acct_sr is None:
                    ev_identity_error = f"invalid_event_account_number:{ev_acct_raw!r}"

        session_id = str(ev.get("bridge_session_id") or "legacy-session")
        seq = int(ev.get("event_seq", 0) or 0)
        cid = ev.get("client_intent_id")
        status = ev.get("status")
        if seq > 0:
            cur = self.db.execute(
                """INSERT OR IGNORE INTO demo_broker_events
                (bridge_session_id, event_seq, kind, client_intent_id, status, emitted_at, raw_json, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    seq,
                    kind,
                    cid,
                    status,
                    ev.get("emitted_at"),
                    raw_line,
                    now.isoformat(),
                ),
            )
            if cur.rowcount == 0:
                return

        if kind == "safety_refusal":
            reason = str(ev.get("reason", "bridge_safety_refusal"))
            self._halt_locked(f"bridge_safety_refusal:{reason}", manual_unlock_required=True)
            if "live" in reason:
                raise LiveAccountForbiddenError(reason)
            return

        if ev_identity_error is not None:
            if kind != "reconcile_snapshot":
                self._halt_locked(ev_identity_error, manual_unlock_required=True)
                return
            if ev_identity_error.startswith(("unexpected_", "invalid_")) and ev_acct_raw not in (
                None,
                0,
                "0",
            ):
                self._halt_locked(ev_identity_error, manual_unlock_required=True)
                return

        if kind != "order_event" or not cid:
            return

        intent_row = self.db.execute(
            "SELECT * FROM demo_broker_intents WHERE client_intent_id=?",
            (cid,),
        ).fetchone()
        if intent_row is None:
            return

        curr_status = str(intent_row["status"])
        intent_type = str(intent_row["intent_type"])

        if status == "accepted":
            # Broker ACK only — NEVER mark position open/closed on `accepted`!
            if curr_status in ("outbox_pending", "submitted"):
                self.db.execute(
                    "UPDATE demo_broker_intents SET status='accepted', updated_at=? WHERE client_intent_id=?",
                    (now.isoformat(), cid),
                )
            return

        if status == "duplicate_ignored":
            if curr_status in ("outbox_pending", "submitted", "accepted"):
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status='duplicate_ignored', error_code=?, updated_at=?
                    WHERE client_intent_id=?""",
                    (ev.get("error_code"), now.isoformat(), cid),
                )
            return

        if status == "unknown_execution":
            err = str(ev.get("error_code") or "bridge_reported_unknown_execution")
            self.db.execute(
                """UPDATE demo_broker_intents
                SET status='unknown_execution', error_code=?, updated_at=?
                WHERE client_intent_id=?""",
                (err, now.isoformat(), cid),
            )
            self._halt_locked(f"unknown_intent_outcome:{cid}", manual_unlock_required=True)
            return

        if status in ("rejected", "cancelled", "expired"):
            if curr_status not in ("filled", "partial", "position_closed_unpriced"):
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status=?, error_code=?, updated_at=?
                    WHERE client_intent_id=?""",
                    (status, ev.get("error_code"), now.isoformat(), cid),
                )
            return

        if status in ("partial", "filled", "position_closed_unpriced"):
            pos_id = (
                int(ev["broker_position_id"])
                if ev.get("broker_position_id")
                else (
                    int(intent_row["broker_position_id"])
                    if intent_row["broker_position_id"] is not None
                    else None
                )
            )
            filled_units = int(ev.get("filled_units", 0) or 0)
            fill_px = str(ev["fill_price"]) if ev.get("fill_price") is not None else None
            sl_conf = int(bool(ev.get("sl_confirmed", False)))
            tp_conf = int(bool(ev.get("tp_confirmed", False)))

            if (
                intent_type == "market_order"
                and status in ("partial", "filled")
                and pos_id is not None
                and fill_px is not None
            ):
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status=?, filled_units=?, broker_position_id=COALESCE(?, broker_position_id),
                        fill_price=?, sl_confirmed=?, tp_confirmed=?, updated_at=?
                    WHERE client_intent_id=?""",
                    (status, filled_units, pos_id, fill_px, sl_conf, tp_conf, now.isoformat(), cid),
                )
                side = str(ev.get("side") or json.loads(intent_row["intent_json"]).get("side"))
                open_comm = (
                    str(ev["commission_usd"]) if ev.get("commission_usd") is not None else None
                )
                self.db.execute(
                    """INSERT INTO demo_broker_positions (
                        broker_position_id, client_intent_id, symbol, side,
                        initial_volume_units, volume_units, priced_closed_units, unpriced_closed_units,
                        entry_price, stop_loss, take_profit, sl_confirmed, tp_confirmed,
                        label, status, opened_at, open_commission_usd
                    ) VALUES (?, ?, 'EURUSD', ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
                    ON CONFLICT(broker_position_id) DO UPDATE SET
                        initial_volume_units=MAX(demo_broker_positions.initial_volume_units, excluded.initial_volume_units),
                        volume_units=excluded.volume_units,
                        stop_loss=excluded.stop_loss,
                        take_profit=excluded.take_profit,
                        sl_confirmed=excluded.sl_confirmed,
                        tp_confirmed=excluded.tp_confirmed,
                        open_commission_usd=COALESCE(excluded.open_commission_usd, demo_broker_positions.open_commission_usd),
                        status='open'""",
                    (
                        pos_id,
                        cid,
                        side,
                        filled_units,
                        filled_units,
                        fill_px,
                        ev.get("stop_loss"),
                        ev.get("take_profit"),
                        sl_conf,
                        tp_conf,
                        f"el:{cid}",
                        now.isoformat(),
                        open_comm,
                    ),
                )
                req_sl = json.loads(intent_row["intent_json"]).get("stop_loss_pips")
                if self.require_stop_loss_confirmation and req_sl is not None and not sl_conf:
                    self._halt_locked(f"unconfirmed_broker_stop_loss:pos={pos_id}")
            elif intent_type == "close_position" and pos_id is not None:
                rem_units = max(0, int(ev.get("remaining_units", 0) or 0))
                raw_deals = ev.get("deals")
                if isinstance(raw_deals, list):
                    for raw_d in raw_deals:
                        self._ingest_close_deal_locked(
                            raw_d,
                            source="close_event",
                            default_intent_id=cid,
                            now=now,
                        )
                if rem_units == 0:
                    self.db.execute(
                        """UPDATE demo_broker_positions
                        SET closed_at=COALESCE(closed_at, ?)
                        WHERE broker_position_id=?""",
                        (now.isoformat(), pos_id),
                    )
                acc = self._recompute_position_close_accounting_locked(
                    pos_id,
                    now,
                    current_open_units=rem_units,
                    closed_by_intent=True,
                )
                cid_deals = self.db.execute(
                    """SELECT volume_units, closing_price
                    FROM demo_broker_close_deals
                    WHERE client_intent_id=?
                    ORDER BY closing_time ASC, closing_deal_id ASC""",
                    (cid,),
                ).fetchall()
                cid_priced_u = sum(int(d["volume_units"]) for d in cid_deals)
                if (
                    status == "position_closed_unpriced"
                    or acc["unpriced_closed_units"] > 0
                    or cid_priced_u <= 0
                ):
                    err_code = str(ev.get("error_code") or "broker_history_deal_pending_reconcile")
                    self.db.execute(
                        """UPDATE demo_broker_intents
                        SET status='position_closed_unpriced', filled_units=?,
                            broker_position_id=COALESCE(?, broker_position_id),
                            fill_price=NULL, error_code=?, updated_at=?
                        WHERE client_intent_id=?""",
                        (cid_priced_u, pos_id, err_code, now.isoformat(), cid),
                    )
                else:
                    cid_vwap = sum(
                        Decimal(int(d["volume_units"])) * Decimal(str(d["closing_price"]))
                        for d in cid_deals
                    ) / Decimal(cid_priced_u)
                    eff_close_status = "filled" if rem_units == 0 else "partial"
                    self.db.execute(
                        """UPDATE demo_broker_intents
                        SET status=?, filled_units=?,
                            broker_position_id=COALESCE(?, broker_position_id),
                            fill_price=?, error_code=NULL, updated_at=?
                        WHERE client_intent_id=?""",
                        (
                            eff_close_status,
                            cid_priced_u,
                            pos_id,
                            _format_price_clean(cid_vwap),
                            now.isoformat(),
                            cid,
                        ),
                    )
            else:
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status=?, filled_units=?, broker_position_id=COALESCE(?, broker_position_id),
                        fill_price=?, sl_confirmed=?, tp_confirmed=?, updated_at=?
                    WHERE client_intent_id=?""",
                    (status, filled_units, pos_id, fill_px, sl_conf, tp_conf, now.isoformat(), cid),
                )

    def _ingest_close_deal_locked(
        self,
        raw_deal: Any,
        *,
        source: Literal["close_event", "reconcile_snapshot"],
        default_intent_id: str | None,
        now: datetime,
    ) -> int | None:
        """Persist a verified broker `HistoricalTrade` close deal keyed by unique `closing_deal_id`.

        Never substitutes `entry_price`, live `Bid`/`Ask`, default `1000` units, or `0` PnL/fees:
        any frame lacking a positive 64-bit `closing_deal_id`, `broker_position_id`, `volume_units`,
        `closing_price`, UTC `closing_time`, `gross_profit_usd`, `net_profit_usd`, `commissions_usd`,
        or `swap_usd` is rejected (`None`) so the position remains `position_closed_unpriced`.
        """
        if not isinstance(raw_deal, dict):
            return None
        closing_deal_id = parse_strict_int64_account_number(
            raw_deal.get("closing_deal_id"), allow_zero=False
        )
        broker_position_id = parse_strict_int64_account_number(
            raw_deal.get("broker_position_id")
            if raw_deal.get("broker_position_id") is not None
            else raw_deal.get("position_id"),
            allow_zero=False,
        )
        if closing_deal_id is None or broker_position_id is None:
            return None

        symbol = str(raw_deal.get("symbol") or "EURUSD").strip().upper()
        if symbol != "EURUSD":
            return None

        vol_units = parse_strict_int64_account_number(
            raw_deal.get("volume_units"), allow_zero=False
        )
        close_px = _parse_decimal_strict(raw_deal.get("closing_price"))
        close_dt = _parse_iso_utc(raw_deal.get("closing_time") or raw_deal.get("closed_at"))
        gross_pnl = _parse_decimal_strict(raw_deal.get("gross_profit_usd"))
        net_pnl = _parse_decimal_strict(raw_deal.get("net_profit_usd"))
        comm_usd = _parse_decimal_strict(raw_deal.get("commissions_usd"))
        swap_usd = _parse_decimal_strict(raw_deal.get("swap_usd"))
        if (
            vol_units is None
            or close_px is None
            or close_px <= Decimal("0")
            or close_dt is None
            or gross_pnl is None
            or net_pnl is None
            or comm_usd is None
            or swap_usd is None
        ):
            return None

        pos_row = self.db.execute(
            "SELECT * FROM demo_broker_positions WHERE broker_position_id=?",
            (broker_position_id,),
        ).fetchone()
        raw_label = str(raw_deal.get("label") or "")

        if pos_row is None:
            if source == "reconcile_snapshot":
                # Adopt ONLY if label explicitly matches a local EventLens market_order intent (`el:<cid>`);
                # ignore pre-existing unrelated account history on the Demo account.
                matched_open_cid = raw_label[3:] if raw_label.startswith("el:") else ""
                matched_open_intent = (
                    self.db.execute(
                        """SELECT client_intent_id, side, requested_units, status
                        FROM demo_broker_intents
                        WHERE client_intent_id=? AND intent_type='market_order'""",
                        (matched_open_cid,),
                    ).fetchone()
                    if matched_open_cid
                    else None
                )
                entry_px_opt = _parse_decimal_strict(raw_deal.get("entry_price"))
                raw_side_opt = str(raw_deal.get("position_side") or "").strip().lower()
                if (
                    matched_open_intent is None
                    or entry_px_opt is None
                    or entry_px_opt <= Decimal("0")
                    or raw_side_opt not in ("buy", "sell")
                ):
                    return None
                entry_dt_opt = _parse_iso_utc(raw_deal.get("entry_time")) or close_dt
                init_adopt_u = max(int(matched_open_intent["requested_units"] or 0), vol_units)
                self.db.execute(
                    """INSERT INTO demo_broker_positions (
                        broker_position_id, client_intent_id, symbol, side,
                        initial_volume_units, volume_units, priced_closed_units, unpriced_closed_units,
                        entry_price, sl_confirmed, tp_confirmed, label, status, opened_at
                    ) VALUES (?, ?, 'EURUSD', ?, ?, 0, 0, ?, ?, 1, 0, ?, 'position_closed_unpriced', ?)""",
                    (
                        broker_position_id,
                        matched_open_cid,
                        raw_side_opt,
                        init_adopt_u,
                        init_adopt_u,
                        str(entry_px_opt),
                        raw_label,
                        entry_dt_opt.isoformat(),
                    ),
                )
                if str(matched_open_intent["status"]) in ("submitted", "accepted", "in_flight"):
                    self.db.execute(
                        """UPDATE demo_broker_intents
                        SET status='filled', filled_units=?, broker_position_id=?,
                            fill_price=?, sl_confirmed=1, updated_at=?
                        WHERE client_intent_id=?""",
                        (
                            init_adopt_u,
                            broker_position_id,
                            str(entry_px_opt),
                            now.isoformat(),
                            matched_open_cid,
                        ),
                    )
                pos_row = self.db.execute(
                    "SELECT * FROM demo_broker_positions WHERE broker_position_id=?",
                    (broker_position_id,),
                ).fetchone()
            else:
                return None

        assert pos_row is not None
        pos_side = str(pos_row["side"]).strip().lower()
        deal_side = str(raw_deal.get("position_side") or pos_side).strip().lower()
        if deal_side not in ("buy", "sell") or deal_side != pos_side:
            return None

        existing_deal = self.db.execute(
            "SELECT client_intent_id FROM demo_broker_close_deals WHERE closing_deal_id=?",
            (closing_deal_id,),
        ).fetchone()
        if existing_deal is not None:
            if default_intent_id and not existing_deal["client_intent_id"]:
                self.db.execute(
                    "UPDATE demo_broker_close_deals SET client_intent_id=? WHERE closing_deal_id=?",
                    (default_intent_id, closing_deal_id),
                )
            return None

        resolved_intent_id = default_intent_id
        if resolved_intent_id is None:
            cand_intents = self.db.execute(
                """SELECT client_intent_id, requested_units
                FROM demo_broker_intents
                WHERE intent_type='close_position'
                  AND broker_position_id=?
                  AND status IN ('position_closed_unpriced', 'submitted', 'accepted', 'in_flight')
                ORDER BY rowid ASC""",
                (broker_position_id,),
            ).fetchall()
            for cand in cand_intents:
                cand_cid = str(cand["client_intent_id"])
                linked_row = self.db.execute(
                    "SELECT COALESCE(SUM(volume_units), 0) AS s FROM demo_broker_close_deals WHERE client_intent_id=?",
                    (cand_cid,),
                ).fetchone()
                linked_u = int(linked_row["s"]) if linked_row is not None else 0
                req_u = (
                    int(cand["requested_units"])
                    if cand["requested_units"] is not None
                    else int(
                        pos_row["initial_volume_units"] or pos_row["volume_units"] or vol_units
                    )
                )
                if linked_u < req_u:
                    resolved_intent_id = cand_cid
                    break

        entry_px_deal = _parse_decimal_strict(raw_deal.get("entry_price"))
        cur = self.db.execute(
            """INSERT OR IGNORE INTO demo_broker_close_deals (
                closing_deal_id, broker_position_id, client_intent_id, symbol, position_side,
                volume_units, entry_price, closing_price, closing_time,
                gross_profit_usd, net_profit_usd, commissions_usd, swap_usd,
                label, source, recorded_at
            ) VALUES (?, ?, ?, 'EURUSD', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                closing_deal_id,
                broker_position_id,
                resolved_intent_id,
                deal_side,
                vol_units,
                str(entry_px_deal) if entry_px_deal is not None else str(pos_row["entry_price"]),
                str(close_px),
                close_dt.isoformat(),
                str(gross_pnl),
                str(net_pnl),
                str(comm_usd),
                str(swap_usd),
                raw_label,
                source,
                now.isoformat(),
            ),
        )
        return closing_deal_id if cur.rowcount > 0 else None

    def _recompute_position_close_accounting_locked(
        self,
        pos_id: int,
        now: datetime,
        *,
        current_open_units: int | None = None,
        closed_by_intent: bool = False,
    ) -> dict[str, int]:
        """Reconcile `demo_broker_positions` and awaiting `close_position` intents from `demo_broker_close_deals`."""
        pos_row = self.db.execute(
            "SELECT * FROM demo_broker_positions WHERE broker_position_id=?",
            (pos_id,),
        ).fetchone()
        if pos_row is None:
            return {
                "initial_volume_units": 0,
                "open_units": 0,
                "priced_closed_units": 0,
                "unpriced_closed_units": 0,
                "deal_count": 0,
            }

        deal_rows = self.db.execute(
            """SELECT * FROM demo_broker_close_deals
            WHERE broker_position_id=?
            ORDER BY closing_time ASC, closing_deal_id ASC""",
            (pos_id,),
        ).fetchall()
        open_u = max(
            0,
            int(pos_row["volume_units"]) if current_open_units is None else int(current_open_units),
        )
        priced_u = sum(int(d["volume_units"]) for d in deal_rows)
        recorded_init_u = int(pos_row["initial_volume_units"] or 0)
        if recorded_init_u > 0:
            init_u = max(recorded_init_u, open_u, priced_u)
        else:
            init_u = max(open_u + priced_u, int(pos_row["volume_units"] or 0))
        closed_total_u = max(0, init_u - open_u)
        unpriced_u = max(0, closed_total_u - priced_u)
        deal_count = len(deal_rows)

        close_intents = self.db.execute(
            """SELECT client_intent_id, status, requested_units
            FROM demo_broker_intents
            WHERE intent_type='close_position' AND broker_position_id=?
            ORDER BY rowid ASC""",
            (pos_id,),
        ).fetchall()
        has_close_intent = closed_by_intent or any(
            str(ci["status"])
            in (
                "partial",
                "filled",
                "position_closed_unpriced",
                "submitted",
                "accepted",
                "in_flight",
            )
            for ci in close_intents
        )

        for ci in close_intents:
            ci_cid = str(ci["client_intent_id"])
            ci_status = str(ci["status"])
            if ci_status != "position_closed_unpriced":
                continue
            ci_deals = [d for d in deal_rows if str(d["client_intent_id"] or "") == ci_cid]
            ci_vol = sum(int(d["volume_units"]) for d in ci_deals)
            req_u = (
                int(ci["requested_units"]) if ci["requested_units"] is not None else closed_total_u
            )
            if ci_vol > 0 and ((req_u > 0 and ci_vol >= req_u) or unpriced_u == 0):
                ci_vwap = sum(
                    Decimal(int(d["volume_units"])) * Decimal(str(d["closing_price"]))
                    for d in ci_deals
                ) / Decimal(ci_vol)
                new_ci_status = "filled" if open_u == 0 else "partial"
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status=?, filled_units=?, fill_price=?, error_code=NULL, updated_at=?
                    WHERE client_intent_id=?""",
                    (
                        new_ci_status,
                        ci_vol,
                        _format_price_clean(ci_vwap),
                        now.isoformat(),
                        ci_cid,
                    ),
                )

        if open_u > 0:
            self.db.execute(
                """UPDATE demo_broker_positions
                SET initial_volume_units=?, volume_units=?, priced_closed_units=?,
                    unpriced_closed_units=?, status='open'
                WHERE broker_position_id=?""",
                (init_u, open_u, priced_u, unpriced_u, pos_id),
            )
            eff_unpriced = unpriced_u
        elif unpriced_u > 0 or deal_count == 0:
            eff_unpriced = max(unpriced_u, 1 if deal_count == 0 else 0)
            closed_ts = str(pos_row["closed_at"]) if pos_row["closed_at"] else now.isoformat()
            self.db.execute(
                """UPDATE demo_broker_positions
                SET initial_volume_units=?, volume_units=0, priced_closed_units=?,
                    unpriced_closed_units=?, status='position_closed_unpriced',
                    closed_at=?, close_price=NULL, realized_net_pnl_usd=NULL,
                    commission_usd=NULL, swap_usd=NULL
                WHERE broker_position_id=?""",
                (init_u, priced_u, eff_unpriced, closed_ts, pos_id),
            )
        else:
            eff_unpriced = 0
            vwap_px = sum(
                Decimal(int(d["volume_units"])) * Decimal(str(d["closing_price"]))
                for d in deal_rows
            ) / Decimal(priced_u)
            tot_net = sum((Decimal(str(d["net_profit_usd"])) for d in deal_rows), Decimal("0.00"))
            tot_comm = sum((Decimal(str(d["commissions_usd"])) for d in deal_rows), Decimal("0.00"))
            tot_swap = sum((Decimal(str(d["swap_usd"])) for d in deal_rows), Decimal("0.00"))
            last_close_time = str(deal_rows[-1]["closing_time"])
            final_status = (
                "closed"
                if (str(pos_row["status"]) == "closed" or has_close_intent)
                else "closed_by_broker_reconcile"
            )
            self.db.execute(
                """UPDATE demo_broker_positions
                SET initial_volume_units=?, volume_units=0, priced_closed_units=?,
                    unpriced_closed_units=0, status=?, closed_at=?, close_price=?,
                    realized_net_pnl_usd=?, commission_usd=?, swap_usd=?
                WHERE broker_position_id=?""",
                (
                    init_u,
                    priced_u,
                    final_status,
                    last_close_time,
                    _format_price_clean(vwap_px),
                    _format_usd_clean(tot_net),
                    _format_usd_clean(tot_comm),
                    _format_usd_clean(tot_swap),
                    pos_id,
                ),
            )

        return {
            "initial_volume_units": init_u,
            "open_units": open_u,
            "priced_closed_units": priced_u,
            "unpriced_closed_units": eff_unpriced,
            "deal_count": deal_count,
        }

    def _expire_or_flag_stale_in_flight_locked(self, now: datetime) -> list[str]:
        """Transition overdue in-flight intents without broker terminal events into `unknown_execution`."""
        rows = self.db.execute(
            """SELECT client_intent_id, expires_at
            FROM demo_broker_intents
            WHERE status IN ('submitted', 'accepted', 'in_flight')"""
        ).fetchall()
        flagged: list[str] = []
        for r in rows:
            cid = str(r["client_intent_id"])
            exp_dt = _parse_iso_utc(r["expires_at"])
            if exp_dt is None:
                continue
            if now > exp_dt + timedelta(seconds=self.in_flight_unknown_timeout_seconds):
                self.db.execute(
                    """UPDATE demo_broker_intents
                    SET status='unknown_execution',
                        error_code='in_flight_intent_timed_out_outcome_unknown',
                        updated_at=?
                    WHERE client_intent_id=?""",
                    (now.isoformat(), cid),
                )
                self._halt_locked(f"unknown_intent_outcome:{cid}", manual_unlock_required=True)
                flagged.append(cid)
        return flagged

    def reconcile(self) -> DemoReconcileResult:
        """Poll bridge events and reconcile local confirmed positions & `History` deals against latest broker snapshot."""
        self.poll_events()
        now = _utc_now(self.clock)
        with self._lock, self.db:
            self._expire_or_flag_stale_in_flight_locked(now)
            unknown_rows = self.db.execute(
                "SELECT client_intent_id FROM demo_broker_intents WHERE status='unknown_execution' ORDER BY rowid"
            ).fetchall()
            unknown_ids = tuple(str(r["client_intent_id"]) for r in unknown_rows)
            if unknown_ids and not self._manual_unlock_required:
                self._halt_locked(
                    f"unknown_intent_outcome:{unknown_ids[0]}", manual_unlock_required=True
                )

            snap_row = self.db.execute(
                """SELECT bridge_session_id, event_seq, raw_json
                FROM demo_broker_events
                WHERE kind='reconcile_snapshot'
                ORDER BY event_rowid DESC LIMIT 1"""
            ).fetchone()
            if snap_row is None:
                drift = ["no_reconcile_snapshot_received_from_cli_bridge"]
                if self._manual_unlock_required:
                    drift.append(f"manual_unlock_required:{self._halted_reason or 'unknown'}")
                return DemoReconcileResult(
                    reconciled_at=now,
                    trigger="missing_broker_snapshot",
                    clean=False,
                    blocks_new_risk=True,
                    drift_reasons=tuple(drift),
                    manual_unlock_required=self._manual_unlock_required,
                    unknown_intent_ids=unknown_ids,
                )

            snap_sess_id = str(snap_row["bridge_session_id"])
            snap_seq_num = int(snap_row["event_seq"])
            max_order_row = self.db.execute(
                """SELECT COALESCE(MAX(event_seq), 0) AS max_seq
                FROM demo_broker_events
                WHERE bridge_session_id=? AND kind='order_event'""",
                (snap_sess_id,),
            ).fetchone()
            snapshot_predates_latest_order_event = (
                int(max_order_row["max_seq"]) > snap_seq_num if max_order_row is not None else False
            )

            snap = json.loads(snap_row["raw_json"])
            snap_ok, snap_reasons, snap_age = self._validate_snapshot_dict_locked(snap, now=now)
            drift_reasons: list[str] = list(snap_reasons)

            broker_positions: dict[int, DemoBrokerPosition] = {}
            raw_open_by_id: dict[int, dict[str, Any]] = {}
            for raw_p in snap.get("open_positions", []):
                bp = DemoBrokerPosition(
                    broker_position_id=int(raw_p["broker_position_id"]),
                    symbol=str(raw_p.get("symbol", "EURUSD")),
                    side=raw_p["side"],
                    volume_units=int(raw_p["volume_units"]),
                    entry_price=Decimal(str(raw_p["entry_price"])),
                    stop_loss=(
                        Decimal(str(raw_p["stop_loss"]))
                        if raw_p.get("stop_loss") is not None
                        else None
                    ),
                    take_profit=(
                        Decimal(str(raw_p["take_profit"]))
                        if raw_p.get("take_profit") is not None
                        else None
                    ),
                    sl_confirmed=bool(raw_p.get("sl_confirmed", False)),
                    tp_confirmed=bool(raw_p.get("tp_confirmed", False)),
                    label=str(raw_p.get("label", "")),
                    opened_at=_parse_iso_utc(raw_p.get("entry_time")),
                )
                broker_positions[bp.broker_position_id] = bp
                raw_open_by_id[bp.broker_position_id] = raw_p

            local_active_rows = self.db.execute(
                """SELECT * FROM demo_broker_positions
                WHERE status IN ('open', 'position_closed_unpriced')
                   OR unpriced_closed_units > 0"""
            ).fetchall()
            local_by_id = {int(r["broker_position_id"]): r for r in local_active_rows}
            was_open_before_reconcile = {
                int(r["broker_position_id"])
                for r in local_active_rows
                if str(r["status"]) == "open"
            }

            orphans: list[int] = []
            closed_at_broker: list[int] = []
            unconfirmed_sl: list[int] = []

            for pid, bp in broker_positions.items():
                if self.require_stop_loss_confirmation and not bp.sl_confirmed:
                    unconfirmed_sl.append(pid)
                    drift_reasons.append(f"broker_position_missing_stop_loss:{pid}")
                if pid not in local_by_id:
                    existing_any = self.db.execute(
                        "SELECT * FROM demo_broker_positions WHERE broker_position_id=?",
                        (pid,),
                    ).fetchone()
                    if existing_any is not None:
                        local_by_id[pid] = existing_any
                        continue
                    # Check if this broker position matches a local market_order intent in unknown/submitted/accepted state
                    matched_cid = bp.label[3:] if bp.label.startswith("el:") else ""
                    matched_intent = (
                        self.db.execute(
                            """SELECT client_intent_id, status FROM demo_broker_intents
                            WHERE client_intent_id=? AND intent_type='market_order'""",
                            (matched_cid,),
                        ).fetchone()
                        if matched_cid
                        else None
                    )
                    if matched_intent is not None:
                        raw_p_row = raw_open_by_id.get(pid, {})
                        open_comm_snap = (
                            str(raw_p_row["commissions_usd"])
                            if raw_p_row.get("commissions_usd") is not None
                            else None
                        )
                        # Adopt confirmed broker position so protective close knows its broker_position_id!
                        self.db.execute(
                            """INSERT INTO demo_broker_positions (
                                broker_position_id, client_intent_id, symbol, side,
                                initial_volume_units, volume_units, priced_closed_units, unpriced_closed_units,
                                entry_price, stop_loss, take_profit, sl_confirmed, tp_confirmed,
                                label, status, opened_at, open_commission_usd
                            ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
                            ON CONFLICT(broker_position_id) DO UPDATE SET
                                initial_volume_units=MAX(demo_broker_positions.initial_volume_units, excluded.initial_volume_units),
                                volume_units=excluded.volume_units,
                                sl_confirmed=excluded.sl_confirmed,
                                open_commission_usd=COALESCE(excluded.open_commission_usd, demo_broker_positions.open_commission_usd),
                                status='open'""",
                            (
                                pid,
                                matched_cid,
                                bp.symbol,
                                bp.side,
                                bp.volume_units,
                                bp.volume_units,
                                str(bp.entry_price),
                                str(bp.stop_loss) if bp.stop_loss is not None else None,
                                str(bp.take_profit) if bp.take_profit is not None else None,
                                int(bp.sl_confirmed),
                                int(bp.tp_confirmed),
                                bp.label,
                                (bp.opened_at or now).isoformat(),
                                open_comm_snap,
                            ),
                        )
                        if str(matched_intent["status"]) in ("submitted", "accepted", "in_flight"):
                            self.db.execute(
                                """UPDATE demo_broker_intents
                                SET status='filled', filled_units=?, broker_position_id=?,
                                    fill_price=?, sl_confirmed=?, tp_confirmed=?, updated_at=?
                                WHERE client_intent_id=?""",
                                (
                                    bp.volume_units,
                                    pid,
                                    str(bp.entry_price),
                                    int(bp.sl_confirmed),
                                    int(bp.tp_confirmed),
                                    now.isoformat(),
                                    matched_cid,
                                ),
                            )
                        adopted_row = self.db.execute(
                            "SELECT * FROM demo_broker_positions WHERE broker_position_id=?",
                            (pid,),
                        ).fetchone()
                        if adopted_row is not None:
                            local_by_id[pid] = adopted_row
                    else:
                        orphans.append(pid)
                        drift_reasons.append(f"orphan_broker_position_not_in_local_state:{pid}")

            # Ingest broker `HistoricalTrade` close deals (`closed_deals`) keyed by unique `closing_deal_id`
            raw_closed_deals = snap.get("closed_deals")
            if isinstance(raw_closed_deals, list):
                for raw_d in raw_closed_deals:
                    inserted_did = self._ingest_close_deal_locked(
                        raw_d,
                        source="reconcile_snapshot",
                        default_intent_id=None,
                        now=now,
                    )
                    if inserted_did is not None and isinstance(raw_d, dict):
                        d_pid = parse_strict_int64_account_number(
                            raw_d.get("broker_position_id")
                            if raw_d.get("broker_position_id") is not None
                            else raw_d.get("position_id"),
                            allow_zero=False,
                        )
                        if d_pid is not None and d_pid not in local_by_id:
                            d_pos_row = self.db.execute(
                                "SELECT * FROM demo_broker_positions WHERE broker_position_id=?",
                                (d_pid,),
                            ).fetchone()
                            if d_pos_row is not None:
                                local_by_id[d_pid] = d_pos_row

            for pid, loc in list(local_by_id.items()):
                if pid in broker_positions:
                    bp = broker_positions[pid]
                    loc_open_u = max(0, int(loc["volume_units"] or 0))
                    if snapshot_predates_latest_order_event and loc_open_u < bp.volume_units:
                        # A newer `order_event` in the same bridge session already closed/reduced `pid`
                        # after this snapshot was emitted; never reopen or inflate `initial_volume_units`!
                        self._recompute_position_close_accounting_locked(
                            pid,
                            now,
                            current_open_units=loc_open_u,
                            closed_by_intent=False,
                        )
                        continue
                    acc = self._recompute_position_close_accounting_locked(
                        pid,
                        now,
                        current_open_units=bp.volume_units,
                        closed_by_intent=False,
                    )
                    init_u = acc["initial_volume_units"]
                    priced_u = acc["priced_closed_units"]
                    if (
                        str(loc["side"]) != bp.side
                        or bp.volume_units > init_u
                        or priced_u > max(0, init_u - bp.volume_units)
                    ):
                        drift_reasons.append(
                            f"position_volume_or_side_mismatch:{pid}:local={loc['side']}/{loc['volume_units']}"
                            f"_vs_broker={bp.side}/{bp.volume_units}"
                        )
                else:
                    if pid in was_open_before_reconcile:
                        closed_at_broker.append(pid)
                    self._recompute_position_close_accounting_locked(
                        pid,
                        now,
                        current_open_units=0,
                        closed_by_intent=False,
                    )

            unpriced_rows = self.db.execute(
                """SELECT broker_position_id, unpriced_closed_units
                FROM demo_broker_positions
                WHERE status='position_closed_unpriced' OR unpriced_closed_units > 0
                ORDER BY broker_position_id ASC"""
            ).fetchall()
            unpriced_ids: list[int] = []
            for ur in unpriced_rows:
                u_pid = int(ur["broker_position_id"])
                u_units = int(ur["unpriced_closed_units"])
                unpriced_ids.append(u_pid)
                drift_reasons.append(
                    f"position_closed_unpriced_awaiting_history:{u_pid}:unpriced_units={u_units}"
                )

            deal_cnt_row = self.db.execute(
                "SELECT COUNT(*) AS c FROM demo_broker_close_deals"
            ).fetchone()
            closed_deal_count = int(deal_cnt_row["c"]) if deal_cnt_row is not None else 0

            if orphans or unconfirmed_sl:
                self._halt_locked(drift_reasons[0])

            if self._manual_unlock_required:
                drift_reasons.append(
                    f"manual_unlock_required:{self._halted_reason or 'ambiguous_execution'}"
                )

            acct_num = parse_strict_int64_account_number(
                snap.get("account_number"), allow_zero=False
            )
            allow_demo_flag = (
                bool(snap["allow_demo_orders"])
                if "allow_demo_orders" in snap and snap["allow_demo_orders"] is not None
                else None
            )
            blocks_new_risk = bool(
                not snap_ok
                or orphans
                or unconfirmed_sl
                or unpriced_ids
                or drift_reasons
                or self._halted_reason
                or self._manual_unlock_required
                or allow_demo_flag is False
            )

            return DemoReconcileResult(
                reconciled_at=now,
                trigger=str(snap.get("trigger", "reconcile")),
                clean=snap_ok and not bool(drift_reasons),
                blocks_new_risk=blocks_new_risk,
                drift_reasons=tuple(drift_reasons),
                orphan_broker_position_ids=tuple(sorted(orphans)),
                closed_at_broker_position_ids=tuple(sorted(closed_at_broker)),
                unpriced_closed_position_ids=tuple(sorted(unpriced_ids)),
                unconfirmed_sl_position_ids=tuple(sorted(unconfirmed_sl)),
                closed_deal_ids_count=closed_deal_count,
                broker_balance_usd=(
                    Decimal(str(snap["balance_usd"])) if snap.get("balance_usd") else None
                ),
                broker_equity_usd=(
                    Decimal(str(snap["equity_usd"])) if snap.get("equity_usd") else None
                ),
                broker_margin_used_usd=(
                    Decimal(str(snap["margin_used_usd"])) if snap.get("margin_used_usd") else None
                ),
                open_positions=tuple(broker_positions.values()),
                bridge_session_id=str(snap_row["bridge_session_id"]),
                snapshot_event_seq=int(snap_row["event_seq"]),
                snapshot_emitted_at=_parse_iso_utc(snap.get("emitted_at")),
                snapshot_age_seconds=snap_age,
                account_number=acct_num,
                account_currency=(
                    str(snap.get("account_currency")).strip().upper()
                    if snap.get("account_currency")
                    else None
                ),
                allow_demo_orders=allow_demo_flag,
                manual_unlock_required=self._manual_unlock_required,
                unknown_intent_ids=unknown_ids,
            )

    def acknowledge_manual_unlock(self, *, note: str) -> DemoReconcileResult:
        """Explicit operator unlock after verifying broker state is reconciled & free of orphans/in-flight orders."""
        cleaned_note = note.strip()
        if len(cleaned_note) < 5:
            raise DemoBrokerSafetyError(
                "acknowledge_manual_unlock requires a non-empty audit note (>= 5 chars)"
            )
        if self._halted_reason is not None and "live_account" in self._halted_reason:
            raise LiveAccountForbiddenError(
                "Cannot unlock an adapter halted due to Live account detection"
            )

        rec = self.reconcile()
        now = _utc_now(self.clock)
        non_unlock_drift = [
            r for r in rec.drift_reasons if not r.startswith("manual_unlock_required:")
        ]
        if non_unlock_drift or rec.orphan_broker_position_ids or rec.unconfirmed_sl_position_ids:
            raise DemoBrokerSafetyError(
                f"Cannot unlock while broker reconciliation has unresolved drift: {non_unlock_drift}"
            )

        with self._lock, self.db:
            in_flight = self.db.execute(
                """SELECT client_intent_id FROM demo_broker_intents
                WHERE status IN ('outbox_pending', 'submitted', 'accepted', 'in_flight')"""
            ).fetchall()
            if in_flight:
                ids = [str(r["client_intent_id"]) for r in in_flight]
                raise DemoBrokerSafetyError(f"Cannot unlock while intents remain in flight: {ids}")

            self.db.execute(
                """UPDATE demo_broker_intents
                SET status='reconciled_after_unknown', updated_at=?
                WHERE status='unknown_execution'""",
                (now.isoformat(),),
            )
            audit_payload = json.dumps(
                {
                    "unlocked_at": now.isoformat(),
                    "previous_halted_reason": self._halted_reason,
                    "note": cleaned_note,
                    "bridge_session_id": rec.bridge_session_id,
                    "snapshot_event_seq": rec.snapshot_event_seq,
                    "open_position_ids": [p.broker_position_id for p in rec.open_positions],
                }
            )
            self._halted_reason = None
            self._manual_unlock_required = False
            self.db.execute(
                """UPDATE demo_adapter_meta
                SET halted_reason=NULL, manual_unlock_required=0, unlock_audit_json=?
                WHERE id=1""",
                (audit_payload,),
            )

        return self.reconcile()

    def get_intent(self, client_intent_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT * FROM demo_broker_intents WHERE client_intent_id=?",
                (client_intent_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def active_pending_intent_ids(self) -> tuple[str, ...]:
        with self._lock:
            placeholders = ",".join("?" for _ in _ACTIVE_PENDING_STATUSES)
            rows = self.db.execute(
                f"SELECT client_intent_id FROM demo_broker_intents WHERE status IN ({placeholders}) ORDER BY rowid ASC",
                _ACTIVE_PENDING_STATUSES,
            ).fetchall()
            return tuple(str(r["client_intent_id"]) for r in rows)

    def open_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM demo_broker_positions WHERE status='open' ORDER BY broker_position_id"
            ).fetchall()
            return [dict(r) for r in rows]

    def unpriced_closed_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.db.execute(
                """SELECT * FROM demo_broker_positions
                WHERE status='position_closed_unpriced' OR unpriced_closed_units > 0
                ORDER BY broker_position_id ASC"""
            ).fetchall()
            return [dict(r) for r in rows]

    def closed_deals(self, *, broker_position_id: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if broker_position_id is not None:
                rows = self.db.execute(
                    """SELECT * FROM demo_broker_close_deals
                    WHERE broker_position_id=?
                    ORDER BY closing_time ASC, closing_deal_id ASC""",
                    (broker_position_id,),
                ).fetchall()
            else:
                rows = self.db.execute(
                    """SELECT * FROM demo_broker_close_deals
                    ORDER BY closing_time ASC, closing_deal_id ASC"""
                ).fetchall()
            return [dict(r) for r in rows]


class CTraderCliDemoExecutionEngine:
    """Runtime `PortfolioState` + `ShadowExecutor` strictly backed by broker-confirmed CLI Demo fills.

    Never uses Shadow simulated fills (`SQLiteShadowLedger.fill_pending` or
    `SQLiteShadowLedger.evaluate_protective_exits`) to represent Demo positions.
    """

    def __init__(
        self,
        *,
        adapter: CTraderCliDemoBrokerAdapter,
        risk_policy: RiskPolicy,
        exit_policy: ProtectiveExitPolicy,
        fallback_initial_equity_usd: Decimal = Decimal("200.00"),
        market_provider: Callable[[datetime], MarketSnapshot] | None = None,
        stop_reason_provider: Callable[[datetime], str | None] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.adapter = adapter
        self.risk_policy = risk_policy
        self.exit_policy = exit_policy
        self.fallback_initial_equity_usd = fallback_initial_equity_usd
        self.market_provider = market_provider
        self.stop_reason_provider = stop_reason_provider
        self.clock = clock
        self._lock = threading.Lock()
        self._stop_exit_reason: str | None = None
        self._peak_favorable_pips: dict[int, Decimal] = {}

    def _compute_snapshot_id(
        self, open_pos: list[dict[str, Any]], pending_ids: tuple[str, ...]
    ) -> str:
        payload = json.dumps(
            {
                "open": [
                    (
                        int(p["broker_position_id"]),
                        str(p["side"]),
                        int(p["volume_units"]),
                    )
                    for p in open_pos
                ],
                "pending": list(pending_ids),
            },
            sort_keys=True,
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        return f"demo-port-{digest}"

    def _update_daily_state(
        self,
        now: datetime,
        equity: Decimal,
        *,
        signed_qty: Decimal,
        has_unpriced_closes: bool,
        valid_snapshot_and_quote: bool,
        invalid_reason: str,
    ) -> tuple[Decimal, Decimal, bool, bool, str]:
        day_id, start_utc, close_utc, rollover_utc = self.exit_policy.effective_window(now)
        limit = min(self.exit_policy.max_daily_loss_usd, self.risk_policy.max_daily_loss_usd)
        with self.adapter._lock, self.adapter.db:
            meta = self.adapter.db.execute("SELECT * FROM demo_adapter_meta WHERE id=1").fetchone()
            last_valid_eq: Decimal | None = (
                Decimal(str(meta["last_valid_equity_usd"]))
                if meta is not None and meta["last_valid_equity_usd"] is not None
                else None
            )
            last_valid_qty = (
                Decimal(str(meta["last_valid_open_qty_eur"] or "0"))
                if meta is not None
                else Decimal("0")
            )
            last_valid_at_dt = _parse_iso_utc(meta["last_valid_at"]) if meta is not None else None

            prior_row = self.adapter.db.execute(
                "SELECT * FROM demo_daily_state WHERE trading_day < ? ORDER BY trading_day DESC LIMIT 1",
                (day_id,),
            ).fetchone()
            prior_baseline: Decimal | None = (
                Decimal(str(prior_row["baseline_equity_usd"])) if prior_row is not None else None
            )

            deal_rows_recent = self.adapter.db.execute(
                "SELECT closing_time, net_profit_usd FROM demo_broker_close_deals"
            ).fetchall()
            has_deal_since_rollover_or_last_valid = False
            today_realized_pnl = Decimal("0.00")
            for dr in deal_rows_recent:
                ct = _parse_iso_utc(dr["closing_time"])
                if ct is None:
                    continue
                if ct >= start_utc:
                    has_deal_since_rollover_or_last_valid = True
                    today_realized_pnl += Decimal(str(dr["net_profit_usd"]))
                elif last_valid_at_dt is not None and ct > last_valid_at_dt:
                    has_deal_since_rollover_or_last_valid = True

            had_carried_or_outage_risk = (
                signed_qty != Decimal("0")
                or last_valid_qty != Decimal("0")
                or has_unpriced_closes
                or has_deal_since_rollover_or_last_valid
            )
            if had_carried_or_outage_risk:
                prior_candidates = [x for x in (last_valid_eq, prior_baseline) if x is not None]
                if prior_candidates:
                    carried_ref: Decimal | None = max(prior_candidates)
                elif today_realized_pnl < Decimal("0"):
                    carried_ref = (equity - today_realized_pnl).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                else:
                    carried_ref = None
            else:
                carried_ref = last_valid_eq

            row = self.adapter.db.execute(
                "SELECT * FROM demo_daily_state WHERE trading_day=?",
                (day_id,),
            ).fetchone()
            if row is None:
                if not valid_snapshot_and_quote:
                    has_prior_ref = 1 if carried_ref is not None else 0
                    provisional = (
                        carried_ref if carried_ref is not None else self.fallback_initial_equity_usd
                    )
                    unconf_reason = (
                        "unconfirmed_open_position_without_valid_quote"
                        if (signed_qty != Decimal("0") or last_valid_qty != Decimal("0"))
                        else invalid_reason
                    )
                    self.adapter.db.execute(
                        """INSERT INTO demo_daily_state
                        (trading_day, session_start_utc, daily_close_utc, session_rollover_utc,
                         baseline_equity_usd, min_equity_seen_usd, loss_limit_triggered,
                         triggered_at, trigger_reason, baseline_valid, baseline_reason,
                         provisional_has_prior_ref, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL, 0, ?, ?, ?)""",
                        (
                            day_id,
                            start_utc.isoformat(),
                            close_utc.isoformat(),
                            rollover_utc.isoformat(),
                            str(provisional),
                            str(provisional),
                            unconf_reason,
                            has_prior_ref,
                            now.isoformat(),
                        ),
                    )
                    return provisional, Decimal("0.00"), False, False, unconf_reason

                if carried_ref is not None:
                    confirmed_base = max(carried_ref, equity)
                    if had_carried_or_outage_risk and carried_ref > equity:
                        base_reason = "confirmed_carried_position_conservative"
                    elif carried_ref > equity:
                        base_reason = "confirmed_after_outage_conservative"
                    elif signed_qty != Decimal("0"):
                        base_reason = "confirmed_marked_to_market"
                    else:
                        base_reason = "confirmed_flat"
                else:
                    confirmed_base = equity
                    base_reason = (
                        "confirmed_marked_to_market"
                        if signed_qty != Decimal("0")
                        else "confirmed_flat"
                    )
                min_seen = min(confirmed_base, equity)
                daily_pnl = (equity - confirmed_base).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
                triggered = daily_pnl <= -limit
                triggered_at = now.isoformat() if triggered else None
                trigger_reason = "daily_loss_limit_breached" if triggered else None
                self.adapter.db.execute(
                    """INSERT INTO demo_daily_state
                    (trading_day, session_start_utc, daily_close_utc, session_rollover_utc,
                     baseline_equity_usd, min_equity_seen_usd, loss_limit_triggered,
                     triggered_at, trigger_reason, baseline_valid, baseline_reason,
                     provisional_has_prior_ref, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 1, ?)""",
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
                        now.isoformat(),
                    ),
                )
                self.adapter.db.execute(
                    """UPDATE demo_adapter_meta
                    SET last_valid_equity_usd=?, last_valid_open_qty_eur=?,
                        last_valid_trading_day=?, last_valid_at=?
                    WHERE id=1""",
                    (str(equity), str(signed_qty), day_id, now.isoformat()),
                )
                return confirmed_base, daily_pnl, triggered, True, base_reason

            if not valid_snapshot_and_quote:
                was_valid_existing = bool(row["baseline_valid"])
                has_ref_existing = bool(row["provisional_has_prior_ref"])
                existing_base = Decimal(str(row["baseline_equity_usd"]))
                if not was_valid_existing and carried_ref is not None:
                    base_out = max(existing_base, carried_ref) if has_ref_existing else carried_ref
                    if base_out != existing_base or not has_ref_existing:
                        self.adapter.db.execute(
                            """UPDATE demo_daily_state
                            SET baseline_equity_usd=?, min_equity_seen_usd=?,
                                provisional_has_prior_ref=1, updated_at=?
                            WHERE trading_day=?""",
                            (str(base_out), str(base_out), now.isoformat(), day_id),
                        )
                else:
                    base_out = existing_base
                trig = bool(row["loss_limit_triggered"])
                reason_out = (
                    str(row["baseline_reason"]) if not was_valid_existing else invalid_reason
                )
                return base_out, Decimal("0.00"), trig, False, reason_out

            was_valid = bool(row["baseline_valid"])
            has_ref = bool(row["provisional_has_prior_ref"])
            if not was_valid:
                candidates_rec: list[Decimal] = [equity]
                if has_ref:
                    candidates_rec.append(Decimal(str(row["baseline_equity_usd"])))
                if carried_ref is not None:
                    candidates_rec.append(carried_ref)
                baseline = max(candidates_rec)
                base_reason = (
                    "confirmed_after_quote_recovery_conservative"
                    if (has_ref or carried_ref is not None)
                    else (
                        "confirmed_marked_to_market"
                        if signed_qty != Decimal("0")
                        else "confirmed_flat"
                    )
                )
            else:
                baseline = Decimal(str(row["baseline_equity_usd"]))
                base_reason = str(row["baseline_reason"])

            min_seen = min(Decimal(str(row["min_equity_seen_usd"])), baseline, equity)
            daily_pnl = (equity - baseline).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            triggered = bool(row["loss_limit_triggered"])
            triggered_at = row["triggered_at"]
            trigger_reason = row["trigger_reason"]
            if not triggered and daily_pnl <= -limit:
                triggered = True
                triggered_at = now.isoformat()
                trigger_reason = "daily_loss_limit_breached"
            self.adapter.db.execute(
                """UPDATE demo_daily_state
                SET baseline_equity_usd=?, min_equity_seen_usd=?, loss_limit_triggered=?,
                    triggered_at=?, trigger_reason=?, baseline_valid=1, baseline_reason=?,
                    provisional_has_prior_ref=1, updated_at=?
                WHERE trading_day=?""",
                (
                    str(baseline),
                    str(min_seen),
                    int(triggered),
                    triggered_at,
                    trigger_reason,
                    base_reason,
                    now.isoformat(),
                    day_id,
                ),
            )
            self.adapter.db.execute(
                """UPDATE demo_adapter_meta
                SET last_valid_equity_usd=?, last_valid_open_qty_eur=?,
                    last_valid_trading_day=?, last_valid_at=?
                WHERE id=1""",
                (str(equity), str(signed_qty), day_id, now.isoformat()),
            )
            return baseline, daily_pnl, triggered, True, base_reason

    async def snapshot(self, *, market: MarketSnapshot | None = None) -> PortfolioSnapshot:
        return await asyncio.to_thread(self.snapshot_sync, market=market)

    def snapshot_sync(self, *, market: MarketSnapshot | None = None) -> PortfolioSnapshot:
        rec = self.adapter.reconcile()
        now = _utc_now(self.clock)
        eff_market = market
        if eff_market is None and self.market_provider is not None:
            eff_market = self.market_provider(now)

        open_pos = self.adapter.open_positions()
        pending_ids = self.adapter.active_pending_intent_ids()
        sides = {str(p["side"]) for p in open_pos}
        has_hedged_conflict = len(sides) > 1
        signed_qty = sum(
            (
                Decimal(int(p["volume_units"]))
                if str(p["side"]) == "buy"
                else -Decimal(int(p["volume_units"]))
                for p in open_pos
            ),
            Decimal("0"),
        )

        acct_identity_ok = (
            rec.account_number is not None
            and rec.account_number > 0
            and (
                self.adapter.expected_demo_account_number is None
                or rec.account_number == self.adapter.expected_demo_account_number
            )
            and rec.account_currency == self.adapter.expected_account_currency
        )
        snap_valid = (
            acct_identity_ok
            and rec.broker_balance_usd is not None
            and rec.broker_equity_usd is not None
            and rec.snapshot_age_seconds is not None
            and -5.0 <= rec.snapshot_age_seconds <= self.adapter.max_snapshot_age_seconds
            and rec.allow_demo_orders is not False
            and not has_hedged_conflict
        )
        balance_usd = (
            rec.broker_balance_usd
            if rec.broker_balance_usd is not None and rec.broker_balance_usd > Decimal("0")
            else self.fallback_initial_equity_usd
        )

        unpriced_rows = self.adapter.unpriced_closed_positions()
        all_deals = self.adapter.closed_deals()

        unrealized = Decimal("0.00")
        valuation_valid = True
        valuation_reason = "flat_no_quote_needed"
        if open_pos:
            qc = validate_market_quote(
                eff_market,
                as_of=now,
                policy=self.exit_policy.quote_quality_policy("EUR/USD"),
            )
            if qc.valid and qc.quote is not None:
                total_u = Decimal("0")
                for p in open_pos:
                    p_qty = (
                        Decimal(int(p["volume_units"]))
                        if str(p["side"]) == "buy"
                        else -Decimal(int(p["volume_units"]))
                    )
                    entry = Decimal(str(p["entry_price"]))
                    mark = qc.quote.bid if p_qty > Decimal("0") else qc.quote.ask
                    total_u += p_qty * (mark - entry)
                unrealized = total_u.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                valuation_valid = True
                valuation_reason = "marked_to_valid_quote"
            else:
                valuation_valid = False
                valuation_reason = f"unreliable_quote:{qc.reason}"

        if unpriced_rows:
            valuation_valid = False
            valuation_reason = "position_closed_unpriced_awaiting_broker_history"

        invalid_base_reason = (
            valuation_reason if not valuation_valid else "missing_or_stale_demo_reconcile_snapshot"
        )
        if valuation_valid and snap_valid:
            equity = max(Decimal("0.01"), balance_usd + unrealized)
        elif snap_valid and rec.broker_equity_usd is not None and signed_qty == Decimal("0"):
            equity = max(Decimal("0.01"), rec.broker_equity_usd)
        else:
            equity = max(Decimal("0.01"), balance_usd)

        baseline, daily_pnl, daily_trig, base_valid, base_reason = self._update_daily_state(
            now,
            equity,
            signed_qty=signed_qty,
            has_unpriced_closes=bool(unpriced_rows),
            valid_snapshot_and_quote=snap_valid and valuation_valid,
            invalid_reason=invalid_base_reason,
        )
        if rec.blocks_new_risk and base_valid:
            base_valid = False
            base_reason = (
                f"demo_reconcile_blocks_new_risk:{','.join(rec.drift_reasons) or 'halted'}"
            )

        realized_pnl = sum(
            (Decimal(str(d["net_profit_usd"])) for d in all_deals),
            Decimal("0.00"),
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        fees_usd = sum(
            (
                abs(Decimal(str(d["commissions_usd"])))
                + abs(min(Decimal("0"), Decimal(str(d["swap_usd"]))))
                for d in all_deals
            ),
            Decimal("0.00"),
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        positions = (
            (Position(instrument="EUR/USD", quantity=signed_qty),)
            if signed_qty != Decimal("0")
            else ()
        )
        as_of = eff_market.as_of if eff_market is not None and eff_market.as_of <= now else now
        return PortfolioSnapshot(
            snapshot_id=self._compute_snapshot_id(open_pos, pending_ids),
            as_of=as_of,
            base_currency="USD",
            equity=equity,
            positions=positions,
            pending_order_ids=pending_ids,
            unrealized_pnl_usd=unrealized,
            realized_pnl_usd=realized_pnl,
            total_fees_usd=fees_usd,
            daily_pnl_usd=daily_pnl,
            daily_baseline_equity_usd=baseline,
            daily_loss_limit_triggered=daily_trig,
            daily_baseline_valid=base_valid,
            daily_baseline_reason=base_reason,
            pending_exit_reason=self._stop_exit_reason,
            valuation_valid=valuation_valid,
            valuation_reason=valuation_reason,
        )

    def open_quantity_eur(self) -> Decimal:
        open_pos = self.adapter.open_positions()
        return sum(
            (
                Decimal(int(p["volume_units"]))
                if str(p["side"]) == "buy"
                else -Decimal(int(p["volume_units"]))
                for p in open_pos
            ),
            Decimal("0"),
        )

    def has_matching_pending_target(self, target: TargetPortfolio) -> bool:
        eur = next((p for p in target.positions if p.instrument == "EUR/USD"), None)
        if eur is None:
            return False
        want_close = eur.weight == Decimal("0")
        want_side = "buy" if eur.weight > Decimal("0") else "sell"
        with self.adapter._lock:
            rows = self.adapter.db.execute(
                """SELECT intent_type, side FROM demo_broker_intents
                WHERE status IN ('outbox_pending', 'submitted', 'accepted', 'in_flight')"""
            ).fetchall()
        for r in rows:
            if want_close and str(r["intent_type"]) == "close_position":
                return True
            if (
                not want_close
                and str(r["intent_type"]) == "market_order"
                and str(r["side"] or "") == want_side
            ):
                return True
        return False

    def cancel_risk_increasing_pending_orders(
        self, *, now: datetime, reason: str
    ) -> list[dict[str, Any]]:
        """Abort any un-published `outbox_pending` market_order rows (published market orders cannot be un-sent)."""
        cancelled: list[dict[str, Any]] = []
        with self.adapter._lock, self.adapter.db:
            rows = self.adapter.db.execute(
                """SELECT client_intent_id, decision_id, intent_json
                FROM demo_broker_intents
                WHERE intent_type='market_order' AND status='outbox_pending'"""
            ).fetchall()
            for r in rows:
                cid = str(r["client_intent_id"])
                self.adapter.db.execute(
                    """UPDATE demo_broker_intents
                    SET status='cancelled', error_code=?, updated_at=?
                    WHERE client_intent_id=?""",
                    (reason, now.astimezone(UTC).isoformat(), cid),
                )
                cancelled.append(
                    {
                        "execution_id": cid,
                        "decision_id": r["decision_id"],
                        "target_id": None,
                        "reason": reason,
                        "cancelled_at": now.astimezone(UTC),
                    }
                )
        return cancelled

    def request_protective_exit_on_stop(self, *, now: datetime, reason: str) -> None:
        del now
        with self._lock:
            self._stop_exit_reason = reason
        for pos in self.adapter.open_positions():
            try:
                self.adapter.submit_close_position(
                    broker_position_id=int(pos["broker_position_id"]),
                    protective_reason=reason,
                )
            except DemoBrokerSafetyError:
                pass

    async def record(self, decision: AgentDecision, assessment: RiskAssessment) -> ExecutionReceipt:
        return await asyncio.to_thread(self.record_sync, decision, assessment)

    def record_sync(self, decision: AgentDecision, assessment: RiskAssessment) -> ExecutionReceipt:
        now = _utc_now(self.clock)
        if decision.target is None:
            return ExecutionReceipt(
                execution_id=f"rej-{decision.decision_id}",
                decision_id=decision.decision_id,
                target_id="none",
                assessment_id=assessment.assessment_id,
                mode="ctrader_cli_demo",
                status="rejected",
                recorded_at=now,
                reason="missing_decision_target",
            )
        if assessment.outcome != "approve":
            return ExecutionReceipt(
                execution_id=f"rej-{decision.decision_id}",
                decision_id=decision.decision_id,
                target_id=decision.target.target_id,
                assessment_id=assessment.assessment_id,
                mode="ctrader_cli_demo",
                status="rejected",
                recorded_at=now,
                reason=f"unapproved_assessment:{','.join(assessment.reasons)}",
            )

        eur_pos = next((p for p in decision.target.positions if p.instrument == "EUR/USD"), None)
        weight = eur_pos.weight if eur_pos is not None else Decimal("0")
        open_pos = self.adapter.open_positions()
        curr_signed = sum(
            int(p["volume_units"]) if str(p["side"]) == "buy" else -int(p["volume_units"])
            for p in open_pos
        )

        # Case 1: CLOSE to flat
        if decision.portfolio_action == "CLOSE" or weight == Decimal("0"):
            if not open_pos:
                return ExecutionReceipt(
                    execution_id=f"noop-{decision.decision_id}",
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="recorded",
                    recorded_at=now,
                    reason="already_flat_at_demo_broker",
                )
            try:
                first_cid: str | None = None
                for p in open_pos:
                    pid = int(p["broker_position_id"])
                    cid = self.adapter.submit_close_position(
                        broker_position_id=pid,
                        client_intent_id=f"close-{decision.decision_id}-{pid}",
                        decision_id=decision.decision_id,
                        assessment_id=assessment.assessment_id,
                        protective_reason="agent_close_target",
                    )
                    if first_cid is None:
                        first_cid = cid
                return ExecutionReceipt(
                    execution_id=first_cid or f"close-{decision.decision_id}",
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="recorded",
                    recorded_at=now,
                    reason="demo_close_position_submitted",
                )
            except DemoBrokerSafetyError as exc:
                return ExecutionReceipt(
                    execution_id=f"rej-{decision.decision_id}",
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="rejected",
                    recorded_at=now,
                    reason=f"demo_close_rejected:{exc}",
                )

        if assessment.approved_quantity_eur is None:
            return ExecutionReceipt(
                execution_id=f"rej-{decision.decision_id}",
                decision_id=decision.decision_id,
                target_id=decision.target.target_id,
                assessment_id=assessment.assessment_id,
                mode="ctrader_cli_demo",
                status="rejected",
                recorded_at=now,
                reason="missing_approved_quantity_eur_on_assessment",
            )
        desired_signed = int(assessment.approved_quantity_eur)

        if desired_signed == curr_signed:
            return ExecutionReceipt(
                execution_id=f"noop-{decision.decision_id}",
                decision_id=decision.decision_id,
                target_id=decision.target.target_id,
                assessment_id=assessment.assessment_id,
                mode="ctrader_cli_demo",
                status="recorded",
                recorded_at=now,
                reason="already_at_approved_demo_target",
            )

        # Case 2: Opposing reversal on cTrader hedging account -> close existing position(s) first (never hedge!)
        if curr_signed != 0 and (curr_signed > 0) != (desired_signed > 0):
            try:
                first_rev_cid: str | None = None
                for p in open_pos:
                    pid = int(p["broker_position_id"])
                    cid = self.adapter.submit_close_position(
                        broker_position_id=pid,
                        client_intent_id=f"revclose-{decision.decision_id}-{pid}",
                        decision_id=decision.decision_id,
                        assessment_id=assessment.assessment_id,
                        protective_reason="reversal_flatten_existing_position_first",
                    )
                    if first_rev_cid is None:
                        first_rev_cid = cid
                return ExecutionReceipt(
                    execution_id=first_rev_cid or f"revclose-{decision.decision_id}",
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="recorded",
                    recorded_at=now,
                    reason="demo_reversal_close_existing_submitted",
                )
            except DemoBrokerSafetyError as exc:
                return ExecutionReceipt(
                    execution_id=f"rej-{decision.decision_id}",
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="rejected",
                    recorded_at=now,
                    reason=f"demo_reversal_close_rejected:{exc}",
                )

        # Case 3: Partial risk reduction on same side -> close delta by broker_position_id
        if curr_signed != 0 and abs(desired_signed) < abs(curr_signed):
            rem_to_close = abs(curr_signed) - abs(desired_signed)
            try:
                first_red_cid: str | None = None
                for p in open_pos:
                    if rem_to_close <= 0:
                        break
                    pid = int(p["broker_position_id"])
                    p_units = int(p["volume_units"])
                    chunk = min(p_units, rem_to_close)
                    cid = self.adapter.submit_close_position(
                        broker_position_id=pid,
                        volume_units=chunk if chunk < p_units else None,
                        client_intent_id=f"reduce-{decision.decision_id}-{pid}",
                        decision_id=decision.decision_id,
                        assessment_id=assessment.assessment_id,
                        protective_reason="agent_partial_reduction",
                    )
                    rem_to_close -= chunk
                    if first_red_cid is None:
                        first_red_cid = cid
                return ExecutionReceipt(
                    execution_id=first_red_cid or f"reduce-{decision.decision_id}",
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="recorded",
                    recorded_at=now,
                    reason="demo_partial_close_position_submitted",
                )
            except DemoBrokerSafetyError as exc:
                return ExecutionReceipt(
                    execution_id=f"rej-{decision.decision_id}",
                    decision_id=decision.decision_id,
                    target_id=decision.target.target_id,
                    assessment_id=assessment.assessment_id,
                    mode="ctrader_cli_demo",
                    status="rejected",
                    recorded_at=now,
                    reason=f"demo_partial_close_rejected:{exc}",
                )

        # Case 4: Risk-increasing open or scale-in -> enforce confirmed daily baseline & daily loss cap, then submit_market_order
        cur_snap = self.snapshot_sync()
        if not cur_snap.daily_baseline_valid:
            return ExecutionReceipt(
                execution_id=f"rej-{decision.decision_id}",
                decision_id=decision.decision_id,
                target_id=decision.target.target_id,
                assessment_id=assessment.assessment_id,
                mode="ctrader_cli_demo",
                status="rejected",
                recorded_at=now,
                reason=f"daily_baseline_unconfirmed:{cur_snap.daily_baseline_reason}",
            )
        eff_daily_limit = min(
            self.exit_policy.max_daily_loss_usd, self.risk_policy.max_daily_loss_usd
        )
        if cur_snap.daily_loss_limit_triggered or cur_snap.daily_pnl_usd <= -eff_daily_limit:
            return ExecutionReceipt(
                execution_id=f"rej-{decision.decision_id}",
                decision_id=decision.decision_id,
                target_id=decision.target.target_id,
                assessment_id=assessment.assessment_id,
                mode="ctrader_cli_demo",
                status="rejected",
                recorded_at=now,
                reason="daily_loss_limit_exceeded",
            )
        delta_units = abs(desired_signed) - abs(curr_signed)
        side: Literal["buy", "sell"] = "buy" if desired_signed > 0 else "sell"
        try:
            return self.adapter.submit_market_order(
                decision=decision,
                assessment=assessment,
                side=side,
                volume_units=delta_units,
                client_intent_id=f"mkt-{decision.decision_id}",
                stop_loss_pips=(
                    float(self.exit_policy.stop_loss_pips)
                    if self.exit_policy.stop_loss_pips is not None
                    else self.adapter.default_stop_loss_pips
                ),
                take_profit_pips=(
                    float(self.exit_policy.take_profit_pips)
                    if self.exit_policy.take_profit_pips is not None
                    else self.adapter.default_take_profit_pips
                ),
            )
        except DemoBrokerSafetyError as exc:
            return ExecutionReceipt(
                execution_id=f"rej-{decision.decision_id}",
                decision_id=decision.decision_id,
                target_id=decision.target.target_id,
                assessment_id=assessment.assessment_id,
                mode="ctrader_cli_demo",
                status="rejected",
                recorded_at=now,
                reason=f"demo_broker_rejected:{exc}",
            )

    def _emit_new_confirmed_fills(
        self, market: MarketSnapshot | None, rec: DemoReconcileResult, now: datetime
    ) -> tuple[tuple[ShadowFill, ...], tuple[ShadowFill, ...]]:
        """Emit confirmed `ShadowFill` audit items strictly from verified broker opens & `HistoricalTrade` deals.

        Invariants:
          1. Entry (`market_order`) fills are keyed by `open_pos:{broker_position_id}` and require
             broker-confirmed `filled_units > 0` and `fill_price > 0`.
          2. Close fills (intent closes, staged partial closes, multi-deal closes, and autonomous
             broker SL/TP closes) are emitted STRICTLY from `demo_broker_close_deals` keyed by
             `deal:{closing_deal_id}`.
          3. Unpriced closes (`position_closed_unpriced` / missing `History`) NEVER emit a
             `ShadowFill` and NEVER substitute `entry_price`, live `Bid`/`Ask`, `1000` units, or `0` PnL.
        """
        del rec
        q_eur = (
            next((q for q in market.quotes if q.instrument == "EUR/USD"), None)
            if market is not None
            else None
        )
        snap_after = self.snapshot_sync(market=market)
        exit_fills: list[ShadowFill] = []
        target_fills: list[ShadowFill] = []

        with self.adapter._lock, self.adapter.db:
            # 1. Emit confirmed `market_order` entry fills keyed by `open_pos:{broker_position_id}`
            ev_rows = self.adapter.db.execute(
                """SELECT event_rowid, bridge_session_id, event_seq, raw_json
                FROM demo_broker_events
                WHERE kind='order_event' AND status IN ('partial', 'filled')
                ORDER BY event_rowid ASC"""
            ).fetchall()
            for r in ev_rows:
                ev = json.loads(r["raw_json"])
                itype = str(ev.get("intent_type") or "")
                if itype != "market_order":
                    continue
                pos_id = int(ev.get("broker_position_id") or 0)
                filled_u = Decimal(int(ev.get("filled_units") or 0))
                if pos_id <= 0 or filled_u <= Decimal("0") or ev.get("fill_price") is None:
                    continue
                fill_key = f"open_pos:{pos_id}"
                already = self.adapter.db.execute(
                    "SELECT 1 FROM demo_emitted_fills WHERE fill_key=?", (fill_key,)
                ).fetchone()
                if already is not None:
                    continue

                cid = str(ev.get("client_intent_id") or "")
                intent_row = (
                    self.adapter.db.execute(
                        "SELECT decision_id FROM demo_broker_intents WHERE client_intent_id=?",
                        (cid,),
                    ).fetchone()
                    if cid
                    else None
                )
                dec_id = (
                    str(intent_row["decision_id"])
                    if intent_row is not None and intent_row["decision_id"]
                    else None
                )
                fill_px = Decimal(str(ev["fill_price"]))
                side = str(ev.get("side") or "buy")
                delta_q = filled_u if side == "buy" else -filled_u

                emitted_dt = _parse_iso_utc(ev.get("emitted_at")) or now
                other_open_rows = self.adapter.db.execute(
                    """SELECT side, initial_volume_units, volume_units, opened_at, closed_at
                    FROM demo_broker_positions
                    WHERE broker_position_id < ?""",
                    (pos_id,),
                ).fetchall()
                prior_q = Decimal("0")
                for op in other_open_rows:
                    op_open_dt = _parse_iso_utc(op["opened_at"])
                    op_close_dt = _parse_iso_utc(op["closed_at"])
                    if (
                        op_open_dt is not None
                        and op_open_dt <= emitted_dt
                        and (op_close_dt is None or op_close_dt > emitted_dt)
                    ):
                        u_op = Decimal(
                            int(op["volume_units"])
                            if int(op["volume_units"]) > 0
                            else int(op["initial_volume_units"] or 0)
                        )
                        prior_q += u_op if str(op["side"]) == "buy" else -u_op
                new_q = prior_q + delta_q

                quote_is_causal = q_eur is not None and q_eur.received_at <= emitted_dt
                q_rx = q_eur.received_at if quote_is_causal and q_eur is not None else emitted_dt
                q_src = (
                    q_eur.source_at
                    if quote_is_causal
                    and q_eur is not None
                    and q_eur.source_at is not None
                    and q_eur.source_at <= emitted_dt
                    else None
                )
                comm_raw = ev.get("commission_usd")
                comm_usd = (
                    abs(Decimal(str(comm_raw))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    if comm_raw is not None
                    else Decimal("0.00")
                )

                sf = ShadowFill(
                    fill_id=f"demoopen-{pos_id}",
                    execution_id=cid or None,
                    decision_id=dec_id,
                    target_id=None,
                    instrument="EUR/USD",
                    kind="target_fill",
                    reason=f"ctrader_cli_demo_fill_pos_{pos_id}",
                    filled_at=emitted_dt,
                    quote_source_at=q_src,
                    quote_received_at=q_rx,
                    quote_available_at=q_rx,
                    prior_quantity=prior_q,
                    delta_quantity=delta_q,
                    new_quantity=new_q,
                    raw_bid=q_eur.bid if quote_is_causal and q_eur is not None else None,
                    raw_ask=q_eur.ask if quote_is_causal and q_eur is not None else None,
                    fill_price=fill_px,
                    commission_usd=comm_usd,
                    commission_source="broker_metadata_usd_per_million",
                    slippage_bps_applied=Decimal("0"),
                    slippage_assumption_unverified=False,
                    realized_pnl_usd=Decimal("0.00"),
                    equity_after_usd=snap_after.equity,
                    snapshot_id_after=snap_after.snapshot_id,
                    synthetic_assumption=False,
                )
                self.adapter.db.execute(
                    "INSERT INTO demo_emitted_fills (fill_key, fill_json, emitted_at) VALUES (?, ?, ?)",
                    (fill_key, sf.model_dump_json(), now.isoformat()),
                )
                target_fills.append(sf)

            # Also emit entry fill if a position was adopted directly via `reconcile_snapshot` after a crash
            adopted_pos_rows = self.adapter.db.execute(
                "SELECT * FROM demo_broker_positions WHERE initial_volume_units > 0 ORDER BY broker_position_id ASC"
            ).fetchall()
            for ap in adopted_pos_rows:
                ap_id = int(ap["broker_position_id"])
                ap_key = f"open_pos:{ap_id}"
                if (
                    self.adapter.db.execute(
                        "SELECT 1 FROM demo_emitted_fills WHERE fill_key=?", (ap_key,)
                    ).fetchone()
                    is not None
                ):
                    continue
                ap_cid = str(ap["client_intent_id"] or "")
                ap_intent = (
                    self.adapter.db.execute(
                        "SELECT decision_id FROM demo_broker_intents WHERE client_intent_id=?",
                        (ap_cid,),
                    ).fetchone()
                    if ap_cid
                    else None
                )
                ap_dec_id = (
                    str(ap_intent["decision_id"])
                    if ap_intent is not None and ap_intent["decision_id"]
                    else None
                )
                ap_u = Decimal(int(ap["initial_volume_units"]))
                ap_side = str(ap["side"])
                ap_delta = ap_u if ap_side == "buy" else -ap_u
                ap_dt = _parse_iso_utc(ap["opened_at"]) or now
                ap_q_causal = q_eur is not None and q_eur.received_at <= ap_dt
                ap_q_rx = q_eur.received_at if ap_q_causal and q_eur is not None else ap_dt
                ap_q_src = (
                    q_eur.source_at
                    if ap_q_causal
                    and q_eur is not None
                    and q_eur.source_at is not None
                    and q_eur.source_at <= ap_dt
                    else None
                )
                ap_comm = (
                    abs(Decimal(str(ap["open_commission_usd"]))).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                    if ap["open_commission_usd"] is not None
                    else Decimal("0.00")
                )
                sf_ap = ShadowFill(
                    fill_id=f"demoopen-{ap_id}",
                    execution_id=ap_cid or None,
                    decision_id=ap_dec_id,
                    target_id=None,
                    instrument="EUR/USD",
                    kind="target_fill",
                    reason=f"ctrader_cli_demo_fill_pos_{ap_id}",
                    filled_at=ap_dt,
                    quote_source_at=ap_q_src,
                    quote_received_at=ap_q_rx,
                    quote_available_at=ap_q_rx,
                    prior_quantity=Decimal("0"),
                    delta_quantity=ap_delta,
                    new_quantity=ap_delta,
                    raw_bid=q_eur.bid if ap_q_causal and q_eur is not None else None,
                    raw_ask=q_eur.ask if ap_q_causal and q_eur is not None else None,
                    fill_price=Decimal(str(ap["entry_price"])),
                    commission_usd=ap_comm,
                    commission_source="broker_metadata_usd_per_million",
                    slippage_bps_applied=Decimal("0"),
                    slippage_assumption_unverified=False,
                    realized_pnl_usd=Decimal("0.00"),
                    equity_after_usd=snap_after.equity,
                    snapshot_id_after=snap_after.snapshot_id,
                    synthetic_assumption=False,
                )
                self.adapter.db.execute(
                    "INSERT INTO demo_emitted_fills (fill_key, fill_json, emitted_at) VALUES (?, ?, ?)",
                    (ap_key, sf_ap.model_dump_json(), now.isoformat()),
                )
                target_fills.append(sf_ap)

            # 2. Emit ALL close fills strictly from `demo_broker_close_deals` (`deal:{closing_deal_id}`)
            pos_rows_with_deals = self.adapter.db.execute(
                """SELECT DISTINCT p.*
                FROM demo_broker_positions p
                JOIN demo_broker_close_deals d ON d.broker_position_id = p.broker_position_id
                ORDER BY p.broker_position_id ASC"""
            ).fetchall()
            for p_row in pos_rows_with_deals:
                pos_id = int(p_row["broker_position_id"])
                orig_side = str(p_row["side"])
                deal_rows = self.adapter.db.execute(
                    """SELECT * FROM demo_broker_close_deals
                    WHERE broker_position_id=?
                    ORDER BY closing_time ASC, closing_deal_id ASC""",
                    (pos_id,),
                ).fetchall()
                total_deal_u = sum(int(d["volume_units"]) for d in deal_rows)
                init_u = max(
                    int(p_row["initial_volume_units"] or 0),
                    int(p_row["volume_units"] or 0) + total_deal_u,
                )
                rem_before = init_u
                for d in deal_rows:
                    deal_id = int(d["closing_deal_id"])
                    vol_int = int(d["volume_units"])
                    rem_after = max(0, rem_before - vol_int)
                    prior_abs = Decimal(rem_before)
                    vol_dec = Decimal(vol_int)
                    prior_q = prior_abs if orig_side == "buy" else -prior_abs
                    delta_q = -vol_dec if orig_side == "buy" else vol_dec
                    new_q = prior_q + delta_q
                    rem_before = rem_after

                    deal_key = f"deal:{deal_id}"
                    already_deal = self.adapter.db.execute(
                        "SELECT 1 FROM demo_emitted_fills WHERE fill_key=?", (deal_key,)
                    ).fetchone()
                    if already_deal is not None:
                        continue

                    close_cid = str(d["client_intent_id"] or "")
                    c_intent_row = (
                        self.adapter.db.execute(
                            "SELECT decision_id, intent_json FROM demo_broker_intents WHERE client_intent_id=?",
                            (close_cid,),
                        ).fetchone()
                        if close_cid
                        else None
                    )
                    c_dec_id = (
                        str(c_intent_row["decision_id"])
                        if c_intent_row is not None and c_intent_row["decision_id"]
                        else None
                    )
                    prot_reason: str | None = None
                    if c_intent_row is not None and c_intent_row["intent_json"]:
                        prot_reason = json.loads(c_intent_row["intent_json"]).get(
                            "protective_reason"
                        )

                    if close_cid:
                        is_agent_close = prot_reason in (
                            "agent_close_target",
                            "agent_partial_reduction",
                            "reversal_flatten_existing_position_first",
                        )
                        kind_lit: Literal["target_fill", "protective_exit"] = (
                            "target_fill" if is_agent_close else "protective_exit"
                        )
                        reason_str = (
                            prot_reason or f"ctrader_cli_demo_close_pos_{pos_id}_deal_{deal_id}"
                        )
                        exec_id: str | None = close_cid
                    else:
                        kind_lit = "protective_exit"
                        reason_str = f"broker_native_sl_or_tp_pos_{pos_id}_deal_{deal_id}"
                        exec_id = (
                            str(p_row["client_intent_id"]) if p_row["client_intent_id"] else None
                        )

                    filled_dt = _parse_iso_utc(d["closing_time"]) or now
                    d_q_causal = q_eur is not None and q_eur.received_at <= filled_dt
                    d_q_rx = q_eur.received_at if d_q_causal and q_eur is not None else filled_dt
                    d_q_src = (
                        q_eur.source_at
                        if d_q_causal
                        and q_eur is not None
                        and q_eur.source_at is not None
                        and q_eur.source_at <= filled_dt
                        else None
                    )
                    d_fill_px = Decimal(str(d["closing_price"]))
                    d_comm = (
                        abs(Decimal(str(d["commissions_usd"])))
                        + abs(min(Decimal("0"), Decimal(str(d["swap_usd"]))))
                    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    d_real_pnl = Decimal(str(d["net_profit_usd"])).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )

                    sf_deal = ShadowFill(
                        fill_id=f"demodeal-{pos_id}-{deal_id}",
                        execution_id=exec_id,
                        decision_id=c_dec_id,
                        target_id=None,
                        instrument="EUR/USD",
                        kind=kind_lit,
                        reason=reason_str,
                        filled_at=filled_dt,
                        quote_source_at=d_q_src,
                        quote_received_at=d_q_rx,
                        quote_available_at=d_q_rx,
                        prior_quantity=prior_q,
                        delta_quantity=delta_q,
                        new_quantity=new_q,
                        raw_bid=q_eur.bid if d_q_causal and q_eur is not None else None,
                        raw_ask=q_eur.ask if d_q_causal and q_eur is not None else None,
                        fill_price=d_fill_px,
                        commission_usd=d_comm,
                        commission_source="broker_metadata_usd_per_million",
                        slippage_bps_applied=Decimal("0"),
                        slippage_assumption_unverified=False,
                        realized_pnl_usd=d_real_pnl,
                        equity_after_usd=snap_after.equity,
                        snapshot_id_after=snap_after.snapshot_id,
                        synthetic_assumption=False,
                    )
                    self.adapter.db.execute(
                        "INSERT INTO demo_emitted_fills (fill_key, fill_json, emitted_at) VALUES (?, ?, ?)",
                        (deal_key, sf_deal.model_dump_json(), now.isoformat()),
                    )
                    if kind_lit == "protective_exit":
                        exit_fills.append(sf_deal)
                    else:
                        target_fills.append(sf_deal)

        if not self.adapter.open_positions():
            with self._lock:
                self._stop_exit_reason = None
                self._peak_favorable_pips.clear()

        return tuple(exit_fills), tuple(target_fills)

    def _evaluate_and_dispatch_protective_closes(
        self, market: MarketSnapshot | None, *, stop_reason: str | None = None
    ) -> list[str]:
        now = _utc_now(self.clock)
        open_pos = self.adapter.open_positions()
        if not open_pos:
            return []

        qc = validate_market_quote(
            market,
            as_of=now,
            policy=self.exit_policy.quote_quality_policy("EUR/USD"),
        )
        quote = qc.quote if qc.valid else None
        snap_prot = self.snapshot_sync(market=market) if market is not None else None
        daily_loss_exit = bool(snap_prot is not None and snap_prot.daily_loss_limit_triggered)
        in_close_cutoff = is_in_daily_close_window(
            now,
            tz_name=self.exit_policy.trading_timezone,
            close_hour=self.exit_policy.daily_close_hour,
            close_minute=self.exit_policy.daily_close_minute,
            rollover_hour=self.exit_policy.rollover_hour,
            rollover_minute=self.exit_policy.rollover_minute,
        )

        dispatched: list[str] = []
        for p in open_pos:
            pid = int(p["broker_position_id"])
            side = str(p["side"])
            units = Decimal(int(p["volume_units"]))
            entry = Decimal(str(p["entry_price"]))
            opened_dt = _parse_iso_utc(p.get("opened_at"))

            trig_reason: str | None = None
            if self._stop_exit_reason is not None:
                trig_reason = self._stop_exit_reason
            elif stop_reason is not None and "close" in stop_reason:
                trig_reason = stop_reason
            elif daily_loss_exit:
                trig_reason = "protective_daily_loss_limit"
            elif in_close_cutoff:
                trig_reason = "daily_close_cutoff_protective_exit"
            elif opened_dt is not None and (now - opened_dt).total_seconds() >= float(
                self.exit_policy.max_holding_seconds
            ):
                trig_reason = "max_holding_horizon_exceeded"
            elif quote is not None:
                mark = quote.bid if side == "buy" else quote.ask
                favorable_pips = (
                    (mark - entry) / EURUSD_PIP if side == "buy" else (entry - mark) / EURUSD_PIP
                )
                pnl_usd = units * (mark - entry if side == "buy" else entry - mark)
                prev_peak = self._peak_favorable_pips.get(pid, favorable_pips)
                peak = max(prev_peak, favorable_pips)
                self._peak_favorable_pips[pid] = peak

                if (
                    self.exit_policy.stop_loss_pips is not None
                    and favorable_pips <= -self.exit_policy.stop_loss_pips
                ):
                    trig_reason = "protective_stop_loss_pips"
                elif pnl_usd <= -self.exit_policy.max_position_loss_usd:
                    trig_reason = "protective_max_position_loss_usd"
                elif (
                    self.exit_policy.take_profit_pips is not None
                    and favorable_pips >= self.exit_policy.take_profit_pips
                ):
                    trig_reason = "protective_take_profit_pips"
                elif (
                    self.exit_policy.take_profit_usd is not None
                    and pnl_usd >= self.exit_policy.take_profit_usd
                ):
                    trig_reason = "protective_take_profit_usd"
                elif (
                    self.exit_policy.trailing_activation_pips is not None
                    and self.exit_policy.trailing_distance_pips is not None
                    and peak >= self.exit_policy.trailing_activation_pips
                    and (peak - favorable_pips) >= self.exit_policy.trailing_distance_pips
                ):
                    trig_reason = "protective_trailing_profit_lock"

            if trig_reason is not None:
                try:
                    cid = self.adapter.submit_close_position(
                        broker_position_id=pid,
                        protective_reason=trig_reason,
                    )
                    dispatched.append(cid)
                except DemoBrokerSafetyError:
                    pass
        return dispatched

    def step_broker_events_and_protection(
        self, market: MarketSnapshot | None, *, stop_reason: str | None = None
    ) -> tuple[tuple[ShadowFill, ...], tuple[ShadowFill, ...]]:
        rec = self.adapter.reconcile()
        now = _utc_now(self.clock)
        exit_fills, target_fills = self._emit_new_confirmed_fills(market, rec, now)
        self._evaluate_and_dispatch_protective_closes(market, stop_reason=stop_reason)
        return exit_fills, target_fills

    def report(self, *, market: MarketSnapshot | None = None) -> dict[str, Any]:
        rec = self.adapter.reconcile()
        snap = self.snapshot_sync(market=market)
        open_qty = self.open_quantity_eur()
        unpriced_positions = self.adapter.unpriced_closed_positions()
        closed_deals = self.adapter.closed_deals()
        pending_close_accounting = bool(unpriced_positions)
        with self.adapter._lock:
            fill_rows = self.adapter.db.execute(
                "SELECT fill_json FROM demo_emitted_fills ORDER BY rowid ASC"
            ).fetchall()
            intent_rows = self.adapter.db.execute(
                "SELECT * FROM demo_broker_intents ORDER BY rowid ASC"
            ).fetchall()
            daily_rows = self.adapter.db.execute(
                "SELECT * FROM demo_daily_state ORDER BY trading_day ASC"
            ).fetchall()
        fills = [json.loads(r["fill_json"]) for r in fill_rows]
        alerts: list[str] = []
        if self.adapter.halted_reason:
            alerts.append(f"DEMO_ADAPTER_HALTED: {self.adapter.halted_reason}")
        if self.adapter.manual_unlock_required:
            alerts.append(
                "MANUAL_UNLOCK_REQUIRED: inspect cTrader Demo Positions/History, run "
                "`eventlens-shadow demo-reconcile`, then `eventlens-shadow demo-unlock --note ...`"
            )
        if pending_close_accounting:
            alerts.append(
                "DEMO_POSITION_CLOSED_UNPRICED: broker position closed without complete History "
                "deal evidence; awaiting closing_deal_id backfill (never substituting 0 PnL)"
            )
        for d in rec.drift_reasons:
            alerts.append(f"DEMO_RECONCILE_DRIFT: {d}")

        return {
            "mode": "ctrader_cli_demo",
            "status": (
                "halted"
                if self.adapter.halted_reason
                else ("pending_close_accounting" if pending_close_accounting else "ok")
            ),
            "halted_reason": self.adapter.halted_reason,
            "manual_unlock_required": self.adapter.manual_unlock_required,
            "pending_close_accounting": pending_close_accounting,
            "unclosed_risk": open_qty != Decimal("0"),
            "quantity_eur": str(open_qty),
            "quantity_lots": str((open_qty / Decimal("100000")).quantize(Decimal("0.01"))),
            "pending_exit_reason": self._stop_exit_reason,
            "realized_pnl_usd": None if pending_close_accounting else str(snap.realized_pnl_usd),
            "confirmed_deals_realized_pnl_usd": str(snap.realized_pnl_usd),
            "total_fees_usd": None if pending_close_accounting else str(snap.total_fees_usd),
            "confirmed_deals_total_fees_usd": str(snap.total_fees_usd),
            "alerts": alerts,
            "recovery_instructions": (
                [
                    "Verify live Demo positions/History in cTrader CLI (`demo-reconcile`), resolve any orphan/unknown/unpriced close, then run `demo-unlock` if required."
                ]
                if (alerts or open_qty != Decimal("0"))
                else []
            ),
            "snapshot": snap.model_dump(mode="json"),
            "reconciliation": rec.model_dump(mode="json"),
            "open_broker_positions": self.adapter.open_positions(),
            "unpriced_closed_positions": unpriced_positions,
            "closed_deals": closed_deals,
            "daily_states": [dict(r) for r in daily_rows],
            "intents": [dict(r) for r in intent_rows],
            "fills": fills,
            "faults": [],
            "receipts": [],
        }


class SimulatedCTraderCliDaemon:
    """Deterministic file-bridged simulator matching `EventLensCliBridge.cs` 1:1 for offline tests.

    Matches C# restart & crash semantics:
      - Fresh `bridge_session_id` and `_event_seq = 0` on EVERY instance!
      - `\n`-bounded byte offset on `order-intents.jsonl` (partial lines are never consumed early).
      - Mandatory UTC `expires_at` enforcement before execution.
      - Two-stage `intent-journal.jsonl` (`in_flight` -> `terminal`) with post-crash inspection of
        simulated broker server `Positions` & `History` (`filled` vs `unknown_execution`).
    """

    def __init__(
        self,
        bridge_dir: Path,
        *,
        is_live: bool = False,
        allow_demo_orders: bool = True,
        account_number: int = 1000001,
        expected_demo_account_number: str | int | None = None,
        account_currency: str = "USD",
        expected_account_currency: str = "USD",
        initial_balance_usd: Decimal = Decimal("200.00"),
        bid: Decimal = Decimal("1.12400"),
        ask: Decimal = Decimal("1.12402"),
        partial_fill_ratio: Decimal | None = None,
        reject_next_reason: str | None = None,
        drop_stop_loss_attachment: bool = False,
        delay_close_history: bool = False,
        split_next_close_legs: list[tuple[int, Decimal]] | None = None,
        default_swap_usd: Decimal = Decimal("0.00"),
        crash_mode: DaemonCrashMode = "none",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        parsed_acct = parse_strict_int64_account_number(account_number, allow_zero=False)
        if parsed_acct is None:
            raise ValueError(
                f"SimulatedCTraderCliDaemon account_number must be a positive 64-bit int, got {account_number!r}"
            )
        self.bridge_dir = bridge_dir.resolve()
        self.bridge_dir.mkdir(parents=True, exist_ok=True)
        self.intents_path = self.bridge_dir / "order-intents.jsonl"
        self.journal_path = self.bridge_dir / "intent-journal.jsonl"
        self.events_path = self.bridge_dir / "execution-events.jsonl"
        self._server_state_path = self.bridge_dir / "simulated-broker-server.json"

        self.is_live = is_live
        self.allow_demo_orders = allow_demo_orders
        self.account_number = parsed_acct
        # Mirror C# `[Parameter] public string ExpectedDemoAccountNumber` (`DefaultValue = "0"` in read-only mode)
        if expected_demo_account_number is None:
            self.expected_demo_account_number_raw: Any = (
                str(self.account_number) if allow_demo_orders else "0"
            )
        elif (
            isinstance(expected_demo_account_number, int)
            and not isinstance(expected_demo_account_number, bool)
            and expected_demo_account_number >= 0
        ):
            self.expected_demo_account_number_raw = str(expected_demo_account_number)
        else:
            self.expected_demo_account_number_raw = expected_demo_account_number
        self.account_currency = account_currency
        self.expected_account_currency = expected_account_currency
        self.balance_usd = initial_balance_usd
        self.bid = bid
        self.ask = ask
        self.partial_fill_ratio = partial_fill_ratio
        self.reject_next_reason = reject_next_reason
        self.drop_stop_loss_attachment = drop_stop_loss_attachment
        self.delay_close_history = delay_close_history
        self.split_next_close_legs = (
            list(split_next_close_legs) if split_next_close_legs is not None else None
        )
        self.default_swap_usd = default_swap_usd
        self.crash_mode: DaemonCrashMode = crash_mode
        self.clock = clock

        # Strictly match EventLensCliBridge.cs: fresh session GUID and _event_seq = 0 on every start!
        self.bridge_session_id = str(uuid4())
        self.bridge_started_at = _utc_now(self.clock).isoformat()
        self._event_seq = 0
        self._intents_offset = 0
        self._enabled = True

        self._next_pos_id = 7001
        self._next_deal_id = 9001
        self._journal_stages: dict[str, str] = {}
        self._journal_orig_units: dict[str, int] = {}
        self._journal_prior_deal_ids: dict[str, set[int]] = {}
        self._positions: dict[int, dict[str, Any]] = {}
        self._pending_orders: dict[int, dict[str, Any]] = {}
        self._history: list[dict[str, Any]] = []
        self._delayed_history: list[dict[str, Any]] = []
        self._load_existing_state()

    def _load_existing_state(self) -> None:
        # 1. Load two-stage intent-journal.jsonl (matching C# LoadIntentJournal)
        if self.journal_path.exists():
            raw = self.journal_path.read_bytes()
            last_nl = raw.rfind(b"\n")
            if last_nl >= 0:
                for ln in raw[: last_nl + 1].decode("utf-8").split("\n"):
                    if not ln.strip():
                        continue
                    try:
                        rec = json.loads(ln)
                        cid = rec.get("client_intent_id")
                        stage = rec.get("stage")
                        status = rec.get("status") or "done"
                        if not cid or not stage:
                            continue
                        if stage == "terminal":
                            self._journal_stages[cid] = f"terminal:{status}"
                            self._journal_orig_units.pop(cid, None)
                            self._journal_prior_deal_ids.pop(cid, None)
                        elif not self._journal_stages.get(cid, "").startswith("terminal:"):
                            self._journal_stages[cid] = "in_flight"
                            if rec.get("orig_units") is not None:
                                self._journal_orig_units[cid] = int(rec["orig_units"])
                            if isinstance(rec.get("prior_closing_deal_ids"), list):
                                self._journal_prior_deal_ids[cid] = {
                                    int(did) for did in rec["prior_closing_deal_ids"]
                                }
                    except ValueError:
                        continue

        # 2. Load simulated broker server state (open Positions, PendingOrders, History, DelayedHistory, Balance)
        #    Note: _event_seq is intentionally NOT restored (starts at 0 on every daemon instance)!
        if self._server_state_path.exists():
            srv = json.loads(self._server_state_path.read_text(encoding="utf-8"))
            self.balance_usd = Decimal(str(srv["balance_usd"]))
            self._next_pos_id = int(srv.get("next_pos_id", 7001))
            self._next_deal_id = int(srv.get("next_deal_id", 9001))
            self._positions = {
                int(p["broker_position_id"]): dict(p) for p in srv.get("positions", [])
            }
            self._pending_orders = {
                int(o["broker_order_id"]): dict(o) for o in srv.get("pending_orders", [])
            }
            self._history = list(srv.get("history", []))
            self._delayed_history = list(srv.get("delayed_history", []))
        elif self.events_path.exists():
            latest_snap: dict[str, Any] | None = None
            raw_ev = self.events_path.read_bytes()
            last_nl_ev = raw_ev.rfind(b"\n")
            if last_nl_ev >= 0:
                for ln in raw_ev[: last_nl_ev + 1].decode("utf-8").split("\n"):
                    if ln.strip():
                        ev = json.loads(ln)
                        if ev.get("kind") == "reconcile_snapshot":
                            latest_snap = ev
            if latest_snap is not None:
                if latest_snap.get("balance_usd") is not None:
                    self.balance_usd = Decimal(str(latest_snap["balance_usd"]))
                self._positions = {
                    int(p["broker_position_id"]): dict(p)
                    for p in latest_snap.get("open_positions", [])
                }
                self._pending_orders = {
                    int(o["broker_order_id"]): dict(o)
                    for o in latest_snap.get("pending_orders", [])
                }
                self._history = list(latest_snap.get("closed_deals", []))
                if self._positions:
                    self._next_pos_id = max(self._next_pos_id, max(self._positions.keys()) + 1)
                if self._history:
                    self._next_deal_id = max(
                        self._next_deal_id,
                        max(int(h.get("closing_deal_id", 9000)) for h in self._history) + 1,
                    )

    def _persist_server_state(self) -> None:
        payload = {
            "balance_usd": str(self.balance_usd),
            "next_pos_id": self._next_pos_id,
            "next_deal_id": self._next_deal_id,
            "positions": list(self._positions.values()),
            "pending_orders": list(self._pending_orders.values()),
            "history": self._history,
            "delayed_history": self._delayed_history,
        }
        self._server_state_path.write_text(json.dumps(payload), encoding="utf-8")

    def _record_journal(self, cid: str, stage: str, status: str) -> None:
        if stage == "terminal":
            self._journal_stages[cid] = f"terminal:{status}"
            self._journal_orig_units.pop(cid, None)
            self._journal_prior_deal_ids.pop(cid, None)
        else:
            self._journal_stages[cid] = "in_flight"
        with self.journal_path.open("ab") as f:
            line = (
                json.dumps(
                    {
                        "client_intent_id": cid,
                        "stage": stage,
                        "status": status,
                        "recorded_at": _utc_now(self.clock).isoformat(),
                    }
                )
                + "\n"
            ).encode("utf-8")
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def _record_close_in_flight_journal(
        self, cid: str, orig_units: int, prior_closing_deal_ids: set[int]
    ) -> None:
        self._journal_stages[cid] = "in_flight"
        self._journal_orig_units[cid] = orig_units
        self._journal_prior_deal_ids[cid] = set(prior_closing_deal_ids)
        with self.journal_path.open("ab") as f:
            line = (
                json.dumps(
                    {
                        "client_intent_id": cid,
                        "stage": "in_flight",
                        "status": "in_flight",
                        "orig_units": orig_units,
                        "prior_closing_deal_ids": sorted(prior_closing_deal_ids),
                        "recorded_at": _utc_now(self.clock).isoformat(),
                    }
                )
                + "\n"
            ).encode("utf-8")
            f.write(line)
            f.flush()
            os.fsync(f.fileno())

    def _build_historical_trade(
        self,
        *,
        pos: dict[str, Any],
        pos_id: int,
        volume_units: int,
        closing_price: Decimal,
        closing_time: str,
        commission_per_1000_usd: Decimal = Decimal("-0.04"),
        swap_usd: Decimal | None = None,
    ) -> dict[str, Any]:
        """Mirror `EventLensCliBridge.SerializeHistoricalTrade` field-for-field."""
        deal_id = self._next_deal_id
        self._next_deal_id += 1
        side = str(pos["side"])
        entry_px = Decimal(str(pos["entry_price"]))
        signed = Decimal(volume_units) if side == "buy" else -Decimal(volume_units)
        gross_pnl = (signed * (closing_price - entry_px)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        comm = (commission_per_1000_usd * (Decimal(volume_units) / Decimal("1000"))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        eff_swap = (swap_usd if swap_usd is not None else self.default_swap_usd).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        net_pnl = (gross_pnl + comm + eff_swap).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return {
            "closing_deal_id": deal_id,
            "broker_position_id": pos_id,
            "symbol": str(pos.get("symbol", "EURUSD")),
            "position_side": side,
            "volume_units": volume_units,
            "entry_price": str(entry_px),
            "closing_price": str(closing_price),
            "gross_profit_usd": str(gross_pnl),
            "net_profit_usd": str(net_pnl),
            "commissions_usd": str(comm),
            "swap_usd": str(eff_swap),
            "label": str(pos.get("label", "")),
            "entry_time": str(pos.get("entry_time") or closing_time),
            "closing_time": closing_time,
        }

    def flush_delayed_history(self, *, emit_snapshot: bool = True) -> int:
        """Release delayed broker `History` deals into visible `History` and optionally emit a snapshot."""
        count = len(self._delayed_history)
        if count > 0:
            self._history.extend(self._delayed_history)
            self._delayed_history.clear()
            self._persist_server_state()
        if emit_snapshot:
            self.emit_reconcile_snapshot("delayed_history_flushed")
        return count

    def inject_unrelated_history_deal(
        self,
        *,
        closing_deal_id: int = 55001,
        broker_position_id: int = 44001,
        side: Literal["buy", "sell"] = "sell",
        volume_units: int = 5000,
        entry_price: str | Decimal = "1.11000",
        closing_price: str | Decimal = "1.10800",
        gross_profit_usd: str | Decimal = "10.00",
        net_profit_usd: str | Decimal = "9.80",
        commissions_usd: str | Decimal = "-0.20",
        swap_usd: str | Decimal = "0.00",
        label: str = "manual-legacy-trade",
    ) -> None:
        now_iso = _utc_now(self.clock).isoformat()
        self._history.append(
            {
                "closing_deal_id": closing_deal_id,
                "broker_position_id": broker_position_id,
                "symbol": "EURUSD",
                "position_side": side,
                "volume_units": volume_units,
                "entry_price": str(entry_price),
                "closing_price": str(closing_price),
                "gross_profit_usd": str(gross_profit_usd),
                "net_profit_usd": str(net_profit_usd),
                "commissions_usd": str(commissions_usd),
                "swap_usd": str(swap_usd),
                "label": label,
                "entry_time": now_iso,
                "closing_time": now_iso,
            }
        )
        self._persist_server_state()

    def _emit_close_position_outcome(
        self,
        *,
        cid: str,
        pos_id: int,
        requested_units: int,
        orig_units: int,
        remaining_units: int,
        new_deals: list[dict[str, Any]],
        recovered_after_crash: bool,
    ) -> None:
        """Mirror `EventLensCliBridge.EmitClosePositionOutcome` 1:1."""
        deal_units = sum(int(d["volume_units"]) for d in new_deals)
        actual_closed_units = max(0, orig_units - remaining_units) if orig_units > 0 else deal_units
        unpriced_units = max(0, actual_closed_units - deal_units)

        if new_deals and deal_units > 0 and unpriced_units == 0:
            vwap_close = sum(
                Decimal(int(d["volume_units"])) * Decimal(str(d["closing_price"]))
                for d in new_deals
            ) / Decimal(deal_units)
            gross_pnl = sum(
                (Decimal(str(d["gross_profit_usd"])) for d in new_deals), Decimal("0.00")
            )
            net_pnl = sum((Decimal(str(d["net_profit_usd"])) for d in new_deals), Decimal("0.00"))
            comm = sum((Decimal(str(d["commissions_usd"])) for d in new_deals), Decimal("0.00"))
            swap = sum((Decimal(str(d["swap_usd"])) for d in new_deals), Decimal("0.00"))
            c_status = "filled" if remaining_units <= 0 else "partial"

            self._record_journal(cid, "terminal", c_status)
            close_ev = self._base_frame("order_event")
            close_ev.update(
                {
                    "client_intent_id": cid,
                    "intent_type": "close_position",
                    "broker_position_id": pos_id,
                    "requested_units": requested_units,
                    "closed_units": actual_closed_units,
                    "priced_units": deal_units,
                    "unpriced_units": 0,
                    "filled_units": deal_units,
                    "remaining_units": remaining_units,
                    "status": c_status,
                    "fill_price": _format_price_clean(vwap_close),
                    "realized_gross_pnl_usd": _format_usd_clean(gross_pnl),
                    "realized_net_pnl_usd": _format_usd_clean(net_pnl),
                    "commission_usd": _format_usd_clean(comm),
                    "swap_usd": _format_usd_clean(swap),
                    "deals": list(new_deals),
                    "recovered_after_crash": recovered_after_crash,
                }
            )
            self._write_event(close_ev)
        else:
            # Never substitute current Bid/Ask, entry_price, or 0 PnL when History deals are missing!
            self._record_journal(cid, "terminal", "position_closed_unpriced")
            unpriced_ev = self._base_frame("order_event")
            unpriced_ev.update(
                {
                    "client_intent_id": cid,
                    "intent_type": "close_position",
                    "broker_position_id": pos_id,
                    "requested_units": requested_units,
                    "closed_units": actual_closed_units,
                    "priced_units": deal_units,
                    "unpriced_units": unpriced_units,
                    "filled_units": deal_units,
                    "remaining_units": remaining_units,
                    "status": "position_closed_unpriced",
                    "fill_price": None,
                    "realized_gross_pnl_usd": None,
                    "realized_net_pnl_usd": None,
                    "commission_usd": None,
                    "swap_usd": None,
                    "deals": list(new_deals),
                    "error_code": "broker_history_deal_pending_reconcile",
                    "recovered_after_crash": recovered_after_crash,
                }
            )
            self._write_event(unpriced_ev)

        trig = f"recovered_in_flight_close:{cid}" if recovered_after_crash else f"after_close:{cid}"
        self.emit_reconcile_snapshot(trig)

    def _try_parse_expected_account_number(self) -> tuple[bool, int, str]:
        if not isinstance(self.expected_demo_account_number_raw, str):
            return False, 0, "invalid_expected_demo_account_number"
        parsed = parse_strict_int64_account_number(
            self.expected_demo_account_number_raw, allow_zero=True
        )
        if parsed is None:
            return False, 0, "invalid_expected_demo_account_number"
        if self.allow_demo_orders and parsed <= 0:
            return False, parsed, "demo_orders_require_explicit_expected_account_number"
        return True, parsed, ""

    def _verify_safety(self, stage: str) -> bool:
        expected_ok, expected_acct, expected_err = self._try_parse_expected_account_number()
        acct_mismatch = expected_ok and expected_acct > 0 and self.account_number != expected_acct
        ccy_mismatch = (
            self.account_currency.strip().upper() != self.expected_account_currency.strip().upper()
        )
        if self.is_live or not expected_ok or acct_mismatch or ccy_mismatch:
            reason = (
                "live_account_forbidden"
                if self.is_live
                else (
                    expected_err
                    if not expected_ok
                    else (
                        "unexpected_demo_account_number"
                        if acct_mismatch
                        else f"unexpected_account_currency:{self.account_currency}"
                    )
                )
            )
            self._write_event(
                {
                    "version": 1,
                    "kind": "safety_refusal",
                    "bridge_session_id": self.bridge_session_id,
                    "bridge_started_at": self.bridge_started_at,
                    "event_seq": self._bump_seq(),
                    "emitted_at": _utc_now(self.clock).isoformat(),
                    "account_mode": "live" if self.is_live else "demo",
                    "is_live": self.is_live,
                    "account_number": self.account_number,
                    "account_currency": self.account_currency,
                    "stage": stage,
                    "reason": reason,
                }
            )
            self._enabled = False
            return False
        return True

    def start(self) -> None:
        if not self._verify_safety("startup"):
            return
        self.emit_reconcile_snapshot("startup")

    def emit_quote_session_and_tick(
        self,
        *,
        session_id: str | None = None,
        sequence: int = 1,
        duration_seconds: int = 43200,
    ) -> str:
        """Helper for offline Demo smoke & runtime integration tests to write valid quote JSONL frames."""
        sid = session_id or str(uuid4())
        now_iso = _utc_now(self.clock).isoformat()
        qpath = self.bridge_dir / f"quotes-{sid}.jsonl"
        if not qpath.exists():
            start_frame = {
                "version": 1,
                "kind": "start",
                "session_id": sid,
                "account_mode": "demo",
                "running_mode": "realtime",
                "symbol": "EURUSD",
                "started_at": now_iso,
                "sample_interval_ms": 200,
                "duration_seconds": duration_seconds,
                "digits": 5,
                "pip_size": "0.0001",
                "commission": "3.5",
                "commission_type": "UsdPerMillionUsdVolume",
            }
            with qpath.open("a", encoding="utf-8") as f:
                f.write(json.dumps(start_frame) + "\n")
        quote_frame = {
            "version": 1,
            "kind": "quote",
            "session_id": sid,
            "sequence": sequence,
            "symbol": "EURUSD",
            "source_at": now_iso,
            "bridge_at": now_iso,
            "bid": str(self.bid),
            "ask": str(self.ask),
        }
        with qpath.open("a", encoding="utf-8") as f:
            f.write(json.dumps(quote_frame) + "\n")
            f.flush()
        return sid

    def inject_orphan_position(
        self,
        *,
        broker_position_id: int = 9999,
        side: Literal["buy", "sell"] = "buy",
        volume_units: int = 1000,
        sl_confirmed: bool = True,
    ) -> None:
        px = self.ask if side == "buy" else self.bid
        self._positions[broker_position_id] = {
            "broker_position_id": broker_position_id,
            "symbol": "EURUSD",
            "side": side,
            "volume_units": volume_units,
            "entry_price": str(px),
            "stop_loss": str(px - Decimal("0.0010")) if sl_confirmed else None,
            "take_profit": None,
            "sl_confirmed": sl_confirmed,
            "tp_confirmed": False,
            "gross_profit_usd": "0",
            "net_profit_usd": "0",
            "commissions_usd": "-0.02",
            "swap_usd": "0",
            "label": "manual-orphan",
            "entry_time": _utc_now(self.clock).isoformat(),
        }
        self._persist_server_state()

    def inject_pending_order(
        self,
        *,
        broker_order_id: int = 8001,
        side: Literal["buy", "sell"] = "buy",
        volume_units: int = 1000,
        target_price: str = "1.12000",
    ) -> None:
        self._pending_orders[broker_order_id] = {
            "broker_order_id": broker_order_id,
            "symbol": "EURUSD",
            "order_type": "Limit",
            "side": side,
            "volume_units": volume_units,
            "target_price": target_price,
            "label": "sim-pending",
        }
        self._persist_server_state()

    def _trigger_autonomous_broker_close(
        self,
        broker_position_id: int,
        *,
        trigger_prefix: str,
        closing_price: Decimal | None,
        use_take_profit: bool,
    ) -> dict[str, Any] | None:
        pos = self._positions.pop(broker_position_id, None)
        if pos is None:
            return None
        fallback_px = self.bid if str(pos["side"]) == "buy" else self.ask
        target_field = "take_profit" if use_take_profit else "stop_loss"
        eff_px = (
            closing_price
            if closing_price is not None
            else (
                Decimal(str(pos[target_field]))
                if pos.get(target_field) is not None
                else fallback_px
            )
        )
        deal = self._build_historical_trade(
            pos=pos,
            pos_id=broker_position_id,
            volume_units=int(pos["volume_units"]),
            closing_price=eff_px,
            closing_time=_utc_now(self.clock).isoformat(),
        )
        self.balance_usd += Decimal(str(deal["net_profit_usd"]))
        if self.delay_close_history:
            self._delayed_history.append(deal)
        else:
            self._history.append(deal)
        self._persist_server_state()
        self.emit_reconcile_snapshot(f"{trigger_prefix}:{broker_position_id}")
        return deal

    def trigger_broker_stop_loss_close(
        self,
        broker_position_id: int,
        *,
        closing_price: Decimal | None = None,
    ) -> dict[str, Any] | None:
        """Simulate native cTrader server-side stop-loss closing a position autonomously."""
        return self._trigger_autonomous_broker_close(
            broker_position_id,
            trigger_prefix="broker_sl_triggered",
            closing_price=closing_price,
            use_take_profit=False,
        )

    def trigger_broker_take_profit_close(
        self,
        broker_position_id: int,
        *,
        closing_price: Decimal | None = None,
    ) -> dict[str, Any] | None:
        """Simulate native cTrader server-side take-profit closing a position autonomously."""
        return self._trigger_autonomous_broker_close(
            broker_position_id,
            trigger_prefix="broker_tp_triggered",
            closing_price=closing_price,
            use_take_profit=True,
        )

    def step(self) -> int:
        if not self._enabled or not self.intents_path.exists():
            return 0
        size = self.intents_path.stat().st_size
        if size < self._intents_offset:
            self._write_order_event(None, None, "rejected", "order_intents_file_truncated")
            self._intents_offset = size
            return 0
        if size == self._intents_offset:
            return 0
        with self.intents_path.open("rb") as f:
            f.seek(self._intents_offset)
            raw_bytes = f.read(size - self._intents_offset)
        if not raw_bytes:
            return 0

        # Consume strictly up to the last '\n'-terminated byte (never drop partial trailing lines)
        last_nl = raw_bytes.rfind(b"\n")
        if last_nl < 0:
            return 0

        chunk = raw_bytes[: last_nl + 1]
        lines = chunk.split(b"\n")
        lines.pop()  # trailing empty byte slice after last '\n'
        processed_now = 0
        offset_cursor = self._intents_offset
        for raw_ln in lines:
            offset_cursor += len(raw_ln) + 1
            self._intents_offset = offset_cursor
            stripped = raw_ln.decode("utf-8").strip()
            if not stripped:
                continue
            self._process_intent_raw(stripped)
            processed_now += 1
        return processed_now

    def _bump_seq(self) -> int:
        self._event_seq += 1
        return self._event_seq

    def _base_frame(self, kind: str) -> dict[str, Any]:
        return {
            "version": 1,
            "kind": kind,
            "bridge_session_id": self.bridge_session_id,
            "bridge_started_at": self.bridge_started_at,
            "event_seq": self._bump_seq(),
            "emitted_at": _utc_now(self.clock).isoformat(),
            "account_mode": "demo",
            "is_live": False,
            "account_number": self.account_number,
            "account_currency": self.account_currency,
        }

    def _write_event(self, payload: dict[str, Any]) -> None:
        with self.events_path.open("ab") as f:
            f.write((json.dumps(payload) + "\n").encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())

    def _write_order_event(
        self,
        cid: str | None,
        itype: str | None,
        status: str,
        error_code: str | None = None,
    ) -> None:
        frame = self._base_frame("order_event")
        frame.update(
            {
                "client_intent_id": cid,
                "intent_type": itype,
                "status": status,
                "error_code": error_code,
            }
        )
        self._write_event(frame)

    def _recover_in_flight_intent(
        self, intent: dict[str, Any], cid: str, itype: str, label: str
    ) -> None:
        if itype == "market_order":
            open_match = next(
                (p for p in self._positions.values() if p.get("label") == label),
                None,
            )
            if open_match is not None:
                units = int(open_match["volume_units"])
                self._record_journal(cid, "terminal", "filled")
                frame = self._base_frame("order_event")
                frame.update(
                    {
                        "client_intent_id": cid,
                        "intent_type": "market_order",
                        "side": open_match["side"],
                        "status": "filled",
                        "broker_position_id": int(open_match["broker_position_id"]),
                        "requested_units": units,
                        "filled_units": units,
                        "fill_price": str(open_match["entry_price"]),
                        "stop_loss": open_match.get("stop_loss"),
                        "take_profit": open_match.get("take_profit"),
                        "sl_confirmed": bool(open_match.get("sl_confirmed", False)),
                        "tp_confirmed": bool(open_match.get("tp_confirmed", False)),
                        "commission_usd": str(open_match.get("commissions_usd", "-0.02")),
                        "recovered_after_crash": True,
                    }
                )
                self._write_event(frame)
                self.emit_reconcile_snapshot(f"recovered_in_flight_fill:{cid}")
                return
        elif itype == "close_position":
            pos_id = int(intent.get("broker_position_id") or 0)
            req_close_u = int(intent.get("volume_units") or 0)
            if pos_id > 0:
                still_open = self._positions.get(pos_id)
                rem_u = int(still_open["volume_units"]) if still_open is not None else 0
                orig_u = self._journal_orig_units.get(cid, 0)
                prior_deals = self._journal_prior_deal_ids.get(cid, set())
                new_deals = [
                    h
                    for h in self._history
                    if int(h.get("broker_position_id") or h.get("position_id") or 0) == pos_id
                    and int(h["closing_deal_id"]) not in prior_deals
                ]
                if (orig_u > 0 and rem_u < orig_u) or (
                    orig_u == 0 and still_open is None and len(new_deals) > 0
                ):
                    eff_req = (
                        req_close_u
                        if req_close_u > 0
                        else (
                            orig_u - rem_u
                            if orig_u > 0
                            else sum(int(d["volume_units"]) for d in new_deals)
                        )
                    )
                    self._emit_close_position_outcome(
                        cid=cid,
                        pos_id=pos_id,
                        requested_units=eff_req,
                        orig_units=orig_u,
                        remaining_units=rem_u,
                        new_deals=new_deals,
                        recovered_after_crash=True,
                    )
                    return

        self._record_journal(cid, "terminal", "unknown_execution")
        self._write_order_event(
            cid,
            itype,
            "unknown_execution",
            "bridge_crash_in_flight_outcome_ambiguous",
        )
        self.emit_reconcile_snapshot(f"unknown_in_flight:{cid}")

    def _process_intent_raw(self, raw_json: str) -> None:
        if not self._verify_safety("order_dispatch"):
            return
        try:
            intent = json.loads(raw_json)
        except ValueError as exc:
            self._write_order_event(
                None, None, "rejected", f"invalid_intent_json:{type(exc).__name__}"
            )
            return

        cid = str(intent.get("client_intent_id") or "").strip()
        if not cid:
            return
        itype = str(intent.get("intent_type") or "")
        label = f"el:{cid}"

        prior_stage = self._journal_stages.get(cid)
        if prior_stage is not None:
            if prior_stage.startswith("terminal:"):
                self._write_order_event(
                    cid, itype, "duplicate_ignored", "duplicate_client_intent_id"
                )
                return
            if prior_stage == "in_flight":
                self._recover_in_flight_intent(intent, cid, itype, label)
                return

        if itype == "market_order" and any(
            p.get("label") == label for p in self._positions.values()
        ):
            self._record_journal(cid, "terminal", "duplicate_ignored")
            self._write_order_event(
                cid, itype, "duplicate_ignored", "duplicate_broker_label_exists"
            )
            return

        if intent.get("symbol") != "EURUSD":
            self._record_journal(cid, "terminal", "rejected")
            self._write_order_event(cid, itype, "rejected", "symbol_mismatch")
            return

        now = _utc_now(self.clock)
        raw_exp = intent.get("expires_at")
        exp_dt = _parse_iso_utc(raw_exp)
        if raw_exp is None:
            self._record_journal(cid, "terminal", "rejected")
            self._write_order_event(cid, itype, "rejected", "missing_intent_expires_at")
            self.emit_reconcile_snapshot(f"after_intent_expired:{cid}")
            return
        if exp_dt is None:
            self._record_journal(cid, "terminal", "rejected")
            self._write_order_event(cid, itype, "rejected", "invalid_intent_expires_at")
            self.emit_reconcile_snapshot(f"after_intent_expired:{cid}")
            return
        if now >= exp_dt:
            self._record_journal(cid, "terminal", "expired")
            self._write_order_event(cid, itype, "expired", "intent_expired_before_execution")
            self.emit_reconcile_snapshot(f"after_intent_expired:{cid}")
            return

        if itype == "reconcile":
            self._record_journal(cid, "terminal", "filled")
            self.emit_reconcile_snapshot(f"explicit_intent:{cid}")
            return

        if not self.allow_demo_orders:
            self._record_journal(cid, "terminal", "rejected")
            self._write_order_event(cid, itype, "rejected", "bridge_demo_orders_disabled")
            self.emit_reconcile_snapshot(f"demo_orders_disabled:{cid}")
            return

        if self.reject_next_reason is not None:
            reason = self.reject_next_reason
            self.reject_next_reason = None
            self._record_journal(cid, "in_flight", "in_flight")
            self._record_journal(cid, "terminal", "rejected")
            self._write_order_event(cid, itype, "rejected", reason)
            self.emit_reconcile_snapshot(f"after_reject:{cid}")
            return

        if itype == "market_order":
            side = str(intent.get("side") or "")
            req_units = int(intent.get("volume_units") or 0)
            if side not in ("buy", "sell") or req_units <= 0 or req_units % 1000 != 0:
                self._record_journal(cid, "terminal", "rejected")
                self._write_order_event(cid, "market_order", "rejected", "invalid_side_or_volume")
                return

            self._record_journal(cid, "in_flight", "in_flight")
            acc = self._base_frame("order_event")
            acc.update(
                {
                    "client_intent_id": cid,
                    "intent_type": "market_order",
                    "side": side,
                    "requested_units": req_units,
                    "filled_units": 0,
                    "status": "accepted",
                    "broker_accept_only": True,
                }
            )
            self._write_event(acc)

            if self.crash_mode == "before_broker_call":
                raise SimulatedBridgeCrashError(
                    "Simulated bridge crash before broker ExecuteMarketOrder"
                )

            filled_units = (
                max(1000, int((Decimal(req_units) * self.partial_fill_ratio) // 1000 * 1000))
                if self.partial_fill_ratio is not None
                else req_units
            )
            px = self.ask if side == "buy" else self.bid
            pos_id = self._next_pos_id
            self._next_pos_id += 1
            sl_pips = intent.get("stop_loss_pips")
            tp_pips = intent.get("take_profit_pips")
            sl_attached = (sl_pips is not None) and not self.drop_stop_loss_attachment
            sl_px = (
                str(
                    px - Decimal(str(sl_pips)) * Decimal("0.0001")
                    if side == "buy"
                    else px + Decimal(str(sl_pips)) * Decimal("0.0001")
                )
                if sl_attached
                else None
            )
            tp_px = (
                str(
                    px + Decimal(str(tp_pips)) * Decimal("0.0001")
                    if side == "buy"
                    else px - Decimal(str(tp_pips)) * Decimal("0.0001")
                )
                if tp_pips is not None
                else None
            )
            open_comm = (Decimal("-0.02") * (Decimal(filled_units) / Decimal("1000"))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            now_iso = _utc_now(self.clock).isoformat()
            self._positions[pos_id] = {
                "broker_position_id": pos_id,
                "symbol": "EURUSD",
                "side": side,
                "volume_units": filled_units,
                "entry_price": str(px),
                "stop_loss": sl_px,
                "take_profit": tp_px,
                "sl_confirmed": sl_attached,
                "tp_confirmed": tp_px is not None,
                "gross_profit_usd": "0",
                "net_profit_usd": str(open_comm),
                "commissions_usd": str(open_comm),
                "swap_usd": "0",
                "label": label,
                "entry_time": now_iso,
            }
            self._persist_server_state()

            if self.crash_mode == "after_broker_call_before_event":
                raise SimulatedBridgeCrashError(
                    "Simulated bridge crash after broker ExecuteMarketOrder before terminal event"
                )

            term_status = "partial" if filled_units < req_units else "filled"
            self._record_journal(cid, "terminal", term_status)
            fill_ev = self._base_frame("order_event")
            fill_ev.update(
                {
                    "client_intent_id": cid,
                    "intent_type": "market_order",
                    "side": side,
                    "status": term_status,
                    "broker_position_id": pos_id,
                    "requested_units": req_units,
                    "filled_units": filled_units,
                    "fill_price": str(px),
                    "stop_loss": sl_px,
                    "take_profit": tp_px,
                    "sl_confirmed": sl_attached,
                    "tp_confirmed": tp_px is not None,
                    "commission_usd": str(open_comm),
                }
            )
            self._write_event(fill_ev)
            self.emit_reconcile_snapshot(f"after_fill:{cid}")
        elif itype == "close_position":
            pos_id = int(intent.get("broker_position_id") or 0)
            pos = self._positions.get(pos_id)
            if pos is None:
                self._record_journal(cid, "terminal", "rejected")
                miss_ev = self._base_frame("order_event")
                miss_ev.update(
                    {
                        "client_intent_id": cid,
                        "intent_type": "close_position",
                        "broker_position_id": pos_id,
                        "status": "rejected",
                        "error_code": "position_not_found_or_already_closed",
                    }
                )
                self._write_event(miss_ev)
                self.emit_reconcile_snapshot(f"close_missing:{cid}")
                return

            orig_units = int(pos["volume_units"])
            req_close = int(intent.get("volume_units") or orig_units)
            close_units = min(orig_units, req_close) if req_close > 0 else orig_units
            rem_units = orig_units - close_units
            prior_deal_ids = {
                int(h["closing_deal_id"])
                for h in self._history
                if int(h.get("broker_position_id") or h.get("position_id") or 0) == pos_id
            }

            self._record_close_in_flight_journal(cid, orig_units, prior_deal_ids)
            acc_close = self._base_frame("order_event")
            acc_close.update(
                {
                    "client_intent_id": cid,
                    "intent_type": "close_position",
                    "broker_position_id": pos_id,
                    "requested_units": close_units,
                    "filled_units": 0,
                    "status": "accepted",
                    "broker_accept_only": True,
                }
            )
            self._write_event(acc_close)

            if self.crash_mode == "before_broker_call":
                raise SimulatedBridgeCrashError(
                    "Simulated bridge crash before broker ClosePosition"
                )

            exit_px = self.bid if pos["side"] == "buy" else self.ask
            close_ts_base = _utc_now(self.clock)
            generated_deals: list[dict[str, Any]] = []
            if self.split_next_close_legs is not None:
                legs = list(self.split_next_close_legs)
                self.split_next_close_legs = None
                leg_sum = sum(u for u, _ in legs)
                if leg_sum != close_units:
                    raise ValueError(
                        f"split_next_close_legs sum={leg_sum} does not match close_units={close_units}"
                    )
                for idx, (leg_u, leg_px) in enumerate(legs):
                    generated_deals.append(
                        self._build_historical_trade(
                            pos=pos,
                            pos_id=pos_id,
                            volume_units=leg_u,
                            closing_price=leg_px,
                            closing_time=(close_ts_base + timedelta(milliseconds=idx)).isoformat(),
                        )
                    )
            else:
                generated_deals.append(
                    self._build_historical_trade(
                        pos=pos,
                        pos_id=pos_id,
                        volume_units=close_units,
                        closing_price=exit_px,
                        closing_time=close_ts_base.isoformat(),
                    )
                )

            for gd in generated_deals:
                self.balance_usd += Decimal(str(gd["net_profit_usd"]))

            if rem_units <= 0:
                self._positions.pop(pos_id, None)
            else:
                pos["volume_units"] = rem_units

            if self.delay_close_history:
                self._delayed_history.extend(generated_deals)
            else:
                self._history.extend(generated_deals)
            self._persist_server_state()

            if self.crash_mode == "after_broker_call_before_event":
                raise SimulatedBridgeCrashError(
                    "Simulated bridge crash after broker ClosePosition before terminal event"
                )

            visible_new_deals = [
                h
                for h in self._history
                if int(h.get("broker_position_id") or h.get("position_id") or 0) == pos_id
                and int(h["closing_deal_id"]) not in prior_deal_ids
            ]
            self._emit_close_position_outcome(
                cid=cid,
                pos_id=pos_id,
                requested_units=close_units,
                orig_units=orig_units,
                remaining_units=rem_units,
                new_deals=visible_new_deals,
                recovered_after_crash=False,
            )
        elif itype == "cancel_pending_order":
            ord_id = int(intent.get("broker_order_id") or 0)
            self._record_journal(cid, "in_flight", "in_flight")
            existed = self._pending_orders.pop(ord_id, None) is not None
            self._persist_server_state()
            can_status = "cancelled" if existed else "rejected"
            self._record_journal(cid, "terminal", can_status)
            can_ev = self._base_frame("order_event")
            can_ev.update(
                {
                    "client_intent_id": cid,
                    "intent_type": "cancel_pending_order",
                    "broker_order_id": ord_id,
                    "status": can_status,
                    "error_code": None if existed else "pending_order_not_found",
                }
            )
            self._write_event(can_ev)
            self.emit_reconcile_snapshot(f"after_cancel:{cid}")
        else:
            self._record_journal(cid, "terminal", "rejected")
            self._write_order_event(cid, itype, "rejected", "unsupported_intent_type")

    def emit_reconcile_snapshot(self, trigger: str) -> None:
        total_notional = sum(
            Decimal(int(p["volume_units"])) * Decimal(str(p["entry_price"]))
            for p in self._positions.values()
        )
        margin = (total_notional / Decimal("200")).quantize(Decimal("0.01"))
        frame = self._base_frame("reconcile_snapshot")
        frame.update(
            {
                "trigger": trigger,
                "allow_demo_orders": self.allow_demo_orders,
                "symbol": "EURUSD",
                "balance_usd": str(self.balance_usd),
                "equity_usd": str(self.balance_usd),
                "margin_used_usd": str(margin),
                "free_margin_usd": str(self.balance_usd - margin),
                "open_positions": list(self._positions.values()),
                "pending_orders": list(self._pending_orders.values()),
                "closed_deals": list(self._history[-200:]),
            }
        )
        self._write_event(frame)
