"""Deterministic file-bridged test simulator matching `EventLensCliBridge.cs` 1:1."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from eventlens.trading.ctrader_cli_broker import (
    SimulatedBridgeCrashError,
    _format_price_clean,
    _format_usd_clean,
    _parse_iso_utc,
    _utc_now,
    parse_strict_int64_account_number,
)

DaemonCrashMode = Literal[
    "none",
    "before_broker_call",
    "after_broker_call_before_event",
]


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
