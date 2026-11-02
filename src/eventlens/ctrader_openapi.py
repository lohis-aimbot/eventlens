"""Headless Python cTrader Open API Demo market-data client and QuoteStore streamer.

Hard safety invariants:
1. Endpoint lock: Only `demo.ctraderapi.com` (or explicit loopback `127.0.0.1`/`localhost` for unit tests)
   is permitted. Any `live` hostname raises `CTraderLiveEndpointForbiddenError`.
2. Account lock: Before sending `ProtoOAAccountAuthReq (2102)`, queries `ProtoOAGetAccountListByAccessTokenReq (2149)`
   and hard-verifies `isLive is False`. Any Live account raises `CTraderLiveAccountForbiddenError`.
3. Secret hygiene: Credentials are read from environment variables / `0600` token state files and masked in
   `repr()` / `str()`.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import os
import ssl
import struct
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from pydantic import SecretStr, model_validator

from .contracts import Contract
from .observer import Basis
from .quotes import BridgeQuote, BridgeStart, BridgeStop, QuoteStore, utc

logger = logging.getLogger(__name__)

_NY_TZ = ZoneInfo("America/New_York")
_SPOT_PRICE_SCALE = Decimal("100000")
_PRECISE_COMMISSION_SCALE = Decimal("100000")
_VOLUME_CENTS_PER_EUR_UNIT = Decimal("100")
_MAX_FRAME_BYTES = 4 * 1024 * 1024
DEMO_EXECUTION_UNAVAILABLE = (
    "demo_execution_unavailable: broker order mirroring is disabled pending "
    "position-aware closes, confirmed fills, durable recovery and reconciliation; "
    "use Shadow accounting with Demo quotes instead"
)


class CTraderOpenApiError(RuntimeError):
    """Base error for cTrader Open API transport, auth, or protocol faults."""


class CTraderDemoExecutionUnavailableError(CTraderOpenApiError):
    """Broker execution is unavailable until its position lifecycle is validated."""


class CTraderLiveEndpointForbiddenError(PermissionError):
    """Raised if any configuration attempts to target a Live cTrader Open API endpoint."""


class CTraderLiveAccountForbiddenError(PermissionError):
    """Raised if an account ID resolves to `isLive=True` instead of a Demo account."""


class ProtoOAPayloadType(enum.IntEnum):
    ERROR_RES = 50
    HEARTBEAT_EVENT = 51
    APPLICATION_AUTH_REQ = 2100
    APPLICATION_AUTH_RES = 2101
    ACCOUNT_AUTH_REQ = 2102
    ACCOUNT_AUTH_RES = 2103
    NEW_ORDER_REQ = 2106
    CLOSE_POSITION_REQ = 2111
    SYMBOLS_LIST_REQ = 2114
    SYMBOLS_LIST_RES = 2115
    SYMBOL_BY_ID_REQ = 2116
    SYMBOL_BY_ID_RES = 2117
    TRADER_REQ = 2121
    TRADER_RES = 2122
    RECONCILE_REQ = 2124
    RECONCILE_RES = 2125
    EXECUTION_EVENT = 2126
    SUBSCRIBE_SPOTS_REQ = 2127
    SUBSCRIBE_SPOTS_RES = 2128
    UNSUBSCRIBE_SPOTS_REQ = 2129
    UNSUBSCRIBE_SPOTS_RES = 2130
    SPOT_EVENT = 2131
    OA_ERROR_RES = 2142
    GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ = 2149
    GET_ACCOUNTS_BY_ACCESS_TOKEN_RES = 2150
    REFRESH_TOKEN_REQ = 2173
    REFRESH_TOKEN_RES = 2174


def is_fx_weekend_closed(ts: datetime) -> bool:
    """Return True when global OTC FX spot is closed (Fri 17:00 NY -> Sun 17:00 NY, DST-aware)."""
    ny = utc(ts).astimezone(_NY_TZ)
    wd = ny.weekday()
    if wd == 4 and ny.hour >= 17:
        return True
    if wd == 5:
        return True
    if wd == 6 and ny.hour < 17:
        return True
    return False


def eur_units_to_protocol_volume_cents(eur_units: Decimal) -> int:
    """Convert signed/unsigned EUR base units (1,000 EUR = 0.01 lot) to cTrader protocol cents (100,000)."""
    abs_units = abs(eur_units)
    if abs_units <= Decimal("0"):
        raise ValueError("Order volume in EUR units must be strictly positive")
    scaled = abs_units * _VOLUME_CENTS_PER_EUR_UNIT
    if scaled != scaled.to_integral_value():
        raise ValueError(f"EUR quantity {eur_units} does not map to integer protocol cents")
    return int(scaled)


def _normalize_decimal(val: Decimal) -> Decimal:
    text = format(val.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return Decimal(text or "0")


def _map_commission_type(raw_val: Any) -> str | None:
    if raw_val in (1, "1", "USD_PER_MILLION_USD", "UsdPerMillionUsdVolume"):
        return "UsdPerMillionUsdVolume"
    if raw_val in (2, "2", "USD_PER_LOT", "UsdPerOneLot"):
        return "UsdPerOneLot"
    if raw_val in (3, "3", "PERCENTAGE_OF_VALUE", "PercentageOfTradingVolume"):
        return "PercentageOfTradingVolume"
    if raw_val in (4, "4", "QUOTE_CCY_PER_LOT", "QuoteCurrencyPerOneLot"):
        return "QuoteCurrencyPerOneLot"
    if isinstance(raw_val, str) and raw_val.strip():
        return raw_val.strip()
    return None


@dataclass(frozen=True)
class CTraderSymbolSpec:
    symbol_id: int
    symbol_name: str
    digits: int
    pip_position: int
    pip_size: Decimal
    commission: Decimal | None
    commission_type: str | None
    min_volume_cents: int
    max_volume_cents: int
    step_volume_cents: int


class CTraderOpenApiConfig(Contract):
    host: str = "demo.ctraderapi.com"
    port: int = 5036
    use_tls: bool = True
    allow_loopback_for_tests: bool = False
    client_id: str
    client_secret: SecretStr
    access_token: SecretStr
    refresh_token: SecretStr | None = None
    account_id: int | None = None
    symbol: str = "EURUSD"
    token_file: Path | None = None
    request_timeout_seconds: float = 15.0
    heartbeat_interval_seconds: float = 10.0

    @model_validator(mode="after")
    def _enforce_demo_endpoint_only(self) -> CTraderOpenApiConfig:
        h = self.host.strip().lower()
        if "live" in h:
            raise CTraderLiveEndpointForbiddenError(
                f"Live cTrader Open API endpoint '{self.host}' is strictly forbidden; Demo only."
            )
        if h == "demo.ctraderapi.com":
            if not self.use_tls:
                raise ValueError("TLS is mandatory for demo.ctraderapi.com")
        elif h in {"127.0.0.1", "localhost"} and self.allow_loopback_for_tests:
            pass
        else:
            raise ValueError(
                f"Untrusted cTrader Open API host '{self.host}'; expected 'demo.ctraderapi.com'."
            )
        if not 1 <= self.port <= 65535:
            raise ValueError("Invalid TCP port")
        if not self.client_id.strip():
            raise ValueError("client_id must be non-empty")
        return self

    @classmethod
    def from_environment(cls) -> CTraderOpenApiConfig:
        host = os.environ.get("CTRADER_OPENAPI_HOST", "demo.ctraderapi.com").strip()
        port = int(os.environ.get("CTRADER_OPENAPI_PORT", "5036").strip())
        client_id = os.environ.get("CTRADER_CLIENT_ID", "").strip()
        client_secret = os.environ.get("CTRADER_CLIENT_SECRET", "").strip()
        access_token = os.environ.get("CTRADER_ACCESS_TOKEN", "").strip()
        refresh_token_raw = os.environ.get("CTRADER_REFRESH_TOKEN", "").strip()
        account_id_raw = os.environ.get("CTRADER_ACCOUNT_ID", "").strip()
        token_file_raw = os.environ.get("CTRADER_TOKEN_FILE", "").strip()
        token_file = Path(token_file_raw).expanduser().resolve() if token_file_raw else None

        if token_file is not None and token_file.is_file():
            try:
                saved = json.loads(token_file.read_text(encoding="utf-8"))
                if isinstance(saved.get("access_token"), str) and saved["access_token"].strip():
                    access_token = saved["access_token"].strip()
                if isinstance(saved.get("refresh_token"), str) and saved["refresh_token"].strip():
                    refresh_token_raw = saved["refresh_token"].strip()
            except Exception as exc:
                logger.warning("Failed reading CTRADER_TOKEN_FILE %s: %s", token_file, exc)

        if not client_id or not client_secret or not access_token:
            raise ValueError(
                "Missing required cTrader Open API credentials in environment: "
                "CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET, CTRADER_ACCESS_TOKEN"
            )
        return cls(
            host=host,
            port=port,
            client_id=client_id,
            client_secret=SecretStr(client_secret),
            access_token=SecretStr(access_token),
            refresh_token=SecretStr(refresh_token_raw) if refresh_token_raw else None,
            account_id=int(account_id_raw) if account_id_raw else None,
            token_file=token_file,
        )


def _persist_tokens_0600(
    token_file: Path,
    *,
    access_token: str,
    refresh_token: str | None,
    updated_at: datetime,
) -> None:
    token_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = token_file.with_suffix(token_file.suffix + f".{os.getpid()}.tmp")
    payload = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "updated_at": utc(updated_at).isoformat(),
    }
    fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, token_file)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


class CTraderOpenApiClient:
    """Async 4-byte Big-Endian JSON TCP client for cTrader Open API (Demo-locked)."""

    def __init__(
        self,
        config: CTraderOpenApiConfig,
        *,
        on_spot_event: Callable[[dict[str, Any], datetime], None] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.config = config
        self._access_token = config.access_token.get_secret_value()
        self._refresh_token = (
            config.refresh_token.get_secret_value() if config.refresh_token is not None else None
        )
        if config.token_file is not None and config.token_file.is_file():
            try:
                saved = json.loads(config.token_file.read_text(encoding="utf-8"))
                if isinstance(saved.get("access_token"), str) and saved["access_token"]:
                    self._access_token = saved["access_token"]
                if isinstance(saved.get("refresh_token"), str) and saved["refresh_token"]:
                    self._refresh_token = saved["refresh_token"]
            except Exception:
                pass
        self._on_spot_event = on_spot_event
        self.clock = clock
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._pending_rpcs: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._write_lock = asyncio.Lock()
        self._closed_event = asyncio.Event()
        self.authenticated_account_id: int | None = None
        self.trader_info: dict[str, Any] | None = None

    @property
    def is_connected(self) -> bool:
        return (
            self._writer is not None
            and not self._writer.is_closing()
            and not self._closed_event.is_set()
        )

    async def wait_closed(self) -> None:
        await self._closed_event.wait()

    async def connect_only(self) -> None:
        ssl_ctx: ssl.SSLContext | None = None
        if self.config.use_tls:
            ssl_ctx = ssl.create_default_context()
        self._closed_event.clear()
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(
                self.config.host,
                self.config.port,
                ssl=ssl_ctx,
                server_hostname=self.config.host if self.config.use_tls else None,
            ),
            timeout=self.config.request_timeout_seconds,
        )
        self._read_task = asyncio.create_task(self._read_loop(), name="ctrader-openapi-read")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="ctrader-openapi-heartbeat"
        )

    async def connect_and_authenticate(self) -> int:
        await self.connect_only()
        await self._rpc(
            ProtoOAPayloadType.APPLICATION_AUTH_REQ,
            {
                "clientId": self.config.client_id,
                "clientSecret": self.config.client_secret.get_secret_value(),
            },
            expected_type=ProtoOAPayloadType.APPLICATION_AUTH_RES,
        )

        acct_res = await self._rpc(
            ProtoOAPayloadType.GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
            {"accessToken": self._access_token},
            expected_type=ProtoOAPayloadType.GET_ACCOUNTS_BY_ACCESS_TOKEN_RES,
        )
        accounts = list(acct_res.get("payload", {}).get("ctidTraderAccount", []))
        if not accounts:
            raise CTraderOpenApiError("No cTrader trading accounts returned for access token")

        target_id = self.config.account_id
        chosen: dict[str, Any] | None = None
        if target_id is not None:
            for item in accounts:
                if int(item.get("ctidTraderAccountId", -1)) == target_id:
                    chosen = item
                    break
            if chosen is None:
                raise CTraderOpenApiError(
                    f"Configured CTRADER_ACCOUNT_ID={target_id} not found in token account list"
                )
        else:
            demo_accounts = [a for a in accounts if a.get("isLive") is False]
            if len(demo_accounts) != 1:
                raise CTraderOpenApiError(
                    f"Expected exactly 1 Demo account when CTRADER_ACCOUNT_ID is omitted; found {len(demo_accounts)}"
                )
            chosen = demo_accounts[0]
            target_id = int(chosen["ctidTraderAccountId"])

        if chosen.get("isLive", True) is not False:
            raise CTraderLiveAccountForbiddenError(
                f"Account ctidTraderAccountId={target_id} has isLive={chosen.get('isLive')!r}; "
                "Live accounts are strictly forbidden."
            )

        assert target_id is not None
        await self._rpc(
            ProtoOAPayloadType.ACCOUNT_AUTH_REQ,
            {
                "ctidTraderAccountId": target_id,
                "accessToken": self._access_token,
            },
            expected_type=ProtoOAPayloadType.ACCOUNT_AUTH_RES,
        )
        self.authenticated_account_id = target_id

        trader_res = await self._rpc(
            ProtoOAPayloadType.TRADER_REQ,
            {"ctidTraderAccountId": target_id},
            expected_type=ProtoOAPayloadType.TRADER_RES,
        )
        self.trader_info = dict(trader_res.get("payload", {}).get("trader", {}))
        return target_id

    async def refresh_access_token(self) -> dict[str, Any]:
        if not self._refresh_token:
            raise CTraderOpenApiError("Cannot refresh access token: no refresh_token configured")
        res = await self._rpc(
            ProtoOAPayloadType.REFRESH_TOKEN_REQ,
            {"refreshToken": self._refresh_token},
            expected_type=ProtoOAPayloadType.REFRESH_TOKEN_RES,
        )
        payload = dict(res.get("payload", {}))
        new_access = str(payload.get("accessToken", "")).strip()
        new_refresh = str(payload.get("refreshToken", "")).strip() or self._refresh_token
        if not new_access:
            raise CTraderOpenApiError("Refresh token response missing accessToken")
        self._access_token = new_access
        self._refresh_token = new_refresh
        if self.config.token_file is not None:
            _persist_tokens_0600(
                self.config.token_file,
                access_token=self._access_token,
                refresh_token=self._refresh_token,
                updated_at=utc(self.clock()),
            )
        return payload

    async def fetch_symbol_spec(self, symbol_name: str = "EURUSD") -> CTraderSymbolSpec:
        acct_id = self._require_demo_account()
        norm_target = symbol_name.replace("/", "").upper()
        list_res = await self._rpc(
            ProtoOAPayloadType.SYMBOLS_LIST_REQ,
            {"ctidTraderAccountId": acct_id},
            expected_type=ProtoOAPayloadType.SYMBOLS_LIST_RES,
        )
        symbols = list(list_res.get("payload", {}).get("symbol", []))
        matched = next(
            (
                s
                for s in symbols
                if str(s.get("symbolName", "")).replace("/", "").upper() == norm_target
            ),
            None,
        )
        if matched is None:
            raise CTraderOpenApiError(f"Symbol {symbol_name!r} not found on Demo account {acct_id}")
        symbol_id = int(matched["symbolId"])

        detail_res = await self._rpc(
            ProtoOAPayloadType.SYMBOL_BY_ID_REQ,
            {"ctidTraderAccountId": acct_id, "symbolId": [symbol_id]},
            expected_type=ProtoOAPayloadType.SYMBOL_BY_ID_RES,
        )
        full_list = list(detail_res.get("payload", {}).get("symbol", []))
        if not full_list:
            raise CTraderOpenApiError(f"Empty symbol details for symbolId={symbol_id}")
        detail = full_list[0]
        digits = int(detail.get("digits", 5))
        pip_position = int(detail.get("pipPosition", 4))
        pip_size = Decimal(10) ** (-pip_position)

        commission: Decimal | None = None
        if detail.get("preciseTradingCommissionRate") is not None:
            commission = _normalize_decimal(
                Decimal(int(detail["preciseTradingCommissionRate"])) / _PRECISE_COMMISSION_SCALE
            )
        elif detail.get("commission") is not None:
            commission = _normalize_decimal(Decimal(str(detail["commission"])))

        comm_type = _map_commission_type(detail.get("commissionType"))
        return CTraderSymbolSpec(
            symbol_id=symbol_id,
            symbol_name=norm_target,
            digits=digits,
            pip_position=pip_position,
            pip_size=pip_size,
            commission=commission,
            commission_type=comm_type,
            min_volume_cents=int(detail.get("minVolume", 100_000)),
            max_volume_cents=int(detail.get("maxVolume", 1_000_000_000)),
            step_volume_cents=int(detail.get("stepVolume", 100_000)),
        )

    async def subscribe_spots(self, symbol_id: int) -> None:
        acct_id = self._require_demo_account()
        await self._rpc(
            ProtoOAPayloadType.SUBSCRIBE_SPOTS_REQ,
            {
                "ctidTraderAccountId": acct_id,
                "symbolId": [symbol_id],
                "subscribeToSpotTimestamp": True,
            },
            expected_type=ProtoOAPayloadType.SUBSCRIBE_SPOTS_RES,
        )

    async def reconcile(self) -> dict[str, Any]:
        acct_id = self._require_demo_account()
        res = await self._rpc(
            ProtoOAPayloadType.RECONCILE_REQ,
            {"ctidTraderAccountId": acct_id},
            expected_type=ProtoOAPayloadType.RECONCILE_RES,
        )
        payload = res.get("payload", {})
        positions: list[dict[str, Any]] = []
        for pos in payload.get("position", []):
            tdata = pos.get("tradeData", {})
            vol_cents = int(tdata.get("volume", 0))
            side = str(tdata.get("tradeSide", "")).upper()
            sign = Decimal("-1") if side in {"SELL", "2"} else Decimal("1")
            qty_eur = sign * (Decimal(vol_cents) / _VOLUME_CENTS_PER_EUR_UNIT)
            positions.append(
                {
                    "position_id": int(pos.get("positionId", 0)),
                    "symbol_id": int(tdata.get("symbolId", 0)),
                    "trade_side": side,
                    "volume_cents": vol_cents,
                    "quantity_eur": qty_eur,
                    "price": Decimal(str(pos["price"])) if pos.get("price") is not None else None,
                }
            )
        return {
            "account_id": acct_id,
            "is_live": False,
            "positions": positions,
            "orders": list(payload.get("order", [])),
        }

    async def send_demo_market_order(
        self,
        *,
        symbol_id: int,
        trade_side: str,
        volume_cents: int,
        label: str = "eventlens-demo",
    ) -> dict[str, Any]:
        """Retained for compatibility; never send an unvalidated broker order."""
        raise CTraderDemoExecutionUnavailableError(DEMO_EXECUTION_UNAVAILABLE)

    async def close(self) -> None:
        self._closed_event.set()
        for task in (self._heartbeat_task, self._read_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._heartbeat_task, self._read_task):
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
        self._heartbeat_task = None
        self._read_task = None
        if self._writer is not None:
            if not self._writer.is_closing():
                self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
        self._reader = None
        for fut in list(self._pending_rpcs.values()):
            if not fut.done():
                fut.set_exception(CTraderOpenApiError("Connection closed"))
        self._pending_rpcs.clear()

    def _require_demo_account(self) -> int:
        if self.authenticated_account_id is None:
            raise CTraderOpenApiError("Demo account is not authenticated")
        return self.authenticated_account_id

    async def _send_raw(self, message: dict[str, Any]) -> None:
        if self._writer is None or self._writer.is_closing():
            raise CTraderOpenApiError("Socket is not connected")
        encoded = json.dumps(message, separators=(",", ":")).encode("utf-8")
        frame = struct.pack(">I", len(encoded)) + encoded
        async with self._write_lock:
            self._writer.write(frame)
            await self._writer.drain()

    async def _rpc(
        self,
        payload_type: ProtoOAPayloadType,
        payload: dict[str, Any],
        *,
        expected_type: ProtoOAPayloadType | None = None,
    ) -> dict[str, Any]:
        cid = f"evl-{uuid4().hex[:16]}"
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending_rpcs[cid] = fut
        try:
            await self._send_raw(
                {
                    "clientMsgId": cid,
                    "payloadType": int(payload_type),
                    "payload": payload,
                }
            )
            resp = await asyncio.wait_for(fut, timeout=self.config.request_timeout_seconds)
        finally:
            self._pending_rpcs.pop(cid, None)

        rtype = int(resp.get("payloadType", -1))
        if rtype in {int(ProtoOAPayloadType.ERROR_RES), int(ProtoOAPayloadType.OA_ERROR_RES)}:
            err_p = resp.get("payload", {})
            raise CTraderOpenApiError(
                f"cTrader Open API error ({err_p.get('errorCode', 'UNKNOWN')}): "
                f"{err_p.get('description', '')}"
            )
        if expected_type is not None and rtype != int(expected_type):
            raise CTraderOpenApiError(
                f"Unexpected response payloadType={rtype}, expected={int(expected_type)}"
            )
        return resp

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._closed_event.is_set():
                await asyncio.sleep(self.config.heartbeat_interval_seconds)
                if self._closed_event.is_set():
                    break
                await self._send_raw(
                    {
                        "payloadType": int(ProtoOAPayloadType.HEARTBEAT_EVENT),
                        "payload": {},
                    }
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("ctrader_heartbeat_stopped: %s", exc)
            self._closed_event.set()

    async def _read_loop(self) -> None:
        assert self._reader is not None
        reader = self._reader
        try:
            while not self._closed_event.is_set():
                hdr = await reader.readexactly(4)
                (length,) = struct.unpack(">I", hdr)
                if length <= 0 or length > _MAX_FRAME_BYTES:
                    raise CTraderOpenApiError(f"Invalid Open API frame length: {length}")
                raw = await reader.readexactly(length)
                received_at = utc(self.clock())
                msg = json.loads(raw.decode("utf-8"))
                ptype = int(msg.get("payloadType", -1))
                cid = msg.get("clientMsgId")
                if ptype == int(ProtoOAPayloadType.HEARTBEAT_EVENT):
                    await self._send_raw(
                        {
                            "payloadType": int(ProtoOAPayloadType.HEARTBEAT_EVENT),
                            "payload": {},
                        }
                    )
                    continue
                if isinstance(cid, str) and cid in self._pending_rpcs:
                    fut = self._pending_rpcs[cid]
                    if not fut.done():
                        fut.set_result(msg)
                    continue
                if ptype == int(ProtoOAPayloadType.SPOT_EVENT) and self._on_spot_event is not None:
                    self._on_spot_event(msg.get("payload", {}), received_at)
        except asyncio.CancelledError:
            raise
        except asyncio.IncompleteReadError:
            self._closed_event.set()
        except Exception as exc:
            logger.warning("ctrader_read_loop_terminated: %s", exc)
            self._closed_event.set()
        finally:
            for fut in list(self._pending_rpcs.values()):
                if not fut.done():
                    fut.set_exception(CTraderOpenApiError("Open API read stream ended"))


class CTraderDemoOrderMirror:
    """Disabled compatibility interface; a reverse order is not a position close."""

    def __init__(self, *, client: CTraderOpenApiClient, symbol_spec: CTraderSymbolSpec) -> None:
        self.client = client
        self.symbol_spec = symbol_spec

    async def submit_market_delta(
        self,
        *,
        delta_quantity_eur: Decimal,
        client_order_label: str = "eventlens-shadow-mirror",
    ) -> dict[str, Any]:
        raise CTraderDemoExecutionUnavailableError(DEMO_EXECUTION_UNAVAILABLE)


class CTraderOpenApiQuoteStreamer:
    """Streams live EURUSD Demo ticks from cTrader Open API into QuoteStore with 12h session rotation."""

    def __init__(
        self,
        *,
        config: CTraderOpenApiConfig,
        quote_store: QuoteStore,
        basis: Basis = "live_poll",
        sample_interval_ms: int = 200,
        session_duration_seconds: int = 86400,
        session_rotate_seconds: float = 43200.0,
        reconnect_initial_backoff_seconds: float = 1.0,
        reconnect_max_backoff_seconds: float = 30.0,
        on_quote_published: Callable[[], None] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 100 <= sample_interval_ms <= 10000:
            raise ValueError("sample_interval_ms must be in [100, 10000]")
        self.config = config
        self.quote_store = quote_store
        self.basis = basis
        self.sample_interval_ms = sample_interval_ms
        self.session_duration_seconds = session_duration_seconds
        self.session_rotate_seconds = session_rotate_seconds
        self.reconnect_initial_backoff_seconds = reconnect_initial_backoff_seconds
        self.reconnect_max_backoff_seconds = reconnect_max_backoff_seconds
        self.on_quote_published = on_quote_published
        self.clock = clock

        self._stop_event = asyncio.Event()
        self.active_client: CTraderOpenApiClient | None = None
        self.active_symbol_spec: CTraderSymbolSpec | None = None

        # Per-session state
        self._session_id: UUID | None = None
        self._session_started_at: datetime | None = None
        self._sequence = 0
        self._raw_ticks = 0
        self._raw_zero_spread_ticks = 0
        self._last_bid: Decimal | None = None
        self._last_ask: Decimal | None = None
        self._last_source_at: datetime | None = None
        self._last_bridge_at: datetime | None = None
        self._last_received_at: datetime | None = None
        self._last_emit_mono: float = -1000.0
        self._last_emitted_positive_spread: bool = False
        self._pending_tick: tuple[datetime, datetime, Decimal, Decimal] | None = None

    async def stop(self) -> None:
        self._stop_event.set()
        if self.active_client is not None:
            await self.active_client.close()

    async def run(self) -> None:
        self._stop_event.clear()
        backoff = self.reconnect_initial_backoff_seconds
        while not self._stop_event.is_set():
            client = CTraderOpenApiClient(
                self.config,
                on_spot_event=self._handle_spot_payload,
                clock=self.clock,
            )
            self.active_client = client
            flush_task: asyncio.Task[None] | None = None
            try:
                try:
                    await client.connect_and_authenticate()
                except CTraderOpenApiError as auth_err:
                    if (
                        "ACCESS_TOKEN" in str(auth_err).upper()
                        and self.config.refresh_token is not None
                    ):
                        await client.close()
                        client = CTraderOpenApiClient(
                            self.config,
                            on_spot_event=self._handle_spot_payload,
                            clock=self.clock,
                        )
                        self.active_client = client
                        await client.connect_only()
                        await client.refresh_access_token()
                        await client.close()
                        continue
                    raise

                spec = await client.fetch_symbol_spec("EURUSD")
                self.active_symbol_spec = spec
                self._open_quote_session(spec)
                await client.subscribe_spots(spec.symbol_id)
                backoff = self.reconnect_initial_backoff_seconds

                flush_task = asyncio.create_task(
                    self._periodic_flush_and_rotate_loop(spec),
                    name="ctrader-openapi-flush",
                )
                stop_wait = asyncio.create_task(self._stop_event.wait())
                closed_wait = asyncio.create_task(client.wait_closed())
                try:
                    await asyncio.wait(
                        {stop_wait, closed_wait},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    for wt in (stop_wait, closed_wait):
                        if not wt.done():
                            wt.cancel()
                    await asyncio.gather(stop_wait, closed_wait, return_exceptions=True)
            except (CTraderLiveEndpointForbiddenError, CTraderLiveAccountForbiddenError):
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._stop_event.is_set():
                    logger.warning("ctrader_openapi_stream_reconnecting: %s", exc)
            finally:
                if flush_task is not None and not flush_task.done():
                    flush_task.cancel()
                    await asyncio.gather(flush_task, return_exceptions=True)
                self._close_quote_session()
                await client.close()
                if self.active_client is client:
                    self.active_client = None

            if not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                except TimeoutError:
                    pass
                backoff = min(self.reconnect_max_backoff_seconds, backoff * 2.0)

    def _open_quote_session(self, spec: CTraderSymbolSpec) -> None:
        now = utc(self.clock())
        sid = uuid4()
        start = BridgeStart(
            version=1,
            kind="start",
            session_id=sid,
            account_mode="demo",
            running_mode="realtime",
            symbol="EURUSD",
            started_at=now,
            sample_interval_ms=self.sample_interval_ms,
            duration_seconds=self.session_duration_seconds,
            source_provider="ctrader_openapi",
            digits=spec.digits,
            pip_size=spec.pip_size,
            commission=spec.commission,
            commission_type=spec.commission_type,
        )
        self.quote_store.start(start, now, basis=self.basis)
        self._session_id = sid
        self._session_started_at = now
        self._sequence = 0
        self._raw_ticks = 0
        self._raw_zero_spread_ticks = 0
        self._last_source_at = None
        self._last_bridge_at = None
        self._last_received_at = None
        self._last_emit_mono = -1000.0
        self._last_emitted_positive_spread = False
        self._pending_tick = None

    def _close_quote_session(self) -> None:
        if self._session_id is None or self._session_started_at is None:
            return
        if self._pending_tick is not None:
            src_ts, br_ts, bid, ask = self._pending_tick
            self._pending_tick = None
            self._emit_quote(src_ts, br_ts, bid, ask)
        now = utc(self.clock())
        min_stop = self._session_started_at
        if self._last_bridge_at is not None and self._last_bridge_at > min_stop:
            min_stop = self._last_bridge_at
        stop_ts = max(now, min_stop)
        throttled = max(0, self._raw_ticks - self._sequence)
        stop = BridgeStop(
            version=1,
            kind="stop",
            session_id=self._session_id,
            stopped_at=stop_ts,
            quotes=self._sequence,
            raw_ticks=self._raw_ticks,
            raw_zero_spread_ticks=self._raw_zero_spread_ticks,
            symbol_mismatch_ticks=0,
            throttled_ticks=throttled,
        )
        try:
            self.quote_store.stop(stop, max(utc(self.clock()), stop_ts))
        except Exception as exc:
            logger.warning("ctrader_openapi_session_stop_warning: %s", exc)
        self._session_id = None
        self._session_started_at = None

    async def _periodic_flush_and_rotate_loop(self, spec: CTraderSymbolSpec) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(0.05)
            if self._session_id is None or self._session_started_at is None:
                continue
            now = utc(self.clock())
            if (now - self._session_started_at).total_seconds() >= self.session_rotate_seconds:
                self._close_quote_session()
                self._open_quote_session(spec)
                continue
            if self._pending_tick is not None:
                mono = asyncio.get_running_loop().time()
                if (mono - self._last_emit_mono) * 1000.0 >= self.sample_interval_ms:
                    src_ts, br_ts, bid, ask = self._pending_tick
                    self._pending_tick = None
                    self._emit_quote(src_ts, br_ts, bid, ask)

    def _handle_spot_payload(self, payload: dict[str, Any], received_at: datetime) -> None:
        if self._session_id is None or self._session_started_at is None:
            return
        spec = self.active_symbol_spec
        quant = Decimal(10) ** (-(spec.digits if spec is not None else 5))

        if payload.get("bid") is not None:
            self._last_bid = (Decimal(int(payload["bid"])) / _SPOT_PRICE_SCALE).quantize(quant)
        if payload.get("ask") is not None:
            self._last_ask = (Decimal(int(payload["ask"])) / _SPOT_PRICE_SCALE).quantize(quant)

        if self._last_bid is None or self._last_ask is None:
            return
        bid = self._last_bid
        ask = self._last_ask
        if bid <= Decimal("0") or ask < bid:
            return

        bridge_at = max(received_at, self._session_started_at)
        if self._last_bridge_at is not None and bridge_at < self._last_bridge_at:
            bridge_at = self._last_bridge_at

        raw_ts = payload.get("timestamp")
        if raw_ts is not None:
            cand_source = datetime.fromtimestamp(int(raw_ts) / 1000.0, tz=UTC)
        else:
            cand_source = bridge_at
        source_at = min(bridge_at, cand_source)
        if self._last_source_at is not None and source_at < self._last_source_at:
            source_at = min(bridge_at, self._last_source_at)

        self._raw_ticks += 1
        positive_spread = ask > bid
        if not positive_spread:
            self._raw_zero_spread_ticks += 1

        mono_now = asyncio.get_running_loop().time()
        due_by_interval = (mono_now - self._last_emit_mono) * 1000.0 >= self.sample_interval_ms
        spread_changed = self._sequence > 0 and (
            positive_spread != self._last_emitted_positive_spread
        )

        if self._sequence == 0 or positive_spread or spread_changed or due_by_interval:
            self._pending_tick = None
            self._emit_quote(source_at, bridge_at, bid, ask)
        else:
            self._pending_tick = (source_at, bridge_at, bid, ask)

    def _emit_quote(
        self,
        source_at: datetime,
        bridge_at: datetime,
        bid: Decimal,
        ask: Decimal,
    ) -> None:
        if self._session_id is None or self._session_started_at is None:
            return
        now = utc(self.clock())
        rec_ts = max(now, bridge_at)
        if self._last_received_at is not None and rec_ts < self._last_received_at:
            rec_ts = self._last_received_at

        next_seq = self._sequence + 1
        bq = BridgeQuote(
            version=1,
            kind="quote",
            session_id=self._session_id,
            sequence=next_seq,
            symbol="EURUSD",
            source_at=source_at,
            bridge_at=bridge_at,
            bid=bid,
            ask=ask,
        )
        raw_line = bq.model_dump_json()
        try:
            self.quote_store.record(bq, rec_ts, raw_line)
        except Exception as exc:
            self.quote_store.failure(rec_ts, f"openapi_quote_reject:{exc}", raw_line)
            return

        self._sequence = next_seq
        self._last_source_at = source_at
        self._last_bridge_at = bridge_at
        self._last_received_at = rec_ts
        self._last_emit_mono = asyncio.get_running_loop().time()
        self._last_emitted_positive_spread = ask > bid
        if self.on_quote_published is not None:
            self.on_quote_published()
