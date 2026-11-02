"""TDD verification suite for 24x5 cloud operation & headless cTrader Open API Demo integration.

Covers:
1. Demo-only endpoint & account hard locks (rejects live.ctraderapi.com and isLive=True accounts; masks secrets).
2. Native 4-byte Big-Endian TLS/TCP JSON Open API protocol lifecycle, delta spot ticks, and QuoteStore provenance.
3. Token rotation (0600 file permissions), reconnect recovery, and seamless QuoteStore session rotation.
4. EUR-unit conversion, read-only reconciliation, and rejection of unvalidated broker order mirroring.
5. Continuous 24x5 ShadowRunner mode (clears stale bounded duration_seconds, FX weekend awareness, SIGTERM drain).
6. Durable daily LLM call budget circuit breaker (caps model spend while keeping protective exits 100% active).
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import struct
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from eventlens.contracts import (
    TRUSTED_COMMISSION_ZERO_SPREAD_PROVIDERS,
    Event,
    MarketSnapshot,
    Quote,
)
from eventlens.ctrader_openapi import (
    CTraderDemoOrderMirror,
    CTraderLiveAccountForbiddenError,
    CTraderLiveEndpointForbiddenError,
    CTraderOpenApiClient,
    CTraderOpenApiConfig,
    CTraderOpenApiQuoteStreamer,
    ProtoOAPayloadType,
    eur_units_to_protocol_volume_cents,
    is_fx_weekend_closed,
)
from eventlens.deepseek import ModelResponse
from eventlens.quotes import (
    QuoteQualityPolicy,
    QuoteStore,
    build_market_snapshot,
    validate_market_quote,
)
from eventlens.runtime import IntradayEventRuntime, ShadowRunner, SQLiteRuntimeStore
from eventlens.sources.english_tier1 import EURUSDEventFilter
from eventlens.sqlite_memory import SQLiteThesisMemory
from eventlens.trading.agent import SinglePassTradingAgent
from eventlens.trading.execution import (
    ProtectiveExitPolicy,
    ShadowCostModel,
    SQLiteShadowLedger,
)
from eventlens.trading.portfolio import DeterministicTargetValidator
from eventlens.trading.risk import DeterministicHardRiskEngine, RiskPolicy


async def _read_frame(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    try:
        hdr = await reader.readexactly(4)
    except asyncio.IncompleteReadError:
        return None
    (length,) = struct.unpack(">I", hdr)
    raw = await reader.readexactly(length)
    return dict(json.loads(raw.decode("utf-8")))


async def _write_frame(writer: asyncio.StreamWriter, msg: dict[str, Any]) -> None:
    raw = json.dumps(msg).encode("utf-8")
    writer.write(struct.pack(">I", len(raw)) + raw)
    await writer.drain()


class _MockOpenApiServer:
    """Deterministic local TCP server speaking cTrader Open API 4-byte BE JSON protocol."""

    def __init__(
        self,
        *,
        accounts: list[dict[str, Any]] | None = None,
        spot_events: list[dict[str, Any]] | None = None,
        drop_first_connection_after_ticks: int | None = None,
    ) -> None:
        self.accounts = accounts or [
            {"ctidTraderAccountId": 12345678, "isLive": False, "traderLogin": 1234567},
            {"ctidTraderAccountId": 87654321, "isLive": True, "traderLogin": 7654321},
        ]
        self.spot_events = spot_events or []
        self.drop_first_connection_after_ticks = drop_first_connection_after_ticks
        self.received_messages: list[dict[str, Any]] = []
        self.connection_count = 0
        self.spot_sub_count = 0
        self._server: asyncio.AbstractServer | None = None
        self.port: int = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_client, "127.0.0.1", 0)
        sock = self._server.sockets[0]
        self.port = int(sock.getsockname()[1])

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.connection_count += 1
        try:
            while True:
                msg = await _read_frame(reader)
                if msg is None:
                    break
                self.received_messages.append(msg)
                ptype = int(msg["payloadType"])
                cid = msg.get("clientMsgId")
                payload = msg.get("payload", {})

                if ptype == ProtoOAPayloadType.HEARTBEAT_EVENT:
                    continue
                if ptype == ProtoOAPayloadType.APPLICATION_AUTH_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.APPLICATION_AUTH_RES),
                            "payload": {},
                        },
                    )
                elif ptype == ProtoOAPayloadType.GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.GET_ACCOUNTS_BY_ACCESS_TOKEN_RES),
                            "payload": {
                                "accessToken": payload.get("accessToken", ""),
                                "ctidTraderAccount": self.accounts,
                            },
                        },
                    )
                elif ptype == ProtoOAPayloadType.ACCOUNT_AUTH_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.ACCOUNT_AUTH_RES),
                            "payload": {"ctidTraderAccountId": payload["ctidTraderAccountId"]},
                        },
                    )
                elif ptype == ProtoOAPayloadType.TRADER_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.TRADER_RES),
                            "payload": {
                                "ctidTraderAccountId": payload["ctidTraderAccountId"],
                                "trader": {
                                    "ctidTraderAccountId": payload["ctidTraderAccountId"],
                                    "balance": 17621,
                                    "moneyDigits": 2,
                                    "leverageInCents": 20000,
                                    "traderLogin": 1234567,
                                },
                            },
                        },
                    )
                elif ptype == ProtoOAPayloadType.SYMBOLS_LIST_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.SYMBOLS_LIST_RES),
                            "payload": {
                                "ctidTraderAccountId": payload["ctidTraderAccountId"],
                                "symbol": [
                                    {
                                        "symbolId": 1,
                                        "symbolName": "EURUSD",
                                        "enabled": True,
                                    }
                                ],
                            },
                        },
                    )
                elif ptype == ProtoOAPayloadType.SYMBOL_BY_ID_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.SYMBOL_BY_ID_RES),
                            "payload": {
                                "ctidTraderAccountId": payload["ctidTraderAccountId"],
                                "symbol": [
                                    {
                                        "symbolId": 1,
                                        "digits": 5,
                                        "pipPosition": 4,
                                        "commissionType": "USD_PER_MILLION_USD",
                                        "preciseTradingCommissionRate": 3000000,
                                        "minVolume": 100000,
                                        "maxVolume": 1000000000,
                                        "stepVolume": 100000,
                                    }
                                ],
                            },
                        },
                    )
                elif ptype == ProtoOAPayloadType.SUBSCRIBE_SPOTS_REQ:
                    self.spot_sub_count += 1
                    sub_idx = self.spot_sub_count
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.SUBSCRIBE_SPOTS_RES),
                            "payload": {"ctidTraderAccountId": payload["ctidTraderAccountId"]},
                        },
                    )
                    sent = 0
                    for ev in self.spot_events:
                        await _write_frame(
                            writer,
                            {
                                "payloadType": int(ProtoOAPayloadType.SPOT_EVENT),
                                "payload": {
                                    "ctidTraderAccountId": payload["ctidTraderAccountId"],
                                    "symbolId": 1,
                                    **ev,
                                },
                            },
                        )
                        sent += 1
                        await asyncio.sleep(0.01)
                        if (
                            sub_idx == 1
                            and self.drop_first_connection_after_ticks is not None
                            and sent >= self.drop_first_connection_after_ticks
                        ):
                            writer.close()
                            await writer.wait_closed()
                            return
                elif ptype == ProtoOAPayloadType.REFRESH_TOKEN_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.REFRESH_TOKEN_RES),
                            "payload": {
                                "tokenType": "bearer",
                                "accessToken": "rotated_access_token_v2",
                                "expiresIn": 2592000,
                                "refreshToken": "rotated_refresh_token_v2",
                            },
                        },
                    )
                elif ptype == ProtoOAPayloadType.RECONCILE_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.RECONCILE_RES),
                            "payload": {
                                "ctidTraderAccountId": payload["ctidTraderAccountId"],
                                "position": [
                                    {
                                        "positionId": 9001,
                                        "price": 1.11868,
                                        "tradeData": {
                                            "symbolId": 1,
                                            "volume": 400000,
                                            "tradeSide": "SELL",
                                        },
                                    }
                                ],
                                "order": [],
                            },
                        },
                    )
                elif ptype == ProtoOAPayloadType.NEW_ORDER_REQ:
                    await _write_frame(
                        writer,
                        {
                            "clientMsgId": cid,
                            "payloadType": int(ProtoOAPayloadType.EXECUTION_EVENT),
                            "payload": {
                                "ctidTraderAccountId": payload["ctidTraderAccountId"],
                                "executionType": "ORDER_FILLED",
                                "position": {
                                    "positionId": 9002,
                                    "tradeData": {
                                        "symbolId": payload["symbolId"],
                                        "volume": payload["volume"],
                                        "tradeSide": payload["tradeSide"],
                                    },
                                },
                            },
                        },
                    )
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if not writer.is_closing():
                writer.close()


def test_ctrader_config_blocks_live_hosts_and_masks_secrets() -> None:
    with pytest.raises(CTraderLiveEndpointForbiddenError):
        CTraderOpenApiConfig(
            host="live.ctraderapi.com",
            client_id="cid",
            client_secret="super_secret_val",
            access_token="secret_access_tok",
        )

    with pytest.raises(CTraderLiveEndpointForbiddenError):
        CTraderOpenApiConfig(
            host="live2.ctraderapi.com",
            client_id="cid",
            client_secret="super_secret_val",
            access_token="secret_access_tok",
        )

    with pytest.raises(ValueError, match="Untrusted cTrader Open API host"):
        CTraderOpenApiConfig(
            host="evil.example.com",
            client_id="cid",
            client_secret="super_secret_val",
            access_token="secret_access_tok",
        )

    cfg = CTraderOpenApiConfig(
        host="demo.ctraderapi.com",
        port=5036,
        client_id="my_client_id",
        client_secret="TOP_SECRET_CLIENT_KEY",
        access_token="TOP_SECRET_ACCESS_TOKEN",
        refresh_token="TOP_SECRET_REFRESH_TOKEN",
        account_id=12345678,
    )
    rendered = f"{cfg!r} {cfg}"
    assert "TOP_SECRET_CLIENT_KEY" not in rendered
    assert "TOP_SECRET_ACCESS_TOKEN" not in rendered
    assert "TOP_SECRET_REFRESH_TOKEN" not in rendered


def test_ctrader_openapi_blocks_live_account_id_before_account_auth() -> None:
    async def _scenario() -> None:
        srv = _MockOpenApiServer()
        await srv.start()
        try:
            cfg = CTraderOpenApiConfig(
                host="127.0.0.1",
                port=srv.port,
                use_tls=False,
                allow_loopback_for_tests=True,
                client_id="cid",
                client_secret="csec",
                access_token="atok",
                account_id=87654321,  # isLive=True in mock server!
            )
            client = CTraderOpenApiClient(cfg)
            with pytest.raises(CTraderLiveAccountForbiddenError):
                await client.connect_and_authenticate()
            await client.close()

            sent_types = [int(m["payloadType"]) for m in srv.received_messages]
            assert int(ProtoOAPayloadType.APPLICATION_AUTH_REQ) in sent_types
            assert int(ProtoOAPayloadType.GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ) in sent_types
            assert int(ProtoOAPayloadType.ACCOUNT_AUTH_REQ) not in sent_types
        finally:
            await srv.close()

    asyncio.run(_scenario())


def test_ctrader_openapi_streams_verified_zero_spread_and_delta_ticks_into_quotestore(
    tmp_path: Path,
) -> None:
    assert "ctrader_openapi" in TRUSTED_COMMISSION_ZERO_SPREAD_PROVIDERS

    async def _scenario() -> None:
        now_ms = int(datetime.now(UTC).timestamp() * 1000) - 200
        srv = _MockOpenApiServer(
            spot_events=[
                {"bid": 111868, "ask": 111868, "timestamp": now_ms},
                {"ask": 111870, "timestamp": now_ms + 30},  # delta-only ask -> positive spread!
                {"bid": 111870, "timestamp": now_ms + 60},  # delta-only bid -> zero spread!
            ]
        )
        await srv.start()
        qstore = QuoteStore(tmp_path / "quotes.sqlite")
        try:
            cfg = CTraderOpenApiConfig(
                host="127.0.0.1",
                port=srv.port,
                use_tls=False,
                allow_loopback_for_tests=True,
                client_id="cid",
                client_secret="csec",
                access_token="atok",
                account_id=12345678,
            )
            streamer = CTraderOpenApiQuoteStreamer(
                config=cfg,
                quote_store=qstore,
                sample_interval_ms=200,
            )
            task = asyncio.create_task(streamer.run())
            deadline = asyncio.get_running_loop().time() + 3.0
            while asyncio.get_running_loop().time() < deadline:
                rep = qstore.report()
                if int(rep.get("quotes", 0)) >= 3:
                    break
                await asyncio.sleep(0.02)
            await streamer.stop()
            await asyncio.wait_for(task, timeout=3.0)

            now_check = datetime.now(UTC)
            obs = qstore.latest("EURUSD", now_check, basis="live_poll")
            assert obs is not None
            assert obs.provider == "ctrader_openapi"
            assert obs.commission == Decimal("30")
            assert obs.commission_type == "UsdPerMillionUsdVolume"
            assert obs.quote.bid == Decimal("1.11870")
            assert obs.quote.ask == Decimal("1.11870")

            snap = build_market_snapshot(qstore, now_check, basis="live_poll")
            assert len(snap.quotes) == 1
            q = snap.quotes[0]
            assert q.has_verified_provenance is True
            assert q.has_verified_commission is True

            rejected = validate_market_quote(
                snap,
                as_of=now_check,
                policy=QuoteQualityPolicy(allow_commission_bearing_zero_spread=False),
            )
            assert rejected.valid is False
            assert rejected.reason == "zero_spread_rejected"

            accepted = validate_market_quote(
                snap,
                as_of=now_check,
                policy=QuoteQualityPolicy(
                    allow_commission_bearing_zero_spread=True,
                    require_verified_provenance=True,
                ),
            )
            assert accepted.valid is True
            assert accepted.reason == "quote_valid"

            qrep = qstore.report()
            assert qrep["session_details"][0]["closed"] is True
            assert qrep["session_details"][0]["missing_sequences"] == 0
            assert qrep["quality_by_basis"]["live_poll"]["positive_spread"] >= 1
            assert qrep["quality_by_basis"]["live_poll"]["zero_spread"] >= 2
        finally:
            qstore.close()
            await srv.close()

    asyncio.run(_scenario())


def test_ctrader_openapi_token_refresh_persists_0600_and_reconnects_cleanly(
    tmp_path: Path,
) -> None:
    async def _scenario() -> None:
        now_ms = int(datetime.now(UTC).timestamp() * 1000) - 200
        srv = _MockOpenApiServer(
            spot_events=[
                {"bid": 111850, "ask": 111852, "timestamp": now_ms},
                {"bid": 111855, "ask": 111857, "timestamp": now_ms + 40},
            ],
            drop_first_connection_after_ticks=1,
        )
        await srv.start()
        token_file = tmp_path / "ctrader_tokens.json"
        qstore = QuoteStore(tmp_path / "quotes_reconn.sqlite")
        try:
            cfg = CTraderOpenApiConfig(
                host="127.0.0.1",
                port=srv.port,
                use_tls=False,
                allow_loopback_for_tests=True,
                client_id="cid",
                client_secret="csec",
                access_token="initial_atok",
                refresh_token="initial_rtok",
                account_id=12345678,
                token_file=token_file,
            )
            # 1. Verify token rotation writes 0600 file
            client = CTraderOpenApiClient(cfg)
            await client.connect_only()
            new_tokens = await client.refresh_access_token()
            await client.close()
            assert new_tokens["accessToken"] == "rotated_access_token_v2"
            assert token_file.is_file()
            mode = stat.S_IMODE(os.stat(token_file).st_mode)
            assert mode == 0o600
            saved = json.loads(token_file.read_text(encoding="utf-8"))
            assert saved["refresh_token"] == "rotated_refresh_token_v2"

            # 2. Verify reconnect opens a second clean QuoteStore session after drop
            streamer = CTraderOpenApiQuoteStreamer(
                config=cfg,
                quote_store=qstore,
                sample_interval_ms=100,
                reconnect_initial_backoff_seconds=0.05,
            )
            task = asyncio.create_task(streamer.run())
            deadline = asyncio.get_running_loop().time() + 4.0
            while asyncio.get_running_loop().time() < deadline:
                rep = qstore.report()
                sdetails = rep.get("session_details", [])
                if len(sdetails) >= 2 and sdetails[1]["quotes"] >= 1:
                    break
                await asyncio.sleep(0.03)
            await streamer.stop()
            await asyncio.wait_for(task, timeout=3.0)

            rep = qstore.report()
            assert len(rep["session_details"]) >= 2
            assert all(s["closed"] for s in rep["session_details"])
            assert all(s["missing_sequences"] == 0 for s in rep["session_details"])
        finally:
            qstore.close()
            await srv.close()

    asyncio.run(_scenario())


def test_ctrader_demo_order_mirror_is_blocked_without_sending_broker_orders() -> None:
    assert eur_units_to_protocol_volume_cents(Decimal("1000")) == 100_000
    assert eur_units_to_protocol_volume_cents(Decimal("4000")) == 400_000

    async def _scenario() -> None:
        srv = _MockOpenApiServer()
        await srv.start()
        try:
            cfg = CTraderOpenApiConfig(
                host="127.0.0.1",
                port=srv.port,
                use_tls=False,
                allow_loopback_for_tests=True,
                client_id="cid",
                client_secret="csec",
                access_token="atok",
                account_id=12345678,
            )
            client = CTraderOpenApiClient(cfg)
            await client.connect_and_authenticate()
            spec = await client.fetch_symbol_spec("EURUSD")
            mirror = CTraderDemoOrderMirror(client=client, symbol_spec=spec)

            # Neither a first entry nor its opposite delta may be sent until
            # position-aware closes, fill confirmation and reconciliation exist.
            for quantity in (Decimal("4000"), Decimal("-4000")):
                with pytest.raises(RuntimeError, match="demo_execution_unavailable"):
                    await mirror.submit_market_delta(delta_quantity_eur=quantity)
            with pytest.raises(RuntimeError, match="demo_execution_unavailable"):
                await client.send_demo_market_order(
                    symbol_id=spec.symbol_id, trade_side="BUY", volume_cents=400_000
                )
            order_msgs = [
                m
                for m in srv.received_messages
                if int(m["payloadType"]) == int(ProtoOAPayloadType.NEW_ORDER_REQ)
            ]
            assert order_msgs == []

            # Read-only reconciliation and market-data access remain available.
            assert (await client.reconcile())["is_live"] is False
            await client.close()
        finally:
            await srv.close()

    asyncio.run(_scenario())


def test_continuous_24x5_runner_clears_stale_duration_and_handles_fx_weekend(
    tmp_path: Path,
) -> None:
    # Friday 21:30 UTC in November (EST = UTC-5 -> Friday 16:30 NY -> OPEN)
    assert is_fx_weekend_closed(datetime(2026, 11, 6, 21, 30, tzinfo=UTC)) is False
    # Friday 22:05 UTC in November (EST = UTC-5 -> Friday 17:05 NY -> CLOSED)
    assert is_fx_weekend_closed(datetime(2026, 11, 6, 22, 5, tzinfo=UTC)) is True
    # Saturday 12:00 UTC -> CLOSED
    assert is_fx_weekend_closed(datetime(2026, 11, 7, 12, 0, tzinfo=UTC)) is True
    # Sunday 21:55 UTC in November (Sunday 16:55 NY -> CLOSED)
    assert is_fx_weekend_closed(datetime(2026, 11, 8, 21, 55, tzinfo=UTC)) is True
    # Sunday 22:05 UTC in November (Sunday 17:05 NY -> OPEN)
    assert is_fx_weekend_closed(datetime(2026, 11, 8, 22, 5, tzinfo=UTC)) is False

    async def _scenario() -> None:
        shadow_db = tmp_path / "shadow.sqlite"
        memory_db = tmp_path / "memory.sqlite"
        state_store = SQLiteRuntimeStore(shadow_db)
        ledger = SQLiteShadowLedger(shadow_db, initial_cash_usd=Decimal("200"))
        memory = SQLiteThesisMemory(memory_db, allow_targets=True)
        try:
            t0 = datetime.now(UTC)
            # Simulate a previous bounded 5s run recorded in shadow_runner_state
            state_store.update_runner_state(
                status="stopped",
                now=t0 - timedelta(seconds=20),
                started_at=t0 - timedelta(seconds=25),
                stopped_at=t0 - timedelta(seconds=20),
                duration_seconds=5.0,
                reset_stop=True,
            )

            def _market(ts: datetime) -> MarketSnapshot:
                return MarketSnapshot(
                    snapshot_id=f"m:{ts.isoformat()}",
                    as_of=ts,
                    quotes=(
                        Quote(
                            instrument="EUR/USD",
                            source_at=ts,
                            bridge_at=ts,
                            received_at=ts,
                            available_at=ts,
                            source_provider="ctrader_openapi",
                            basis="live_poll",
                            bid=Decimal("1.11800"),
                            ask=Decimal("1.11802"),
                            commission=Decimal("30"),
                            commission_type="UsdPerMillionUsdVolume",
                        ),
                    ),
                )

            class _NoopAgent:
                async def decide(self, context: Any) -> Any:
                    raise AssertionError("Should not be called")

            runtime = IntradayEventRuntime(
                memory=memory,
                portfolio_state=ledger,
                market_provider=_market,
                agent=_NoopAgent(),  # type: ignore[arg-type]
                target_validator=DeterministicTargetValidator(),
                risk_engine=DeterministicHardRiskEngine(policy=RiskPolicy()),
                shadow_executor=ledger,
                state_store=state_store,
                event_filter=EURUSDEventFilter(),
            )
            with pytest.raises(RuntimeError, match="demo_execution_unavailable"):
                ShadowRunner(runtime=runtime, ledger=ledger, ctrader_demo_mirror=True)
            runner = ShadowRunner(
                runtime=runtime,
                ledger=ledger,
                poll_news=False,
                quote_interval_seconds=0.05,
                protection_interval_seconds=0.05,
            )
            run_task = asyncio.create_task(runner.run(duration_seconds=None))
            await asyncio.sleep(0.12)

            # Verify stale 5.0s duration_seconds was cleared on continuous start!
            st = state_store.runner_status()
            assert st["status"] == "running"
            assert st["duration_seconds"] is None
            assert runner._active_stop_reason() is None

            runner.stop(reason="signal_sigterm")
            summary = await asyncio.wait_for(run_task, timeout=3.0)
            assert summary["stop_reason"] == "signal_sigterm"
            assert summary["continuous"] is True
            assert summary["duration_seconds"] is None
        finally:
            ledger.close()
            state_store.close()

    asyncio.run(_scenario())


class _SequencedMockModel:
    provider = "deepseek"
    model = "deepseek-chat"

    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.calls = 0

    async def complete_json(self, **_: Any) -> ModelResponse:
        idx = min(self.calls, len(self.responses) - 1)
        self.calls += 1
        return ModelResponse(
            content=json.dumps(self.responses[idx]),
            model_id="deepseek-chat",
            input_tokens=120,
            output_tokens=60,
        )


def test_daily_llm_budget_circuit_breaker_caps_model_calls_without_blocking_protection(
    tmp_path: Path,
) -> None:
    async def _scenario() -> None:
        shadow_db = tmp_path / "shadow_budget.sqlite"
        memory_db = tmp_path / "memory_budget.sqlite"
        state_store = SQLiteRuntimeStore(shadow_db)
        risk_policy = RiskPolicy(
            min_equity_usd=Decimal("50"),
            max_daily_loss_usd=Decimal("50"),
            leverage=Decimal("200"),
            max_position_weight=Decimal("0.25"),
            min_volume_eur=Decimal("1000"),
            max_volume_eur=Decimal("10000000"),
            volume_step_eur=Decimal("1000"),
        )
        exit_policy = ProtectiveExitPolicy(
            stop_loss_pips=Decimal("10.0"),
            max_holding_seconds=1800,
        )
        ledger = SQLiteShadowLedger(
            shadow_db,
            initial_cash_usd=Decimal("200"),
            cost_model=ShadowCostModel(commission_bps=Decimal("0"), slippage_bps=Decimal("0")),
            exit_policy=exit_policy,
            risk_policy=risk_policy,
        )
        memory = SQLiteThesisMemory(memory_db, allow_targets=True)
        current_bid = Decimal("1.11800")
        current_ask = Decimal("1.11802")

        def _market(ts: datetime) -> MarketSnapshot:
            return MarketSnapshot(
                snapshot_id=f"mkt:{ts.timestamp()}:{current_bid}",
                as_of=ts,
                quotes=(
                    Quote(
                        instrument="EUR/USD",
                        source_at=ts,
                        bridge_at=ts,
                        received_at=ts,
                        available_at=ts,
                        source_provider="ctrader_openapi",
                        basis="live_poll",
                        bid=current_bid,
                        ask=current_ask,
                        commission=Decimal("30"),
                        commission_type="UsdPerMillionUsdVolume",
                    ),
                ),
            )

        ev1_text = "US ISM Manufacturing PMI Actual: 56.2 vs Consensus Forecast: 54.7"
        ev2_text = "ECB Lagarde signals Eurozone growth headwinds persist in Q4"
        ev3_text = "US Factory Orders rise more than expected in September release"

        mock_model = _SequencedMockModel(
            [
                {
                    "memory_action": "create",
                    "thesis_id": None,
                    "status": "active",
                    "narrative": "Strong US ISM Manufacturing beat supports USD vs EUR",
                    "instruments": ["EUR/USD"],
                    "invalidation_conditions": ["EUR/USD >= 1.1210"],
                    "confidence": 0.75,
                    "evidence_role": "support",
                    "evidence_quote": ev1_text,
                    "invalidate_thesis_ids": [],
                    "portfolio_action": "SET_TARGET",
                    "target_direction": "short",
                    "target_weight": "-0.12",
                    "target_thesis_ids": ["__current__"],
                    "rationale": "Open short EUR/USD on strong US ISM beat",
                },
                {
                    "memory_action": "ignore",
                    "thesis_id": None,
                    "status": None,
                    "narrative": None,
                    "instruments": None,
                    "invalidation_conditions": None,
                    "confidence": None,
                    "evidence_role": None,
                    "evidence_quote": None,
                    "invalidate_thesis_ids": [],
                    "portfolio_action": "HOLD",
                    "target_direction": "flat",
                    "target_weight": "0",
                    "target_thesis_ids": [],
                    "rationale": "Hold existing short thesis",
                },
            ]
        )
        agent = SinglePassTradingAgent(
            mock_model,  # type: ignore[arg-type]
            max_position_weight=risk_policy.max_position_weight,
        )
        runtime = IntradayEventRuntime(
            memory=memory,
            portfolio_state=ledger,
            market_provider=_market,
            agent=agent,
            target_validator=DeterministicTargetValidator(),
            risk_engine=DeterministicHardRiskEngine(policy=risk_policy),
            shadow_executor=ledger,
            state_store=state_store,
            max_daily_llm_calls=2,
        )
        try:
            t0 = datetime.now(UTC)
            r1 = await runtime.run_cycle(
                Event(
                    event_id="tier1:ism-1",
                    source="tier1_english",
                    source_type="news",
                    source_reference="https://example.com/1",
                    source_timestamp=t0,
                    received_timestamp=t0,
                    raw_text=ev1_text,
                )
            )
            assert r1.status == "shadow_recorded"
            fills = ledger.fill_pending(_market(datetime.now(UTC)), risk_policy=risk_policy)
            assert len(fills) == 1
            assert Decimal(ledger.report()["quantity_eur"]) < Decimal("0")

            t1 = datetime.now(UTC)
            r2 = await runtime.run_cycle(
                Event(
                    event_id="tier1:ecb-2",
                    source="tier1_english",
                    source_type="news",
                    source_reference="https://example.com/2",
                    source_timestamp=t1,
                    received_timestamp=t1,
                    raw_text=ev2_text,
                )
            )
            assert r2.status == "no_target"
            assert mock_model.calls == 2

            # 3rd event on the same UTC day MUST trip the daily LLM budget breaker without calling LLM!
            t2 = datetime.now(UTC)
            r3 = await runtime.run_cycle(
                Event(
                    event_id="tier1:orders-3",
                    source="tier1_english",
                    source_type="news",
                    source_reference="https://example.com/3",
                    source_timestamp=t2,
                    received_timestamp=t2,
                    raw_text=ev3_text,
                )
            )
            assert r3.status == "expired"
            assert "daily_llm_budget_exhausted" in r3.reason
            assert mock_model.calls == 2  # Strictly zero extra LLM calls!

            # Verify protective stop-loss still executes cleanly when EUR/USD spikes +15 pips against short!
            current_bid = Decimal("1.11960")
            current_ask = Decimal("1.11962")
            exit_fills = ledger.evaluate_protective_exits(_market(datetime.now(UTC)))
            assert len(exit_fills) == 1
            assert exit_fills[0].reason == "protective_stop_loss"
            assert Decimal(ledger.report()["quantity_eur"]) == Decimal("0")
        finally:
            ledger.close()
            state_store.close()

    asyncio.run(_scenario())


def test_shadow_cli_continuous_and_ctrader_flag_validation(tmp_path: Path, capsys: Any) -> None:
    """CLI guards reject unsafe/contradictory cTrader & continuous flag combinations."""
    from eventlens.shadow_cli import main

    # Reject before credentials, databases, network or model calls, with or without Open API.
    for flags in ([], ["--ctrader-openapi"]):
        with pytest.raises(SystemExit) as exc1:
            main(["start", "--ctrader-demo-mirror-orders", "--duration", "10", *flags])
        assert exc1.value.code == 2
        assert "demo_execution_unavailable" in capsys.readouterr().err

    # 2. --ctrader-openapi and --quote-bridge-dir are mutually exclusive
    with pytest.raises(SystemExit) as exc2:
        main(
            [
                "start",
                "--ctrader-openapi",
                "--quote-bridge-dir",
                str(tmp_path),
                "--duration",
                "10",
            ]
        )
    assert exc2.value.code == 2

    # 3. --max-daily-llm-calls <= 0 must fail
    with pytest.raises(SystemExit) as exc3:
        main(["start", "--continuous", "--max-daily-llm-calls", "0"])
    assert exc3.value.code == 2

    # 4. Bounded --duration > 604800 without --continuous must fail
    with pytest.raises(SystemExit) as exc4:
        main(["start", "--duration", "604801"])
    assert exc4.value.code == 2
