"""Shadow-only intraday runtime CLI: start, stop, status, report, and crash recovery."""

import argparse
import asyncio
import json
import logging
import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from .contracts import AgentContext, AgentDecision, MarketSnapshot
from .deepseek import DeepSeekClient
from .observer import ObserverStore
from .quotes import QuoteStore, build_market_snapshot
from .runtime import IntradayEventRuntime, ShadowRunner, SQLiteRuntimeStore
from .sources.wallstreetcn import WallStreetCNFilter, WallStreetCNSource
from .sqlite_memory import SQLiteThesisMemory
from .trading.agent import SinglePassTradingAgent
from .trading.execution import ProtectiveExitPolicy, ShadowCostModel, SQLiteShadowLedger
from .trading.portfolio import LOT_UNITS_EUR, DeterministicTargetValidator
from .trading.risk import DeterministicHardRiskEngine, RiskPolicy


class _AbstainWithoutModelAgent:
    """Safe offline agent used when --call-model is not passed; never invents trades."""

    async def decide(self, context: AgentContext) -> AgentDecision:
        raise RuntimeError(
            "Model calls are disabled; pass --call-model to enable DeepSeek reasoning"
        )


def _load_latest_market(quotes_path: Path, basis: str) -> MarketSnapshot | None:
    if not quotes_path.is_file():
        return None
    qstore = QuoteStore(quotes_path, readonly=True)
    try:
        return build_market_snapshot(
            qstore,
            datetime.now(UTC),
            basis="synthetic_fixture" if basis == "synthetic_fixture" else "live_poll",
        )
    finally:
        qstore.close()


def _build_policies(
    args: argparse.Namespace,
) -> tuple[Decimal, RiskPolicy, ProtectiveExitPolicy]:
    profile = getattr(args, "account_profile", "pepperstone_razor_200")
    kill_switch = bool(getattr(args, "kill_switch", False))
    max_quote_age_ms = int(getattr(args, "max_quote_age", 2.0) * 1000)
    raw_holding = getattr(args, "max_holding_seconds", None)

    if profile == "pepperstone_razor_200":
        initial_cash = Decimal("200.00")
        risk = RiskPolicy.pepperstone_razor_200(
            kill_switch_active=kill_switch,
            max_quote_age_ms=max_quote_age_ms,
        )
        exit_pol = ProtectiveExitPolicy.pepperstone_razor_200(
            max_holding_seconds=int(raw_holding) if raw_holding is not None else 900,
            max_quote_age_ms=max_quote_age_ms,
        )
    else:
        initial_cash = Decimal("10000.00")
        risk = RiskPolicy(
            kill_switch_active=kill_switch,
            max_quote_age_ms=max_quote_age_ms,
        )
        exit_pol = ProtectiveExitPolicy(
            max_holding_seconds=int(raw_holding) if raw_holding is not None else 1800,
            max_quote_age_ms=max_quote_age_ms,
        )

    if getattr(args, "initial_cash", None) is not None:
        initial_cash = Decimal(str(args.initial_cash))
    risk_updates: dict[str, Any] = {}
    exit_updates: dict[str, Any] = {}
    if getattr(args, "leverage", None) is not None:
        risk_updates["leverage"] = Decimal(str(args.leverage))
    if getattr(args, "min_lot", None) is not None:
        risk_updates["min_volume_eur"] = (Decimal(str(args.min_lot)) * LOT_UNITS_EUR).quantize(
            Decimal("1")
        )
    if getattr(args, "max_lot", None) is not None:
        risk_updates["max_volume_eur"] = (Decimal(str(args.max_lot)) * LOT_UNITS_EUR).quantize(
            Decimal("1")
        )
    if getattr(args, "lot_step", None) is not None:
        risk_updates["volume_step_eur"] = (Decimal(str(args.lot_step)) * LOT_UNITS_EUR).quantize(
            Decimal("1")
        )
    if getattr(args, "min_equity", None) is not None:
        risk_updates["min_equity_usd"] = Decimal(str(args.min_equity))
    if getattr(args, "max_daily_loss", None) is not None:
        daily_loss = Decimal(str(args.max_daily_loss))
        risk_updates["max_daily_loss_usd"] = daily_loss
        exit_updates["max_daily_loss_usd"] = daily_loss
    if getattr(args, "max_position_loss", None) is not None:
        exit_updates["max_position_loss_usd"] = Decimal(str(args.max_position_loss))
    if getattr(args, "take_profit", None) is not None:
        tp = Decimal(str(args.take_profit))
        exit_updates["take_profit_usd"] = tp if tp > Decimal("0") else None

    if risk_updates:
        risk = risk.model_copy(update=risk_updates)
    if exit_updates:
        exit_pol = exit_pol.model_copy(update=exit_updates)
    return initial_cash, risk, exit_pol


async def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    now = datetime.now(UTC)
    initial_cash_usd, risk_policy, exit_policy = _build_policies(args)
    args.shadow_db.parent.mkdir(parents=True, exist_ok=True)
    state_store = SQLiteRuntimeStore(args.shadow_db)
    ledger = SQLiteShadowLedger(
        args.shadow_db,
        initial_cash_usd=initial_cash_usd,
        cost_model=ShadowCostModel(),
        exit_policy=exit_policy,
        risk_policy=risk_policy,
    )
    try:
        if args.command == "stop":
            state_store.request_stop(now)
            return {
                "scope": "shadow_only",
                "execution_authorized": False,
                "action": "stop_requested",
                "requested_at": now.isoformat(),
                "runner": state_store.runner_status(),
            }

        raw_basis = getattr(args, "basis", "live_poll")
        basis: Literal["live_poll", "synthetic_fixture"] = (
            "synthetic_fixture" if raw_basis == "synthetic_fixture" else "live_poll"
        )
        if args.command == "status":
            market = _load_latest_market(args.quotes_db, basis)
            snap = ledger.snapshot_sync(market=market)
            return {
                "scope": "shadow_only",
                "execution_authorized": False,
                "runner": state_store.runner_status(),
                "portfolio": snap.model_dump(mode="json"),
            }

        if args.command == "report":
            market = _load_latest_market(args.quotes_db, basis)
            return {
                "scope": "shadow_only",
                "execution_authorized": False,
                "ledger": ledger.report(market=market),
                "runtime": state_store.summary_report(),
            }

        args.memory_db.parent.mkdir(parents=True, exist_ok=True)
        memory = SQLiteThesisMemory(args.memory_db, allow_targets=True)
        quote_bridge_dir: Path | None = getattr(args, "quote_bridge_dir", None)
        if quote_bridge_dir is not None:
            args.quotes_db.parent.mkdir(parents=True, exist_ok=True)
        qstore = (
            QuoteStore(args.quotes_db, readonly=quote_bridge_dir is None)
            if (args.quotes_db.is_file() or quote_bridge_dir is not None)
            else None
        )
        if getattr(args, "poll_news", False):
            args.observer_db.parent.mkdir(parents=True, exist_ok=True)
        ostore = (
            ObserverStore(
                args.observer_db,
                readonly=not getattr(args, "poll_news", False),
            )
            if (args.observer_db.is_file() or getattr(args, "poll_news", False))
            else None
        )
        try:
            runner_ref: list[ShadowRunner] = []

            def market_provider(ts: datetime) -> MarketSnapshot:
                if runner_ref:
                    runner_ref[0]._poll_quote_bridge_sync()
                if qstore is not None:
                    return build_market_snapshot(qstore, ts, basis=basis)
                return MarketSnapshot(
                    snapshot_id=f"market-empty:EURUSD:{ts.isoformat()}",
                    as_of=ts,
                    quotes=(),
                )

            call_model = getattr(args, "call_model", False)
            agent = (
                SinglePassTradingAgent(
                    DeepSeekClient.from_environment(),
                    max_quote_age=timedelta(seconds=getattr(args, "max_quote_age", 2.0)),
                )
                if call_model
                else _AbstainWithoutModelAgent()
            )
            wscn_filter = WallStreetCNFilter()
            runtime = IntradayEventRuntime(
                memory=memory,
                portfolio_state=ledger,
                market_provider=market_provider,
                agent=agent,
                target_validator=DeterministicTargetValidator(),
                risk_engine=DeterministicHardRiskEngine(policy=risk_policy),
                shadow_executor=ledger,
                state_store=state_store,
                event_filter=wscn_filter,
            )

            if args.command == "recover":
                expired = ledger.expire_pending_orders()
                recovered = await runtime.recover_incomplete()
                market = market_provider(datetime.now(UTC))
                exits = ledger.evaluate_protective_exits(market)
                return {
                    "scope": "shadow_only",
                    "execution_authorized": False,
                    "expired_orders": expired,
                    "recovered_events": [r.model_dump(mode="json") for r in recovered],
                    "protective_exits": [f.model_dump(mode="json") for f in exits],
                    "portfolio": ledger.snapshot_sync(market=market).model_dump(mode="json"),
                }

            allow_baseline = getattr(args, "allow_baseline", None)
            runner = ShadowRunner(
                runtime=runtime,
                ledger=ledger,
                observer_store=ostore,
                quote_store=qstore,
                quote_bridge_dir=quote_bridge_dir,
                news_source=WallStreetCNSource() if args.poll_news else None,
                basis=basis,
                poll_news=args.poll_news,
                allow_baseline_revisions=(
                    bool(allow_baseline) if allow_baseline is not None else not args.poll_news
                ),
                news_poll_interval_seconds=args.news_interval,
                event_scan_interval_seconds=args.event_interval,
                quote_interval_seconds=args.quote_interval,
                protection_interval_seconds=args.protection_interval,
            )
            runner_ref.append(runner)
            return await runner.run(duration_seconds=args.duration)
        finally:
            if qstore is not None:
                qstore.close()
            if ostore is not None:
                ostore.close()
    finally:
        ledger.close()
        state_store.close()


def _add_common_db_args(p: argparse.ArgumentParser, *, suppress_defaults: bool = False) -> None:
    default_flag = argparse.SUPPRESS if suppress_defaults else None
    p.add_argument(
        "--shadow-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/shadow-ledger.sqlite"),
        help="SQLite database for shadow ledger, runtime state, and audit log",
    )
    p.add_argument(
        "--memory-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/shadow-memory.sqlite"),
        help="SQLite database for durable event evidence and thesis memory",
    )
    p.add_argument(
        "--observer-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/observer.sqlite"),
        help="SQLite database for captured WallStreetCN revisions",
    )
    p.add_argument(
        "--quotes-db",
        type=Path,
        default=default_flag if suppress_defaults else Path("data/live-quotes.sqlite"),
        help="SQLite database for Demo EUR/USD quote observations",
    )
    p.add_argument(
        "--account-profile",
        choices=("pepperstone_razor_200", "institutional_10k"),
        default=default_flag if suppress_defaults else "pepperstone_razor_200",
        help="Account sizing and risk profile (default: pepperstone_razor_200 = $200 USD, 200x leverage, 0.01..100 lots)",
    )
    p.add_argument(
        "--initial-cash",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override initial cash in USD",
    )
    p.add_argument(
        "--leverage",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override account leverage multiplier (e.g. 200)",
    )
    p.add_argument(
        "--min-lot",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override minimum trade size in standard lots (e.g. 0.01)",
    )
    p.add_argument(
        "--max-lot",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override maximum trade size in standard lots (e.g. 100)",
    )
    p.add_argument(
        "--lot-step",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override lot step increment in standard lots (e.g. 0.01)",
    )
    p.add_argument(
        "--max-position-loss",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override single-position protective stop-loss in USD",
    )
    p.add_argument(
        "--take-profit",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override single-position protective take-profit in USD (0 to disable)",
    )
    p.add_argument(
        "--max-daily-loss",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override daily loss limit in USD",
    )
    p.add_argument(
        "--min-equity",
        type=str,
        default=default_flag if suppress_defaults else None,
        help="Override hard minimum equity floor in USD",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        default=default_flag if suppress_defaults else False,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    _add_common_db_args(parser, suppress_defaults=False)
    sub = parser.add_subparsers(dest="command", required=True)

    start_p = sub.add_parser("start", help="Run bounded automated Shadow loop in a single process")
    _add_common_db_args(start_p, suppress_defaults=True)
    start_p.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Bounded runtime duration in seconds (0 < duration <= 3600)",
    )
    start_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )
    start_p.add_argument("--call-model", action="store_true", help="Opt in to paid DeepSeek calls")
    start_p.add_argument(
        "--poll-news",
        action="store_true",
        help="Poll public WallStreetCN news/calendar endpoints during the run",
    )
    start_p.add_argument(
        "--allow-baseline",
        action="store_true",
        default=None,
        help="Allow initial baseline snapshot revisions to trigger runtime cycles",
    )
    start_p.add_argument(
        "--quote-bridge-dir",
        type=Path,
        default=None,
        help="Optional EventLensQuoteBridge sandbox directory to tail live Demo EUR/USD quotes",
    )
    start_p.add_argument("--news-interval", type=float, default=20.0)
    start_p.add_argument("--event-interval", type=float, default=1.0)
    start_p.add_argument("--quote-interval", type=float, default=0.5)
    start_p.add_argument("--protection-interval", type=float, default=0.5)
    start_p.add_argument("--max-quote-age", type=float, default=2.0)
    start_p.add_argument("--max-holding-seconds", type=int, default=None)
    start_p.add_argument("--kill-switch", action="store_true")

    stop_p = sub.add_parser("stop", help="Signal a running Shadow loop to stop cleanly")
    _add_common_db_args(stop_p, suppress_defaults=True)

    status_p = sub.add_parser("status", help="Inspect current Shadow account and runner status")
    _add_common_db_args(status_p, suppress_defaults=True)
    status_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )

    report_p = sub.add_parser("report", help="Read full Shadow ledger, lifecycle, and audit report")
    _add_common_db_args(report_p, suppress_defaults=True)
    report_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )

    recover_p = sub.add_parser(
        "recover", help="Reconcile incomplete pre-crash decisions and expired orders"
    )
    _add_common_db_args(recover_p, suppress_defaults=True)
    recover_p.add_argument(
        "--basis",
        choices=("live_poll", "synthetic_fixture"),
        default="live_poll",
    )
    recover_p.add_argument("--max-quote-age", type=float, default=2.0)
    recover_p.add_argument("--kill-switch", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "start":
        if not math.isfinite(args.duration) or not 0 < args.duration <= 3600:
            parser.error("--duration must be a finite number in (0, 3600] seconds")
        if args.poll_news and args.news_interval < 10.0:
            parser.error("--news-interval must be at least 10 seconds when polling live news")
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    result = asyncio.run(_run_command(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
