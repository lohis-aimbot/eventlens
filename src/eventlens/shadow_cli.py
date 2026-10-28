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
from .sources.english_tier1 import EURUSDEventFilter, Tier1EnglishSource
from .sources.wallstreetcn import WallStreetCNSource
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
) -> tuple[Decimal, RiskPolicy, ProtectiveExitPolicy, ShadowCostModel]:
    profile = getattr(args, "account_profile", "pepperstone_razor_200")
    kill_switch = bool(getattr(args, "kill_switch", False))
    max_quote_age_ms = int(getattr(args, "max_quote_age", 2.0) * 1000)
    raw_holding = getattr(args, "max_holding_seconds", None)
    explicit_zero_spread = getattr(args, "allow_commission_zero_spread", None)

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
        cost_model = ShadowCostModel.pepperstone_razor()
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
        cost_model = ShadowCostModel()

    if getattr(args, "initial_cash", None) is not None:
        initial_cash = Decimal(str(args.initial_cash))
    risk_updates: dict[str, Any] = {}
    exit_updates: dict[str, Any] = {}
    if explicit_zero_spread is not None:
        allow_zs = bool(explicit_zero_spread)
        risk_updates["allow_commission_bearing_zero_spread"] = allow_zs
        exit_updates["allow_commission_bearing_zero_spread"] = allow_zs
        cost_model = ShadowCostModel(
            name=cost_model.name,
            slippage_bps=cost_model.slippage_bps,
            commission_bps=cost_model.commission_bps,
            allow_commission_bearing_zero_spread=allow_zs,
        )
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
    return initial_cash, risk, exit_pol, cost_model


async def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    now = datetime.now(UTC)
    initial_cash_usd, risk_policy, exit_policy, cost_model = _build_policies(args)
    args.shadow_db.parent.mkdir(parents=True, exist_ok=True)
    state_store = SQLiteRuntimeStore(args.shadow_db)
    ledger = SQLiteShadowLedger(
        args.shadow_db,
        initial_cash_usd=initial_cash_usd,
        cost_model=cost_model,
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
            rep = ledger.report(market=market)
            return {
                "scope": "shadow_only",
                "execution_authorized": False,
                "unclosed_risk": rep["unclosed_risk"],
                "alerts": rep["alerts"],
                "recovery_instructions": rep["recovery_instructions"],
                "runner": state_store.runner_status(),
                "portfolio": snap.model_dump(mode="json"),
            }

        if args.command == "report":
            market = _load_latest_market(args.quotes_db, basis)
            rep = ledger.report(market=market)
            return {
                "scope": "shadow_only",
                "execution_authorized": False,
                "unclosed_risk": rep["unclosed_risk"],
                "alerts": rep["alerts"],
                "recovery_instructions": rep["recovery_instructions"],
                "ledger": rep,
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
                    allow_commission_bearing_zero_spread=(
                        risk_policy.allow_commission_bearing_zero_spread
                    ),
                )
                if call_model
                else _AbstainWithoutModelAgent()
            )
            ev_filter = EURUSDEventFilter()
            runtime = IntradayEventRuntime(
                memory=memory,
                portfolio_state=ledger,
                market_provider=market_provider,
                agent=agent,
                target_validator=DeterministicTargetValidator(),
                risk_engine=DeterministicHardRiskEngine(policy=risk_policy),
                shadow_executor=ledger,
                state_store=state_store,
                event_filter=ev_filter,
            )

            if args.command == "recover":
                if quote_bridge_dir is not None and qstore is not None:
                    bridge_runner = ShadowRunner(
                        runtime=runtime,
                        ledger=ledger,
                        quote_store=qstore,
                        quote_bridge_dir=quote_bridge_dir,
                        basis=basis,
                        poll_news=False,
                    )
                    runner_ref.append(bridge_runner)
                expired = ledger.expire_pending_orders()
                recovered = await runtime.recover_incomplete()
                if getattr(args, "close_on_recover", False):
                    ledger.request_protective_exit_on_stop(
                        now=datetime.now(UTC),
                        reason="protective_exit_on_manual_recover",
                    )
                market = market_provider(datetime.now(UTC))
                exits = list(ledger.evaluate_protective_exits(market))
                wait_seconds = float(getattr(args, "wait_for_quote_seconds", 0.0) or 0.0)
                if wait_seconds > 0.0:
                    deadline = asyncio.get_running_loop().time() + wait_seconds
                    while (
                        Decimal(ledger.report(market=market)["quantity_eur"]) != Decimal("0")
                        and asyncio.get_running_loop().time() < deadline
                    ):
                        await asyncio.sleep(0.25)
                        market = market_provider(datetime.now(UTC))
                        new_exits = ledger.evaluate_protective_exits(market)
                        exits.extend(new_exits)
                rep = ledger.report(market=market)
                return {
                    "scope": "shadow_only",
                    "execution_authorized": False,
                    "unclosed_risk": rep["unclosed_risk"],
                    "alerts": rep["alerts"],
                    "recovery_instructions": rep["recovery_instructions"],
                    "expired_orders": expired,
                    "recovered_events": [r.model_dump(mode="json") for r in recovered],
                    "protective_exits": [f.model_dump(mode="json") for f in exits],
                    "portfolio": ledger.snapshot_sync(market=market).model_dump(mode="json"),
                }

            allow_baseline = getattr(args, "allow_baseline", None)
            news_source_mode = getattr(args, "news_source", "tier1_english")
            active_news_source: Any = None
            supplemental_cal: WallStreetCNSource | None = None
            if args.poll_news:
                if news_source_mode == "wallstreetcn":
                    active_news_source = WallStreetCNSource()
                else:
                    active_news_source = Tier1EnglishSource()
                    if getattr(args, "supplement_calendar_actuals", False):
                        supplemental_cal = WallStreetCNSource()
            runner = ShadowRunner(
                runtime=runtime,
                ledger=ledger,
                observer_store=ostore,
                quote_store=qstore,
                quote_bridge_dir=quote_bridge_dir,
                news_source=active_news_source,
                supplemental_calendar_source=supplemental_cal,
                basis=basis,
                poll_news=args.poll_news,
                allow_baseline_revisions=(
                    bool(allow_baseline) if allow_baseline is not None else not args.poll_news
                ),
                bundle_simultaneous=bool(getattr(args, "bundle_events", True)),
                news_bundle_window_seconds=float(getattr(args, "bundle_window_seconds", 60.0)),
                news_poll_interval_seconds=args.news_interval,
                event_scan_interval_seconds=args.event_interval,
                quote_interval_seconds=args.quote_interval,
                protection_interval_seconds=args.protection_interval,
                exit_buffer_seconds=args.exit_buffer_seconds,
                exit_drain_timeout_seconds=args.exit_drain_timeout,
                strict_window_coverage=not getattr(args, "no_strict_window_coverage", False),
                close_on_stop=getattr(args, "close_on_stop", False),
                confirm_handoff=getattr(args, "confirm_handoff", False),
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
        "--allow-commission-zero-spread",
        action=argparse.BooleanOptionalAction,
        default=default_flag if suppress_defaults else None,
        help="Explicitly allow or forbid commission-bearing zero-spread quotes from verified ctrader_algo sessions",
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
        help="Bounded runtime duration in seconds (0 < duration <= 86400)",
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
        help="Poll live Tier-1 English FX & official central bank feeds (investingLive, ECB, Fed, US BEA, ForexFactory)",
    )
    start_p.add_argument(
        "--news-source",
        choices=("tier1_english", "wallstreetcn"),
        default="tier1_english",
        help="Live news/macro source stack when --poll-news is enabled (default: tier1_english)",
    )
    start_p.add_argument(
        "--supplement-calendar-actuals",
        action="store_true",
        help="Optionally poll structured numeric calendar actuals alongside tier1_english (without Chinese news)",
    )
    start_p.add_argument(
        "--bundle-events",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Bundle simultaneous macro calendar indicators and same-provider burst headlines into single LLM calls",
    )
    start_p.add_argument(
        "--bundle-window-seconds",
        type=float,
        default=60.0,
        help="Time bucket in seconds for grouping simultaneous same-provider news headlines (0 disables news bundling)",
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
    start_p.add_argument(
        "--exit-buffer-seconds",
        type=float,
        default=60.0,
        help="Minimum remaining run/quote-bridge buffer after max_holding_seconds required for new risk entries",
    )
    start_p.add_argument(
        "--exit-drain-timeout",
        type=float,
        default=60.0,
        help="Seconds to keep quote and protection tasks running during shutdown to drain open positions",
    )
    start_p.add_argument(
        "--no-strict-window-coverage",
        action="store_true",
        help="Disable strict remaining run/quote-bridge window check for new entries",
    )
    start_p.add_argument(
        "--close-on-stop",
        action="store_true",
        help="Request protective exit on open shadow position when stop or duration cutoff occurs",
    )
    start_p.add_argument(
        "--confirm-handoff",
        action="store_true",
        help="Explicitly confirm handoff of an open position at shutdown without draining to flat",
    )
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
    recover_p.add_argument(
        "--quote-bridge-dir",
        type=Path,
        default=None,
        help="Optional EventLensQuoteBridge sandbox directory to poll fresh live quotes during recovery",
    )
    recover_p.add_argument(
        "--wait-for-quote-seconds",
        type=float,
        default=0.0,
        help="Seconds to wait for a fresh live quote to execute pending protective exits during recovery",
    )
    recover_p.add_argument(
        "--close-on-recover",
        action="store_true",
        help="Request immediate protective exit on any open shadow position during recovery using a fresh quote",
    )
    recover_p.add_argument("--max-quote-age", type=float, default=2.0)
    recover_p.add_argument("--kill-switch", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "start":
        if not math.isfinite(args.duration) or not 0 < args.duration <= 86400:
            parser.error("--duration must be a finite number in (0, 86400] seconds")
        if args.poll_news and args.news_interval < 10.0:
            parser.error("--news-interval must be at least 10 seconds when polling live news")
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    result = asyncio.run(_run_command(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
