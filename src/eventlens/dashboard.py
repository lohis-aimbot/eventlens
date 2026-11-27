"""Read-only dashboard snapshot aggregator, secret-safe exporter, and local HTTP viewer."""

from __future__ import annotations

import json
import mimetypes
import os
import re
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .contracts import AgentContext, AgentDecision, RiskAssessment
from .trading.agent import SINGLE_PASS_PROMPT_VERSION, SINGLE_PASS_SYSTEM_PROMPT
from .trading.risk import EURUSD_PIP, trading_day_window

DASHBOARD_SCHEMA_VERSION = "dashboard-snapshot-v1"

_SECRET_REDACTION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bsk-[A-Za-z0-9]{16,}\b"), "[REDACTED_API_KEY]"),
    (re.compile(r"projects/[^/\s'\"`]+"), "projects/[REDACTED]"),
    (re.compile(r"\bproject-[a-z0-9-]{6,}\b"), "[REDACTED_PROJECT]"),
    (re.compile(r"\b[0-9A-F]{6}-[0-9A-F]{6}-[0-9A-F]{6}\b"), "[REDACTED_BILLING]"),
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), "[REDACTED_EMAIL]"),
    (re.compile(r"/(?:var/lib|opt/eventlens|etc/eventlens|Users)/[^\s'\"`]+"), "[HOST_PATH]"),
    (
        re.compile(r'("?(?:account_number|ctid|password|pwd|api_key)"?\s*[:=]\s*"?)\d{4,}\b', re.I),
        r"\1[REDACTED]",
    ),
)

_REASON_ZH_MAP: dict[str, str] = {
    "approved": "硬风控全项核验通过",
    "equity_below_minimum_floor": "账户净值低于最低熔断线（当前设定保护）",
    "daily_loss_limit_exceeded": "已触发单日最大亏损保护上限",
    "reversal_blocked_by_daily_loss_limit": "已触发单日亏损上限，禁止反手开仓",
    "daily_close_cutoff_active": "纽约 16:55 收盘防跳空窗口内，暂停新开仓",
    "pre_close_entry_cutoff_active": "纽约收盘前 2 小时禁开新仓窗口（防临近强平短持）",
    "pre_close_entry_cutoff_active_at_fill": "成交时已进入纽约收盘前禁开新仓窗口",
    "kill_switch_active": "人工紧急停机开关已激活",
    "reversal_blocked_by_kill_switch": "人工紧急停机开关已激活，禁止反手",
    "pending_orders_outstanding": "仍有未完结委托单处理中",
    "stale_market_quote": "实时报价过期，拒绝开仓",
    "missing_market_quote": "缺少实时报价，拒绝开仓",
    "spread_too_wide": "当前点差超过硬风控上限",
    "ex_ante_stop_risk_exceeds_budget": "按预设止损测算的单笔风险超出 2% 预算",
    "below_minimum_trade_volume": "2% 风险预算允许的可开手数低于最小 0.01 手",
    "margin_utilization_exceeds_limit": "保证金占用比例超出安全上限",
    "delayed_event_catchup_blocks_new_risk": "新闻或日历发布滞后过久，防追高/追跌拦截",
    "daily_baseline_unconfirmed": "跨日基准净值尚未最终确认，暂停新风险",
    "protective_exit_pending": "存在待执行的保护性平仓指令",
    "unreliable_position_valuation": "当前持仓实时估值异常",
    "missing_active_thesis_backing": "缺少有效绑定档案（旧版规则）",
    "entry_blocked_outside_allowed_session": "非高流动性时段（仅允许 london/overlap 欧/美盘开新仓）",
    "outside_allowed_entry_session": "非欧/美高流动性时段空仓前置跳过（省 LLM 额度）",
    "chase_5m_move_exceeds_limit": "5 分钟同向脉冲超过 4.0p，硬风控拦截追单",
    "extreme_htf_4h_countertrend_blocked": "4 小时主趋势波动 >= 30p，硬风控禁止逆势抄底/摸顶",
}

_REASON_EN_MAP: dict[str, str] = {
    "approved": "All hard-risk controls passed",
    "equity_below_minimum_floor": "Account equity below minimum safety floor",
    "daily_loss_limit_exceeded": "Daily maximum loss limit reached",
    "reversal_blocked_by_daily_loss_limit": "Daily loss limit triggered; reversal blocked",
    "daily_close_cutoff_active": "Within NY 16:55 rollover cutoff; new risk paused",
    "pre_close_entry_cutoff_active": "Within 2h pre-close entry cutoff before NY 16:55 close",
    "pre_close_entry_cutoff_active_at_fill": "Entered pre-close entry cutoff window before fill",
    "kill_switch_active": "Manual emergency kill-switch active",
    "reversal_blocked_by_kill_switch": "Kill-switch active; reversal blocked",
    "pending_orders_outstanding": "Outstanding broker order pending settlement",
    "stale_market_quote": "Market quote stale; new risk rejected",
    "missing_market_quote": "Market quote missing; new risk rejected",
    "spread_too_wide": "Bid-ask spread exceeds hard-risk cap",
    "ex_ante_stop_risk_exceeds_budget": "Ex-ante stop-loss risk exceeds 2% budget",
    "below_minimum_trade_volume": "Risk-budgeted volume below 0.01 lot minimum",
    "margin_utilization_exceeds_limit": "Margin utilization exceeds limit",
    "delayed_event_catchup_blocks_new_risk": "Delayed catch-up wire; chasing blocked",
    "daily_baseline_unconfirmed": "Cross-day equity baseline unconfirmed",
    "protective_exit_pending": "Protective exit order pending",
    "unreliable_position_valuation": "Open position valuation unreliable",
    "missing_active_thesis_backing": "Missing active legacy thesis backing",
    "entry_blocked_outside_allowed_session": "Outside allowed high-liquidity FX sessions (london/overlap)",
    "outside_allowed_entry_session": "Skipped pre-LLM outside allowed FX sessions while flat",
    "chase_5m_move_exceeds_limit": "Same-direction 5m impulse exceeds 4.0p chase cap",
    "extreme_htf_4h_countertrend_blocked": "4h HTF trend move >= 30p; extreme counter-trend blocked",
}

_EXIT_REASON_EN: dict[str, str] = {
    "protective_take_profit": "Take Profit",
    "protective_trailing_stop": "Trailing Stop",
    "protective_stop_loss": "Hard Stop-Loss",
    "protective_max_holding_time": "Max Hold Horizon",
    "max_holding_horizon_exceeded": "Max Hold Horizon",
    "protective_max_holding_horizon": "Max Hold Horizon",
    "protective_daily_close": "NY Close Cutoff",
    "protective_daily_loss_limit": "Daily Loss Guard",
    "protective_max_position_loss": "Max Position USD Stop",
    "ai_close": "AI Active Close",
    "broker_closed": "Broker Closed",
    "broker_deal": "Broker Closed",
    "open": "OPEN",
}

_EXIT_REASON_ZH: dict[str, str] = {
    "protective_take_profit": "止盈触发",
    "protective_trailing_stop": "移动止盈锁利",
    "protective_stop_loss": "硬止损保护",
    "protective_max_holding_time": "持仓超时保护",
    "max_holding_horizon_exceeded": "持仓超时保护",
    "protective_max_holding_horizon": "持仓超时保护",
    "protective_daily_close": "纽约收盘保护平仓",
    "protective_daily_loss_limit": "日内亏损熔断平仓",
    "protective_max_position_loss": "单笔最大美元亏损止损",
    "ai_close": "AI 主动信号平仓",
    "broker_closed": "经纪商触发平仓（止盈/止损）",
    "broker_deal": "经纪商触发平仓",
    "open": "持仓中",
}


def _translate_reason_zh(raw: str) -> str:
    head = raw.split(":", 1)[0]
    if head in _REASON_ZH_MAP:
        return _REASON_ZH_MAP[head]
    if head == "ambiguous_macro_semantics":
        return "宏观数据初/终值或预期基准语义含糊，暂不开新仓"
    if head == "unverified_decision_evidence":
        detail = raw.split(":", 1)[1] if ":" in raw else ""
        if "missing_short_window_coverage" in detail:
            return "报价刚恢复，短线 1m/5m/15m 价格轨迹尚不完整"
        return f"行情核验未通过（{detail}）"
    if head == "stale_or_degraded_source_blocks_new_risk":
        return "新闻采集源状态降级或过期，暂停开新仓"
    return raw


def _translate_reason_en(raw: str) -> str:
    head = raw.split(":", 1)[0]
    if head in _REASON_EN_MAP:
        return _REASON_EN_MAP[head]
    if head == "ambiguous_macro_semantics":
        return "Macro prelim/revision consensus semantics ambiguous"
    if head == "unverified_decision_evidence":
        detail = raw.split(":", 1)[1] if ":" in raw else ""
        if "missing_short_window_coverage" in detail:
            return "Post-gap short-window (1m/5m/15m) quote coverage incomplete"
        return f"Market evidence verification failed ({detail})"
    if head == "stale_or_degraded_source_blocks_new_risk":
        return "News collector stream stale or degraded"
    return raw


def _translate_exit_reason(code: str | None) -> tuple[str, str]:
    if not code:
        return "经纪商触发平仓", "Broker Closed"
    if code in _EXIT_REASON_ZH and code in _EXIT_REASON_EN:
        return _EXIT_REASON_ZH[code], _EXIT_REASON_EN[code]
    if "take_profit" in code:
        return "止盈触发", "Take Profit"
    if "trailing" in code:
        return "移动止盈锁利", "Trailing Stop"
    if "stop_loss" in code:
        return "硬止损保护", "Hard Stop-Loss"
    if "holding" in code or "horizon" in code:
        return "持仓超时保护", "Max Hold Horizon"
    if "daily_close" in code:
        return "纽约收盘保护平仓", "NY Close Cutoff"
    if "ai" in code or "decision" in code:
        return "AI 主动平仓", "AI Active Close"
    return code, code


def _open_ro(path: Path | None) -> sqlite3.Connection | None:
    if path is None or not path.is_file():
        return None
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _table_exists(conn: sqlite3.Connection | None, name: str) -> bool:
    if conn is None:
        return False
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _safe_float(val: Any, default: float | None = None) -> float | None:
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        return None


def _trading_day_str(ts_iso: str | None) -> str:
    dt = _parse_iso(ts_iso)
    if dt is None:
        return datetime.now(UTC).strftime("%Y-%m-%d")
    day_iso, _, _, _ = trading_day_window(dt)
    return day_iso


def _pip_delta(
    *,
    side: str,
    entry_price: float | None,
    exit_price: float | None,
) -> float | None:
    if entry_price is None or exit_price is None or entry_price <= 0 or exit_price <= 0:
        return None
    diff = (exit_price - entry_price) if side == "long" else (entry_price - exit_price)
    return round(diff / float(EURUSD_PIP), 1)


def sanitize_string(text: str, extra_secrets: tuple[str, ...] = ()) -> str:
    cleaned = text
    for pattern, repl in _SECRET_REDACTION_PATTERNS:
        cleaned = pattern.sub(repl, cleaned)
    for secret in extra_secrets:
        if secret and len(secret) >= 5:
            cleaned = cleaned.replace(secret, "[REDACTED]")
    return cleaned


def _sanitize_recursive(obj: Any, extra_secrets: tuple[str, ...]) -> Any:
    if isinstance(obj, str):
        return sanitize_string(obj, extra_secrets=extra_secrets)
    if isinstance(obj, list):
        return [_sanitize_recursive(v, extra_secrets) for v in obj]
    if isinstance(obj, tuple):
        return [_sanitize_recursive(v, extra_secrets) for v in obj]
    if isinstance(obj, dict):
        redacted_keys = {"account_number", "ctid", "api_key", "password", "secret", "token"}
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if str(k).lower() in redacted_keys:
                continue
            out[str(k)] = _sanitize_recursive(v, extra_secrets)
        return out
    return obj


def sanitize_and_verify_snapshot(
    payload: dict[str, Any],
    *,
    indent: int | None = 2,
) -> str:
    """Recursively strip host paths/identifiers and verify zero leaked credentials in JSON output."""
    env_secrets: list[str] = []
    for var in (
        "DEEPSEEK_API_KEY",
        "CTRADER_DEMO_ACCOUNT_NUMBER",
        "CTRADER_CTID",
        "CTRADER_CLIENT_SECRET",
    ):
        val = (os.environ.get(var) or "").strip()
        if len(val) >= 5 and val != "replace_with":
            env_secrets.append(val)
    sanitized = _sanitize_recursive(payload, tuple(env_secrets))
    if indent is None:
        rendered = json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"))
    else:
        rendered = json.dumps(sanitized, ensure_ascii=False, indent=indent)
    for sec in env_secrets:
        if sec in rendered:
            raise ValueError("Refused to export dashboard snapshot containing sensitive env value")
    return rendered


_REVIEW_HEADER_RE = re.compile(
    r"^\[INTERNAL STATEFUL REVIEW\]\s*trigger=([^\s(]+)\s*\(([^)]+)\)", re.I
)
_LEADING_BRACKET_TAGS_RE = re.compile(r"^(?:\[[^\[\]\n]+\]\s*)+")
_EURUSD_RULE_RE = re.compile(r"(EUR/USD\s*(?:mid|bid|ask)?\s*(?:>=|<=|>|<)\s*\d+(?:\.\d+)?)", re.I)


def _extract_headline_and_meta(raw_text: str) -> tuple[str, dict[str, str] | None]:
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    review_meta: dict[str, str] | None = None
    cleaned_lines: list[str] = []
    for ln in lines:
        if ln.startswith("[INTERNAL STATEFUL REVIEW]"):
            m = _REVIEW_HEADER_RE.match(ln)
            if m:
                t_kind = m.group(1).strip()
                t_detail = m.group(2).strip()
                rule_m = _EURUSD_RULE_RE.search(ln)
                review_meta = {
                    "trigger_kind": t_kind,
                    "trigger_rule": rule_m.group(1).strip() if rule_m else t_detail[:64],
                }
            else:
                review_meta = {"trigger_kind": "periodic_review", "trigger_rule": "Stateful Review"}
            continue
        stripped = _LEADING_BRACKET_TAGS_RE.sub("", ln).strip()
        if not stripped:
            continue
        low = stripped.lower()
        if (
            low.startswith("source url:")
            or low.startswith("published:")
            or low.startswith("categories:")
            or stripped in ("Market", "News", "Markets")
        ):
            continue
        cleaned_lines.append(stripped)
    joined = cleaned_lines[0] if cleaned_lines else " ".join(lines)
    if len(joined) < 36 and len(cleaned_lines) > 1:
        joined = f"{joined} — {cleaned_lines[1]}"
    joined = re.sub(r"\s+", " ", joined).strip()
    headline = joined[:160] + ("…" if len(joined) > 160 else "")
    return headline, review_meta


def _load_latest_quote(
    quotes_conn: sqlite3.Connection | None,
    broker_conn: sqlite3.Connection | None,
) -> dict[str, Any]:
    if quotes_conn is not None and _table_exists(quotes_conn, "quote_observations"):
        row = quotes_conn.execute(
            "SELECT quote, received_at FROM quote_observations WHERE symbol IN ('EUR/USD', 'EURUSD') "
            "ORDER BY received_at DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            try:
                q = json.loads(row["quote"])
                bid = _safe_float(q.get("bid"))
                ask = _safe_float(q.get("ask"))
                mid = round((bid + ask) / 2.0, 5) if (bid is not None and ask is not None) else None
                spread = (
                    round((ask - bid) / float(EURUSD_PIP), 2)
                    if (bid is not None and ask is not None)
                    else None
                )
                return {
                    "instrument": "EUR/USD",
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "spread_pips": spread,
                    "received_at": q.get("received_at") or row["received_at"],
                }
            except (json.JSONDecodeError, TypeError):
                pass
    if broker_conn is not None and _table_exists(broker_conn, "demo_broker_events"):
        row = broker_conn.execute(
            "SELECT raw_json, emitted_at FROM demo_broker_events WHERE kind='reconcile_snapshot' "
            "ORDER BY event_rowid DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            try:
                snap = json.loads(row["raw_json"])
                bid = _safe_float(snap.get("bid"))
                ask = _safe_float(snap.get("ask"))
                mid = round((bid + ask) / 2.0, 5) if (bid is not None and ask is not None) else None
                spread = (
                    round((ask - bid) / float(EURUSD_PIP), 2)
                    if (bid is not None and ask is not None)
                    else None
                )
                return {
                    "instrument": "EUR/USD",
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "spread_pips": spread,
                    "received_at": snap.get("emitted_at") or row["emitted_at"],
                }
            except (json.JSONDecodeError, TypeError):
                pass
    return {
        "instrument": "EUR/USD",
        "bid": None,
        "ask": None,
        "mid": None,
        "spread_pips": None,
        "received_at": None,
    }


def _collect_decisions_and_funnel(
    shadow_conn: sqlite3.Connection | None,
    mem_conn: sqlite3.Connection | None,
    *,
    max_decisions: int,
    pinned_decision_ids: set[str] | None = None,
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any] | None,
    dict[str, str],
    dict[str, dict[str, int]],
    dict[str, int],
    str,
]:
    decisions_out: list[dict[str, Any]] = []
    system_prompts: dict[str, str] = {SINGLE_PASS_PROMPT_VERSION: SINGLE_PASS_SYSTEM_PROMPT}
    latest_card: dict[str, Any] | None = None
    llm_prov = (os.environ.get("LLM_PROVIDER") or "auto").strip().lower()
    has_gem = bool((os.environ.get("GEMINI_API_KEY") or "").strip())
    has_ds = bool((os.environ.get("DEEPSEEK_API_KEY") or "").strip())
    gem_mdl = (os.environ.get("GEMINI_MODEL") or "gemini-4-hydrogen").strip() or "gemini-4-hydrogen"
    gem_lvl = (os.environ.get("GEMINI_THINKING_LEVEL") or "low").strip() or "low"
    ds_mdl = (
        os.environ.get("DEEPSEEK_MODEL") or "deepseek-reasoner"
    ).strip() or "deepseek-reasoner"
    if llm_prov == "gemini" or (llm_prov == "auto" and has_gem and not has_ds):
        active_model = f"{gem_mdl} ({gem_lvl})"
    elif llm_prov == "auto" and has_gem and has_ds:
        active_model = f"{gem_mdl} ({gem_lvl}) -> {ds_mdl} fallback"
    else:
        active_model = ds_mdl
    pinned = pinned_decision_ids or set()

    mem_raw_by_event: dict[str, str] = {}
    mem_dec_by_event: dict[str, AgentDecision] = {}
    if mem_conn is not None and _table_exists(mem_conn, "raw_events"):
        for r in mem_conn.execute("SELECT event_id, payload FROM raw_events"):
            try:
                ev_obj = json.loads(r["payload"])
                mem_raw_by_event[str(r["event_id"])] = str(ev_obj.get("raw_text") or "")
            except (json.JSONDecodeError, TypeError):
                pass
    if mem_conn is not None and _table_exists(mem_conn, "decisions"):
        for r in mem_conn.execute(
            "SELECT event_id, payload FROM decisions ORDER BY memory_version DESC"
        ):
            try:
                dec_obj = AgentDecision.model_validate_json(r["payload"])
                mem_dec_by_event[str(r["event_id"])] = dec_obj
                if latest_card is None and dec_obj.market_state_card is not None:
                    latest_card = dec_obj.market_state_card.model_dump(mode="json")
            except (ValueError, TypeError):
                pass

    funnel = {
        "total_events": 0,
        "filtered_out_events": 0,
        "evaluated_events": 0,
        "proposed_trades": 0,
        "approved_trades": 0,
        "risk_rejected": 0,
        "hold_or_ignore": 0,
    }
    daily_funnel: dict[str, dict[str, int]] = {}

    def _day_bucket(day: str) -> dict[str, int]:
        return daily_funnel.setdefault(
            day,
            {
                "events_evaluated": 0,
                "signals_proposed": 0,
                "signals_approved": 0,
                "signals_rejected": 0,
            },
        )

    if shadow_conn is not None and _table_exists(shadow_conn, "runtime_events"):
        all_rows = shadow_conn.execute(
            "SELECT * FROM runtime_events ORDER BY received_at DESC, updated_at DESC"
        ).fetchall()
        non_filtered_added = 0
        for row in all_rows:
            state = str(row["state"])
            ts_str = str(row["received_at"] or row["updated_at"])
            t_day = _trading_day_str(ts_str)
            funnel["total_events"] += 1
            if state == "filtered_out":
                funnel["filtered_out_events"] += 1

            decision: AgentDecision | None = mem_dec_by_event.get(str(row["event_id"]))
            if decision is None and row["decision_json"]:
                try:
                    decision = AgentDecision.model_validate_json(row["decision_json"])
                except ValueError:
                    decision = None

            assessment: RiskAssessment | None = None
            if row["assessment_json"]:
                try:
                    assessment = RiskAssessment.model_validate_json(row["assessment_json"])
                except ValueError:
                    assessment = None

            ctx: AgentContext | None = None
            if row["context_json"]:
                try:
                    ctx = AgentContext.model_validate_json(row["context_json"])
                except ValueError:
                    ctx = None

            if decision is not None:
                funnel["evaluated_events"] += 1
                _day_bucket(t_day)["events_evaluated"] += 1
                if latest_card is None and decision.market_state_card is not None:
                    latest_card = decision.market_state_card.model_dump(mode="json")
                if decision.prompt_version and decision.prompt_version not in system_prompts:
                    system_prompts[decision.prompt_version] = SINGLE_PASS_SYSTEM_PROMPT

                act = decision.portfolio_action
                if act in ("SET_TARGET", "CLOSE"):
                    funnel["proposed_trades"] += 1
                    _day_bucket(t_day)["signals_proposed"] += 1
                else:
                    funnel["hold_or_ignore"] += 1

            if assessment is not None:
                if assessment.outcome == "approve":
                    funnel["approved_trades"] += 1
                    _day_bucket(t_day)["signals_approved"] += 1
                elif assessment.outcome == "reject":
                    funnel["risk_rejected"] += 1
                    _day_bucket(t_day)["signals_rejected"] += 1
            elif state == "risk_rejected":
                funnel["risk_rejected"] += 1
                _day_bucket(t_day)["signals_rejected"] += 1

            row_dec_id = decision.decision_id if decision is not None else row["decision_id"]
            is_pinned = bool(row_dec_id and str(row_dec_id) in pinned)
            if state == "filtered_out" and not is_pinned:
                continue
            if not is_pinned and non_filtered_added >= max_decisions:
                continue
            if state != "filtered_out":
                non_filtered_added += 1

            raw_text = (
                ctx.event.raw_text
                if ctx is not None
                else mem_raw_by_event.get(str(row["event_id"]), "")
            )
            source = ctx.event.source if ctx is not None else "event"
            source_type = ctx.event.source_type if ctx is not None else "news"
            trajectory_out: dict[str, Any] | None = None
            if ctx is not None:
                q_list = ctx.market.quotes
                spread_pips: str | None = None
                if q_list:
                    sp = (q_list[0].ask - q_list[0].bid) / EURUSD_PIP
                    spread_pips = f"{sp:.2f}"
                traj = ctx.market.price_trajectory
                if traj is not None:
                    trajectory_out = {
                        "mid": str(traj.current_mid),
                        "spread_pips": spread_pips,
                        "c1m": str(traj.change_1m_pips)
                        if traj.change_1m_pips is not None
                        else None,
                        "c5m": str(traj.change_5m_pips)
                        if traj.change_5m_pips is not None
                        else None,
                        "c15m": str(traj.change_15m_pips)
                        if traj.change_15m_pips is not None
                        else None,
                        "c30m": str(traj.change_30m_pips)
                        if traj.change_30m_pips is not None
                        else None,
                        "h15m": str(traj.window_15m_high_mid)
                        if traj.window_15m_high_mid is not None
                        else None,
                        "l15m": str(traj.window_15m_low_mid)
                        if traj.window_15m_low_mid is not None
                        else None,
                        "coverage": traj.short_window_coverage_status,
                    }
                elif q_list:
                    mid_px = (q_list[0].bid + q_list[0].ask) / Decimal("2")
                    trajectory_out = {
                        "mid": f"{mid_px:.5f}",
                        "spread_pips": spread_pips,
                        "c1m": None,
                        "c5m": None,
                        "c15m": None,
                        "c30m": None,
                        "h15m": None,
                        "l15m": None,
                        "coverage": "quote_only",
                    }

            target_dir = "flat"
            target_weight = "0"
            if decision is not None and decision.target and decision.target.positions:
                w = decision.target.positions[0].weight
                target_weight = str(w)
                target_dir = "long" if w > 0 else ("short" if w < 0 else "flat")

            reasons_raw = list(assessment.reasons) if assessment is not None else []
            if not reasons_raw and state in (
                "risk_rejected",
                "target_rejected",
                "reasoning_failed",
            ):
                reasons_raw = [str(row["reason"])]

            headline, review_meta = (
                _extract_headline_and_meta(raw_text) if raw_text else (str(row["event_id"]), None)
            )
            decisions_out.append(
                {
                    "decision_id": row_dec_id,
                    "event_id": str(row["event_id"]),
                    "trading_day": t_day,
                    "timestamp": ts_str,
                    "source": source,
                    "source_type": source_type,
                    "headline": headline,
                    "review_meta": review_meta,
                    "raw_text": raw_text[:4000],
                    "state": state,
                    "state_reason": str(row["reason"]),
                    "portfolio_action": decision.portfolio_action if decision is not None else None,
                    "target_direction": target_dir,
                    "target_weight": target_weight,
                    "strategy_hypothesis": decision.strategy_hypothesis
                    if decision is not None
                    else "unspecified",
                    "model_version": decision.model_version if decision is not None else None,
                    "prompt_version": decision.prompt_version
                    if decision is not None
                    else SINGLE_PASS_PROMPT_VERSION,
                    "rationale": decision.rationale if decision is not None else str(row["reason"]),
                    "reasoning_content": decision.reasoning_content
                    if decision is not None
                    else None,
                    "reasoning_tokens": decision.reasoning_tokens if decision is not None else 0,
                    "input_tokens": int(row["input_tokens"] or 0),
                    "output_tokens": int(row["output_tokens"] or 0),
                    "receipt_to_decision_ms": row["receipt_to_decision_ms"],
                    "market_state_card": (
                        decision.market_state_card.model_dump(mode="json")
                        if decision is not None and decision.market_state_card is not None
                        else None
                    ),
                    "trajectory": trajectory_out,
                    "risk": {
                        "outcome": assessment.outcome if assessment is not None else None,
                        "approved_quantity_eur": (
                            str(assessment.approved_quantity_eur)
                            if assessment is not None
                            and assessment.approved_quantity_eur is not None
                            else None
                        ),
                        "reasons": reasons_raw,
                        "reasons_zh": [_translate_reason_zh(r) for r in reasons_raw],
                        "reasons_en": [_translate_reason_en(r) for r in reasons_raw],
                    },
                }
            )
    elif mem_dec_by_event:
        # Fallback if only memory DB is supplied
        for ev_id, dec in list(mem_dec_by_event.items())[:max_decisions]:
            t_day = _trading_day_str(dec.completed_at.isoformat())
            raw_text = mem_raw_by_event.get(ev_id, "")
            headline, review_meta = (
                _extract_headline_and_meta(raw_text) if raw_text else (ev_id, None)
            )
            target_dir = "flat"
            target_weight = "0"
            if dec.target and dec.target.positions:
                w = dec.target.positions[0].weight
                target_weight = str(w)
                target_dir = "long" if w > 0 else ("short" if w < 0 else "flat")
            decisions_out.append(
                {
                    "decision_id": dec.decision_id,
                    "event_id": ev_id,
                    "trading_day": t_day,
                    "timestamp": dec.completed_at.isoformat(),
                    "source": "memory",
                    "source_type": "news",
                    "headline": headline,
                    "review_meta": review_meta,
                    "raw_text": raw_text[:4000],
                    "state": "memory_committed",
                    "state_reason": dec.portfolio_action or "HOLD",
                    "portfolio_action": dec.portfolio_action,
                    "target_direction": target_dir,
                    "target_weight": target_weight,
                    "strategy_hypothesis": dec.strategy_hypothesis,
                    "model_version": dec.model_version,
                    "prompt_version": dec.prompt_version,
                    "rationale": dec.rationale,
                    "reasoning_content": dec.reasoning_content,
                    "reasoning_tokens": dec.reasoning_tokens,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "receipt_to_decision_ms": None,
                    "market_state_card": (
                        dec.market_state_card.model_dump(mode="json")
                        if dec.market_state_card is not None
                        else None
                    ),
                    "trajectory": None,
                    "risk": {
                        "outcome": None,
                        "approved_quantity_eur": None,
                        "reasons": [],
                        "reasons_zh": [],
                        "reasons_en": [],
                    },
                }
            )

    if latest_card is None and mem_conn is not None and _table_exists(mem_conn, "thesis_revisions"):
        row = mem_conn.execute(
            "SELECT payload FROM thesis_revisions WHERE thesis_id='market-state-watch' ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            try:
                th = json.loads(row["payload"])
                narr = str(th.get("narrative") or "")
                bias = "neutral"
                note = narr
                if narr.startswith("[") and "] " in narr:
                    bias, note = narr[1:].split("] ", 1)
                conds = th.get("invalidation_conditions") or []
                latest_card = {
                    "market_bias": bias
                    if bias in ("bullish_eur", "bearish_eur", "neutral", "mixed")
                    else "neutral",
                    "market_state_note": note,
                    "watch_condition": conds[0]
                    if (th.get("status") == "active" and conds)
                    else None,
                    "updated_at": th.get("updated_at"),
                    "source_event_id": (th.get("supporting_event_ids") or [None])[-1],
                }
            except (json.JSONDecodeError, TypeError, ValueError):
                pass

    return decisions_out, latest_card, system_prompts, daily_funnel, funnel, active_model


def _collect_demo_trades_and_account(
    broker_conn: sqlite3.Connection,
    latest_quote: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    intents_by_cid: dict[str, dict[str, Any]] = {}
    close_decision_by_pos: dict[int, tuple[str | None, str]] = {}
    if _table_exists(broker_conn, "demo_broker_intents"):
        for r in broker_conn.execute("SELECT * FROM demo_broker_intents ORDER BY created_at ASC"):
            d = dict(r)
            cid = str(d["client_intent_id"])
            intents_by_cid[cid] = d
            if d.get("intent_type") == "close_position" and d.get("broker_position_id"):
                pos_id = int(d["broker_position_id"])
                reason = "ai_close"
                try:
                    raw_i = json.loads(d.get("intent_json") or "{}")
                    reason = str(raw_i.get("protective_reason") or "").strip() or (
                        "ai_close" if d.get("decision_id") else "broker_closed"
                    )
                except (json.JSONDecodeError, TypeError):
                    pass
                close_decision_by_pos[pos_id] = (d.get("decision_id"), reason)

    trades: list[dict[str, Any]] = []
    open_pos_out: dict[str, Any] | None = None

    deals_by_pos: dict[int, list[dict[str, Any]]] = {}
    if _table_exists(broker_conn, "demo_broker_close_deals"):
        for r in broker_conn.execute(
            "SELECT * FROM demo_broker_close_deals ORDER BY closing_time ASC, closing_deal_id ASC"
        ):
            d = dict(r)
            deals_by_pos.setdefault(int(d["broker_position_id"]), []).append(d)

    if _table_exists(broker_conn, "demo_broker_positions"):
        for r in broker_conn.execute("SELECT * FROM demo_broker_positions ORDER BY opened_at DESC"):
            p = dict(r)
            pos_id = int(p["broker_position_id"])
            side = "long" if str(p["side"]).lower() in ("buy", "long") else "short"
            pos_cid = p.get("client_intent_id")
            intent = intents_by_cid.get(str(pos_cid)) if pos_cid else None
            entry_dec_id = intent.get("decision_id") if intent else None
            entry_px = _safe_float(p.get("entry_price"))
            open_ts = p.get("opened_at")
            open_comm = _safe_float(p.get("open_commission_usd"), 0.0) or 0.0

            if str(p.get("status")) == "open":
                units = int(p.get("volume_units") or 0)
                mid_px = _safe_float(latest_quote.get("mid"))
                unr_pips = _pip_delta(side=side, entry_price=entry_px, exit_price=mid_px)
                unr_usd = (
                    round((unr_pips or 0.0) * 0.1 * (units / 1000.0), 2)
                    if unr_pips is not None
                    else 0.0
                )
                open_pos_out = {
                    "side": side,
                    "side_zh": "做多" if side == "long" else "做空",
                    "side_en": "LONG" if side == "long" else "SHORT",
                    "lots": round(units / 100_000.0, 2),
                    "units_eur": units,
                    "entry_price": entry_px,
                    "stop_loss": _safe_float(p.get("stop_loss")),
                    "take_profit": _safe_float(p.get("take_profit")),
                    "opened_at": open_ts,
                    "decision_id": entry_dec_id,
                    "unrealized_pips": unr_pips,
                    "unrealized_pnl_usd": unr_usd,
                }
                trades.append(
                    {
                        "trade_id": f"pos-{pos_id}",
                        "trading_day": _trading_day_str(open_ts),
                        "status": "open",
                        "side": side,
                        "side_zh": "做多" if side == "long" else "做空",
                        "side_en": "LONG" if side == "long" else "SHORT",
                        "units_eur": units,
                        "lots": round(units / 100_000.0, 2),
                        "entry_price": entry_px,
                        "exit_price": mid_px,
                        "stop_loss": _safe_float(p.get("stop_loss")),
                        "take_profit": _safe_float(p.get("take_profit")),
                        "pips": unr_pips,
                        "net_pnl_usd": unr_usd,
                        "gross_pnl_usd": unr_usd,
                        "commissions_usd": round(abs(open_comm), 2),
                        "opened_at": open_ts,
                        "closed_at": None,
                        "holding_seconds": None,
                        "exit_reason_code": "open",
                        "exit_reason_zh": "持仓中",
                        "exit_reason_en": "OPEN",
                        "decision_id": entry_dec_id,
                        "close_decision_id": None,
                    }
                )
            else:
                pos_deals = deals_by_pos.get(pos_id, [])
                close_dec_id, close_reason = close_decision_by_pos.get(
                    pos_id, (None, "broker_closed")
                )
                if pos_deals:
                    last_deal = pos_deals[-1]
                    close_px = _safe_float(last_deal.get("closing_price")) or _safe_float(
                        p.get("close_price")
                    )
                    closed_ts = str(last_deal.get("closing_time") or p.get("closed_at") or open_ts)
                    gross = sum(
                        _safe_float(d.get("gross_profit_usd"), 0.0) or 0.0 for d in pos_deals
                    )
                    comm = sum(
                        abs(_safe_float(d.get("commissions_usd"), 0.0) or 0.0) for d in pos_deals
                    )
                    swap = sum(_safe_float(d.get("swap_usd"), 0.0) or 0.0 for d in pos_deals)
                    net = sum(_safe_float(d.get("net_profit_usd"), 0.0) or 0.0 for d in pos_deals)
                    total_units = sum(int(d.get("volume_units") or 0) for d in pos_deals) or int(
                        p.get("initial_volume_units") or p.get("volume_units") or 0
                    )
                else:
                    close_px = _safe_float(p.get("close_price"))
                    closed_ts = str(p.get("closed_at") or open_ts)
                    net = _safe_float(p.get("realized_net_pnl_usd"), 0.0) or 0.0
                    comm = abs(_safe_float(p.get("commission_usd"), 0.0) or 0.0)
                    swap = _safe_float(p.get("swap_usd"), 0.0) or 0.0
                    gross = net + comm - swap
                    total_units = int(p.get("initial_volume_units") or p.get("volume_units") or 0)
                dt_open = _parse_iso(open_ts)
                dt_close = _parse_iso(closed_ts)
                hold_s = (
                    max(0, int((dt_close - dt_open).total_seconds()))
                    if (dt_open is not None and dt_close is not None)
                    else None
                )
                pips = _pip_delta(side=side, entry_price=entry_px, exit_price=close_px)
                exit_zh, exit_en = _translate_exit_reason(close_reason)
                trades.append(
                    {
                        "trade_id": f"pos-{pos_id}",
                        "trading_day": _trading_day_str(closed_ts),
                        "status": "closed",
                        "side": side,
                        "side_zh": "做多" if side == "long" else "做空",
                        "side_en": "LONG" if side == "long" else "SHORT",
                        "units_eur": total_units,
                        "lots": round(total_units / 100_000.0, 2),
                        "entry_price": entry_px,
                        "exit_price": close_px,
                        "stop_loss": _safe_float(p.get("stop_loss")),
                        "take_profit": _safe_float(p.get("take_profit")),
                        "pips": pips,
                        "net_pnl_usd": round(net, 2),
                        "gross_pnl_usd": round(gross, 2),
                        "commissions_usd": round(comm, 2),
                        "opened_at": open_ts,
                        "closed_at": closed_ts,
                        "holding_seconds": hold_s,
                        "exit_reason_code": close_reason,
                        "exit_reason_zh": exit_zh,
                        "exit_reason_en": exit_en,
                        "decision_id": entry_dec_id,
                        "close_decision_id": close_dec_id,
                    }
                )

    daily_state_map: dict[str, dict[str, Any]] = {}
    if _table_exists(broker_conn, "demo_daily_state"):
        for r in broker_conn.execute("SELECT * FROM demo_daily_state ORDER BY trading_day ASC"):
            d = dict(r)
            daily_state_map[str(d["trading_day"])] = {
                "baseline_equity_usd": _safe_float(d.get("baseline_equity_usd")),
                "min_equity_seen_usd": _safe_float(d.get("min_equity_seen_usd")),
                "loss_limit_triggered": bool(d.get("loss_limit_triggered")),
                "session_start_utc": d.get("session_start_utc"),
            }

    equity = 0.0
    balance = 0.0
    if _table_exists(broker_conn, "demo_broker_events"):
        snap_row = broker_conn.execute(
            "SELECT raw_json FROM demo_broker_events WHERE kind='reconcile_snapshot' "
            "ORDER BY event_rowid DESC LIMIT 1"
        ).fetchone()
        if snap_row is not None:
            try:
                snap = json.loads(snap_row["raw_json"])
                equity = _safe_float(snap.get("equity_usd"), 0.0) or 0.0
                balance = _safe_float(snap.get("balance_usd"), equity) or equity
            except (json.JSONDecodeError, TypeError):
                pass

    latest_day = max(daily_state_map) if daily_state_map else None
    baseline = daily_state_map[latest_day]["baseline_equity_usd"] if latest_day else None
    if equity <= 0 and baseline is not None:
        equity = baseline
        balance = baseline
    daily_pnl = round(equity - baseline, 2) if (baseline is not None and equity > 0) else 0.0
    realized = round(
        sum(float(t["net_pnl_usd"] or 0.0) for t in trades if t["status"] == "closed"),
        2,
    )
    fees = round(sum(float(t["commissions_usd"] or 0.0) for t in trades), 2)
    account = {
        "mode": "demo",
        "currency": "USD",
        "equity_usd": round(equity, 2),
        "balance_usd": round(balance, 2),
        "daily_pnl_usd": daily_pnl,
        "daily_baseline_equity_usd": round(baseline, 2) if baseline is not None else None,
        "daily_loss_limit_usd": 8.0,
        "daily_loss_remaining_usd": round(max(0.0, 8.0 + min(0.0, daily_pnl)), 2),
        "realized_pnl_usd": realized,
        "unrealized_pnl_usd": open_pos_out["unrealized_pnl_usd"] if open_pos_out else 0.0,
        "total_fees_usd": fees,
        "open_position": open_pos_out,
    }
    return account, trades, daily_state_map


def _collect_shadow_trades_and_account(
    shadow_conn: sqlite3.Connection,
    latest_quote: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    acc_row = (
        shadow_conn.execute("SELECT * FROM shadow_account WHERE id=1").fetchone()
        if _table_exists(shadow_conn, "shadow_account")
        else None
    )
    daily_state_map: dict[str, dict[str, Any]] = {}
    if _table_exists(shadow_conn, "shadow_daily_state"):
        for r in shadow_conn.execute("SELECT * FROM shadow_daily_state ORDER BY trading_day ASC"):
            d = dict(r)
            daily_state_map[str(d["trading_day"])] = {
                "baseline_equity_usd": _safe_float(d.get("baseline_equity_usd")),
                "min_equity_seen_usd": _safe_float(d.get("min_equity_seen_usd")),
                "loss_limit_triggered": bool(d.get("loss_limit_triggered")),
                "session_start_utc": d.get("session_start_utc"),
            }

    decision_by_exec: dict[str, str] = {}
    if _table_exists(shadow_conn, "shadow_receipts"):
        for r in shadow_conn.execute("SELECT execution_id, decision_id FROM shadow_receipts"):
            decision_by_exec[str(r["execution_id"])] = str(r["decision_id"])

    trades: list[dict[str, Any]] = []
    if _table_exists(shadow_conn, "shadow_fills"):
        open_leg: dict[str, Any] | None = None
        for r in shadow_conn.execute("SELECT * FROM shadow_fills ORDER BY filled_at ASC"):
            try:
                f = json.loads(r["fill_json"])
            except (json.JSONDecodeError, TypeError):
                continue
            exec_id = str(r["execution_id"] or "")
            dec_id = decision_by_exec.get(exec_id)
            px = _safe_float(f.get("fill_price"))
            fee = abs(_safe_float(f.get("fee_usd"), 0.0) or 0.0)
            net_realized = _safe_float(f.get("realized_pnl_usd"), 0.0) or 0.0
            delta_q = _safe_float(f.get("delta_quantity"), 0.0) or 0.0
            new_q = _safe_float(f.get("new_quantity"), 0.0) or 0.0
            reason = str(f.get("reason") or "shadow_fill")
            filled_at = str(f.get("filled_at") or r["filled_at"])

            if open_leg is None and new_q != 0.0:
                open_leg = {
                    "side": "long" if new_q > 0 else "short",
                    "units_eur": int(abs(new_q)),
                    "entry_price": px,
                    "opened_at": filled_at,
                    "open_fee": fee,
                    "decision_id": dec_id,
                }
            elif open_leg is not None and new_q == 0.0:
                dt_o = _parse_iso(open_leg["opened_at"])
                dt_c = _parse_iso(filled_at)
                hold_s = max(0, int((dt_c - dt_o).total_seconds())) if (dt_o and dt_c) else None
                side = str(open_leg["side"])
                total_fee = round(float(open_leg["open_fee"]) + fee, 2)
                pips = _pip_delta(side=side, entry_price=open_leg["entry_price"], exit_price=px)
                net_pnl = round(net_realized - total_fee, 2)
                exit_zh, exit_en = _translate_exit_reason(reason)
                trades.append(
                    {
                        "trade_id": str(r["fill_id"]),
                        "trading_day": _trading_day_str(filled_at),
                        "status": "closed",
                        "side": side,
                        "side_zh": "做多" if side == "long" else "做空",
                        "side_en": "LONG" if side == "long" else "SHORT",
                        "units_eur": int(abs(delta_q)) or open_leg["units_eur"],
                        "lots": round((int(abs(delta_q)) or open_leg["units_eur"]) / 100_000.0, 2),
                        "entry_price": open_leg["entry_price"],
                        "exit_price": px,
                        "stop_loss": None,
                        "take_profit": None,
                        "pips": pips,
                        "net_pnl_usd": net_pnl,
                        "gross_pnl_usd": round(net_realized, 2),
                        "commissions_usd": total_fee,
                        "opened_at": open_leg["opened_at"],
                        "closed_at": filled_at,
                        "holding_seconds": hold_s,
                        "exit_reason_code": reason,
                        "exit_reason_zh": exit_zh,
                        "exit_reason_en": exit_en,
                        "decision_id": open_leg["decision_id"],
                        "close_decision_id": dec_id,
                    }
                )
                open_leg = None

    cash = _safe_float(acc_row["cash_usd"], 200.0) if acc_row else 200.0
    realized_acc = _safe_float(acc_row["realized_pnl_usd"], 0.0) if acc_row else 0.0
    fees_acc = _safe_float(acc_row["total_fees_usd"], 0.0) if acc_row else 0.0
    qty = _safe_float(acc_row["quantity_eur"], 0.0) if acc_row else 0.0
    avg_entry = _safe_float(acc_row["avg_entry_price"]) if acc_row else None
    mid_px = _safe_float(latest_quote.get("mid"))
    open_pos_out: dict[str, Any] | None = None
    unr = 0.0
    if acc_row and qty and qty != 0.0:
        side = "long" if qty > 0 else "short"
        unr_pips = _pip_delta(side=side, entry_price=avg_entry, exit_price=mid_px)
        unr = (
            round((unr_pips or 0.0) * 0.1 * (abs(qty) / 1000.0), 2) if unr_pips is not None else 0.0
        )
        open_pos_out = {
            "side": side,
            "side_zh": "做多" if side == "long" else "做空",
            "side_en": "LONG" if side == "long" else "SHORT",
            "lots": round(abs(qty) / 100_000.0, 2),
            "units_eur": int(abs(qty)),
            "entry_price": avg_entry,
            "stop_loss": None,
            "take_profit": None,
            "opened_at": acc_row["opened_at"],
            "decision_id": None,
            "unrealized_pips": unr_pips,
            "unrealized_pnl_usd": unr,
        }
    equity = round((cash or 0.0) + unr, 2)
    latest_day = max(daily_state_map) if daily_state_map else None
    baseline = daily_state_map[latest_day]["baseline_equity_usd"] if latest_day else None
    daily_pnl = round(equity - baseline, 2) if baseline is not None else 0.0
    account = {
        "mode": "shadow",
        "currency": "USD",
        "equity_usd": equity,
        "balance_usd": round(cash or 0.0, 2),
        "daily_pnl_usd": daily_pnl,
        "daily_baseline_equity_usd": round(baseline, 2) if baseline is not None else None,
        "daily_loss_limit_usd": 8.0,
        "daily_loss_remaining_usd": round(max(0.0, 8.0 + min(0.0, daily_pnl)), 2),
        "realized_pnl_usd": round(realized_acc or 0.0, 2),
        "unrealized_pnl_usd": unr,
        "total_fees_usd": round(fees_acc or 0.0, 2),
        "open_position": open_pos_out,
    }
    return account, list(reversed(trades)), daily_state_map


def _build_reports_and_kpis(
    *,
    account: dict[str, Any],
    trades: list[dict[str, Any]],
    daily_state_map: dict[str, dict[str, Any]],
    daily_funnel: dict[str, dict[str, int]],
    funnel: dict[str, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    closed_trades = [t for t in trades if t["status"] == "closed"]
    closed_asc = sorted(
        closed_trades, key=lambda t: str(t.get("closed_at") or t.get("opened_at") or "")
    )

    all_days = sorted(
        set(daily_state_map.keys())
        | set(daily_funnel.keys())
        | {str(t["trading_day"]) for t in trades}
    )
    if not all_days:
        all_days = [_trading_day_str(datetime.now(UTC).isoformat())]

    daily_reports: list[dict[str, Any]] = []
    for day in sorted(all_days, reverse=True):
        day_trades = [t for t in closed_asc if t["trading_day"] == day]
        df = daily_funnel.get(day, {})
        try:
            is_saturday = date.fromisoformat(day).weekday() == 5
        except ValueError:
            is_saturday = False
        if is_saturday and not day_trades and int(df.get("events_evaluated", 0)) == 0:
            continue
        net_usd = round(sum(float(t["net_pnl_usd"] or 0.0) for t in day_trades), 2)
        gross_usd = round(sum(float(t["gross_pnl_usd"] or 0.0) for t in day_trades), 2)
        comm_usd = round(sum(float(t["commissions_usd"] or 0.0) for t in day_trades), 2)
        net_pips = round(sum(float(t["pips"] or 0.0) for t in day_trades), 1)
        wins = [t for t in day_trades if float(t["net_pnl_usd"] or 0.0) > 0]
        losses = [t for t in day_trades if float(t["net_pnl_usd"] or 0.0) < 0]
        ds = daily_state_map.get(day, {})
        base_eq = ds.get("baseline_equity_usd")
        daily_reports.append(
            {
                "trading_day": day,
                "baseline_equity_usd": round(base_eq, 2) if base_eq is not None else None,
                "ending_equity_usd": round(base_eq + net_usd, 2) if base_eq is not None else None,
                "net_pnl_usd": net_usd,
                "gross_pnl_usd": gross_usd,
                "commissions_usd": comm_usd,
                "net_pips": net_pips,
                "trade_count": len(day_trades),
                "win_count": len(wins),
                "loss_count": len(losses),
                "win_rate_pct": round(100.0 * len(wins) / len(day_trades), 1)
                if day_trades
                else None,
                "events_evaluated": int(df.get("events_evaluated", 0)),
                "signals_proposed": int(df.get("signals_proposed", 0)),
                "signals_approved": int(df.get("signals_approved", 0)),
                "signals_rejected": int(df.get("signals_rejected", 0)),
            }
        )

    # Equity curve built chronologically from starting equity + closed deals + live mark
    curve: list[dict[str, Any]] = []
    live_eq = float(account.get("equity_usd") or 0.0)
    total_realized = sum(float(t["net_pnl_usd"] or 0.0) for t in closed_asc)
    start_eq = (
        daily_state_map[min(daily_state_map)]["baseline_equity_usd"]
        if daily_state_map and daily_state_map[min(daily_state_map)].get("baseline_equity_usd")
        else round(live_eq - total_realized, 2)
    )
    if start_eq is None:
        start_eq = live_eq
    first_ts = (
        daily_state_map[min(daily_state_map)].get("session_start_utc")
        if daily_state_map
        else (closed_asc[0].get("opened_at") if closed_asc else datetime.now(UTC).isoformat())
    )
    cum = 0.0
    curve.append(
        {
            "ts": first_ts or datetime.now(UTC).isoformat(),
            "trading_day": min(all_days),
            "equity_usd": round(float(start_eq), 2),
            "cum_pnl_usd": 0.0,
            "label": "初始基准",
            "label_en": "Initial Baseline",
        }
    )
    for t in closed_asc:
        pnl = float(t["net_pnl_usd"] or 0.0)
        cum = round(cum + pnl, 2)
        curve.append(
            {
                "ts": str(t.get("closed_at") or t.get("opened_at")),
                "trading_day": str(t["trading_day"]),
                "equity_usd": round(float(start_eq) + cum, 2),
                "cum_pnl_usd": cum,
                "label": f"{t['side_zh']} {t['exit_reason_zh']} ({pnl:+.2f} USD)",
                "label_en": f"{t.get('side_en', t['side'].upper())} {t.get('exit_reason_en', t['exit_reason_code'])} ({pnl:+.2f} USD)",
            }
        )
    if live_eq > 0 and (len(curve) == 1 or abs(live_eq - curve[-1]["equity_usd"]) >= 0.01):
        curve.append(
            {
                "ts": datetime.now(UTC).isoformat(),
                "trading_day": max(all_days),
                "equity_usd": round(live_eq, 2),
                "cum_pnl_usd": round(live_eq - float(start_eq), 2),
                "label": "最新净值",
                "label_en": "Latest Equity",
            }
        )

    wins_all = [t for t in closed_trades if float(t["net_pnl_usd"] or 0.0) > 0]
    losses_all = [t for t in closed_trades if float(t["net_pnl_usd"] or 0.0) < 0]
    gross_wins = sum(float(t["net_pnl_usd"] or 0.0) for t in wins_all)
    gross_losses = abs(sum(float(t["net_pnl_usd"] or 0.0) for t in losses_all))
    holds = [
        int(t["holding_seconds"]) for t in closed_trades if t.get("holding_seconds") is not None
    ]
    pips_list = [float(t["pips"]) for t in closed_trades if t.get("pips") is not None]
    traded_days = [d for d in daily_reports if d["trade_count"] > 0]
    best_day = max(traded_days, key=lambda d: d["net_pnl_usd"]) if traded_days else None
    worst_day = min(traded_days, key=lambda d: d["net_pnl_usd"]) if traded_days else None
    kpis: dict[str, Any] = {
        "net_pnl_usd": round(total_realized, 2),
        "total_trades": len(closed_trades),
        "win_count": len(wins_all),
        "loss_count": len(losses_all),
        "win_rate_pct": round(100.0 * len(wins_all) / len(closed_trades), 1)
        if closed_trades
        else None,
        "profit_factor": round(gross_wins / gross_losses, 2) if gross_losses > 0 else None,
        "avg_win_usd": round(gross_wins / len(wins_all), 2) if wins_all else None,
        "avg_loss_usd": round(gross_losses / len(losses_all), 2) if losses_all else None,
        "avg_pips": round(sum(pips_list) / len(pips_list), 1) if pips_list else None,
        "avg_holding_seconds": int(sum(holds) / len(holds)) if holds else None,
        "best_day": (
            {"trading_day": best_day["trading_day"], "net_pnl_usd": best_day["net_pnl_usd"]}
            if best_day
            else None
        ),
        "worst_day": (
            {"trading_day": worst_day["trading_day"], "net_pnl_usd": worst_day["net_pnl_usd"]}
            if worst_day
            else None
        ),
        "funnel": funnel,
    }
    return daily_reports, curve, kpis


def build_dashboard_snapshot(
    *,
    shadow_db: Path,
    memory_db: Path | None = None,
    quotes_db: Path | None = None,
    cli_demo_db: Path | None = None,
    evolution_db: Path | None = None,
    active_strategy_path: Path | None = None,
    vm_status: str = "running",
    max_decisions: int = 80,
) -> dict[str, Any]:
    """Assemble a sanitized, read-only dashboard snapshot from local EventLens SQLite stores."""
    from .evolution import load_evolution_dashboard_summary

    eff_cli_db = cli_demo_db or (shadow_db.parent / "cli-demo-broker.sqlite")
    eff_evo_db = (
        evolution_db if evolution_db is not None else (shadow_db.parent / "evolution.sqlite")
    )
    if active_strategy_path is not None:
        eff_active_path = active_strategy_path
    elif (
        shadow_db.parent.name == "data"
        and (shadow_db.parent.parent / "evolution" / "active_strategy.json").is_file()
    ):
        eff_active_path = shadow_db.parent.parent / "evolution" / "active_strategy.json"
    else:
        eff_active_path = shadow_db.parent / "active_strategy.json"
    shadow_conn = _open_ro(shadow_db)
    mem_conn = _open_ro(memory_db)
    quotes_conn = _open_ro(quotes_db)
    broker_conn = _open_ro(eff_cli_db)
    try:
        latest_quote = _load_latest_quote(quotes_conn, broker_conn)

        use_broker = broker_conn is not None and _table_exists(broker_conn, "demo_broker_events")
        has_broker_rows = bool(
            use_broker
            and broker_conn is not None
            and broker_conn.execute("SELECT 1 FROM demo_broker_events LIMIT 1").fetchone()
            is not None
        )
        if has_broker_rows and broker_conn is not None:
            account, trades, daily_state_map = _collect_demo_trades_and_account(
                broker_conn, latest_quote
            )
        elif shadow_conn is not None:
            account, trades, daily_state_map = _collect_shadow_trades_and_account(
                shadow_conn, latest_quote
            )
        else:
            account = {
                "mode": "demo",
                "currency": "USD",
                "equity_usd": 0.0,
                "balance_usd": 0.0,
                "daily_pnl_usd": 0.0,
                "daily_baseline_equity_usd": None,
                "daily_loss_limit_usd": 8.0,
                "daily_loss_remaining_usd": 8.0,
                "realized_pnl_usd": 0.0,
                "unrealized_pnl_usd": 0.0,
                "total_fees_usd": 0.0,
                "open_position": None,
            }
            trades = []
            daily_state_map = {}

        pinned_decision_ids: set[str] = set()
        for t in trades:
            if t.get("decision_id"):
                pinned_decision_ids.add(str(t["decision_id"]))
            if t.get("close_decision_id"):
                pinned_decision_ids.add(str(t["close_decision_id"]))

        decisions, market_state_card, system_prompts, daily_funnel, funnel, detected_model = (
            _collect_decisions_and_funnel(
                shadow_conn,
                mem_conn,
                max_decisions=max_decisions,
                pinned_decision_ids=pinned_decision_ids,
            )
        )

        runner_info: dict[str, Any] = {
            "status": vm_status,
            "heartbeat_at": None,
            "started_at": None,
        }
        if shadow_conn is not None and _table_exists(shadow_conn, "shadow_runner_state"):
            rr = shadow_conn.execute("SELECT * FROM shadow_runner_state WHERE id=1").fetchone()
            if rr is not None:
                runner_info = {
                    "status": str(rr["status"]),
                    "heartbeat_at": rr["heartbeat_at"],
                    "started_at": rr["started_at"],
                    "stopped_at": rr["stopped_at"],
                }
        account["runner"] = runner_info

        daily_reports, equity_curve, kpis = _build_reports_and_kpis(
            account=account,
            trades=trades,
            daily_state_map=daily_state_map,
            daily_funnel=daily_funnel,
            funnel=funnel,
        )
        evo_summary = load_evolution_dashboard_summary(
            evolution_db=eff_evo_db,
            active_strategy_path=eff_active_path,
            shadow_db=shadow_db,
            cli_demo_db=eff_cli_db if eff_cli_db.is_file() else None,
        )
        evo_params = evo_summary.get("active_params") or {}
        evo_ver = str(evo_summary.get("active_version") or "v5.0")
        eff_prompt_ver = (
            SINGLE_PASS_PROMPT_VERSION
            if evo_ver == "v5.0"
            else f"{SINGLE_PASS_PROMPT_VERSION}+{evo_ver}"
        )

        return {
            "schema_version": DASHBOARD_SCHEMA_VERSION,
            "generated_at": datetime.now(UTC).isoformat(),
            "vm_status": vm_status,
            "model_config": {
                "model": detected_model,
                "prompt_version": eff_prompt_ver,
                "evolution_version": evo_ver,
                "risk_profile": {
                    "stop_loss_pips": float(evo_params.get("stop_loss_pips", 14.0)),
                    "take_profit_pips": float(evo_params.get("take_profit_pips", 24.0)),
                    "trailing_activation_pips": evo_params.get("trailing_activation_pips", 10.0),
                    "trailing_distance_pips": evo_params.get("trailing_distance_pips", 6.0),
                    "max_holding_seconds": int(evo_params.get("max_holding_seconds", 14400)),
                    "entry_cutoff_minutes_before_close": 120,
                    "allowed_entry_sessions": ["london", "overlap"],
                    "max_same_direction_chase_5m_pips": float(
                        evo_params.get("max_same_direction_chase_5m_pips", 4.0)
                    ),
                    "max_countertrend_htf_4h_pips": float(
                        evo_params.get("max_countertrend_htf_4h_pips", 30.0)
                    ),
                    "min_equity_usd": 140.0,
                    "max_daily_loss_usd": 8.0,
                    "max_risk_per_trade_pct": 2.0,
                },
            },
            "evolution": evo_summary,
            "account": account,
            "quote": latest_quote,
            "market_state_card": market_state_card
            or {
                "market_bias": "neutral",
                "market_state_note": "等待下一条宏观催化剂事件更新市场状态卡。",
                "watch_condition": None,
                "updated_at": None,
                "source_event_id": None,
            },
            "kpis": kpis,
            "equity_curve": equity_curve,
            "daily_reports": daily_reports,
            "trades": trades,
            "decisions": decisions,
            "system_prompts": system_prompts,
        }
    finally:
        for conn in (shadow_conn, mem_conn, quotes_conn, broker_conn):
            if conn is not None:
                conn.close()


def export_dashboard_snapshot(
    *,
    output_path: Path,
    shadow_db: Path,
    memory_db: Path | None = None,
    quotes_db: Path | None = None,
    cli_demo_db: Path | None = None,
    evolution_db: Path | None = None,
    active_strategy_path: Path | None = None,
    vm_status: str = "running",
    max_decisions: int = 80,
) -> dict[str, Any]:
    snapshot = build_dashboard_snapshot(
        shadow_db=shadow_db,
        memory_db=memory_db,
        quotes_db=quotes_db,
        cli_demo_db=cli_demo_db,
        evolution_db=evolution_db,
        active_strategy_path=active_strategy_path,
        vm_status=vm_status,
        max_decisions=max_decisions,
    )
    rendered = sanitize_and_verify_snapshot(snapshot)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp_path.write_text(rendered, encoding="utf-8")
    tmp_path.replace(output_path)
    return {
        "output_path": str(output_path),
        "bytes": len(rendered.encode("utf-8")),
        "decisions_exported": len(snapshot["decisions"]),
        "trades_exported": len(snapshot["trades"]),
        "daily_reports_exported": len(snapshot["daily_reports"]),
    }


def _resolve_static_dir() -> Path:
    repo_root_candidate = Path(__file__).resolve().parents[2] / "docs" / "dashboard"
    if repo_root_candidate.is_dir():
        return repo_root_candidate
    return Path.cwd() / "docs" / "dashboard"


def serve_dashboard(
    *,
    host: str = "127.0.0.1",
    port: int = 8787,
    shadow_db: Path,
    memory_db: Path | None = None,
    quotes_db: Path | None = None,
    observer_db: Path | None = None,
    cli_demo_db: Path | None = None,
    evolution_db: Path | None = None,
    active_strategy_path: Path | None = None,
    static_dir: Path | None = None,
) -> None:
    """Serve the local Chinese EventLens PWA + live read-only /api/snapshot & /api/chart-lab."""
    doc_dir = (static_dir or _resolve_static_dir()).resolve()
    chart_lab_cache: dict[str, Any] = {"ts": 0.0, "body": b""}

    class _DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:
            req_path = urlparse(self.path).path
            if req_path == "/api/snapshot":
                snapshot = build_dashboard_snapshot(
                    shadow_db=shadow_db,
                    memory_db=memory_db,
                    quotes_db=quotes_db,
                    cli_demo_db=cli_demo_db,
                    evolution_db=evolution_db,
                    active_strategy_path=active_strategy_path,
                )
                body = sanitize_and_verify_snapshot(snapshot).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if req_path == "/api/chart-lab":
                import time

                from .research_lab import build_chart_lab_payload

                now_mono = time.monotonic()
                if not chart_lab_cache["body"] or (now_mono - float(chart_lab_cache["ts"])) > 60.0:
                    payload = build_chart_lab_payload(
                        quotes_db=quotes_db,
                        observer_db=observer_db,
                        shadow_db=shadow_db,
                        memory_db=memory_db,
                        cli_demo_db=cli_demo_db,
                    )
                    for k in ("_bars_m1", "_bars_m5_full", "_bars_m15_full", "_bars_h1_full"):
                        payload.pop(k, None)
                    chart_lab_cache["body"] = sanitize_and_verify_snapshot(
                        payload, indent=None
                    ).encode("utf-8")
                    chart_lab_cache["ts"] = now_mono
                body = chart_lab_cache["body"]
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            rel = (
                "index.html"
                if req_path in ("/", "/dashboard", "/dashboard/")
                else req_path.lstrip("/")
            )
            if rel.startswith("dashboard/"):
                rel = rel[len("dashboard/") :]
            target = (doc_dir / rel).resolve()
            if not str(target).startswith(str(doc_dir)) or not target.is_file():
                target = doc_dir / "index.html"
            if not target.is_file():
                self.send_error(HTTPStatus.NOT_FOUND, "Dashboard assets not found")
                return
            content_type, _ = mimetypes.guess_type(str(target))
            data = target.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type or "application/octet-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    with ThreadingHTTPServer((host, port), _DashboardHandler) as server:
        server.serve_forever()
