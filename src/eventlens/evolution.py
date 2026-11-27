"""Autonomous strategy evolution engine, hot-reload profile, and dual-trigger rollback guard."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import threading
from bisect import bisect_left
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Self
from uuid import uuid4

from pydantic import Field, model_validator

from .contracts import (
    AgentContext,
    Contract,
    Event,
    MarketSnapshot,
    MemorySnapshot,
    PortfolioSnapshot,
    PriceTrajectorySummary,
    Quote,
    Timestamp,
)
from .dashboard import (
    _collect_demo_trades_and_account,
    _collect_shadow_trades_and_account,
    _extract_headline_and_meta,
    _open_ro,
    _parse_iso,
    _safe_float,
    _table_exists,
    sanitize_string,
)
from .deepseek import JSONModelClient, ModelCallError
from .quotes import classify_fx_session
from .research_lab import (
    _extract_target_direction,
    _price_at_timestamp,
    aggregate_quote_bars,
    apply_event_cluster_dedup,
    detect_optimal_entries,
    evaluate_anti_turtle_gate,
    is_tradable_qualified_setup,
    score_decision_net_r,
    simulate_single_exit_policy,
)
from .trading.agent import (
    SINGLE_PASS_PROMPT_VERSION,
    SINGLE_PASS_SYSTEM_PROMPT,
    SinglePassTradingAgent,
)
from .trading.execution import SQLiteShadowLedger
from .trading.risk import DeterministicHardRiskEngine

logger = logging.getLogger(__name__)

EVOLUTION_APP_ID = 0x45564556  # 'EVEV'
MIN_CAPTURE_RATE = 0.40
MIN_SCORE_IMPROVEMENT = 0.08
ROLLBACK_CONSECUTIVE_LOSSES = 2
ROLLBACK_LIVE_REALIZED_R = -2.0
ROLLBACK_REWARD_SCORE_FLOOR = -1.5
ARENA_CLUSTER_TRIGGER = 6
ARENA_HOURS_TRIGGER = 48.0
ARENA_COOLDOWN_HOURS = 6.0
MAX_ARENA_SLICE_EVENTS = 14
DEFAULT_MAX_DAILY_ARENA_LLM_CALLS = 60


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None or dt.utcoffset() is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _fmt_dec(val: Decimal) -> str:
    norm = val.normalize()
    s = format(norm, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


class StrategyProfile(Contract):
    """Bounded, hot-reloadable Strategy v5+ configuration evolved under immutable safety floors."""

    version_id: str = Field(min_length=2, max_length=32)
    parent_version_id: str | None = None
    created_at: Timestamp
    status: Literal["baseline", "promoted", "rolled_back"] = "baseline"
    summary_zh: str = Field(min_length=1, max_length=300)
    dynamic_prompt_rules: tuple[str, ...] = ()

    # Bounded mutable strategy knobs (capital protection floors remain permanently locked elsewhere)
    max_same_direction_chase_5m_pips: Decimal = Field(
        default=Decimal("4.0"), ge=Decimal("3.5"), le=Decimal("6.5")
    )
    prompt_chase_5m_pips: Decimal = Field(
        default=Decimal("3.5"), ge=Decimal("3.0"), le=Decimal("6.0")
    )
    max_countertrend_htf_4h_pips: Decimal = Field(
        default=Decimal("30.0"), ge=Decimal("20.0"), le=Decimal("45.0")
    )
    stop_loss_pips: Decimal = Field(default=Decimal("14.0"), ge=Decimal("10.0"), le=Decimal("18.0"))
    take_profit_pips: Decimal = Field(
        default=Decimal("24.0"), ge=Decimal("15.0"), le=Decimal("36.0")
    )
    trailing_activation_pips: Decimal | None = Field(
        default=Decimal("10.0"), ge=Decimal("8.0"), le=Decimal("18.0")
    )
    trailing_distance_pips: Decimal | None = Field(
        default=Decimal("6.0"), ge=Decimal("4.0"), le=Decimal("10.0")
    )
    max_holding_seconds: int = Field(default=14400, ge=5400, le=21600)

    @model_validator(mode="after")
    def _validate_profile_bounds(self) -> Self:
        if len(self.dynamic_prompt_rules) > 6:
            raise ValueError("dynamic_prompt_rules cannot exceed 6 items")
        sanitized_rules: list[str] = []
        for raw_rule in self.dynamic_prompt_rules:
            cleaned = sanitize_string(str(raw_rule)).strip()
            if not 4 <= len(cleaned) <= 240:
                raise ValueError("Each dynamic prompt rule must be 4..240 chars after sanitization")
            sanitized_rules.append(cleaned)
        object.__setattr__(self, "dynamic_prompt_rules", tuple(sanitized_rules))
        object.__setattr__(
            self, "summary_zh", sanitize_string(self.summary_zh).strip()[:300] or "Strategy profile"
        )
        if self.prompt_chase_5m_pips > self.max_same_direction_chase_5m_pips:
            raise ValueError("prompt_chase_5m_pips cannot exceed max_same_direction_chase_5m_pips")
        if self.take_profit_pips < (self.stop_loss_pips * Decimal("1.3")):
            raise ValueError("take_profit_pips must preserve >= 1.3x SL R:R floor")
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
                "trailing_distance_pips must be strictly below trailing_activation_pips"
            )
        return self

    @classmethod
    def baseline_v5(cls, now: datetime | None = None) -> StrategyProfile:
        ts = _utc(now) if now is not None else datetime.now(UTC)
        return cls(
            version_id="v5.0",
            parent_version_id=None,
            created_at=ts,
            status="baseline",
            summary_zh="Strategy v5.0 基线：Gemini 4 Hydrogen + T2_mid_swing (14p/24p/+10p/6p/4h) + 伦敦/重叠时段与 5m/4h 硬护栏。",
            dynamic_prompt_rules=(),
        )

    def profile_hash(self) -> str:
        payload = {
            "chase_5m": _fmt_dec(self.max_same_direction_chase_5m_pips),
            "prompt_chase_5m": _fmt_dec(self.prompt_chase_5m_pips),
            "htf_4h": _fmt_dec(self.max_countertrend_htf_4h_pips),
            "sl": _fmt_dec(self.stop_loss_pips),
            "tp": _fmt_dec(self.take_profit_pips),
            "trail_act": _fmt_dec(self.trailing_activation_pips)
            if self.trailing_activation_pips is not None
            else None,
            "trail_dist": _fmt_dec(self.trailing_distance_pips)
            if self.trailing_distance_pips is not None
            else None,
            "max_hold_s": self.max_holding_seconds,
            "rules": list(self.dynamic_prompt_rules),
        }
        canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]

    def to_prompt_version(self) -> str:
        if self.version_id == "v5.0":
            return SINGLE_PASS_PROMPT_VERSION
        return f"{SINGLE_PASS_PROMPT_VERSION}+{self.version_id}"

    def render_system_prompt(self) -> str:
        """Render SINGLE_PASS_SYSTEM_PROMPT updated with this profile's bounded knobs & lessons."""
        prompt = SINGLE_PASS_SYSTEM_PROMPT
        htf_s = _fmt_dec(self.max_countertrend_htf_4h_pips)
        htf_num = f"{float(self.max_countertrend_htf_4h_pips):.1f}"
        chase_s = _fmt_dec(self.prompt_chase_5m_pips)
        sl_s = _fmt_dec(self.stop_loss_pips)
        tp_s = _fmt_dec(self.take_profit_pips)
        hold_hours = round(self.max_holding_seconds / 3600.0, 1)
        hold_s = str(int(hold_hours)) if hold_hours.is_integer() else str(hold_hours)

        prompt = prompt.replace(
            "change_240m_pips <= -30.0", f"change_240m_pips <= -{htf_num}"
        ).replace("change_240m_pips >= +30.0", f"change_240m_pips >= +{htf_num}")
        prompt = prompt.replace(
            "15.0 <= |change_240m_pips| < 30.0", f"15.0 <= |change_240m_pips| < {htf_s}"
        )
        prompt = prompt.replace("change_5m_pips >= 3.5p", f"change_5m_pips >= {chase_s}p")

        if self.trailing_activation_pips is not None and self.trailing_distance_pips is not None:
            t_act = _fmt_dec(self.trailing_activation_pips)
            t_dist = _fmt_dec(self.trailing_distance_pips)
            lock_floor = _fmt_dec(self.trailing_activation_pips - self.trailing_distance_pips)
            trail_desc = (
                f"移动锁盈 `+{t_act}p` 激活回撤 `{t_dist}p`（保底锁利 `>= +{lock_floor}p`）"
            )
        else:
            trail_desc = "无移动止盈（直达固定止盈/止损）"

        prompt = prompt.replace(
            "硬止损 `14p`、目标止盈 `24p`、移动锁盈 `+10p` 激活回撤 `6p`（保底锁利 `>= +4p`）、最长持仓 `4 小时`",
            f"硬止损 `{sl_s}p`、目标止盈 `{tp_s}p`、{trail_desc}、最长持仓 `{hold_s} 小时`",
        )
        if self.dynamic_prompt_rules:
            bullets = "\n".join(f"   - {rule}" for rule in self.dynamic_prompt_rules)
            prompt = (
                prompt.rstrip()
                + f"\n\n5. 云端自动进化实战铁律（版本 {self.version_id}，严禁缩头乌龟式踏空）：\n{bullets}\n"
            )
        return prompt


def save_active_strategy_atomic(path: Path, profile: StrategyProfile) -> None:
    """Atomically persist the active StrategyProfile JSON for zero-downtime runtime hot-reload."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(profile.model_dump_json(indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def load_active_strategy_file(path: Path) -> StrategyProfile | None:
    """Safely load and validate an active StrategyProfile file; returns None if absent or invalid."""
    if not path.is_file():
        return None
    try:
        return StrategyProfile.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("invalid_active_strategy_file path=%s err=%s", path, exc)
        return None


class LiveStrategyReloader:
    """Watches active_strategy.json by mtime and hot-reloads bounded knobs safely."""

    def __init__(
        self,
        active_strategy_path: Path,
        *,
        agent: SinglePassTradingAgent | Any | None = None,
        risk_engine: DeterministicHardRiskEngine | None = None,
        ledger: SQLiteShadowLedger | None = None,
        cli_demo_adapter: Any = None,
    ) -> None:
        self.active_strategy_path = active_strategy_path
        self.agent = agent
        self.risk_engine = risk_engine
        self.ledger = ledger
        self.cli_demo_adapter = cli_demo_adapter
        self._last_mtime_ns: int | None = None
        self._current_profile: StrategyProfile | None = None
        self._exit_policy_dirty = False

    @property
    def current_profile(self) -> StrategyProfile | None:
        return self._current_profile

    def reload_if_changed(self, has_open_exposure: bool = False) -> StrategyProfile | None:
        if not self.active_strategy_path.is_file():
            return self._current_profile
        try:
            mtime_ns = self.active_strategy_path.stat().st_mtime_ns
        except OSError:
            return self._current_profile

        if self._last_mtime_ns != mtime_ns:
            self._last_mtime_ns = mtime_ns
            loaded = load_active_strategy_file(self.active_strategy_path)
            if loaded is None:
                return self._current_profile
            self._current_profile = loaded
            self._exit_policy_dirty = True
            self._apply_entry_knobs(loaded)
            logger.info(
                "strategy_profile_hot_reloaded version=%s chase5m=%s htf4h=%s sl=%s tp=%s",
                loaded.version_id,
                loaded.max_same_direction_chase_5m_pips,
                loaded.max_countertrend_htf_4h_pips,
                loaded.stop_loss_pips,
                loaded.take_profit_pips,
            )

        if self._exit_policy_dirty and not has_open_exposure and self._current_profile is not None:
            self._apply_exit_knobs(self._current_profile)
            self._exit_policy_dirty = False

        return self._current_profile

    def _apply_entry_knobs(self, profile: StrategyProfile) -> None:
        if self.agent is not None:
            if hasattr(self.agent, "system_prompt_override"):
                self.agent.system_prompt_override = profile.render_system_prompt()
            if hasattr(self.agent, "prompt_version_override"):
                self.agent.prompt_version_override = profile.to_prompt_version()
            if hasattr(self.agent, "ex_ante_stop_loss_pips"):
                self.agent.ex_ante_stop_loss_pips = profile.stop_loss_pips

        if self.risk_engine is not None:
            # Hard floors (max_risk_per_trade_pct, max_daily_loss_usd, min_equity_usd,
            # allowed_entry_sessions, kill_switch_active) are NEVER touched here.
            self.risk_engine.policy = self.risk_engine.policy.model_copy(
                update={
                    "max_same_direction_chase_5m_pips": profile.max_same_direction_chase_5m_pips,
                    "max_countertrend_htf_4h_pips": profile.max_countertrend_htf_4h_pips,
                    "ex_ante_stop_loss_pips": profile.stop_loss_pips,
                }
            )

    def _apply_exit_knobs(self, profile: StrategyProfile) -> None:
        if self.ledger is not None:
            self.ledger.exit_policy = self.ledger.exit_policy.model_copy(
                update={
                    "stop_loss_pips": profile.stop_loss_pips,
                    "take_profit_pips": profile.take_profit_pips,
                    "trailing_activation_pips": profile.trailing_activation_pips,
                    "trailing_distance_pips": profile.trailing_distance_pips,
                    "max_holding_seconds": profile.max_holding_seconds,
                }
            )
        if self.cli_demo_adapter is not None:
            self.cli_demo_adapter.default_stop_loss_pips = float(profile.stop_loss_pips)
            self.cli_demo_adapter.default_take_profit_pips = float(profile.take_profit_pips)


class SQLiteEvolutionStore:
    """Durable SQLite ledger for matured reward settlement, lineage, rollbacks & blacklist."""

    def __init__(self, path: Path) -> None:
        if str(path) == ":memory:":
            raise ValueError("Use a filesystem path for SQLiteEvolutionStore")
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self._lock, self.db:
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS strategy_versions (
                    version_id TEXT PRIMARY KEY,
                    parent_version_id TEXT,
                    profile_hash TEXT NOT NULL,
                    profile_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    promoted_at TEXT NOT NULL,
                    rolled_back_at TEXT,
                    rollback_reason TEXT,
                    arena_incumbent_score REAL,
                    arena_candidate_score REAL,
                    arena_capture_rate REAL
                );
                CREATE TABLE IF NOT EXISTS scored_events (
                    event_id TEXT PRIMARY KEY,
                    event_ts INTEGER NOT NULL,
                    event_iso TEXT NOT NULL,
                    session TEXT NOT NULL,
                    source TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    strategy_version TEXT NOT NULL,
                    runtime_action TEXT NOT NULL,
                    runtime_direction TEXT,
                    filter_reason TEXT,
                    reward_regime TEXT NOT NULL,
                    reward_score REAL,
                    actual_r_net REAL,
                    mae_penalty REAL NOT NULL DEFAULT 0,
                    missed_r REAL,
                    miss_tier INTEGER,
                    has_qualified_setup INTEGER NOT NULL DEFAULT 0,
                    cluster_id INTEGER,
                    cluster_primary INTEGER NOT NULL DEFAULT 0,
                    context_json TEXT,
                    settled_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_scored_events_ts ON scored_events(event_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_scored_events_ver ON scored_events(strategy_version, cluster_primary);

                CREATE TABLE IF NOT EXISTS evolution_runs (
                    run_id TEXT PRIMARY KEY,
                    run_at TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    active_version_before TEXT NOT NULL,
                    active_version_after TEXT NOT NULL,
                    newly_scored_events INTEGER NOT NULL DEFAULT 0,
                    matured_clusters_total INTEGER NOT NULL DEFAULT 0,
                    cumulative_reward_score REAL NOT NULL DEFAULT 0,
                    capture_rate REAL NOT NULL DEFAULT 1,
                    consecutive_losses INTEGER NOT NULL DEFAULT 0,
                    live_realized_r REAL NOT NULL DEFAULT 0,
                    llm_calls_used INTEGER NOT NULL DEFAULT 0,
                    detail TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_evolution_runs_at ON evolution_runs(run_at DESC);

                CREATE TABLE IF NOT EXISTS blacklisted_hashes (
                    profile_hash TEXT PRIMARY KEY,
                    version_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    blacklisted_at TEXT NOT NULL
                );
                """
            )
            self.db.execute(f"PRAGMA application_id={EVOLUTION_APP_ID}")

    def close(self) -> None:
        with self._lock:
            self.db.close()

    def ensure_baseline(self, now: datetime) -> tuple[StrategyProfile, dict[str, Any]]:
        now_utc = _utc(now)
        with self._lock, self.db:
            active_row = self.db.execute(
                """
                SELECT * FROM strategy_versions
                WHERE status IN ('baseline', 'promoted')
                ORDER BY promoted_at DESC, rowid DESC LIMIT 1
                """
            ).fetchone()
            if active_row is None:
                base = StrategyProfile.baseline_v5(now=now_utc)
                self.db.execute(
                    """
                    INSERT OR REPLACE INTO strategy_versions (
                        version_id, parent_version_id, profile_hash, profile_json, status,
                        created_at, promoted_at, rolled_back_at, rollback_reason,
                        arena_incumbent_score, arena_candidate_score, arena_capture_rate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL)
                    """,
                    (
                        base.version_id,
                        None,
                        base.profile_hash(),
                        base.model_dump_json(),
                        "baseline",
                        now_utc.isoformat(),
                        now_utc.isoformat(),
                    ),
                )
                active_row = self.db.execute(
                    "SELECT * FROM strategy_versions WHERE version_id=?",
                    (base.version_id,),
                ).fetchone()
        assert active_row is not None
        prof = StrategyProfile.model_validate_json(str(active_row["profile_json"]))
        return prof, dict(active_row)

    def get_version(self, version_id: str) -> tuple[StrategyProfile, dict[str, Any]] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT * FROM strategy_versions WHERE version_id=?",
                (version_id,),
            ).fetchone()
            if row is None:
                return None
            return StrategyProfile.model_validate_json(str(row["profile_json"])), dict(row)

    def version_for_timestamp(self, event_iso: str) -> StrategyProfile:
        with self._lock:
            row = self.db.execute(
                """
                SELECT profile_json FROM strategy_versions
                WHERE promoted_at <= ?
                ORDER BY promoted_at DESC, rowid DESC LIMIT 1
                """,
                (event_iso,),
            ).fetchone()
            if row is None:
                row = self.db.execute(
                    "SELECT profile_json FROM strategy_versions ORDER BY rowid ASC LIMIT 1"
                ).fetchone()
            if row is None:
                return StrategyProfile.baseline_v5()
            return StrategyProfile.model_validate_json(str(row["profile_json"]))

    def next_version_id(self) -> str:
        with self._lock:
            rows = self.db.execute("SELECT version_id FROM strategy_versions").fetchall()
            max_minor = 0
            for r in rows:
                m = re.match(r"^v5\.(\d+)$", str(r["version_id"]))
                if m:
                    max_minor = max(max_minor, int(m.group(1)))
            return f"v5.{max_minor + 1}"

    def promote_candidate(
        self,
        candidate: StrategyProfile,
        *,
        now: datetime,
        incumbent_score: float,
        candidate_score: float,
        capture_rate: float,
    ) -> StrategyProfile:
        now_utc = _utc(now)
        new_id = self.next_version_id()
        promoted = candidate.model_copy(
            update={
                "version_id": new_id,
                "created_at": now_utc,
                "status": "promoted",
            }
        )
        with self._lock, self.db:
            self.db.execute(
                """
                INSERT INTO strategy_versions (
                    version_id, parent_version_id, profile_hash, profile_json, status,
                    created_at, promoted_at, arena_incumbent_score, arena_candidate_score,
                    arena_capture_rate
                ) VALUES (?, ?, ?, ?, 'promoted', ?, ?, ?, ?, ?)
                """,
                (
                    promoted.version_id,
                    promoted.parent_version_id,
                    promoted.profile_hash(),
                    promoted.model_dump_json(),
                    now_utc.isoformat(),
                    now_utc.isoformat(),
                    round(incumbent_score, 3),
                    round(candidate_score, 3),
                    round(capture_rate, 3),
                ),
            )
        return promoted

    def rollback_version(
        self,
        *,
        active: StrategyProfile,
        now: datetime,
        reason: str,
    ) -> StrategyProfile:
        now_utc = _utc(now)
        target_parent_id = active.parent_version_id or "v5.0"
        with self._lock, self.db:
            self.db.execute(
                """
                UPDATE strategy_versions
                SET status='rolled_back', rolled_back_at=?, rollback_reason=?
                WHERE version_id=?
                """,
                (now_utc.isoformat(), sanitize_string(reason)[:240], active.version_id),
            )
            self.db.execute(
                """
                INSERT OR REPLACE INTO blacklisted_hashes (profile_hash, version_id, reason, blacklisted_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    active.profile_hash(),
                    active.version_id,
                    sanitize_string(reason)[:240],
                    now_utc.isoformat(),
                ),
            )
            parent_row = self.db.execute(
                "SELECT * FROM strategy_versions WHERE version_id=?",
                (target_parent_id,),
            ).fetchone()
            if parent_row is None:
                parent_row = self.db.execute(
                    "SELECT * FROM strategy_versions WHERE version_id='v5.0'"
                ).fetchone()
            assert parent_row is not None
            new_status: Literal["baseline", "promoted", "rolled_back"] = (
                "baseline" if str(parent_row["version_id"]) == "v5.0" else "promoted"
            )
            parent_prof = StrategyProfile.model_validate_json(
                str(parent_row["profile_json"])
            ).model_copy(update={"status": new_status})
            self.db.execute(
                "UPDATE strategy_versions SET status=?, profile_json=?, promoted_at=? WHERE version_id=?",
                (
                    new_status,
                    parent_prof.model_dump_json(),
                    now_utc.isoformat(),
                    parent_prof.version_id,
                ),
            )
            return parent_prof

    def blacklisted_hashes(self) -> set[str]:
        with self._lock:
            return {
                str(r["profile_hash"])
                for r in self.db.execute("SELECT profile_hash FROM blacklisted_hashes").fetchall()
            }

    def upsert_scored_events(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self._lock, self.db:
            for r in rows:
                self.db.execute(
                    """
                    INSERT OR REPLACE INTO scored_events (
                        event_id, event_ts, event_iso, session, source, headline,
                        strategy_version, runtime_action, runtime_direction, filter_reason,
                        reward_regime, reward_score, actual_r_net, mae_penalty,
                        missed_r, miss_tier, has_qualified_setup,
                        cluster_id, cluster_primary, context_json, settled_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        r["event_id"],
                        int(r["event_ts"]),
                        r["event_iso"],
                        r["session"],
                        r["source"],
                        r["headline"],
                        r["strategy_version"],
                        r["runtime_action"],
                        r.get("runtime_direction"),
                        r.get("filter_reason"),
                        r["reward_regime"],
                        r.get("reward_score"),
                        r.get("actual_r_net"),
                        float(r.get("mae_penalty") or 0.0),
                        r.get("missed_r"),
                        r.get("miss_tier"),
                        1 if r.get("has_qualified_setup") else 0,
                        r.get("cluster_id"),
                        1 if r.get("cluster_primary") else 0,
                        r.get("context_json"),
                        r["settled_at"],
                    ),
                )

    def record_run(self, summary: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute(
                """
                INSERT INTO evolution_runs (
                    run_id, run_at, outcome, active_version_before, active_version_after,
                    newly_scored_events, matured_clusters_total, cumulative_reward_score,
                    capture_rate, consecutive_losses, live_realized_r, llm_calls_used, detail
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid4()),
                    summary["run_at"],
                    summary["outcome"],
                    summary["active_version_before"],
                    summary["active_version_after"],
                    int(summary.get("newly_scored_events") or 0),
                    int(summary.get("matured_clusters_total") or 0),
                    float(summary.get("cumulative_reward_score") or 0.0),
                    float(summary.get("capture_rate") or 1.0),
                    int(summary.get("consecutive_losses") or 0),
                    float(summary.get("live_realized_r") or 0.0),
                    int(summary.get("llm_calls_used") or 0),
                    sanitize_string(str(summary.get("detail") or ""))[:500],
                ),
            )

    def daily_arena_llm_calls(self, now: datetime) -> int:
        day_prefix = _utc(now).strftime("%Y-%m-%d")
        with self._lock:
            row = self.db.execute(
                "SELECT COALESCE(SUM(llm_calls_used), 0) AS s FROM evolution_runs WHERE run_at >= ?",
                (f"{day_prefix}T00:00:00",),
            ).fetchone()
            return int(row["s"]) if row is not None else 0

    def last_arena_time(self) -> datetime | None:
        with self._lock:
            row = self.db.execute(
                """
                SELECT run_at FROM evolution_runs
                WHERE outcome IN ('promoted', 'no_upgrade')
                ORDER BY run_at DESC LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            return _parse_iso(str(row["run_at"]))


def _load_closed_live_trades(
    *,
    shadow_db: Path,
    cli_demo_db: Path | None,
) -> list[dict[str, Any]]:
    eff_cli = cli_demo_db or (shadow_db.parent / "cli-demo-broker.sqlite")
    broker_conn = _open_ro(eff_cli)
    shadow_conn = _open_ro(shadow_db)
    try:
        has_broker = bool(
            broker_conn is not None
            and _table_exists(broker_conn, "demo_broker_events")
            and broker_conn.execute("SELECT 1 FROM demo_broker_events LIMIT 1").fetchone()
            is not None
        )
        if has_broker and broker_conn is not None:
            _, trades, _ = _collect_demo_trades_and_account(broker_conn, {})
        elif shadow_conn is not None:
            _, trades, _ = _collect_shadow_trades_and_account(shadow_conn, {})
        else:
            trades = []
        return [t for t in trades if t.get("status") == "closed"]
    finally:
        if broker_conn is not None:
            broker_conn.close()
        if shadow_conn is not None:
            shadow_conn.close()


def _compute_live_rollback_metrics(
    *,
    closed_trades: list[dict[str, Any]],
    promoted_at: str,
    sl_pips: Decimal,
) -> tuple[int, float, int]:
    """Compute trailing consecutive losses and cumulative realized R since promoted_at."""
    relevant = [
        t
        for t in closed_trades
        if str(t.get("opened_at") or t.get("closed_at") or "") >= promoted_at
    ]
    relevant.sort(key=lambda t: str(t.get("closed_at") or t.get("opened_at") or ""))
    consec_losses = 0
    for t in reversed(relevant):
        pnl = _safe_float(t.get("net_pnl_usd"), 0.0) or 0.0
        if pnl < 0.0:
            consec_losses += 1
        else:
            break

    sl_f = max(0.1, float(sl_pips))
    realized_r = 0.0
    for t in relevant:
        pnl = _safe_float(t.get("net_pnl_usd"), 0.0) or 0.0
        units = float(t.get("units_eur") or 0)
        if units > 0:
            one_r_usd = sl_f * 0.1 * (units / 1000.0)
            realized_r += pnl / max(0.25, one_r_usd)
        else:
            pips = _safe_float(t.get("pips"), 0.0) or 0.0
            realized_r += pips / sl_f
    return consec_losses, round(realized_r, 3), len(relevant)


def _load_runtime_candidates_for_settlement(
    shadow_db: Path,
) -> list[dict[str, Any]]:
    """Read eligible decision or v5-guarded runtime_events strictly without touching blob tables."""
    conn = _open_ro(shadow_db)
    if conn is None or not _table_exists(conn, "runtime_events"):
        if conn is not None:
            conn.close()
        return []
    try:
        rows = conn.execute(
            """
            SELECT event_id, source_reference, state, reason, received_at, updated_at,
                   context_json, decision_id, decision_json
            FROM runtime_events
            WHERE decision_json IS NOT NULL
               OR reason LIKE 'same_direction_5m_chase_exceeded%'
               OR reason LIKE 'countertrend_4h_move_exceeded%'
            ORDER BY received_at ASC
            """
        ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(dict(r))
        return out
    finally:
        conn.close()


def _parse_event_metrics(
    row: dict[str, Any],
) -> tuple[str, datetime | None, str, str, str | None, str]:
    ev_id = str(row["event_id"])
    src = ev_id.split(":", 1)[0] if ":" in ev_id else "unknown"
    reason = sanitize_string(str(row.get("reason") or ""))
    state = str(row.get("state") or "")
    dec_obj: dict[str, Any] = {}
    if row.get("decision_json"):
        try:
            parsed = json.loads(str(row["decision_json"]))
            if isinstance(parsed, dict):
                dec_obj = parsed
        except ValueError:
            pass

    ctx_obj: dict[str, Any] = {}
    if row.get("context_json"):
        try:
            parsed_ctx = json.loads(str(row["context_json"]))
            if isinstance(parsed_ctx, dict):
                ctx_obj = parsed_ctx
        except ValueError:
            pass

    raw_text = str((ctx_obj.get("event") or {}).get("raw_text") or ev_id)
    headline, _ = _extract_headline_and_meta(raw_text)
    headline = sanitize_string(headline)[:180] or ev_id

    ts_raw = (
        dec_obj.get("completed_at")
        or (ctx_obj.get("as_of"))
        or row.get("received_at")
        or row.get("updated_at")
    )
    dt = _parse_iso(str(ts_raw)) if ts_raw else None

    if state in ("risk_rejected", "target_rejected") or reason.startswith(
        (
            "same_direction_5m_chase_exceeded",
            "countertrend_4h_move_exceeded",
            "outside_allowed_entry_session",
        )
    ):
        eff_action = "HOLD"
        eff_dir = None
    else:
        eff_action = str(dec_obj.get("portfolio_action") or "HOLD")
        eff_dir = _extract_target_direction(dec_obj)

    return ev_id, dt, src, headline, eff_dir, eff_action


def _cluster_record_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "t": int(row["event_ts"]),
        "event_id": row["event_id"],
        "score": row["reward_score"],
        "r_net": row["actual_r_net"],
        "mae_penalty": row["mae_penalty"],
        "regime": row["reward_regime"],
        "has_qualified_setup": bool(row["has_qualified_setup"]),
        "missed_r": row.get("missed_r"),
        "miss_tier": row.get("miss_tier"),
    }


def _build_synthetic_context_from_bars(
    *,
    event_id: str,
    ev_dt: datetime,
    headline: str,
    source: str,
    bars_m1: list[dict[str, Any]],
    m1_ts: list[int],
) -> AgentContext | None:
    ev_ts = int(ev_dt.timestamp())
    bar = _price_at_timestamp(bars_m1, m1_ts, ev_ts, tolerance_sec=600)
    if bar is None:
        return None
    mid = Decimal(str(bar["c"]))
    sp_pips = max(Decimal("0.1"), Decimal(str(bar.get("sa") or "0.2")))
    half = (sp_pips * Decimal("0.0001")) / Decimal("2")
    bid = (mid - half).quantize(Decimal("0.00001"))
    ask = max(bid, (mid + half).quantize(Decimal("0.00001")))
    q_ts = datetime.fromtimestamp(int(bar["t"]), tz=UTC)
    as_of = max(ev_dt, q_ts)

    def _chg(min_ago: int) -> Decimal | None:
        ref = _price_at_timestamp(bars_m1, m1_ts, ev_ts - min_ago * 60, tolerance_sec=900)
        if ref is None:
            return None
        diff = (Decimal(str(bar["c"])) - Decimal(str(ref["c"]))) / Decimal("0.0001")
        return diff.quantize(Decimal("0.1"))

    quote = Quote(
        instrument="EUR/USD",
        source_at=q_ts,
        bridge_at=q_ts,
        received_at=q_ts,
        available_at=q_ts,
        source_provider="ctrader_algo",
        basis="live_poll",
        bid=bid,
        ask=ask,
        commission=Decimal("3"),
        commission_type="UsdPerOneLot",
    )
    traj = PriceTrajectorySummary(
        instrument="EUR/USD",
        as_of=as_of,
        current_mid=mid,
        change_1m_pips=_chg(1),
        change_5m_pips=_chg(5),
        change_15m_pips=_chg(15),
        change_30m_pips=_chg(30),
        change_60m_pips=_chg(60),
        change_240m_pips=_chg(240),
        fx_session=classify_fx_session(as_of),
    )
    snap = MarketSnapshot(
        snapshot_id=f"evo-snap:{event_id}:{ev_ts}",
        as_of=as_of,
        quotes=(quote,),
        price_trajectory=traj,
    )
    ev = Event(
        event_id=event_id,
        source=source,
        source_type="news",
        source_reference=f"https://eventlens.local/evolution/{event_id}",
        source_timestamp=ev_dt,
        received_timestamp=ev_dt,
        raw_text=headline,
    )
    return AgentContext(
        event=ev,
        memory=MemorySnapshot(memory_version=0, as_of=as_of, theses=()),
        portfolio=PortfolioSnapshot(
            snapshot_id=f"evo-pf:{ev_ts}",
            as_of=as_of,
            base_currency="USD",
            equity=Decimal("165.00"),
        ),
        market=snap,
        as_of=as_of,
    )


def _strip_reasoning_from_context_json(raw_json: str | None) -> str | None:
    if not raw_json or len(raw_json) > 64_000:
        return None
    try:
        AgentContext.model_validate_json(raw_json)
        return sanitize_string(raw_json)
    except Exception:
        return None


_META_OPTIMIZER_SYSTEM_PROMPT = """你是 EventLens 量化策略自动进化器（Meta-Optimizer）。
你的职责是根据当前线上策略参数、近期「踏空重罚事件（missed_qualified_setup）」和「亏损实盘单」，提议 1~2 个更强但安全守界的候选策略配置，在严禁缩头乌龟漏单的前提下提升样本净期望得分（Net-R）。

你只被允许修改以下受严格边界限制的字段（本金保命红线如 2% 单笔风险、8 USD 日亏熔断、150 USD 最低净值、london/overlap 时段门控已在底层永久锁死，绝不能也不必触碰）：
- `summary_zh`: 1 句简明中文说明本候选策略改进目的（<=120字）
- `dynamic_prompt_rules`: 字符串数组，最多 6 条简短实战规则（每条 8~150 字），例如针对误放行噪音或踏空真催化的精准判据
- `max_same_direction_chase_5m_pips`: 小数，严格限制在 [3.5, 6.5]
- `prompt_chase_5m_pips`: 小数，严格限制在 [3.0, 6.0]，且 <= max_same_direction_chase_5m_pips
- `max_countertrend_htf_4h_pips`: 小数，严格限制在 [20.0, 45.0]
- `stop_loss_pips`: 小数，严格限制在 [10.0, 18.0]
- `take_profit_pips`: 小数，严格限制在 [15.0, 36.0]，且 >= stop_loss_pips * 1.3
- `trailing_activation_pips`: 小数，严格限制在 [8.0, 18.0]（或 null 表示不启用移动止盈，此时 trailing_distance_pips 也必须为 null）
- `trailing_distance_pips`: 小数，严格限制在 [4.0, 10.0]，且 < trailing_activation_pips
- `max_holding_seconds`: 整数，严格限制在 [5400, 21600]（如 14400 = 4h）

反缩头乌龟硬纪律：
- 空仓漏掉 +0.5R 以上或 T2/T3 行情会按 100%~125% 全额倒扣分，且合格机会捕获率低于 40% 会一票否决淘汰。因此如果近期出现多起踏空（如 5m/4h 门禁过死或 Prompt 过于保守），应适度放宽对应阈值或写明何时应果断介入。
- 严格禁止输出与 blacklisted_hashes 重复的配置。
仅输出合法 JSON：{"candidates": [ ... ]}（最多 2 个对象，不要 Markdown 代码块）。"""


def _check_v5_candidate_risk_pass(
    *,
    profile: StrategyProfile,
    ctx: AgentContext,
    side: str,
) -> bool:
    traj = ctx.market.price_trajectory
    if traj is None:
        return True
    sess = traj.fx_session or classify_fx_session(ctx.as_of)
    if sess not in ("london", "overlap"):
        return False
    if traj.change_5m_pips is not None:
        signed_5m = traj.change_5m_pips if side == "long" else -traj.change_5m_pips
        if signed_5m > profile.max_same_direction_chase_5m_pips:
            return False
    if traj.change_240m_pips is not None:
        counter_4h = -traj.change_240m_pips if side == "long" else traj.change_240m_pips
        if counter_4h >= profile.max_countertrend_htf_4h_pips:
            return False
    return True


async def _evaluate_profile_on_slice(
    *,
    profile: StrategyProfile,
    slice_items: list[dict[str, Any]],
    bars_m1: list[dict[str, Any]],
    m1_ts: list[int],
    algo_markers: list[dict[str, Any]],
    llm_client: JSONModelClient,
) -> dict[str, Any]:
    marker_ts = [int(m["t"]) for m in algo_markers]
    records: list[dict[str, Any]] = []
    llm_calls = 0

    for item in slice_items:
        ctx: AgentContext = item["context"]
        ev_ts = int(item["event_ts"])
        sess = str(item["session"])

        def _replay_clock(anchor: datetime = ctx.as_of) -> datetime:
            return anchor + timedelta(milliseconds=10)

        agent = SinglePassTradingAgent(
            llm_client,
            max_quote_age=timedelta(seconds=30),
            allow_commission_bearing_zero_spread=True,
            max_risk_per_trade_pct=Decimal("0.02"),
            ex_ante_stop_loss_pips=profile.stop_loss_pips,
            system_prompt_override=profile.render_system_prompt(),
            prompt_version_override=profile.to_prompt_version(),
            clock=_replay_clock,
        )
        eff_action = "HOLD"
        eff_side: str | None = None
        try:
            llm_calls += 1
            run_res = await agent.decide_with_usage(ctx)
            prop = run_res.proposal
            eff_action = prop.portfolio_action
            eff_side = prop.target_direction if prop.target_direction in ("long", "short") else None
            if eff_action == "SET_TARGET" and eff_side in ("long", "short"):
                if not _check_v5_candidate_risk_pass(profile=profile, ctx=ctx, side=eff_side):
                    eff_action = "HOLD"
                    eff_side = None
        except (ModelCallError, Exception) as exc:
            logger.warning(
                "arena_eval_call_failed ver=%s ev=%s err=%s",
                profile.version_id,
                item["event_id"],
                exc,
            )
            eff_action = "API_ERROR"
            eff_side = None

        matched_tier: int | None = None
        if marker_ts:
            pos = bisect_left(marker_ts, ev_ts - 900)
            while pos < len(marker_ts) and marker_ts[pos] <= ev_ts + 900:
                mt = int(algo_markers[pos].get("tier") or 1)
                matched_tier = mt if matched_tier is None else max(matched_tier, mt)
                pos += 1

        sim_long = simulate_single_exit_policy(
            bars_m1,
            ev_ts,
            "long",
            sl_pips=float(profile.stop_loss_pips),
            tp_pips=float(profile.take_profit_pips),
            trail_act_pips=(
                float(profile.trailing_activation_pips)
                if profile.trailing_activation_pips is not None
                else None
            ),
            trail_dist_pips=(
                float(profile.trailing_distance_pips)
                if profile.trailing_distance_pips is not None
                else None
            ),
            max_hold_min=max(30, profile.max_holding_seconds // 60),
            m1_ts=m1_ts,
        )
        sim_short = simulate_single_exit_policy(
            bars_m1,
            ev_ts,
            "short",
            sl_pips=float(profile.stop_loss_pips),
            tp_pips=float(profile.take_profit_pips),
            trail_act_pips=(
                float(profile.trailing_activation_pips)
                if profile.trailing_activation_pips is not None
                else None
            ),
            trail_dist_pips=(
                float(profile.trailing_distance_pips)
                if profile.trailing_distance_pips is not None
                else None
            ),
            max_hold_min=max(30, profile.max_holding_seconds // 60),
            m1_ts=m1_ts,
        )
        qual_ok, qual_tier = is_tradable_qualified_setup(
            sess, sim_long, sim_short, matched_tier=matched_tier
        )
        sc = score_decision_net_r(
            action=eff_action,
            side=eff_side,
            long_eval=sim_long,
            short_eval=sim_short,
            has_qualified_setup=qual_ok,
            setup_tier=qual_tier,
        )
        records.append(
            {
                "t": ev_ts,
                "event_id": item["event_id"],
                "score": sc["score"],
                "r_net": sc["r_net"],
                "mae_penalty": sc["mae_penalty"],
                "regime": sc["regime"],
                "has_qualified_setup": qual_ok,
            }
        )

    deduped = apply_event_cluster_dedup(records, cluster_window_sec=300)
    gate = evaluate_anti_turtle_gate(deduped, min_capture_rate=MIN_CAPTURE_RATE)
    primary_valid = [r for r in deduped if r.get("cluster_primary") and r.get("score") is not None]
    total_score = round(sum(float(r["score"]) for r in primary_valid), 3)
    mean_score = round(total_score / len(primary_valid), 3) if primary_valid else 0.0
    return {
        "profile": profile,
        "total_score": total_score,
        "mean_score": mean_score,
        "scored_clusters": len(primary_valid),
        "anti_turtle": gate,
        "llm_calls": llm_calls,
    }


def _select_arena_slice(
    *,
    settled_rows: list[dict[str, Any]],
    raw_by_id: dict[str, dict[str, Any]],
    bars_m1: list[dict[str, Any]],
    m1_ts: list[int],
    max_items: int = MAX_ARENA_SLICE_EVENTS,
) -> list[dict[str, Any]]:
    primary = [r for r in settled_rows if r.get("cluster_primary")]
    misses = sorted(
        [r for r in primary if r.get("reward_regime") == "missed_qualified_setup"],
        key=lambda r: float(r.get("reward_score") or 0.0),
    )
    losses = sorted(
        [
            r
            for r in primary
            if r.get("reward_regime") == "active_entry"
            and float(r.get("actual_r_net") or 0.0) < 0.0
        ],
        key=lambda r: float(r.get("actual_r_net") or 0.0),
    )
    others = sorted(
        [r for r in primary if r not in misses and r not in losses],
        key=lambda r: int(r["event_ts"]),
        reverse=True,
    )
    chosen_ordered: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for bucket in (misses[:6], losses[:4], others):
        for r in bucket:
            if len(chosen_ordered) >= max_items:
                break
            eid = str(r["event_id"])
            if eid in seen_ids:
                continue
            raw_ctx = r.get("context_json") or (raw_by_id.get(eid) or {}).get("context_json")
            ctx: AgentContext | None = None
            if raw_ctx:
                try:
                    ctx = AgentContext.model_validate_json(str(raw_ctx))
                except Exception:
                    ctx = None
            if ctx is None:
                ev_dt = _parse_iso(str(r["event_iso"]))
                if ev_dt is None:
                    continue
                ctx = _build_synthetic_context_from_bars(
                    event_id=eid,
                    ev_dt=ev_dt,
                    headline=str(r["headline"]),
                    source=str(r["source"]),
                    bars_m1=bars_m1,
                    m1_ts=m1_ts,
                )
            if ctx is None:
                continue
            seen_ids.add(eid)
            chosen_ordered.append(
                {
                    "event_id": eid,
                    "event_ts": int(r["event_ts"]),
                    "session": str(r["session"]),
                    "context": ctx,
                }
            )
    chosen_ordered.sort(key=lambda x: int(x["event_ts"]))
    return chosen_ordered


async def _propose_candidate_profiles(
    *,
    llm_client: JSONModelClient,
    incumbent: StrategyProfile,
    settled_rows: list[dict[str, Any]],
    blacklist: set[str],
    now: datetime,
) -> tuple[list[StrategyProfile], int]:
    primary = [r for r in settled_rows if r.get("cluster_primary")]
    severe_misses = sorted(
        [r for r in primary if r.get("reward_regime") == "missed_qualified_setup"],
        key=lambda r: float(r.get("reward_score") or 0.0),
    )[:6]
    losing_entries = sorted(
        [
            r
            for r in primary
            if r.get("reward_regime") == "active_entry"
            and float(r.get("actual_r_net") or 0.0) < 0.0
        ],
        key=lambda r: float(r.get("actual_r_net") or 0.0),
    )[:4]

    payload = {
        "incumbent_profile": {
            "version_id": incumbent.version_id,
            "max_same_direction_chase_5m_pips": _fmt_dec(
                incumbent.max_same_direction_chase_5m_pips
            ),
            "prompt_chase_5m_pips": _fmt_dec(incumbent.prompt_chase_5m_pips),
            "max_countertrend_htf_4h_pips": _fmt_dec(incumbent.max_countertrend_htf_4h_pips),
            "stop_loss_pips": _fmt_dec(incumbent.stop_loss_pips),
            "take_profit_pips": _fmt_dec(incumbent.take_profit_pips),
            "trailing_activation_pips": _fmt_dec(incumbent.trailing_activation_pips)
            if incumbent.trailing_activation_pips is not None
            else None,
            "trailing_distance_pips": _fmt_dec(incumbent.trailing_distance_pips)
            if incumbent.trailing_distance_pips is not None
            else None,
            "max_holding_seconds": incumbent.max_holding_seconds,
            "dynamic_prompt_rules": list(incumbent.dynamic_prompt_rules),
        },
        "blacklisted_hashes": sorted(blacklist),
        "missed_opportunities_sample": [
            {
                "headline": r["headline"],
                "session": r["session"],
                "action": r["runtime_action"],
                "filter_reason": r.get("filter_reason"),
                "missed_r": r.get("missed_r"),
                "miss_tier": r.get("miss_tier"),
                "penalty_score": r.get("reward_score"),
            }
            for r in severe_misses
        ],
        "losing_trades_sample": [
            {
                "headline": r["headline"],
                "session": r["session"],
                "direction": r.get("runtime_direction"),
                "actual_r_net": r.get("actual_r_net"),
                "score": r.get("reward_score"),
            }
            for r in losing_entries
        ],
    }

    try:
        reply = await llm_client.complete_json(
            system=_META_OPTIMIZER_SYSTEM_PROMPT,
            user=json.dumps(payload, ensure_ascii=False),
        )
    except (ModelCallError, Exception) as exc:
        logger.warning("meta_optimizer_proposal_failed err=%s", exc)
        return [], 1

    try:
        parsed = json.loads(reply.content)
        raw_cands = parsed.get("candidates") if isinstance(parsed, dict) else []
        if not isinstance(raw_cands, list):
            raw_cands = []
    except ValueError:
        return [], 1

    inc_hash = incumbent.profile_hash()
    valid_cands: list[StrategyProfile] = []
    seen_hashes: set[str] = {inc_hash} | set(blacklist)
    for idx, rc in enumerate(raw_cands[:3]):
        if not isinstance(rc, dict):
            continue
        try:
            cand = StrategyProfile(
                version_id=f"cand-{idx + 1}",
                parent_version_id=incumbent.version_id,
                created_at=now,
                status="promoted",
                summary_zh=str(rc.get("summary_zh") or "自动进化候选策略"),
                dynamic_prompt_rules=tuple(rc.get("dynamic_prompt_rules") or ()),
                max_same_direction_chase_5m_pips=Decimal(
                    str(
                        rc.get(
                            "max_same_direction_chase_5m_pips",
                            incumbent.max_same_direction_chase_5m_pips,
                        )
                    )
                ),
                prompt_chase_5m_pips=Decimal(
                    str(rc.get("prompt_chase_5m_pips", incumbent.prompt_chase_5m_pips))
                ),
                max_countertrend_htf_4h_pips=Decimal(
                    str(
                        rc.get(
                            "max_countertrend_htf_4h_pips", incumbent.max_countertrend_htf_4h_pips
                        )
                    )
                ),
                stop_loss_pips=Decimal(str(rc.get("stop_loss_pips", incumbent.stop_loss_pips))),
                take_profit_pips=Decimal(
                    str(rc.get("take_profit_pips", incumbent.take_profit_pips))
                ),
                trailing_activation_pips=(
                    Decimal(str(rc["trailing_activation_pips"]))
                    if rc.get("trailing_activation_pips") is not None
                    else (
                        None
                        if "trailing_activation_pips" in rc
                        and rc.get("trailing_activation_pips") is None
                        else incumbent.trailing_activation_pips
                    )
                ),
                trailing_distance_pips=(
                    Decimal(str(rc["trailing_distance_pips"]))
                    if rc.get("trailing_distance_pips") is not None
                    else (
                        None
                        if "trailing_distance_pips" in rc
                        and rc.get("trailing_distance_pips") is None
                        else incumbent.trailing_distance_pips
                    )
                ),
                max_holding_seconds=int(
                    rc.get("max_holding_seconds", incumbent.max_holding_seconds)
                ),
            )
        except Exception as exc:
            logger.info("rejected_invalid_candidate idx=%s reason=%s", idx, exc)
            continue
        h = cand.profile_hash()
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        valid_cands.append(cand)
        if len(valid_cands) >= 2:
            break

    return valid_cands, 1


async def run_evolution_tick(
    *,
    evolution_db: Path,
    active_strategy_path: Path,
    shadow_db: Path,
    quotes_db: Path,
    observer_db: Path | None = None,
    cli_demo_db: Path | None = None,
    llm_client: JSONModelClient | None = None,
    force_arena: bool = False,
    maturity_seconds: int = 14400,
    max_daily_arena_llm_calls: int = DEFAULT_MAX_DAILY_ARENA_LLM_CALLS,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Run one causal reward settlement, dual-trigger rollback check, and 6-cluster/48h Arena tick."""
    del (
        observer_db
    )  # Query only lightweight runtime_events & live-quotes; never touch observer_payloads.
    now = _utc(clock())
    now_iso = now.isoformat()
    now_ts = int(now.timestamp())
    store = SQLiteEvolutionStore(evolution_db)
    try:
        active, active_meta = store.ensure_baseline(now)
        active_ver_before = active.version_id
        if not active_strategy_path.is_file():
            save_active_strategy_atomic(active_strategy_path, active)

        # Phase 2: Causal settlement of matured london/overlap events within trailing 21d quotes
        bars_m1: list[dict[str, Any]] = []
        bars_m5: list[dict[str, Any]] = []
        algo_markers: list[dict[str, Any]] = []
        if quotes_db.is_file():
            agg = aggregate_quote_bars(quotes_db, symbol="EURUSD", basis="live_poll", days=21)
            bars_m1 = agg["bars_m1"]
            bars_m5 = agg["bars_m5"]
            algo_markers = detect_optimal_entries(bars_m1, bars_m5)
        m1_ts = [int(b["t"]) for b in bars_m1]
        marker_ts = [int(m["t"]) for m in algo_markers]
        last_bar_ts = m1_ts[-1] if m1_ts else 0

        raw_rows = _load_runtime_candidates_for_settlement(shadow_db)
        raw_by_id = {str(r["event_id"]): r for r in raw_rows}
        min_future_bars_req = max(7200, min(int(maturity_seconds), 14400))

        existing_scored: dict[str, dict[str, Any]] = {
            str(r["event_id"]): dict(r)
            for r in store.db.execute("SELECT * FROM scored_events").fetchall()
        }

        staged_for_dedup: list[dict[str, Any]] = []
        newly_settled_count = 0

        if bars_m1:
            # First retain existing settled events not overridden
            seen_events: set[str] = set()
            for r in raw_rows:
                ev_id, ev_dt, src, headline, eff_dir, eff_action = _parse_event_metrics(r)
                if ev_dt is None:
                    continue
                ev_ts = int(ev_dt.timestamp())
                sess = classify_fx_session(ev_dt)
                if sess not in ("london", "overlap"):
                    continue
                if ev_id in existing_scored:
                    rec = existing_scored[ev_id]
                    staged_for_dedup.append(
                        {
                            **_cluster_record_from_row(rec),
                            "_full": rec,
                        }
                    )
                    seen_events.add(ev_id)
                    continue
                if (now_ts - ev_ts) < maturity_seconds:
                    continue
                if last_bar_ts < (ev_ts + min_future_bars_req):
                    continue
                if _price_at_timestamp(bars_m1, m1_ts, ev_ts, tolerance_sec=600) is None:
                    continue

                ev_iso = ev_dt.isoformat()
                prof_at_ev = store.version_for_timestamp(ev_iso)
                matched_tier: int | None = None
                if marker_ts:
                    pos = bisect_left(marker_ts, ev_ts - 900)
                    while pos < len(marker_ts) and marker_ts[pos] <= ev_ts + 900:
                        mt = int(algo_markers[pos].get("tier") or 1)
                        matched_tier = mt if matched_tier is None else max(matched_tier, mt)
                        pos += 1

                sim_long = simulate_single_exit_policy(
                    bars_m1,
                    ev_ts,
                    "long",
                    sl_pips=float(prof_at_ev.stop_loss_pips),
                    tp_pips=float(prof_at_ev.take_profit_pips),
                    trail_act_pips=(
                        float(prof_at_ev.trailing_activation_pips)
                        if prof_at_ev.trailing_activation_pips is not None
                        else None
                    ),
                    trail_dist_pips=(
                        float(prof_at_ev.trailing_distance_pips)
                        if prof_at_ev.trailing_distance_pips is not None
                        else None
                    ),
                    max_hold_min=max(30, prof_at_ev.max_holding_seconds // 60),
                    m1_ts=m1_ts,
                )
                sim_short = simulate_single_exit_policy(
                    bars_m1,
                    ev_ts,
                    "short",
                    sl_pips=float(prof_at_ev.stop_loss_pips),
                    tp_pips=float(prof_at_ev.take_profit_pips),
                    trail_act_pips=(
                        float(prof_at_ev.trailing_activation_pips)
                        if prof_at_ev.trailing_activation_pips is not None
                        else None
                    ),
                    trail_dist_pips=(
                        float(prof_at_ev.trailing_distance_pips)
                        if prof_at_ev.trailing_distance_pips is not None
                        else None
                    ),
                    max_hold_min=max(30, prof_at_ev.max_holding_seconds // 60),
                    m1_ts=m1_ts,
                )
                qual_ok, qual_tier = is_tradable_qualified_setup(
                    sess, sim_long, sim_short, matched_tier=matched_tier
                )
                sc = score_decision_net_r(
                    action=eff_action,
                    side=eff_dir,
                    long_eval=sim_long,
                    short_eval=sim_short,
                    has_qualified_setup=qual_ok,
                    setup_tier=qual_tier,
                )
                newly_settled_count += 1
                full_row = {
                    "event_id": ev_id,
                    "event_ts": ev_ts,
                    "event_iso": ev_iso,
                    "session": sess,
                    "source": src,
                    "headline": headline,
                    "strategy_version": prof_at_ev.version_id,
                    "runtime_action": eff_action,
                    "runtime_direction": eff_dir,
                    "filter_reason": sanitize_string(str(r.get("reason") or "")) or None,
                    "reward_regime": str(sc["regime"]),
                    "reward_score": sc["score"],
                    "actual_r_net": sc["r_net"],
                    "mae_penalty": float(sc.get("mae_penalty") or 0.0),
                    "missed_r": sc.get("missed_r"),
                    "miss_tier": sc.get("miss_tier"),
                    "has_qualified_setup": qual_ok,
                    "context_json": _strip_reasoning_from_context_json(r.get("context_json")),
                    "settled_at": now_iso,
                }
                staged_for_dedup.append(
                    {
                        "t": ev_ts,
                        "event_id": ev_id,
                        "score": sc["score"],
                        "r_net": sc["r_net"],
                        "mae_penalty": float(sc.get("mae_penalty") or 0.0),
                        "regime": str(sc["regime"]),
                        "has_qualified_setup": qual_ok,
                        "missed_r": sc.get("missed_r"),
                        "miss_tier": sc.get("miss_tier"),
                        "_full": full_row,
                    }
                )
                seen_events.add(ev_id)

            # Include previously scored rows even if trimmed from runtime_events
            for ev_id, rec in existing_scored.items():
                if ev_id not in seen_events:
                    staged_for_dedup.append(
                        {
                            **_cluster_record_from_row(rec),
                            "_full": rec,
                        }
                    )

        if staged_for_dedup:
            deduped = apply_event_cluster_dedup(staged_for_dedup, cluster_window_sec=300)
            to_upsert: list[dict[str, Any]] = []
            for row in deduped:
                full = dict(row["_full"])
                full["reward_regime"] = row["regime"]
                full["reward_score"] = row["score"]
                full["actual_r_net"] = row["r_net"]
                full["mae_penalty"] = float(row.get("mae_penalty") or 0.0)
                full["cluster_id"] = row["cluster_id"]
                full["cluster_primary"] = bool(row["cluster_primary"])
                to_upsert.append(full)
            store.upsert_scored_events(to_upsert)

        all_settled = [
            dict(r)
            for r in store.db.execute(
                "SELECT * FROM scored_events ORDER BY event_ts ASC"
            ).fetchall()
        ]
        dedup_records = [
            _cluster_record_from_row(r) | {"cluster_primary": bool(r["cluster_primary"])}
            for r in all_settled
        ]
        gate_overall = evaluate_anti_turtle_gate(dedup_records, min_capture_rate=MIN_CAPTURE_RATE)
        total_clusters = sum(1 for r in all_settled if r.get("cluster_primary"))
        cum_reward_total = round(
            sum(
                float(r["reward_score"])
                for r in all_settled
                if r.get("cluster_primary") and r.get("reward_score") is not None
            ),
            3,
        )

        # Phase 3: Dual-Trigger Auto-Rollback Guard (applies to promoted versions v5.1+)
        promoted_at = str(active_meta["promoted_at"])
        closed_live = _load_closed_live_trades(shadow_db=shadow_db, cli_demo_db=cli_demo_db)
        consec_losses, live_realized_r, live_trades_count = _compute_live_rollback_metrics(
            closed_trades=closed_live,
            promoted_at=promoted_at,
            sl_pips=active.stop_loss_pips,
        )
        ver_primary_rows = [
            r
            for r in all_settled
            if r.get("cluster_primary") and str(r["strategy_version"]) == active.version_id
        ]
        ver_reward_sum = round(
            sum(
                float(r["reward_score"])
                for r in ver_primary_rows
                if r.get("reward_score") is not None
            ),
            3,
        )

        rollback_reasons: list[str] = []
        if active.version_id != "v5.0" and active.parent_version_id:
            if consec_losses >= ROLLBACK_CONSECUTIVE_LOSSES:
                rollback_reasons.append(
                    f"consecutive_losses={consec_losses}>={ROLLBACK_CONSECUTIVE_LOSSES}"
                )
            if live_realized_r <= ROLLBACK_LIVE_REALIZED_R:
                rollback_reasons.append(
                    f"live_realized_r={live_realized_r}<={ROLLBACK_LIVE_REALIZED_R}"
                )
            if ver_primary_rows and ver_reward_sum <= ROLLBACK_REWARD_SCORE_FLOOR:
                rollback_reasons.append(
                    f"anti_turtle_reward={ver_reward_sum}<={ROLLBACK_REWARD_SCORE_FLOOR}"
                )

        if rollback_reasons:
            reason_str = ";".join(rollback_reasons)
            restored = store.rollback_version(active=active, now=now, reason=reason_str)
            save_active_strategy_atomic(active_strategy_path, restored)
            summary = {
                "run_at": now_iso,
                "outcome": "rolled_back",
                "active_version_before": active_ver_before,
                "active_version_after": restored.version_id,
                "newly_scored_events": newly_settled_count,
                "matured_clusters_total": total_clusters,
                "cumulative_reward_score": cum_reward_total,
                "version_reward_score": ver_reward_sum,
                "capture_rate": gate_overall["capture_rate"],
                "consecutive_losses": consec_losses,
                "live_realized_r": live_realized_r,
                "live_trades_since_promotion": live_trades_count,
                "llm_calls_used": 0,
                "detail": f"Rolled back {active_ver_before} -> {restored.version_id}: {reason_str}",
            }
            store.record_run(summary)
            return summary

        # Phase 4: Fast 6-Cluster / 48h Meta-Optimizer & Head-to-Head Arena
        clusters_since_promo = sum(
            1
            for r in all_settled
            if r.get("cluster_primary") and str(r["strategy_version"]) == active.version_id
        )
        last_arena = store.last_arena_time()
        hours_since_arena = (
            (now - last_arena).total_seconds() / 3600.0
            if last_arena is not None
            else (ARENA_HOURS_TRIGGER if total_clusters >= 3 else 0.0)
        )
        trigger_ready = total_clusters > 0 and (
            force_arena
            or (
                (
                    clusters_since_promo >= ARENA_CLUSTER_TRIGGER
                    or hours_since_arena >= ARENA_HOURS_TRIGGER
                )
                and (last_arena is None or hours_since_arena >= ARENA_COOLDOWN_HOURS)
            )
        )

        if llm_client is None or not trigger_ready:
            summary = {
                "run_at": now_iso,
                "outcome": "settled_only" if total_clusters > 0 else "insufficient_data",
                "active_version_before": active_ver_before,
                "active_version_after": active.version_id,
                "newly_scored_events": newly_settled_count,
                "matured_clusters_total": total_clusters,
                "clusters_since_promotion": clusters_since_promo,
                "cumulative_reward_score": cum_reward_total,
                "capture_rate": gate_overall["capture_rate"],
                "consecutive_losses": consec_losses,
                "live_realized_r": live_realized_r,
                "llm_calls_used": 0,
                "detail": (
                    f"Settled {newly_settled_count} events; clusters={clusters_since_promo}/{ARENA_CLUSTER_TRIGGER}"
                ),
            }
            store.record_run(summary)
            return summary

        used_today = store.daily_arena_llm_calls(now)
        if used_today >= max_daily_arena_llm_calls:
            summary = {
                "run_at": now_iso,
                "outcome": "skipped_budget",
                "active_version_before": active_ver_before,
                "active_version_after": active.version_id,
                "newly_scored_events": newly_settled_count,
                "matured_clusters_total": total_clusters,
                "cumulative_reward_score": cum_reward_total,
                "capture_rate": gate_overall["capture_rate"],
                "consecutive_losses": consec_losses,
                "live_realized_r": live_realized_r,
                "llm_calls_used": 0,
                "detail": f"Daily arena LLM budget reached: {used_today}/{max_daily_arena_llm_calls}",
            }
            store.record_run(summary)
            return summary

        slice_items = _select_arena_slice(
            settled_rows=all_settled,
            raw_by_id=raw_by_id,
            bars_m1=bars_m1,
            m1_ts=m1_ts,
            max_items=MAX_ARENA_SLICE_EVENTS,
        )
        if not slice_items or not bars_m1:
            summary = {
                "run_at": now_iso,
                "outcome": "insufficient_data",
                "active_version_before": active_ver_before,
                "active_version_after": active.version_id,
                "newly_scored_events": newly_settled_count,
                "matured_clusters_total": total_clusters,
                "cumulative_reward_score": cum_reward_total,
                "capture_rate": gate_overall["capture_rate"],
                "consecutive_losses": consec_losses,
                "live_realized_r": live_realized_r,
                "llm_calls_used": 0,
                "detail": "No replayable slice contexts available yet",
            }
            store.record_run(summary)
            return summary

        llm_calls_used = 0
        blacklist = store.blacklisted_hashes()
        candidates, prop_calls = await _propose_candidate_profiles(
            llm_client=llm_client,
            incumbent=active,
            settled_rows=all_settled,
            blacklist=blacklist,
            now=now,
        )
        llm_calls_used += prop_calls

        if not candidates:
            summary = {
                "run_at": now_iso,
                "outcome": "no_upgrade",
                "active_version_before": active_ver_before,
                "active_version_after": active.version_id,
                "newly_scored_events": newly_settled_count,
                "matured_clusters_total": total_clusters,
                "cumulative_reward_score": cum_reward_total,
                "capture_rate": gate_overall["capture_rate"],
                "consecutive_losses": consec_losses,
                "live_realized_r": live_realized_r,
                "llm_calls_used": llm_calls_used,
                "detail": "Meta-Optimizer proposed no valid unblacklisted candidate",
            }
            store.record_run(summary)
            return summary

        inc_eval = await _evaluate_profile_on_slice(
            profile=active,
            slice_items=slice_items,
            bars_m1=bars_m1,
            m1_ts=m1_ts,
            algo_markers=algo_markers,
            llm_client=llm_client,
        )
        llm_calls_used += int(inc_eval["llm_calls"])

        inc_cap = float(inc_eval["anti_turtle"]["capture_rate"])
        best_winner: dict[str, Any] | None = None
        cand_notes: list[str] = []
        for cand in candidates:
            if used_today + llm_calls_used + len(slice_items) > max_daily_arena_llm_calls + len(
                slice_items
            ):
                break
            c_eval = await _evaluate_profile_on_slice(
                profile=cand,
                slice_items=slice_items,
                bars_m1=bars_m1,
                m1_ts=m1_ts,
                algo_markers=algo_markers,
                llm_client=llm_client,
            )
            llm_calls_used += int(c_eval["llm_calls"])
            c_gate = evaluate_anti_turtle_gate(
                [],  # not used directly; inspect c_eval["anti_turtle"]
            )
            del c_gate
            # Enforce relative capture floor >= max(0.40, incumbent_capture_rate - 0.10)
            rel_floor = max(MIN_CAPTURE_RATE, round(inc_cap - 0.10, 3))
            c_cap = float(c_eval["anti_turtle"]["capture_rate"])
            anti_ok = bool(c_eval["anti_turtle"]["passed"]) and c_cap >= rel_floor
            mean_diff = round(float(c_eval["mean_score"]) - float(inc_eval["mean_score"]), 3)
            cand_notes.append(
                f"{cand.version_id}:score={c_eval['total_score']}/{c_eval['mean_score']},cap={c_cap},anti={anti_ok}"
            )
            if (
                anti_ok
                and float(c_eval["total_score"]) > float(inc_eval["total_score"])
                and mean_diff >= MIN_SCORE_IMPROVEMENT
            ):
                if best_winner is None or float(c_eval["mean_score"]) > float(
                    best_winner["mean_score"]
                ):
                    best_winner = c_eval

        if best_winner is not None:
            promoted = store.promote_candidate(
                best_winner["profile"],
                now=now,
                incumbent_score=float(inc_eval["total_score"]),
                candidate_score=float(best_winner["total_score"]),
                capture_rate=float(best_winner["anti_turtle"]["capture_rate"]),
            )
            save_active_strategy_atomic(active_strategy_path, promoted)
            summary = {
                "run_at": now_iso,
                "outcome": "promoted",
                "active_version_before": active_ver_before,
                "active_version_after": promoted.version_id,
                "newly_scored_events": newly_settled_count,
                "matured_clusters_total": total_clusters,
                "cumulative_reward_score": cum_reward_total,
                "capture_rate": float(best_winner["anti_turtle"]["capture_rate"]),
                "consecutive_losses": consec_losses,
                "live_realized_r": live_realized_r,
                "llm_calls_used": llm_calls_used,
                "incumbent_arena_score": inc_eval["total_score"],
                "candidate_arena_score": best_winner["total_score"],
                "detail": f"Promoted {promoted.version_id} ({inc_eval['total_score']} -> {best_winner['total_score']}): {promoted.summary_zh}",
            }
            store.record_run(summary)
            return summary

        summary = {
            "run_at": now_iso,
            "outcome": "no_upgrade",
            "active_version_before": active_ver_before,
            "active_version_after": active.version_id,
            "newly_scored_events": newly_settled_count,
            "matured_clusters_total": total_clusters,
            "cumulative_reward_score": cum_reward_total,
            "capture_rate": gate_overall["capture_rate"],
            "consecutive_losses": consec_losses,
            "live_realized_r": live_realized_r,
            "llm_calls_used": llm_calls_used,
            "incumbent_arena_score": inc_eval["total_score"],
            "detail": f"Incumbent {active.version_id} ({inc_eval['total_score']}) retained; "
            + "; ".join(cand_notes),
        }
        store.record_run(summary)
        return summary
    finally:
        store.close()


def load_evolution_dashboard_summary(
    *,
    evolution_db: Path | None,
    active_strategy_path: Path | None,
    shadow_db: Path | None = None,
    cli_demo_db: Path | None = None,
) -> dict[str, Any]:
    """Read-only summary of Auto-Evolution state, anti-turtle scores, and version lineage for Dashboard."""
    active = load_active_strategy_file(active_strategy_path) if active_strategy_path else None
    evo_conn = _open_ro(evolution_db)
    if evo_conn is None:
        prof = active or StrategyProfile.baseline_v5()
        return {
            "enabled": active is not None,
            "active_version": prof.version_id,
            "active_summary_zh": prof.summary_zh,
            "dynamic_prompt_rules": list(prof.dynamic_prompt_rules),
            "active_params": {
                "max_same_direction_chase_5m_pips": float(prof.max_same_direction_chase_5m_pips),
                "prompt_chase_5m_pips": float(prof.prompt_chase_5m_pips),
                "max_countertrend_htf_4h_pips": float(prof.max_countertrend_htf_4h_pips),
                "stop_loss_pips": float(prof.stop_loss_pips),
                "take_profit_pips": float(prof.take_profit_pips),
                "trailing_activation_pips": (
                    float(prof.trailing_activation_pips)
                    if prof.trailing_activation_pips is not None
                    else None
                ),
                "trailing_distance_pips": (
                    float(prof.trailing_distance_pips)
                    if prof.trailing_distance_pips is not None
                    else None
                ),
                "max_holding_seconds": prof.max_holding_seconds,
            },
            "cumulative_reward_score": 0.0,
            "capture_rate": 1.0,
            "captured_clusters": 0,
            "missed_clusters": 0,
            "matured_clusters_total": 0,
            "clusters_since_promotion": 0,
            "arena_cluster_target": ARENA_CLUSTER_TRIGGER,
            "consecutive_losses": 0,
            "rollback_loss_limit": ROLLBACK_CONSECUTIVE_LOSSES,
            "rollback_reward_floor": ROLLBACK_REWARD_SCORE_FLOOR,
            "recent_misses": [],
            "recent_scored": [],
            "versions": [],
            "recent_runs": [],
        }

    try:
        ver_rows = (
            [
                dict(r)
                for r in evo_conn.execute(
                    "SELECT * FROM strategy_versions ORDER BY created_at DESC, rowid DESC LIMIT 12"
                ).fetchall()
            ]
            if _table_exists(evo_conn, "strategy_versions")
            else []
        )
        active_ver_row = next(
            (v for v in ver_rows if v.get("status") in ("baseline", "promoted")),
            None,
        )
        if active is None and active_ver_row is not None:
            active = StrategyProfile.model_validate_json(str(active_ver_row["profile_json"]))
        if active is None:
            active = StrategyProfile.baseline_v5()

        all_primary = (
            [
                dict(r)
                for r in evo_conn.execute(
                    """
                    SELECT event_id, event_ts, event_iso, session, source, headline,
                           strategy_version, runtime_action, runtime_direction, filter_reason,
                           reward_regime, reward_score, actual_r_net, mae_penalty,
                           missed_r, miss_tier, has_qualified_setup, cluster_id
                    FROM scored_events
                    WHERE cluster_primary = 1
                    ORDER BY event_ts DESC
                    """
                ).fetchall()
            ]
            if _table_exists(evo_conn, "scored_events")
            else []
        )
        dedup_records = [
            _cluster_record_from_row(r) | {"cluster_primary": True} for r in all_primary
        ]
        gate = evaluate_anti_turtle_gate(dedup_records, min_capture_rate=MIN_CAPTURE_RATE)
        cum_score = round(
            sum(float(r["reward_score"]) for r in all_primary if r.get("reward_score") is not None),
            3,
        )
        clusters_since_promo = sum(
            1 for r in all_primary if str(r["strategy_version"]) == active.version_id
        )
        misses = [
            {
                "event_id": r["event_id"],
                "event_iso": r["event_iso"],
                "session": r["session"],
                "headline": sanitize_string(str(r["headline"])),
                "runtime_action": r["runtime_action"],
                "filter_reason": r.get("filter_reason"),
                "missed_r": r.get("missed_r"),
                "miss_tier": r.get("miss_tier"),
                "reward_score": r.get("reward_score"),
            }
            for r in all_primary
            if r.get("reward_regime") == "missed_qualified_setup"
        ][:8]

        recent_scored = [
            {
                "event_id": r["event_id"],
                "event_iso": r["event_iso"],
                "session": r["session"],
                "headline": sanitize_string(str(r["headline"])),
                "strategy_version": r["strategy_version"],
                "runtime_action": r["runtime_action"],
                "runtime_direction": r.get("runtime_direction"),
                "reward_regime": r["reward_regime"],
                "reward_score": r.get("reward_score"),
                "actual_r_net": r.get("actual_r_net"),
                "missed_r": r.get("missed_r"),
                "miss_tier": r.get("miss_tier"),
            }
            for r in all_primary[:15]
        ]

        runs = (
            [
                {
                    "run_at": r["run_at"],
                    "outcome": r["outcome"],
                    "active_version_before": r["active_version_before"],
                    "active_version_after": r["active_version_after"],
                    "cumulative_reward_score": r["cumulative_reward_score"],
                    "capture_rate": r["capture_rate"],
                    "detail": sanitize_string(str(r["detail"])),
                }
                for r in evo_conn.execute(
                    "SELECT * FROM evolution_runs ORDER BY run_at DESC, rowid DESC LIMIT 10"
                ).fetchall()
            ]
            if _table_exists(evo_conn, "evolution_runs")
            else []
        )

        consec_losses = 0
        if shadow_db is not None and active_ver_row is not None:
            closed_live = _load_closed_live_trades(shadow_db=shadow_db, cli_demo_db=cli_demo_db)
            consec_losses, _, _ = _compute_live_rollback_metrics(
                closed_trades=closed_live,
                promoted_at=str(active_ver_row["promoted_at"]),
                sl_pips=active.stop_loss_pips,
            )

        versions_out = [
            {
                "version_id": v["version_id"],
                "parent_version_id": v["parent_version_id"],
                "status": v["status"],
                "created_at": v["created_at"],
                "promoted_at": v["promoted_at"],
                "rolled_back_at": v.get("rolled_back_at"),
                "rollback_reason": v.get("rollback_reason"),
                "arena_incumbent_score": v.get("arena_incumbent_score"),
                "arena_candidate_score": v.get("arena_candidate_score"),
                "arena_capture_rate": v.get("arena_capture_rate"),
                "summary_zh": StrategyProfile.model_validate_json(
                    str(v["profile_json"])
                ).summary_zh,
            }
            for v in ver_rows
        ]

        return {
            "enabled": True,
            "active_version": active.version_id,
            "active_summary_zh": active.summary_zh,
            "dynamic_prompt_rules": list(active.dynamic_prompt_rules),
            "active_params": {
                "max_same_direction_chase_5m_pips": float(active.max_same_direction_chase_5m_pips),
                "prompt_chase_5m_pips": float(active.prompt_chase_5m_pips),
                "max_countertrend_htf_4h_pips": float(active.max_countertrend_htf_4h_pips),
                "stop_loss_pips": float(active.stop_loss_pips),
                "take_profit_pips": float(active.take_profit_pips),
                "trailing_activation_pips": (
                    float(active.trailing_activation_pips)
                    if active.trailing_activation_pips is not None
                    else None
                ),
                "trailing_distance_pips": (
                    float(active.trailing_distance_pips)
                    if active.trailing_distance_pips is not None
                    else None
                ),
                "max_holding_seconds": active.max_holding_seconds,
            },
            "cumulative_reward_score": cum_score,
            "capture_rate": gate["capture_rate"],
            "captured_clusters": gate["captured_clusters"],
            "missed_clusters": gate["missed_clusters"],
            "matured_clusters_total": len(all_primary),
            "clusters_since_promotion": clusters_since_promo,
            "arena_cluster_target": ARENA_CLUSTER_TRIGGER,
            "consecutive_losses": consec_losses,
            "rollback_loss_limit": ROLLBACK_CONSECUTIVE_LOSSES,
            "rollback_reward_floor": ROLLBACK_REWARD_SCORE_FLOOR,
            "recent_misses": misses,
            "recent_scored": recent_scored,
            "versions": versions_out,
            "recent_runs": runs,
        }
    finally:
        evo_conn.close()
