"""Chronological research episodes with a durable paid-call journal; no trading."""

import fcntl
import gzip
import json
import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Self, cast

from pydantic import Field, model_validator

from ..contracts import Contract, Event, Identifier, Timestamp
from ..deepseek import JSONRequestSettings, ModelCallError, ModelResponse
from ..reasoning import Proposal
from .decision_protocol import DecisionReplayProtocol, ResearchPositionProposal
from .historical_context import ReplayThesisRevision
from .replay_inference import (
    MEMORY_PROMPT,
    MEMORY_PROMPT_VERSION,
    PORTFOLIO_PROMPT,
    PORTFOLIO_PROMPT_VERSION,
    ProcessClock,
    ReplayInferenceCase,
    ReplayInferenceConfig,
    ReplayInferenceResult,
    ReplayInferenceRunner,
    ReplayJSONClient,
    SystemProcessClock,
    digest,
    freeze_manifest,
)
from .research_data import ResearchCatalog
from .research_replay import ResearchWindow

logger = logging.getLogger(__name__)
PINNED_CATALOG_ID = "c28b8ce3864687b571c6c24339106409196a10a0e830fc84951e0d6c3556275f"


class SeriesEpisode(Contract):
    event_id: Identifier
    event: Event | None = None
    window: ResearchWindow | None = None
    unavailable_reason: str | None = None


class SeriesPlan(Contract):
    run_id: Identifier
    catalog: ResearchCatalog
    episodes: tuple[SeriesEpisode, ...]
    latency_scenario: Identifier
    assumed_market_delay_ms: int = Field(strict=True, ge=0)
    max_api_calls: int = Field(default=32, strict=True, ge=1, le=32)

    @model_validator(mode="after")
    def exact_chronological_catalog(self) -> Self:
        if digest(self.catalog.model_dump_json()) != PINNED_CATALOG_ID:
            raise ValueError("Catalog must match the entire pinned eight-event manifest")
        if tuple(e.event_id for e in self.episodes) != tuple(
            e.event_id for e in self.catalog.events
        ):
            raise ValueError("Episodes must exactly match the complete chronological catalog")
        if self.latency_scenario not in {
            s.name for s in DecisionReplayProtocol().latency_scenarios
        }:
            raise ValueError("Unknown pinned latency scenario")
        for episode, entry in zip(self.episodes, self.catalog.events):
            if episode.event and (
                episode.event.event_id != entry.event_id
                or episode.event.source_timestamp != entry.source_time
                or episode.event.source_reference != entry.source_reference
                or episode.event.source_type != "macro"
            ):
                raise ValueError("Archived event conflicts with catalog metadata")
            if episode.window and (
                episode.window.event_id != entry.event_id
                or episode.window.source_time != entry.source_time
                or episode.window.source_reference != entry.source_reference
            ):
                raise ValueError("Market window conflicts with catalog metadata")
        return self


class SeriesManifest(Contract):
    version: str = "chronological-series-v1"
    run_id: Identifier
    plan_id: Identifier
    protocol_id: Identifier
    config: ReplayInferenceConfig
    memory_prompt_version: Identifier
    portfolio_prompt_version: Identifier
    memory_prompt: str
    portfolio_prompt: str
    memory_schema_hash: Identifier
    portfolio_schema_hash: Identifier
    created_at: Timestamp

    @property
    def experiment_id(self) -> str:
        return digest(self.model_dump_json(exclude={"created_at"}))


def freeze_series(
    plan: SeriesPlan,
    config: ReplayInferenceConfig = ReplayInferenceConfig(),
    *,
    created_at: datetime | None = None,
) -> SeriesManifest:
    plan = SeriesPlan.model_validate_json(plan.model_dump_json())
    config = ReplayInferenceConfig.model_validate_json(config.model_dump_json())
    return SeriesManifest(
        run_id=plan.run_id,
        plan_id=digest(plan.model_dump_json()),
        protocol_id=DecisionReplayProtocol().protocol_id,
        config=config,
        memory_prompt_version=MEMORY_PROMPT_VERSION,
        portfolio_prompt_version=PORTFOLIO_PROMPT_VERSION,
        memory_prompt=MEMORY_PROMPT,
        portfolio_prompt=PORTFOLIO_PROMPT,
        memory_schema_hash=digest(json.dumps(Proposal.model_json_schema(), sort_keys=True)),
        portfolio_schema_hash=digest(
            json.dumps(ResearchPositionProposal.model_json_schema(), sort_keys=True)
        ),
        created_at=created_at or datetime.now(UTC),
    )


def _pack(text: str) -> bytes:
    return gzip.compress(text.encode(), mtime=0)


def _unpack(blob: bytes) -> str:
    return gzip.decompress(blob).decode()


class SeriesBlocked(RuntimeError):
    """An in-flight/unknown case must be reviewed, not automatically billed again."""


class SeriesStore:
    """One canonical local registry; results and memory commit in one transaction."""

    def __init__(self, path: Path, *, readonly: bool = False) -> None:
        self.path = path.resolve()
        self.readonly = readonly
        if not readonly:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(
            f"file:{self.path}?mode=ro" if readonly else str(self.path), uri=readonly
        )
        self.db.row_factory = sqlite3.Row
        if not readonly:
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS series (
                    run_id TEXT PRIMARY KEY, manifest BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS episodes (
                    run_id TEXT NOT NULL, event_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                    state TEXT NOT NULL, status TEXT, reason TEXT, manifest BLOB, result BLOB,
                    quality TEXT,
                    PRIMARY KEY (run_id, event_id), UNIQUE (run_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS calls (
                    call_id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, event_id TEXT NOT NULL,
                    stage TEXT NOT NULL, input_id TEXT NOT NULL, input_json BLOB NOT NULL,
                    state TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT,
                    response BLOB, error_kind TEXT
                );
                CREATE TABLE IF NOT EXISTS revisions (
                    run_id TEXT NOT NULL, thesis_id TEXT NOT NULL, revision INTEGER NOT NULL,
                    event_id TEXT NOT NULL, record BLOB NOT NULL,
                    PRIMARY KEY (run_id, thesis_id, revision)
                );
            """)

    def close(self) -> None:
        self.db.close()

    @contextmanager
    def lock(self) -> Iterator[None]:
        if self.readonly:
            raise ValueError("Read-only registry cannot run episodes")
        with self.path.with_suffix(self.path.suffix + ".lock").open("a") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise SeriesBlocked("Another runner owns this registry") from exc
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    def prepare(self, plan: SeriesPlan, manifest: SeriesManifest) -> None:
        row = self.db.execute(
            "SELECT manifest FROM series WHERE run_id=?", (plan.run_id,)
        ).fetchone()
        if row:
            stored = SeriesManifest.model_validate_json(_unpack(row["manifest"]))
            if stored.experiment_id != manifest.experiment_id:
                raise ValueError("Run ID already binds different data, prompts or configuration")
            return
        with self.db:
            self.db.execute(
                "INSERT INTO series VALUES (?,?)", (plan.run_id, _pack(manifest.model_dump_json()))
            )
            self.db.executemany(
                "INSERT INTO episodes (run_id,event_id,ordinal,state,quality) VALUES (?,?,?,'pending',?)",
                [
                    (
                        plan.run_id,
                        e.event_id,
                        i,
                        json.dumps(
                            {
                                "data_id": e.window.data_id,
                                "observations": len(e.window.observations),
                                "zero_spread_observations": sum(
                                    q.bid == q.ask for q in e.window.observations
                                ),
                                "max_observation_gap_ms": max(
                                    (
                                        int((b.timestamp - a.timestamp).total_seconds() * 1000)
                                        for a, b in zip(
                                            e.window.observations, e.window.observations[1:]
                                        )
                                    ),
                                    default=0,
                                ),
                                "scope": "full_window_audit_only_not_model_input",
                            }
                        )
                        if e.window
                        else None,
                    )
                    for i, e in enumerate(plan.episodes)
                ],
            )

    def episode(self, run_id: str, event_id: str) -> sqlite3.Row:
        row = self.db.execute(
            "SELECT * FROM episodes WHERE run_id=? AND event_id=?", (run_id, event_id)
        ).fetchone()
        if row is None:
            raise ValueError("Unknown episode")
        return cast(sqlite3.Row, row)

    def history(self, run_id: str) -> tuple[ReplayThesisRevision, ...]:
        return tuple(
            ReplayThesisRevision.model_validate_json(_unpack(r["record"]))
            for r in self.db.execute(
                "SELECT record FROM revisions WHERE run_id=? ORDER BY thesis_id,revision", (run_id,)
            )
        )

    def call_count(self, run_id: str) -> int:
        return int(
            self.db.execute("SELECT count(*) FROM calls WHERE run_id=?", (run_id,)).fetchone()[0]
        )

    def begin_episode(self, run_id: str, event_id: str, manifest: Contract) -> None:
        with self.db:
            changed = self.db.execute(
                "UPDATE episodes SET state='running',manifest=? WHERE run_id=? AND event_id=? AND state='pending'",
                (_pack(manifest.model_dump_json()), run_id, event_id),
            ).rowcount
            if changed != 1:
                raise SeriesBlocked("Episode is not pending")

    def begin_call(
        self, run_id: str, event_id: str, stage: str, user: str, started: datetime, limit: int
    ) -> int:
        if self.db.execute(
            "SELECT 1 FROM calls WHERE run_id=? AND event_id=? AND state='pending' LIMIT 1",
            (run_id, event_id),
        ).fetchone():
            raise ModelCallError("Prior request completion is unknown; no automatic retry")
        if self.call_count(run_id) >= limit:
            raise ModelCallError("Series API call limit reached")
        with self.db:
            cursor = self.db.execute(
                "INSERT INTO calls (run_id,event_id,stage,input_id,input_json,state,started_at) VALUES (?,?,?,?,?,'pending',?)",
                (run_id, event_id, stage, digest(user), _pack(user), started.isoformat()),
            )
            return int(cursor.lastrowid or 0)

    def finish_call(
        self,
        call_id: int,
        completed: datetime,
        response: ModelResponse | None,
        error_kind: str | None,
    ) -> None:
        with self.db:
            self.db.execute(
                "UPDATE calls SET state='finished',completed_at=?,response=?,error_kind=? WHERE call_id=? AND state='pending'",
                (
                    completed.isoformat(),
                    _pack(response.model_dump_json()) if response else None,
                    error_kind,
                    call_id,
                ),
            )

    def finish_episode(
        self,
        run_id: str,
        event_id: str,
        status: str,
        reason: str,
        result: ReplayInferenceResult | None = None,
    ) -> None:
        row = self.episode(run_id, event_id)
        if row["state"] == "completed":
            raise ValueError("Completed episode is immutable")
        if result:
            if row["state"] != "running" or result.manifest.run_id != run_id:
                raise ValueError("Result does not belong to this running episode")
            stored = json.loads(_unpack(row["manifest"]))
            if stored != result.manifest.model_dump(mode="json"):
                raise ValueError("Result manifest conflicts with the frozen episode")
            if any(record.run_id != run_id for record in result.generated_memory):
                raise ValueError("Cross-run memory commit rejected")
        with self.db:
            for record in result.generated_memory if result else ():
                self.db.execute(
                    "INSERT INTO revisions VALUES (?,?,?,?,?)",
                    (
                        run_id,
                        record.thesis.thesis_id,
                        record.thesis.revision,
                        event_id,
                        _pack(record.model_dump_json()),
                    ),
                )
            self.db.execute(
                "UPDATE episodes SET state='completed',status=?,reason=?,result=? WHERE run_id=? AND event_id=?",
                (
                    status,
                    reason,
                    _pack(result.model_dump_json()) if result else None,
                    run_id,
                    event_id,
                ),
            )

    def abandon_interrupted(self, run_id: str, event_id: str) -> None:
        """Explicit operator action: retain unknown calls, discard no recorded evidence."""
        with self.lock():
            if self.episode(run_id, event_id)["state"] != "running":
                raise ValueError("Only interrupted running episodes can be abandoned")
            self.finish_episode(
                run_id, event_id, "interrupted_unknown", "operator_abandoned_no_retry"
            )

    def report(self, run_id: str) -> dict[str, object]:
        row = self.db.execute("SELECT manifest FROM series WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown series")
        manifest = SeriesManifest.model_validate_json(_unpack(row["manifest"]))
        episodes = []
        for episode in self.db.execute(
            "SELECT * FROM episodes WHERE run_id=? ORDER BY ordinal", (run_id,)
        ):
            result = (
                ReplayInferenceResult.model_validate_json(_unpack(episode["result"]))
                if episode["result"]
                else None
            )
            episodes.append(
                {
                    "event_id": episode["event_id"],
                    "state": episode["state"],
                    "status": episode["status"],
                    "reason": episode["reason"],
                    "direction": result.proposal.target_direction
                    if result and result.proposal and result.status == "validated_proposal"
                    else None,
                    "proposed_direction": result.proposal.target_direction
                    if result and result.proposal
                    else None,
                    "processing_ms": result.total_processing_ms if result else None,
                    "memory_action": result.memory_proposal.action
                    if result and result.memory_proposal
                    else None,
                    "input_revisions": result.initial_context.model_input.selected_revision_count
                    if result and result.initial_context
                    else None,
                    "generated_revisions": len(result.generated_memory) if result else 0,
                    "window_quality": json.loads(episode["quality"])
                    if episode["quality"]
                    else None,
                    "portfolio_input_id": result.portfolio_context.model_input_id
                    if result and result.portfolio_context
                    else None,
                    "portfolio_input_cutoff": result.portfolio_context.model_input.as_of.isoformat()
                    if result and result.portfolio_context
                    else None,
                    "portfolio_quote_time": result.portfolio_context.model_input.quote.observation.timestamp.isoformat()
                    if result and result.portfolio_context
                    else None,
                    "budget_diagnostics": [
                        d.model_dump(mode="json") for d in result.budget_diagnostics
                    ]
                    if result
                    else [],
                }
            )
        calls = self.db.execute(
            "SELECT * FROM calls WHERE run_id=? ORDER BY call_id", (run_id,)
        ).fetchall()
        replies = [
            ModelResponse.model_validate_json(_unpack(c["response"]))
            for c in calls
            if c["response"]
        ]
        return {
            "run_id": run_id,
            "experiment_id": manifest.experiment_id,
            "execution_authorized": False,
            "episodes": episodes,
            "api_calls_reserved": len(calls),
            "unknown_calls": sum(c["state"] == "pending" for c in calls),
            "known_input_tokens": sum(r.input_tokens for r in replies),
            "known_output_tokens": sum(r.output_tokens for r in replies),
            "calls_without_usage": len(calls) - len(replies),
            "serving_models": sorted({r.model_id or "unreported" for r in replies}),
            "serving_fingerprints": sorted({r.system_fingerprint or "unreported" for r in replies}),
            "stored_revisions": len(self.history(run_id)),
        }


class JournaledClient:
    def __init__(
        self,
        client: ReplayJSONClient,
        store: SeriesStore,
        plan: SeriesPlan,
        event_id: str,
        clock: ProcessClock,
    ) -> None:
        self.client, self.store, self.plan = client, store, plan
        self.event_id, self.clock = event_id, clock
        self.provider, self.model = client.provider, client.model

    @property
    def request_settings(self) -> JSONRequestSettings:
        return self.client.request_settings

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        stage = "memory" if system == MEMORY_PROMPT else "portfolio"
        if system not in {MEMORY_PROMPT, PORTFOLIO_PROMPT}:
            raise ValueError("Unknown frozen prompt")
        call_id = self.store.begin_call(
            self.plan.run_id, self.event_id, stage, user, self.clock.now(), self.plan.max_api_calls
        )
        try:
            response = await self.client.complete_json(system=system, user=user)
        except (ModelCallError, TimeoutError) as exc:
            self.store.finish_call(call_id, self.clock.now(), None, type(exc).__name__)
            raise
        # Cancellation/crash remains pending. Do not guess provider billing or retry it.
        self.store.finish_call(call_id, self.clock.now(), response, None)
        return response


class SeriesRunner:
    def __init__(
        self,
        store: SeriesStore,
        client: ReplayJSONClient,
        *,
        clock: ProcessClock | None = None,
        config: ReplayInferenceConfig = ReplayInferenceConfig(),
    ) -> None:
        self.store, self.client, self.config = store, client, config
        self.clock = clock or SystemProcessClock()

    async def run(self, plan: SeriesPlan, *, max_events: int | None = None) -> dict[str, object]:
        plan = SeriesPlan.model_validate_json(plan.model_dump_json())
        manifest = freeze_series(plan, self.config, created_at=self.clock.now())
        if (
            self.client.provider != self.config.provider
            or self.client.model != self.config.requested_model
            or self.client.request_settings != self.config.client_settings
        ):
            raise ValueError("Frozen client configuration mismatch")
        if max_events is not None and max_events < 1:
            raise ValueError("Positive max_events required")
        with self.store.lock():
            self.store.prepare(plan, manifest)
            processed = 0
            for index, episode in enumerate(plan.episodes):
                row = self.store.episode(plan.run_id, episode.event_id)
                if row["state"] == "completed":
                    continue
                if row["state"] == "running":
                    raise SeriesBlocked(
                        "Interrupted episode: review journal, then explicitly abandon; no automatic retry"
                    )
                if max_events is not None and processed >= max_events:
                    break
                if episode.event is None or episode.window is None:
                    self.store.finish_episode(
                        plan.run_id,
                        episode.event_id,
                        "data_unavailable",
                        episode.unavailable_reason or "event_or_window_missing",
                    )
                elif self.store.call_count(plan.run_id) >= plan.max_api_calls:
                    self.store.finish_episode(
                        plan.run_id,
                        episode.event_id,
                        "call_limit_reached",
                        "series_api_budget_exhausted",
                    )
                else:
                    case = ReplayInferenceCase(
                        run_id=plan.run_id,
                        current_event_id=episode.event_id,
                        archived_events=tuple(
                            e.event for e in plan.episodes[: index + 1] if e.event
                        ),
                        window=episode.window,
                        latency_scenario=plan.latency_scenario,
                        assumed_market_delay_ms=plan.assumed_market_delay_ms,
                        memory_history=self.store.history(plan.run_id),
                    )
                    event_manifest = freeze_manifest(case, self.config).model_copy(
                        update={"created_at": self.clock.now()}
                    )
                    self.store.begin_episode(plan.run_id, episode.event_id, event_manifest)
                    wrapped = JournaledClient(
                        self.client, self.store, plan, episode.event_id, self.clock
                    )
                    result = await ReplayInferenceRunner(wrapped, clock=self.clock).run(
                        case, event_manifest
                    )
                    if any(
                        r["state"] == "pending"
                        for r in self.store.db.execute(
                            "SELECT state FROM calls WHERE run_id=? AND event_id=?",
                            (plan.run_id, episode.event_id),
                        )
                    ):
                        raise SeriesBlocked(
                            "Call completion is unknown; episode retained for operator review"
                        )
                    self.store.finish_episode(
                        plan.run_id, episode.event_id, result.status, result.reason, result
                    )
                processed += 1
                logger.info(
                    "series_episode_finished run_id=%s event_id=%s status=%s",
                    plan.run_id,
                    episode.event_id,
                    self.store.episode(plan.run_id, episode.event_id)["status"],
                )
            return self.store.report(plan.run_id)
