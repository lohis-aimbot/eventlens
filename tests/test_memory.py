import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from eventlens.contracts import AgentDecision, Event, Thesis, ThesisChange
from eventlens.memory import MemoryConflict
from eventlens.memory_cli import demo
from eventlens.sqlite_memory import EventAlreadyProcessed, IdentityConflict, SQLiteThesisMemory

T = datetime(2026, 1, 1, tzinfo=UTC)


def event(key="e1", time=T):
    return Event(
        event_id=key,
        source="fixture",
        source_type="news",
        source_reference=key,
        source_timestamp=time,
        received_timestamp=time,
        raw_text="Synthetic evidence",
    )


def decision(key="d1", trigger="e1", version=0, revision=1, time=T, evidence=None, thesis_id="t1"):
    thesis = Thesis(
        thesis_id=thesis_id,
        revision=revision,
        status="active",
        narrative="Watch supply",
        instruments=("OIL",),
        supporting_event_ids=evidence or (trigger,),
        invalidation_conditions=("Restoration",),
        confidence=0.5,
        created_at=T,
        updated_at=time,
        review_at=time + timedelta(hours=1),
    )
    return AgentDecision(
        decision_id=key,
        event_id=trigger,
        based_on_memory_version=version,
        based_on_snapshot_id="none",
        provider="fake",
        model_version="fake1",
        prompt_version="p1",
        started_at=time,
        completed_at=time,
        valid_until=time + timedelta(minutes=1),
        thesis_changes=(
            ThesisChange(expected_revision=revision - 1, thesis=thesis, reason="new evidence"),
        ),
        rationale="fixture",
    )


def run(coro):
    return asyncio.run(coro)


def test_restart_history_and_evidence(tmp_path):
    path = tmp_path / "memory.db"
    store = SQLiteThesisMemory(path, clock=lambda: T)
    run(store.record_event(event()))
    assert run(store.commit(decision())) == 1
    reopened = SQLiteThesisMemory(path)
    assert run(reopened.snapshot(as_of=T)).theses[0].revision == 1
    history = run(reopened.history("t1"))
    assert history[0].trigger_event_id == "e1"
    assert history[0].reason == "new evidence"
    assert run(reopened.get_event("e1")) == event()


def test_duplicate_and_conflicting_ids(tmp_path):
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: T)
    run(store.record_event(event()))
    run(store.record_event(event()))
    run(store.commit(decision()))
    assert run(store.commit(decision())) == 1
    with pytest.raises(EventAlreadyProcessed):
        run(store.commit(decision(key="different")))
    with pytest.raises(IdentityConflict):
        run(store.record_event(event().model_copy(update={"raw_text": "changed"})))
    with pytest.raises(IdentityConflict):
        run(store.commit(decision().model_copy(update={"rationale": "changed"})))
    assert run(store.snapshot(as_of=T)).memory_version == 1


def test_actual_commit_time_controls_historical_visibility(tmp_path):
    now = T
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: now)
    run(store.record_event(event()))
    now = T + timedelta(seconds=10)
    run(store.commit(decision()))
    assert run(store.snapshot(as_of=T + timedelta(seconds=9))).theses == ()
    assert run(store.snapshot(as_of=now)).memory_version == 1
    with pytest.raises(ValueError):
        run(store.snapshot(as_of=datetime(2026, 1, 1)))


def test_late_recorded_or_missing_evidence_rejected(tmp_path):
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: T + timedelta(seconds=5))
    with pytest.raises(ValueError, match="Missing evidence"):
        run(store.commit(decision()))
    run(store.record_event(event()))
    with pytest.raises(ValueError, match="unavailable"):
        run(store.commit(decision()))
    assert run(store.snapshot(as_of=T + timedelta(minutes=1))).memory_version == 0


def test_two_connections_cannot_overwrite_new_state(tmp_path):
    path = tmp_path / "memory.db"
    first = SQLiteThesisMemory(path, clock=lambda: T)
    second = SQLiteThesisMemory(path, clock=lambda: T)
    run(first.record_event(event()))
    run(first.record_event(event("e2")))

    async def compete():
        return await asyncio.gather(
            first.commit(decision()),
            second.commit(decision(key="d2", trigger="e2")),
            return_exceptions=True,
        )

    results = run(compete())
    assert sum(isinstance(r, MemoryConflict) for r in results) == 1
    assert sum(r == 1 for r in results) == 1
    assert len(run(first.history("t1"))) == 1


def test_transaction_rolls_back_after_actual_sql_write(tmp_path):
    path = tmp_path / "memory.db"
    store = SQLiteThesisMemory(path, clock=lambda: T)
    run(store.record_event(event()))
    # Force a disk-layer failure after the decision insert, before the revision insert.
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TRIGGER fail_revision BEFORE INSERT ON thesis_revisions BEGIN SELECT RAISE(ABORT, 'injected failure'); END;"
        )
    with pytest.raises(sqlite3.IntegrityError):
        run(store.commit(decision()))
    assert run(store.snapshot(as_of=T)).memory_version == 0
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] == 0
        db.execute("DROP TRIGGER fail_revision")
    assert run(store.commit(decision())) == 1


def test_stale_revision_and_expired_decision(tmp_path):
    now = T
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: now)
    run(store.record_event(event()))
    run(store.record_event(event("e2")))
    run(store.commit(decision()))
    with pytest.raises(MemoryConflict):
        run(store.commit(decision("d2", "e2", version=1, revision=1)))
    now = T + timedelta(minutes=2)
    with pytest.raises(ValueError, match="expired"):
        run(store.commit(decision("d2", "e2", version=1, revision=2)))
    # Exact delivery retry stays idempotent even after its expiry.
    assert run(store.commit(decision())) == 1


def test_demo_can_repeat_without_extra_revisions(tmp_path, capsys):
    path = tmp_path / "demo.db"
    run(demo(path))
    run(demo(path))
    store = SQLiteThesisMemory(path)
    history = run(store.history("demo-supply"))
    assert len(history) == 3
    assert history[-1].thesis.status == "invalidated"
    assert history[-1].thesis.contradicting_event_ids == ("demo-event-2",)
    assert run(store.snapshot(as_of=T + timedelta(minutes=1))).theses[0].revision == 2


def test_atomic_multi_thesis_validation(tmp_path):
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: T)
    run(store.record_event(event()))
    good = decision()
    bad_change = decision(thesis_id="t2", evidence=("e1", "missing")).thesis_changes[0]
    combined = good.model_copy(update={"thesis_changes": good.thesis_changes + (bad_change,)})
    with pytest.raises(ValueError):
        run(store.commit(combined))
    assert run(store.history("t1")) == ()


def test_no_change_decision_tracks_ignored_event(tmp_path):
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: T)
    run(store.record_event(event()))
    ignored = decision().model_copy(
        update={"thesis_changes": (), "rationale": "Irrelevant; ignore"}
    )
    assert run(store.commit(ignored)) == 1
    assert run(store.snapshot(as_of=T)).theses == ()
    assert run(store.commit(ignored)) == 1


def test_revision_can_weaken_thesis_without_losing_history(tmp_path):
    now = T
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: now)
    run(store.record_event(event()))
    run(store.commit(decision()))
    now = T + timedelta(seconds=20)
    run(store.record_event(event("e2", now)))
    update = decision("d2", "e2", version=1, revision=2, time=now)
    revised = update.thesis_changes[0].thesis.model_copy(
        update={
            "confidence": 0.2,
            "supporting_event_ids": ("e1",),
            "contradicting_event_ids": ("e2",),
            "narrative": "Evidence weakens supply concern",
        }
    )
    update = update.model_copy(
        update={
            "thesis_changes": (
                ThesisChange(expected_revision=1, thesis=revised, reason="Counterevidence"),
            )
        }
    )
    run(store.commit(update))
    history = run(store.history("t1"))
    assert [row.thesis.confidence for row in history] == [0.5, 0.2]
    assert run(store.snapshot(as_of=T)).theses[0].confidence == 0.5


def test_future_receipt_rejected(tmp_path):
    store = SQLiteThesisMemory(tmp_path / "memory.db", clock=lambda: T)
    with pytest.raises(ValueError):
        run(store.record_event(event(time=T + timedelta(seconds=1))))
    assert run(store.get_event("e1")) is None
