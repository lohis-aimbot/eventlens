"""Inspect durable memory or run a clearly synthetic memory-only demonstration."""

import argparse
import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .contracts import AgentDecision, Event, Thesis, ThesisChange
from .deepseek import DeepSeekClient
from .reasoning import MemoryReasoner
from .sqlite_memory import SQLiteThesisMemory


async def demo(path: Path) -> None:
    # A deterministic simulated clock is used only in this fixture demonstration.
    now = datetime(2026, 1, 1, tzinfo=UTC)
    memory = SQLiteThesisMemory(path, clock=lambda: now)
    narratives = [
        "Unconfirmed disruption: watch only.",
        "Confirmed disruption strengthens the supply thesis.",
        "Verified restoration invalidates the supply thesis.",
    ]
    support: list[str] = []
    versions = []
    for index, narrative in enumerate(narratives):
        now = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=index)
        event = Event(
            event_id=f"demo-event-{index}",
            source="synthetic",
            source_type="news",
            source_reference=f"fixture:{index}",
            source_timestamp=now,
            received_timestamp=now,
            raw_text=narrative,
        )
        await memory.record_event(event)
        await memory.record_event(event)  # Exact repeat: no extra evidence row.
        if index < 2:
            support.append(event.event_id)
        thesis = Thesis(
            thesis_id="demo-supply",
            revision=index + 1,
            status="invalidated" if index == 2 else "active",
            narrative=narrative,
            instruments=("OIL",),
            supporting_event_ids=tuple(support),
            contradicting_event_ids=(event.event_id,) if index == 2 else (),
            invalidation_conditions=("Verified supply restoration",),
            confidence=(0.3, 0.7, 0.1)[index],
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=now,
            review_at=now + timedelta(hours=1),
        )
        decision = AgentDecision(
            decision_id=f"demo-decision-{index}",
            event_id=event.event_id,
            based_on_memory_version=index,
            based_on_snapshot_id="fixture-no-account",
            provider="deterministic-fixture",
            model_version="fixture-1",
            prompt_version="none",
            started_at=now,
            completed_at=now,
            valid_until=now + timedelta(seconds=30),
            thesis_changes=(
                ThesisChange(expected_revision=index, thesis=thesis, reason=narrative),
            ),
            rationale="Preset memory test; no LLM or trading",
        )
        version = await memory.commit(decision)
        assert await memory.commit(decision) == version
        versions.append(version)
    # New connection/instance: demonstrate persistence without live process state.
    reopened = SQLiteThesisMemory(path)
    print(
        json.dumps(
            {
                "synthetic": True,
                "memory_versions": versions,
                "history": [
                    row.model_dump(mode="json") for row in await reopened.history("demo-supply")
                ],
            },
            indent=2,
        )
    )


async def run(args: argparse.Namespace) -> None:
    if args.command == "demo":
        await demo(args.database)
        return
    if args.command == "reason":
        if args.database == Path("data/memory-demo.sqlite"):
            raise ValueError("Choose a separate --database PATH for real model reasoning")
        event = Event.model_validate_json(args.event_file.read_text(encoding="utf-8"))
        reasoning_result = await MemoryReasoner(
            DeepSeekClient.from_environment(), SQLiteThesisMemory(args.database)
        ).process(event)
        print(reasoning_result.model_dump_json(indent=2))
        return
    if not args.database.is_file():
        raise ValueError("Database does not exist; run demo or create a memory store first")
    memory = SQLiteThesisMemory(args.database)
    if args.command == "history":
        history_rows = [row.model_dump(mode="json") for row in await memory.history(args.thesis_id)]
        print(json.dumps(history_rows, indent=2))
    elif args.command == "event":
        stored_event = await memory.get_event(args.event_id)
        print(stored_event.model_dump_json(indent=2) if stored_event else "null")
    else:
        cutoff = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(UTC)
        print((await memory.snapshot(as_of=cutoff)).model_dump_json(indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("data/memory-demo.sqlite"))
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "demo", help="Use a dedicated synthetic demo database, not a production store"
    )
    commands.add_parser("history").add_argument("thesis_id")
    commands.add_parser("event").add_argument("event_id")
    commands.add_parser("snapshot").add_argument("--as-of")
    commands.add_parser("reason", help="Run one DeepSeek thesis update from a local event JSON file").add_argument("event_file", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
