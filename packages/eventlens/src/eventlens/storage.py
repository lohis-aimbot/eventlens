"""Append-only event storage shared by SQLite and PostgreSQL."""

from datetime import datetime

from sqlalchemy import JSON, Column, MetaData, String, Table, create_engine, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .core import Event, require_aware

metadata = MetaData()
events = Table(
    "events_v1",
    metadata,
    Column("event_id", String(64), primary_key=True),
    Column("payload", JSON, nullable=False),
)


class EventStore:
    def __init__(self, url: str = "sqlite:///eventlens.db") -> None:
        self.engine = create_engine(url)
        if self.engine.dialect.name not in {"sqlite", "postgresql"}:
            raise ValueError("Only SQLite and PostgreSQL are supported")
        metadata.create_all(self.engine)

    def put(self, event: Event) -> bool:
        factory = sqlite_insert if self.engine.dialect.name == "sqlite" else pg_insert
        statement = factory(events).values(
            event_id=event.event_id, payload=event.model_dump(mode="json")
        )
        inserted = statement.on_conflict_do_nothing(index_elements=["event_id"]).returning(
            events.c.event_id
        )
        with self.engine.begin() as connection:
            result = connection.execute(inserted)
            return result.scalar_one_or_none() is not None

    def list(self, *, as_of: datetime | None = None) -> list[Event]:
        if as_of is not None:
            require_aware(as_of)
        with self.engine.connect() as connection:
            result = [
                Event.model_validate(row[0]) for row in connection.execute(select(events.c.payload))
            ]
        return sorted(
            (e for e in result if as_of is None or e.available_at <= as_of),
            key=lambda e: (e.available_at, e.event_id),
        )

    def close(self) -> None:
        self.engine.dispose()
