"""Fetch public archived Fed text and combine local windows; no LLM/broker calls."""

import argparse
import asyncio
import gzip
from datetime import UTC, datetime
from pathlib import Path

from eventlens.contracts import Event
from eventlens.fed import FedMonetarySource, FeedError, SourceRejected, ValidatedItem
from eventlens.research_data import ResearchCatalog, read_window
from eventlens.series_replay import SeriesEpisode, SeriesPlan


async def prepare(catalog_path: Path, windows: Path, run_id: str) -> SeriesPlan:
    catalog = ResearchCatalog.model_validate_json(catalog_path.read_bytes())
    source = FedMonetarySource()
    episodes = []
    for entry in catalog.events:
        event = None
        reason = None
        try:
            text = await source.fetch_article(
                ValidatedItem(entry.event_id, entry.source_reference, entry.source_time)
            )
            event = Event(
                event_id=entry.event_id,
                source="federalreserve.gov-archived",
                source_type="macro",
                source_reference=entry.source_reference,
                source_timestamp=entry.source_time,
                received_timestamp=datetime.now(UTC),
                raw_text=text,
                author="Federal Open Market Committee",
            )
        except (FeedError, SourceRejected):
            reason = "official_archive_fetch_failed"
        path = windows / f"fomc-{entry.source_time:%Y-%m-%d}-window.json.gz"
        window = None
        try:
            window = read_window(path)
        except FileNotFoundError:
            reason = "market_window_missing"
        except ValueError:
            reason = "market_window_invalid"
        episodes.append(
            SeriesEpisode(
                event_id=entry.event_id,
                event=event,
                window=window,
                unavailable_reason=reason,
            )
        )
    return SeriesPlan(
        run_id=run_id,
        catalog=catalog,
        episodes=tuple(episodes),
        latency_scenario="slow_reasoning_assumed",
        assumed_market_delay_ms=250,
        max_api_calls=32,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=Path("configs/fomc-2025.json"))
    parser.add_argument("--windows", type=Path, default=Path("data"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Reserve a NEW output before network access. A preparation crash leaves an
    # invalid/incomplete file, never an apparently complete usable experiment.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as stream:
        plan = asyncio.run(prepare(args.catalog, args.windows, args.run_id))
        encoded = plan.model_dump_json().encode()
        stream.write(gzip.compress(encoded, mtime=0) if args.output.suffix == ".gz" else encoded)
    print(f"Prepared {len(plan.episodes)} declared episodes; no model calls. {args.output}")


if __name__ == "__main__":
    main()
