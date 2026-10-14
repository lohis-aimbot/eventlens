"""Offline plan or explicitly paid, one-event research inference; never trading."""

import argparse
import asyncio
import gzip
import json
import logging
import os
from pathlib import Path

from .deepseek import DeepSeekClient, ModelCallError
from .replay_inference import (
    MEMORY_PROMPT,
    PORTFOLIO_PROMPT,
    ReplayInferenceCase,
    ReplayInferenceRunner,
    freeze_manifest,
)


def read_case(path: Path) -> ReplayInferenceCase:
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
    return ReplayInferenceCase.model_validate_json(raw)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="Print frozen metadata without a model call")
    plan.add_argument("case", type=Path)
    run = commands.add_parser("run", help="Two-stage paid research call; no orders")
    run.add_argument("case", type=Path)
    run.add_argument("--call-model", action="store_true", help="Explicitly opt in to API charges")
    run.add_argument("--output", type=Path, required=True, help="A NEW dedicated run directory")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    if args.command == "run" and not args.call_model:
        parser.error("run requires --call-model; plan is offline and free")
    case = read_case(args.case)
    manifest = freeze_manifest(case)
    if args.command == "plan":
        print(manifest.model_dump_json(indent=2))
        return
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise ModelCallError("Set DEEPSEEK_API_KEY locally; never paste or commit the key")
    # A pre-existing directory is deliberately not resumed: failed/unknown calls can
    # already have been billed. Preserve it and choose a new identified experiment.
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "manifest.json").write_text(manifest.model_dump_json(indent=2))
    (args.output / "prompts.json").write_text(
        json.dumps({"memory": MEMORY_PROMPT, "portfolio": PORTFOLIO_PROMPT}, indent=2)
    )
    client = DeepSeekClient(
        key, timeout_seconds=manifest.config.client_settings.timeout_seconds, max_retries=0
    )
    result = asyncio.run(ReplayInferenceRunner(client).run(case, manifest))
    (args.output / "result.json.gz").write_bytes(
        gzip.compress(result.model_dump_json().encode(), mtime=0)
    )
    print(f"status={result.status} reason={result.reason}")
    print(f"processing_ms={result.total_processing_ms} api_attempts={len(result.attempts)}")
    if result.proposal is not None:
        print(f"research_direction={result.proposal.target_direction}; execution_authorized=false")
    print(f"audit={args.output / 'result.json.gz'}")


if __name__ == "__main__":
    main()
