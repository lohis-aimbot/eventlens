"""Public pre-move fingerprints protect the existing frozen research experiments."""

import hashlib
import json
from pathlib import Path

from eventlens.research.decision_protocol import DecisionReplayProtocol, ResearchPositionProposal
from eventlens.research.replay_inference import (
    MEMORY_PROMPT,
    PORTFOLIO_PROMPT,
    ReplayInferenceCase,
    freeze_manifest,
)
from eventlens.research.series_replay import SeriesPlan, freeze_series


def test_v012_research_identity_is_unchanged():
    expected = json.loads(Path(__file__).with_name("v012-layout-baseline.json").read_text())
    case = ReplayInferenceCase.model_validate_json(
        Path("examples/synthetic-inference-case.json").read_bytes()
    )
    plan = SeriesPlan.model_validate_json(Path("examples/synthetic-series-plan.json").read_bytes())
    actual = {
        "protocol_id": DecisionReplayProtocol().protocol_id,
        "proposal_schema": ResearchPositionProposal.model_json_schema(),
        "memory_prompt_sha256": hashlib.sha256(MEMORY_PROMPT.encode()).hexdigest(),
        "portfolio_prompt_sha256": hashlib.sha256(PORTFOLIO_PROMPT.encode()).hexdigest(),
        "synthetic_case_experiment_id": freeze_manifest(case).experiment_id,
        "synthetic_series_experiment_id": freeze_series(plan).experiment_id,
        "configs": {
            name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
            for name in expected["configs"]
        },
    }
    assert actual == expected
