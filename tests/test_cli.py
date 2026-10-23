"""Grouped commands retain the existing interfaces and paid-call boundaries."""

import importlib
import json
import subprocess
import sys

import pytest

from eventlens.cli import main


@pytest.mark.parametrize(
    "prefix,module",
    [
        (["memory"], "memory_cli"),
        (["source", "fed"], "fed_cli"),
        (["research", "accounting"], "research_cli"),
        (["research", "market"], "market_cli"),
        (["research", "labels"], "replay_cli"),
        (["research", "decision"], "decision_cli"),
        (["research", "infer"], "inference_cli"),
        (["research", "series"], "series_cli"),
        (["observe"], "observer_cli"),
        (["shadow"], "shadow_cli"),
    ],
)
def test_existing_help_is_unchanged_and_dispatch_restores_argv(prefix, module, monkeypatch, capsys):
    legacy_name = {
        "research_cli": "eventlens-research",
        "inference_cli": "eventlens-infer",
        "observer_cli": "eventlens-observe",
        "shadow_cli": "eventlens-shadow",
    }.get(module, f"eventlens-{module.removesuffix('_cli')}")
    monkeypatch.setattr(sys, "argv", [legacy_name, "--help"])
    with pytest.raises(SystemExit) as legacy:
        importlib.import_module(f"eventlens.{module}").main()
    expected = capsys.readouterr().out
    original = sys.argv
    with pytest.raises(SystemExit) as grouped:
        main([*prefix, "--help"])
    assert grouped.value.code == legacy.value.code == 0
    assert capsys.readouterr().out == expected
    assert sys.argv is original


@pytest.mark.parametrize("command", ["infer", "series"])
def test_grouped_paid_run_still_requires_opt_in(tmp_path, command, monkeypatch, capsys):
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    extra = ["--output", str(tmp_path / "result")] if command == "infer" else []
    fixture = "inference-case" if command == "infer" else "series-plan"
    with pytest.raises(SystemExit) as rejected:
        main(["research", command, "run", f"examples/synthetic-{fixture}.json", *extra])
    assert rejected.value.code == 2
    assert "--call-model" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == []


def test_grouped_check_keeps_execution_disabled(capsys):
    main(["research", "decision", "check", "examples/synthetic-position-proposal.json"])
    result = json.loads(capsys.readouterr().out)
    assert result["execution_authorized"] is False


def test_runtime_foundation_does_not_load_research():
    # Inspect a fresh interpreter; previous tests may have imported research modules.
    code = """
import importlib
import sys
for name in (
    'contracts', 'deepseek', 'memory', 'sqlite_memory', 'reasoning', 'latency',
    'sources.events', 'sources.fed', 'trading.agent', 'trading.portfolio',
    'trading.risk', 'trading.execution', 'runtime', 'cli',
):
    importlib.import_module('eventlens.' + name)
assert not any(n.startswith('eventlens.research') for n in sys.modules)
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_top_level_help_does_not_import_command_implementations():
    code = """
import sys
from eventlens.cli import main
try:
    main(['--help'])
except SystemExit as exc:
    assert exc.code == 0
assert 'eventlens.fed_cli' not in sys.modules
assert 'eventlens.inference_cli' not in sys.modules
assert 'eventlens.series_cli' not in sys.modules
assert 'eventlens.shadow_cli' not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_observer_help_describes_only_source_capture(capsys):
    with pytest.raises(SystemExit) as help_result:
        main(["observe", "--help"])
    assert help_result.value.code == 0
    text = capsys.readouterr().out
    assert "no LLM" in text and "capture" in text and "report" in text
