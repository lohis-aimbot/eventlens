"""Tests for GeminiClient (Gemini 4 thinkingLevel), FailoverModelClient, and provider wiring."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from eventlens.contracts import (
    AgentContext,
    Event,
    MarketSnapshot,
    MemorySnapshot,
    PortfolioSnapshot,
    Quote,
)
from eventlens.deepseek import DeepSeekClient, ModelCallError
from eventlens.gemini import (
    FailoverModelClient,
    GeminiClient,
    build_llm_client_from_environment,
)
from eventlens.trading.agent import SINGLE_PASS_PROMPT_VERSION, SinglePassTradingAgent


def test_gemini_client_request_schema_thought_parsing_and_tokens() -> None:
    captured: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-4-hydrogen:generateContent"
        )
        assert request.headers["x-goog-api-key"] == "gem-secret-key"
        body = json.loads(request.content.decode("utf-8"))
        captured.append(body)
        return httpx.Response(
            200,
            json={
                "modelVersion": "gemini-4-hydrogen-001",
                "candidates": [
                    {
                        "finishReason": "STOP",
                        "content": {
                            "parts": [
                                {"text": "Thinking step 1: clean claims beat.", "thought": True},
                                {
                                    "text": '```json\n{"portfolio_action":"SET_TARGET","target_direction":"short","target_weight":"-0.15","rationale":"Claims beat favors USD."}\n```'
                                },
                            ]
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 420,
                    "candidatesTokenCount": 55,
                    "thoughtsTokenCount": 18,
                },
            },
        )

    client = GeminiClient(
        "gem-secret-key",
        model="gemini-4-hydrogen",
        thinking_level="low",
        transport=httpx.MockTransport(handler),
    )
    res = asyncio.run(client.complete_json(system="SYS", user='{"hello":"world"}'))

    assert len(captured) == 1
    req_json = captured[0]
    assert req_json["systemInstruction"] == {"parts": [{"text": "SYS"}]}
    assert req_json["contents"] == [{"role": "user", "parts": [{"text": '{"hello":"world"}'}]}]
    gen_cfg = req_json["generationConfig"]
    assert gen_cfg["responseMimeType"] == "application/json"
    assert gen_cfg["thinkingConfig"] == {"thinkingLevel": "low"}
    assert "temperature" not in gen_cfg
    assert "thinkingBudget" not in gen_cfg

    assert res.provider == "gemini"
    assert res.model_id == "gemini-4-hydrogen-001"
    assert res.input_tokens == 420
    assert res.output_tokens == 55
    assert res.reasoning_tokens == 18
    assert res.reasoning_content == "Thinking step 1: clean claims beat."
    assert json.loads(res.content)["portfolio_action"] == "SET_TARGET"


def test_gemini_client_retries_429_and_fails_fast_on_402() -> None:
    calls = {"retry": 0, "pay": 0}

    def retry_handler(_: httpx.Request) -> httpx.Response:
        calls["retry"] += 1
        if calls["retry"] == 1:
            return httpx.Response(429, json={"error": {"message": "rate limit"}})
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {"finishReason": "STOP", "content": {"parts": [{"text": '{"ok":true}'}]}}
                ],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 5},
            },
        )

    client_ok = GeminiClient(
        "gem-key",
        max_retries=1,
        transport=httpx.MockTransport(retry_handler),
    )
    res = asyncio.run(client_ok.complete_json(system="s", user="u"))
    assert calls["retry"] == 2
    assert res.content == '{"ok":true}'

    def pay_handler(_: httpx.Request) -> httpx.Response:
        calls["pay"] += 1
        return httpx.Response(402, json={"error": {"message": "prepayment depleted"}})

    client_pay = GeminiClient(
        "gem-key",
        max_retries=2,
        transport=httpx.MockTransport(pay_handler),
    )
    with pytest.raises(ModelCallError, match="Gemini HTTP 402") as exc_info:
        asyncio.run(client_pay.complete_json(system="s", user="u"))
    assert calls["pay"] == 1  # Non-retryable 402 never wastes retries
    assert exc_info.value.retryable is False


def test_failover_client_and_agent_decision_provenance(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    ts = datetime(2026, 11, 25, 13, 30, 0, tzinfo=UTC)
    market = MarketSnapshot(
        snapshot_id="snap-1",
        as_of=ts,
        quotes=(
            Quote(
                instrument="EUR/USD",
                source_at=ts - timedelta(milliseconds=50),
                bridge_at=ts - timedelta(milliseconds=25),
                received_at=ts,
                available_at=ts,
                source_provider="ctrader_algo",
                basis="live_poll",
                bid=Decimal("1.10000"),
                ask=Decimal("1.10000"),
                commission=Decimal("3"),
                commission_type="UsdPerMillionUsdVolume",
            ),
        ),
    )
    ctx = AgentContext(
        event=Event(
            event_id="ev-claims",
            source="investinglive_fx",
            source_type="news",
            source_reference="https://example.com/claims",
            source_timestamp=ts - timedelta(seconds=1),
            received_timestamp=ts,
            raw_text="US initial jobless claims 189K vs 200K estimate.",
        ),
        memory=MemorySnapshot(as_of=ts, memory_version=0, theses=()),
        portfolio=PortfolioSnapshot(
            snapshot_id="port-1",
            as_of=ts,
            base_currency="USD",
            equity=Decimal("200"),
            positions=(),
        ),
        market=market,
        as_of=ts,
    )

    def gem_fail(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream overloaded secret-should-not-log")

    def ds_ok(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "model": "deepseek-reasoner",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "content": json.dumps(
                                {
                                    "portfolio_action": "SET_TARGET",
                                    "target_direction": "short",
                                    "target_weight": "-0.15",
                                    "strategy_hypothesis": "fundamental_breakout",
                                    "market_bias": "bearish_eur",
                                    "market_state_note": "初请低于预期利好美元。",
                                    "watch_condition": None,
                                    "rationale": "初请强劲引发美元走强，顺势空 EUR/USD。",
                                }
                            ),
                            "reasoning_content": "claims beat 189k vs 200k",
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 500,
                    "completion_tokens": 80,
                    "completion_tokens_details": {"reasoning_tokens": 20},
                },
            },
        )

    gem_client = GeminiClient(
        "gem-secret-abc",
        max_retries=0,
        transport=httpx.MockTransport(gem_fail),
    )
    ds_client = DeepSeekClient.reasoner(
        "ds-secret-xyz",
        max_retries=0,
        transport=httpx.MockTransport(ds_ok),
    )
    failover = FailoverModelClient(primary=gem_client, fallback=ds_client)
    agent = SinglePassTradingAgent(
        failover,
        allow_commission_bearing_zero_spread=True,
        clock=lambda: ts + timedelta(milliseconds=10),
    )

    result = asyncio.run(agent.decide_with_usage(ctx))
    assert result.decision.provider == "deepseek"
    assert result.decision.model_version == "deepseek-reasoner"
    assert result.decision.prompt_version == SINGLE_PASS_PROMPT_VERSION
    assert result.decision.portfolio_action == "SET_TARGET"
    assert "gem-secret-abc" not in caplog.text
    assert "ds-secret-xyz" not in caplog.text
    assert "secret-should-not-log" not in caplog.text

    strict_only = FailoverModelClient(primary=gem_client, fallback=None)
    with pytest.raises(ModelCallError):
        asyncio.run(strict_only.complete_json(system="s", user="u"))


def test_build_llm_client_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("GEMINI_MODEL", raising=False)
    monkeypatch.delenv("GEMINI_THINKING_LEVEL", raising=False)
    monkeypatch.delenv("DEEPSEEK_MODEL", raising=False)

    monkeypatch.setenv("GEMINI_API_KEY", "gem-k")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "ds-k")
    auto_both = build_llm_client_from_environment(provider="auto")
    assert isinstance(auto_both, FailoverModelClient)
    assert isinstance(auto_both.primary, GeminiClient)
    assert auto_both.primary.model == "gemini-4-hydrogen"
    assert auto_both.primary.thinking_level == "low"
    assert isinstance(auto_both.fallback, DeepSeekClient)

    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    gem_only = build_llm_client_from_environment(provider="auto")
    assert isinstance(gem_only, GeminiClient)

    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "ds-k")
    ds_only = build_llm_client_from_environment(provider="auto")
    assert isinstance(ds_only, DeepSeekClient)

    with pytest.raises(ModelCallError):
        build_llm_client_from_environment(provider="gemini")
