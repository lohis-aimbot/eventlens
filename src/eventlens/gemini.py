"""Google Gemini 4 generateContent adapter and failover JSON client; secrets never logged."""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Self

import httpx

from .deepseek import (
    DeepSeekClient,
    JSONModelClient,
    ModelCallError,
    ModelResponse,
    _read_dotenv_values,
    _strip_markdown_json_fence,
)

logger = logging.getLogger(__name__)

VALID_GEMINI_THINKING_LEVELS: frozenset[str] = frozenset({"low", "medium", "high", "xhigh"})
_RETRYABLE_HTTP_CODES: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})


def _normalize_thinking_level(raw: str | None) -> str | None:
    if raw is None:
        return None
    cleaned = raw.strip().lower()
    if not cleaned or cleaned in {"none", "off", "disabled"}:
        return None
    if cleaned not in VALID_GEMINI_THINKING_LEVELS:
        raise ValueError(
            f"Unsupported Gemini thinking_level={raw!r}; expected one of {sorted(VALID_GEMINI_THINKING_LEVELS)}"
        )
    return cleaned


class GeminiClient:
    """Official Google AI Studio REST JSON client supporting Gemini 4 thinkingLevel."""

    provider = "gemini"
    model = "gemini-4-hydrogen"

    def __init__(
        self,
        api_key: str,
        *,
        model: str = "gemini-4-hydrogen",
        thinking_level: str | None = "low",
        max_tokens: int = 4096,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 30.0,
        max_retries: int = 2,
    ) -> None:
        clean_model = model.strip()
        norm_level = _normalize_thinking_level(thinking_level)
        if (
            not api_key
            or not clean_model
            or timeout_seconds <= 0
            or max_tokens <= 0
            or max_retries < 0
        ):
            raise ValueError(
                "API key, non-empty model, positive timeout/max_tokens and nonnegative retries required"
            )
        self._api_key = api_key
        self.model = clean_model
        self._thinking_level = norm_level
        self._max_tokens = max_tokens
        self._transport = transport
        self._timeout = timeout_seconds
        self._max_retries = max_retries
        self.endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{clean_model}:generateContent"
        )

    @property
    def thinking_level(self) -> str | None:
        return self._thinking_level

    @classmethod
    def from_environment(
        cls,
        *,
        dotenv_path: Path | None = None,
        model: str | None = None,
        thinking_level: str | None = None,
    ) -> Self:
        dotenv_vals = _read_dotenv_values(dotenv_path)
        key = os.environ.get("GEMINI_API_KEY") or dotenv_vals.get("GEMINI_API_KEY")
        if not key:
            raise ModelCallError("Set GEMINI_API_KEY locally; never commit or paste the key")
        eff_model = (
            model
            or os.environ.get("GEMINI_MODEL")
            or dotenv_vals.get("GEMINI_MODEL")
            or "gemini-4-hydrogen"
        ).strip()
        raw_level = (
            thinking_level
            if thinking_level is not None
            else (
                os.environ.get("GEMINI_THINKING_LEVEL")
                or dotenv_vals.get("GEMINI_THINKING_LEVEL")
                or "low"
            )
        )
        return cls(key, model=eff_model, thinking_level=raw_level)

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        gen_cfg: dict[str, Any] = {
            "responseMimeType": "application/json",
            "maxOutputTokens": self._max_tokens,
        }
        if self._thinking_level is not None and self.model.startswith("gemini-4"):
            gen_cfg["thinkingConfig"] = {"thinkingLevel": self._thinking_level}

        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": user}]}],
            "generationConfig": gen_cfg,
        }
        headers = {"x-goog-api-key": self._api_key, "Content-Type": "application/json"}

        attempt = 0
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
            while attempt <= self._max_retries:
                try:
                    response = await client.post(self.endpoint, headers=headers, json=payload)
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    if attempt == self._max_retries:
                        raise ModelCallError(
                            "Gemini request failed after bounded retries", retryable=True
                        ) from exc
                else:
                    if response.status_code in _RETRYABLE_HTTP_CODES:
                        if attempt == self._max_retries:
                            raise ModelCallError(
                                f"Gemini temporary HTTP {response.status_code}", retryable=True
                            )
                    elif response.is_error:
                        raise ModelCallError(f"Gemini HTTP {response.status_code}")
                    else:
                        return self._parse(response)
                await asyncio.sleep(min(0.5 * 2**attempt, 2.0))
                attempt += 1
        raise AssertionError("Unreachable retry loop")

    def _parse(self, response: httpx.Response) -> ModelResponse:
        try:
            raw: Any = response.json()
            cand = raw["candidates"][0]
            finish_reason = str(cand.get("finishReason") or "STOP").upper()
            if finish_reason not in {"STOP", ""}:
                raise ModelCallError(f"Gemini response was incomplete ({finish_reason})")
            parts = cand["content"]["parts"]
            thought_chunks: list[str] = []
            answer_chunks: list[str] = []
            for part in parts:
                if not isinstance(part, dict) or "text" not in part:
                    continue
                text_val = str(part["text"])
                if part.get("thought") is True:
                    if text_val.strip():
                        thought_chunks.append(text_val.strip())
                else:
                    answer_chunks.append(text_val)
            non_empty_answers = [chunk for chunk in answer_chunks if chunk.strip()]
            if not non_empty_answers:
                raise ModelCallError("Malformed or empty Gemini response")
            cleaned_content = _strip_markdown_json_fence("".join(non_empty_answers))
            usage = raw.get("usageMetadata") or {}
            input_tokens = int(usage.get("promptTokenCount") or 0)
            output_tokens = int(usage.get("candidatesTokenCount") or 0)
            reasoning_tokens = int(usage.get("thoughtsTokenCount") or 0)
            reasoning_content = "\n".join(thought_chunks) if thought_chunks else None
            return ModelResponse(
                content=cleaned_content,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                provider="gemini",
                model_id=str(raw.get("modelVersion") or self.model),
                reasoning_content=reasoning_content,
                reasoning_tokens=reasoning_tokens,
            )
        except ModelCallError:
            raise
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ModelCallError("Malformed or empty Gemini response") from exc


class FailoverModelClient:
    """Calls primary JSONModelClient and fails over cleanly to fallback only on ModelCallError."""

    def __init__(
        self,
        *,
        primary: JSONModelClient,
        fallback: JSONModelClient | None = None,
    ) -> None:
        self.primary = primary
        self.fallback = fallback
        self.provider = primary.provider
        self.model = primary.model

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        try:
            reply = await self.primary.complete_json(system=system, user=user)
            return reply.model_copy(
                update={
                    "provider": reply.provider or self.primary.provider,
                    "model_id": reply.model_id or self.primary.model,
                }
            )
        except ModelCallError as exc:
            if self.fallback is None:
                raise
            logger.warning(
                "llm_primary_failed_falling_back primary_provider=%s primary_model=%s "
                "fallback_provider=%s fallback_model=%s reason=%s",
                self.primary.provider,
                self.primary.model,
                self.fallback.provider,
                self.fallback.model,
                exc,
            )
            fb_reply = await self.fallback.complete_json(system=system, user=user)
            return fb_reply.model_copy(
                update={
                    "provider": fb_reply.provider or self.fallback.provider,
                    "model_id": fb_reply.model_id or self.fallback.model,
                }
            )


def build_llm_client_from_environment(
    *,
    provider: str = "auto",
    gemini_model: str | None = None,
    gemini_thinking_level: str | None = None,
    deepseek_model: str | None = None,
    dotenv_path: Path | None = None,
) -> JSONModelClient:
    """Resolve Gemini primary + DeepSeek fallback client stack from env/.env without leaking keys."""
    dotenv_vals = _read_dotenv_values(dotenv_path)
    raw_provider = (
        provider
        if provider.strip().lower() != "auto"
        else (os.environ.get("LLM_PROVIDER") or dotenv_vals.get("LLM_PROVIDER") or "auto")
    )
    eff_provider = raw_provider.strip().lower()
    if eff_provider not in {"auto", "gemini", "deepseek"}:
        raise ValueError(
            f"Unsupported llm provider={raw_provider!r}; expected auto, gemini, or deepseek"
        )

    if eff_provider == "gemini":
        return GeminiClient.from_environment(
            dotenv_path=dotenv_path,
            model=gemini_model,
            thinking_level=gemini_thinking_level,
        )
    if eff_provider == "deepseek":
        return DeepSeekClient.from_environment(dotenv_path=dotenv_path, model=deepseek_model)

    gemini_key = os.environ.get("GEMINI_API_KEY") or dotenv_vals.get("GEMINI_API_KEY")
    deepseek_key = os.environ.get("DEEPSEEK_API_KEY") or dotenv_vals.get("DEEPSEEK_API_KEY")
    if gemini_key and deepseek_key:
        return FailoverModelClient(
            primary=GeminiClient.from_environment(
                dotenv_path=dotenv_path,
                model=gemini_model,
                thinking_level=gemini_thinking_level,
            ),
            fallback=DeepSeekClient.from_environment(
                dotenv_path=dotenv_path,
                model=deepseek_model,
            ),
        )
    if gemini_key:
        return GeminiClient.from_environment(
            dotenv_path=dotenv_path,
            model=gemini_model,
            thinking_level=gemini_thinking_level,
        )
    if deepseek_key:
        return DeepSeekClient.from_environment(dotenv_path=dotenv_path, model=deepseek_model)
    raise ModelCallError(
        "Set GEMINI_API_KEY and/or DEEPSEEK_API_KEY locally; never commit or paste the key"
    )
