"""DeepSeek Chat Completions adapter; credentials never enter model input or Git."""

import asyncio
import os
from typing import Any, Protocol, Self

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class ModelResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    content: str = Field(min_length=1)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)


class JSONModelClient(Protocol):
    provider: str
    model: str

    async def complete_json(self, *, system: str, user: str) -> ModelResponse: ...


class ModelCallError(RuntimeError):
    """No validated model response; caller must not commit a thesis update."""


class DeepSeekClient:
    provider = "deepseek"
    model = "deepseek-flash"
    endpoint = "https://api.deepseek.com/chat/completions"

    def __init__(
        self,
        api_key: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 45,
        max_retries: int = 2,
    ) -> None:
        if not api_key or timeout_seconds <= 0 or max_retries < 0:
            raise ValueError("API key, positive timeout and nonnegative retries required")
        self._api_key = api_key
        self._transport = transport
        self._timeout = timeout_seconds
        self._max_retries = max_retries

    @classmethod
    def from_environment(cls) -> Self:
        key = os.environ.get("DEEPSEEK_API_KEY")
        if not key:
            raise ModelCallError("Set DEEPSEEK_API_KEY locally; never commit or paste the key")
        return cls(key)

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
            "max_tokens": 1200,
            "stream": False,
        }
        headers = {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
            for attempt in range(self._max_retries + 1):
                try:
                    response = await client.post(self.endpoint, headers=headers, json=payload)
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    if attempt == self._max_retries:
                        raise ModelCallError(
                            "DeepSeek request failed after bounded retries"
                        ) from exc
                else:
                    if response.status_code in {408, 429, 500, 502, 503, 504}:
                        if attempt == self._max_retries:
                            raise ModelCallError(f"DeepSeek temporary HTTP {response.status_code}")
                    elif response.is_error:
                        # Avoid logging provider response bodies or credential-bearing headers.
                        raise ModelCallError(f"DeepSeek HTTP {response.status_code}")
                    else:
                        return self._parse(response)
                await asyncio.sleep(min(0.5 * 2**attempt, 2))
        raise AssertionError("Unreachable retry loop")

    @staticmethod
    def _parse(response: httpx.Response) -> ModelResponse:
        try:
            raw: Any = response.json()
            first = raw["choices"][0]
            if first["finish_reason"] != "stop":
                raise ModelCallError("DeepSeek response was incomplete")
            usage = raw["usage"]
            return ModelResponse(
                content=first["message"]["content"],
                input_tokens=usage["prompt_tokens"],
                output_tokens=usage["completion_tokens"],
            )
        except (KeyError, IndexError, TypeError, ValueError, ValidationError) as exc:
            raise ModelCallError("Malformed or empty DeepSeek response") from exc
