"""DeepSeek Chat Completions adapter; credentials never enter model input or Git."""

import asyncio
import os
from pathlib import Path
from typing import Any, Protocol, Self

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .contracts import Contract


class JSONRequestSettings(Contract):
    endpoint: str = "https://api.deepseek.com/chat/completions"
    timeout_seconds: float = Field(gt=0)
    max_retries: int = Field(ge=0)
    max_tokens: int = Field(default=1200, gt=0)
    response_format: str = "json_object"
    thinking: str = "disabled"
    stream: bool = False
    temperature: str = "provider_default_omitted"


class ModelResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    content: str = Field(min_length=1)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    model_id: str | None = None
    system_fingerprint: str | None = None
    reasoning_content: str | None = None
    reasoning_tokens: int = Field(default=0, ge=0)


class JSONModelClient(Protocol):
    provider: str
    model: str

    async def complete_json(self, *, system: str, user: str) -> ModelResponse: ...


class ModelCallError(RuntimeError):
    """No validated model response; caller must not commit a thesis update."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


def _strip_markdown_json_fence(text: str) -> str:
    """Unwrap optional ```json ... ``` fences emitted when json_object fallback is used."""
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].strip() == "```":
            inner = "\n".join(lines[1:-1]).strip()
            if inner:
                return inner
    return text


def _read_dotenv_values(dotenv_path: Path | None) -> dict[str, str]:
    values: dict[str, str] = {}
    if "PYTEST_CURRENT_TEST" in os.environ:
        return values
    env_file = dotenv_path or Path(".env")
    if not env_file.is_file():
        return values
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        values[k.strip()] = v.strip().strip("'\"")
    return values


class DeepSeekClient:
    provider = "deepseek"
    model = "deepseek-flash"
    endpoint = "https://api.deepseek.com/chat/completions"

    def __init__(
        self,
        api_key: str,
        *,
        model: str = "deepseek-flash",
        thinking_enabled: bool | None = None,
        max_tokens: int | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float | None = None,
        max_retries: int = 2,
    ) -> None:
        clean_model = model.strip()
        is_reasoner = (
            thinking_enabled
            if thinking_enabled is not None
            else (clean_model == "deepseek-reasoner")
        )
        eff_timeout = (
            timeout_seconds if timeout_seconds is not None else (120.0 if is_reasoner else 45.0)
        )
        eff_max_tokens = max_tokens if max_tokens is not None else (8192 if is_reasoner else 1200)
        if (
            not api_key
            or not clean_model
            or eff_timeout <= 0
            or eff_max_tokens <= 0
            or max_retries < 0
        ):
            raise ValueError(
                "API key, non-empty model, positive timeout/max_tokens and nonnegative retries required"
            )
        self._api_key = api_key
        self.model = clean_model
        self._thinking_enabled = is_reasoner
        self._max_tokens = eff_max_tokens
        self._transport = transport
        self._timeout = eff_timeout
        self._max_retries = max_retries

    @classmethod
    def reasoner(
        cls,
        api_key: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 120.0,
        max_tokens: int = 8192,
        max_retries: int = 2,
    ) -> Self:
        """Instantiate DeepSeek Reasoner (`deepseek-reasoner`) with CoT capture enabled."""
        return cls(
            api_key,
            model="deepseek-reasoner",
            thinking_enabled=True,
            max_tokens=max_tokens,
            transport=transport,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )

    @property
    def request_settings(self) -> JSONRequestSettings:
        return JSONRequestSettings(
            endpoint=self.endpoint,
            timeout_seconds=self._timeout,
            max_retries=self._max_retries,
            max_tokens=self._max_tokens,
            thinking="enabled" if self._thinking_enabled else "disabled",
        )

    @classmethod
    def from_environment(
        cls,
        *,
        dotenv_path: Path | None = None,
        model: str | None = None,
    ) -> Self:
        dotenv_vals = _read_dotenv_values(dotenv_path)
        key = os.environ.get("DEEPSEEK_API_KEY") or dotenv_vals.get("DEEPSEEK_API_KEY")
        if not key:
            raise ModelCallError("Set DEEPSEEK_API_KEY locally; never commit or paste the key")
        eff_model = (
            model
            or os.environ.get("DEEPSEEK_MODEL")
            or dotenv_vals.get("DEEPSEEK_MODEL")
            or "deepseek-reasoner"
        ).strip()
        return cls(key, model=eff_model)

    async def complete_json(self, *, system: str, user: str) -> ModelResponse:
        base_payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "response_format": {"type": "json_object"},
            "max_tokens": self._max_tokens,
            "stream": False,
        }
        if not self._thinking_enabled:
            base_payload["thinking"] = {"type": "disabled"}

        headers = {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
        payload = dict(base_payload)
        dropped_response_format = False
        attempt = 0
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
            while attempt <= self._max_retries:
                try:
                    response = await client.post(self.endpoint, headers=headers, json=payload)
                except (httpx.TimeoutException, httpx.TransportError) as exc:
                    if attempt == self._max_retries:
                        raise ModelCallError(
                            "DeepSeek request failed after bounded retries", retryable=True
                        ) from exc
                else:
                    if (
                        response.status_code == 400
                        and self._thinking_enabled
                        and not dropped_response_format
                    ):
                        # Some upstream endpoints reject json_object on deepseek-reasoner; retry once cleanly.
                        payload = {k: v for k, v in base_payload.items() if k != "response_format"}
                        dropped_response_format = True
                        continue
                    if response.status_code in {408, 429, 500, 502, 503, 504}:
                        if attempt == self._max_retries:
                            raise ModelCallError(
                                f"DeepSeek temporary HTTP {response.status_code}", retryable=True
                            )
                    elif response.is_error:
                        # Avoid logging provider response bodies or credential-bearing headers.
                        raise ModelCallError(f"DeepSeek HTTP {response.status_code}")
                    else:
                        return self._parse(response)
                await asyncio.sleep(min(0.5 * 2**attempt, 2))
                attempt += 1
        raise AssertionError("Unreachable retry loop")

    @staticmethod
    def _parse(response: httpx.Response) -> ModelResponse:
        try:
            raw: Any = response.json()
            first = raw["choices"][0]
            if first["finish_reason"] != "stop":
                raise ModelCallError("DeepSeek response was incomplete")
            msg = first["message"]
            usage = raw["usage"]
            details = usage.get("completion_tokens_details") or {}
            raw_reasoning = msg.get("reasoning_content")
            reasoning_content = (
                str(raw_reasoning).strip()
                if isinstance(raw_reasoning, str) and raw_reasoning.strip()
                else None
            )
            reasoning_tokens = int(details.get("reasoning_tokens") or 0)
            cleaned_content = _strip_markdown_json_fence(str(msg["content"]))
            return ModelResponse(
                content=cleaned_content,
                input_tokens=usage["prompt_tokens"],
                output_tokens=usage["completion_tokens"],
                model_id=raw.get("model"),
                system_fingerprint=raw.get("system_fingerprint"),
                reasoning_content=reasoning_content,
                reasoning_tokens=reasoning_tokens,
            )
        except (KeyError, IndexError, TypeError, ValueError, ValidationError) as exc:
            raise ModelCallError("Malformed or empty DeepSeek response") from exc
