from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from typing import Any

from app.core.config import Settings, get_settings

try:
    from pydantic_ai import Agent
    from pydantic_ai.models.openai import OpenAIChatModel, OpenAIChatModelSettings
    from pydantic_ai.providers.deepseek import DeepSeekProvider
    from pydantic_ai.providers.openai import OpenAIProvider
except Exception:  # pragma: no cover - optional dependency bootstrap
    Agent = None
    OpenAIChatModel = None
    OpenAIChatModelSettings = None
    DeepSeekProvider = None
    OpenAIProvider = None


@dataclass(slots=True, frozen=True)
class ModelRuntimeInfo:
    configured_model_name: str
    provider: str
    provider_model_name: str
    credentials_available: bool
    deepseek_thinking: str | None = None
    deepseek_reasoning_effort: str | None = None

    def metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "model_name": self.configured_model_name,
            "provider": self.provider,
        }
        if self.deepseek_thinking is not None:
            metadata["deepseek_thinking"] = self.deepseek_thinking
        if self.deepseek_reasoning_effort is not None:
            metadata["deepseek_reasoning_effort"] = self.deepseek_reasoning_effort
        return metadata


def split_model_name(configured_model_name: str) -> tuple[str, str]:
    normalized = configured_model_name.strip()
    if ":" not in normalized:
        return "pydantic_ai", normalized
    provider, model_name = normalized.split(":", 1)
    return provider.strip().lower(), model_name.strip()


def get_model_runtime_info(
    configured_model_name: str,
    *,
    settings: Settings | None = None,
    deepseek_api_key_override: str | None = None,
) -> ModelRuntimeInfo:
    settings = settings or get_settings()
    provider, provider_model_name = split_model_name(configured_model_name)
    credentials_available = model_credentials_available(
        configured_model_name,
        settings=settings,
        deepseek_api_key_override=deepseek_api_key_override,
    )
    return ModelRuntimeInfo(
        configured_model_name=configured_model_name,
        provider=provider,
        provider_model_name=provider_model_name,
        credentials_available=credentials_available,
        deepseek_thinking=(
            settings.deepseek_thinking if provider == "deepseek" else None
        ),
        deepseek_reasoning_effort=(
            settings.deepseek_reasoning_effort
            if provider == "deepseek" and settings.deepseek_thinking == "enabled"
            else None
        ),
    )


def model_credentials_available(
    configured_model_name: str,
    *,
    settings: Settings | None = None,
    deepseek_api_key_override: str | None = None,
) -> bool:
    settings = settings or get_settings()
    provider, _ = split_model_name(configured_model_name)
    if provider == "deepseek":
        return bool(deepseek_api_key_override or settings.resolved_deepseek_api_key)
    if provider == "openai":
        return bool(os.getenv("OPENAI_API_KEY"))
    if provider == "anthropic":
        return bool(os.getenv("ANTHROPIC_API_KEY"))
    if provider in {"gemini", "google"}:
        return bool(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))
    return True


def build_model(
    configured_model_name: str,
    *,
    settings: Settings | None = None,
    deepseek_api_key_override: str | None = None,
) -> tuple[Any, ModelRuntimeInfo]:
    settings = settings or get_settings()
    info = get_model_runtime_info(
        configured_model_name,
        settings=settings,
        deepseek_api_key_override=deepseek_api_key_override,
    )

    if Agent is None or OpenAIChatModel is None:
        raise RuntimeError("pydantic_ai_unavailable")

    if info.provider == "deepseek":
        if DeepSeekProvider is None or OpenAIChatModelSettings is None:
            raise RuntimeError("deepseek_provider_unavailable")
        api_key = deepseek_api_key_override or settings.resolved_deepseek_api_key
        if not api_key:
            raise RuntimeError("model_not_configured")
        return (
            OpenAIChatModel(
                info.provider_model_name,
                provider=DeepSeekProvider(api_key=api_key),
                settings=_deepseek_model_settings(settings),
            ),
            info,
        )

    if info.provider == "openai":
        if OpenAIProvider is None:
            raise RuntimeError("openai_provider_unavailable")
        return (
            OpenAIChatModel(
                info.provider_model_name,
                provider=OpenAIProvider(),
            ),
            info,
        )

    return configured_model_name, info


async def run_structured_model(
    *,
    configured_model_name: str,
    output_type: type[Any],
    instructions: str,
    prompt: str,
    timeout_ms: int,
    settings: Settings | None = None,
    deepseek_api_key_override: str | None = None,
) -> tuple[Any, ModelRuntimeInfo]:
    if Agent is None:
        raise RuntimeError("pydantic_ai_unavailable")

    model, info = build_model(
        configured_model_name,
        settings=settings,
        deepseek_api_key_override=deepseek_api_key_override,
    )
    agent = Agent(
        model,
        output_type=output_type,
        instructions=instructions,
    )
    result = await asyncio.wait_for(
        agent.run(prompt, output_type=output_type),
        timeout=max(1000, timeout_ms) / 1000,
    )
    raw_output = getattr(result, "output", None)
    if raw_output is None:
        raw_output = getattr(result, "data", None)
    return output_type.model_validate(raw_output), info


def _deepseek_model_settings(settings: Settings) -> dict[str, Any]:
    extra_body: dict[str, Any] = {
        "thinking": {"type": settings.deepseek_thinking},
    }
    if settings.deepseek_thinking == "enabled":
        extra_body["reasoning_effort"] = settings.deepseek_reasoning_effort
    return OpenAIChatModelSettings(extra_body=extra_body)
