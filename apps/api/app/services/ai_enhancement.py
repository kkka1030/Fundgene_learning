from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel, Field, field_validator

from app.core.config import get_settings
from app.runtime.model_factory import (
    ModelRuntimeInfo,
    get_model_runtime_info,
    model_credentials_available,
    run_structured_model,
)


FORBIDDEN_MODEL_OUTPUT_TERMS = (
    "保证收益",
    "稳赚",
    "保本收益",
    "无风险收益",
    "一定赚钱",
    "直接买入",
    "立即买入",
    "直接卖出",
    "立即卖出",
    "清仓",
    "满仓",
    "梭哈",
    "自动交易",
    "自动下单",
    "连接券商",
    "连接交易账户",
    "guaranteed return",
    "buy now",
    "sell now",
)


class NewsAnalysisEnhancement(BaseModel):
    facts: list[str] = Field(min_length=2, max_length=6)
    impact_paths: list[str] = Field(min_length=2, max_length=5)
    uncertainty_notes: list[str] = Field(min_length=2, max_length=4)
    beginner_translation: str = Field(min_length=20, max_length=600)
    recommended_next_actions: list[str] = Field(min_length=2, max_length=4)

    @field_validator(
        "facts",
        "impact_paths",
        "uncertainty_notes",
        "recommended_next_actions",
    )
    @classmethod
    def normalize_items(cls, value: list[str]) -> list[str]:
        normalized = [" ".join(item.strip().split()) for item in value if item.strip()]
        if not normalized:
            raise ValueError("list output cannot be empty")
        return normalized

    @field_validator("beginner_translation")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        normalized = " ".join(value.strip().split())
        if not normalized:
            raise ValueError("text output cannot be empty")
        return normalized


class PortfolioAnalysisEnhancement(BaseModel):
    summary: str = Field(min_length=20, max_length=600)
    risk_exposure: list[str] = Field(min_length=1, max_length=4)
    concentration_flags: list[str] = Field(min_length=1, max_length=4)
    allocation_balance: list[str] = Field(min_length=1, max_length=4)
    recommended_next_actions: list[str] = Field(min_length=2, max_length=4)

    @field_validator(
        "risk_exposure",
        "concentration_flags",
        "allocation_balance",
        "recommended_next_actions",
    )
    @classmethod
    def normalize_items(cls, value: list[str]) -> list[str]:
        normalized = [" ".join(item.strip().split()) for item in value if item.strip()]
        if not normalized:
            raise ValueError("list output cannot be empty")
        return normalized

    @field_validator("summary")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        normalized = " ".join(value.strip().split())
        if not normalized:
            raise ValueError("summary cannot be empty")
        return normalized


T = TypeVar("T", bound=BaseModel)


@dataclass(slots=True)
class EnhancementResult:
    status: str
    fallback_reason: str | None = None
    model_info: ModelRuntimeInfo | None = None


@dataclass(slots=True)
class NewsEnhancementResult(EnhancementResult):
    payload: dict[str, Any] | None = None


@dataclass(slots=True)
class PortfolioEnhancementResult(EnhancementResult):
    summary: str | None = None
    risk_exposure: list[str] | None = None
    concentration_flags: list[str] | None = None
    allocation_balance: list[str] | None = None
    recommended_next_actions: list[str] | None = None


def enhance_news_analysis_payload(
    *,
    item_context: dict[str, Any],
    rule_payload: dict[str, Any],
    configured_model_name: str | None = None,
    deepseek_api_key_override: str | None = None,
) -> NewsEnhancementResult:
    settings = get_settings()
    model_name = configured_model_name or settings.advisor_model
    model_info = get_model_runtime_info(
        model_name,
        settings=settings,
        deepseek_api_key_override=deepseek_api_key_override,
    )
    if not _model_enhancement_enabled(
        configured_model_name=model_name,
        deepseek_api_key_override=deepseek_api_key_override,
    ):
        return NewsEnhancementResult(
            status="skipped",
            fallback_reason=(
                "model_disabled"
                if settings.agent_mode.strip().lower() == "deterministic"
                else "model_not_configured"
            ),
            model_info=model_info,
            payload=rule_payload,
        )

    prompt = json.dumps(
        {
            "task": "Enhance an already rule-grounded FundGene news/policy analysis.",
            "item": item_context,
            "rule_payload": rule_payload,
            "constraints": [
                "Keep facts grounded in the item and rule payload only.",
                "Do not create return promises, direct buy/sell instructions, full-position/clear-position language, or automation claims.",
                "Keep the output useful for beginner fund investors.",
                "Return only the structured schema fields.",
            ],
        },
        ensure_ascii=False,
    )
    try:
        model_result = _run_model_enhancement(
            output_type=NewsAnalysisEnhancement,
            prompt=prompt,
            instructions=(
                "You improve FundGene's beginner-safe news analysis text. "
                "You may rewrite and clarify, but you must not add unsupported facts, "
                "trading instructions, return promises, or automation capability."
            ),
            configured_model_name=model_name,
            deepseek_api_key_override=deepseek_api_key_override,
        )
        if isinstance(model_result, tuple):
            enhanced, model_info = model_result
        else:
            enhanced = model_result
    except Exception as exc:
        return NewsEnhancementResult(
            status="fallback",
            fallback_reason=exc.__class__.__name__,
            model_info=model_info,
            payload=rule_payload,
        )

    payload = {
        **rule_payload,
        "facts": _use_if_long_enough(enhanced.facts, rule_payload["facts"], minimum=2),
        "impact_paths": _use_if_long_enough(
            enhanced.impact_paths,
            rule_payload["impact_paths"],
            minimum=2,
        ),
        "uncertainty_notes": _use_if_long_enough(
            enhanced.uncertainty_notes,
            rule_payload["uncertainty_notes"],
            minimum=2,
        ),
        "beginner_translation": enhanced.beginner_translation,
        "recommended_next_actions": _use_if_long_enough(
            enhanced.recommended_next_actions,
            rule_payload["recommended_next_actions"],
            minimum=2,
        ),
    }
    if _contains_forbidden_model_output(payload):
        return NewsEnhancementResult(
            status="fallback",
            fallback_reason="unsafe_model_output",
            model_info=model_info,
            payload=rule_payload,
        )

    return NewsEnhancementResult(status="enhanced", model_info=model_info, payload=payload)


def enhance_portfolio_analysis_text(
    *,
    portfolio_context: dict[str, Any],
    summary: str,
    risk_exposure: list[str],
    concentration_flags: list[str],
    allocation_balance: list[str],
    recommended_next_actions: list[str],
) -> PortfolioEnhancementResult:
    settings = get_settings()
    model_info = get_model_runtime_info(settings.advisor_model, settings=settings)
    if not _model_enhancement_enabled():
        return PortfolioEnhancementResult(
            status="skipped",
            fallback_reason="model_not_configured_or_disabled",
            model_info=model_info,
            summary=summary,
            risk_exposure=risk_exposure,
            concentration_flags=concentration_flags,
            allocation_balance=allocation_balance,
            recommended_next_actions=recommended_next_actions,
        )

    rule_payload = {
        "summary": summary,
        "risk_exposure": risk_exposure,
        "concentration_flags": concentration_flags,
        "allocation_balance": allocation_balance,
        "recommended_next_actions": recommended_next_actions,
    }
    prompt = json.dumps(
        {
            "task": "Enhance an already rule-computed FundGene portfolio report.",
            "portfolio_context": portfolio_context,
            "rule_payload": rule_payload,
            "constraints": [
                "Weights, risk buckets, concentration findings, and total value are already computed. Do not recalculate or contradict them.",
                "Do not create direct trading instructions, return promises, full-position/clear-position language, or automation claims.",
                "Use beginner-safe Chinese explanation text.",
                "Return only the structured schema fields.",
            ],
        },
        ensure_ascii=False,
    )
    try:
        model_result = _run_model_enhancement(
            output_type=PortfolioAnalysisEnhancement,
            prompt=prompt,
            instructions=(
                "You improve FundGene's portfolio report text. The numeric analysis is fixed. "
                "You may make explanations clearer, but you must not invent holdings, "
                "change weights, promise returns, or give direct buy/sell/clear/full-position instructions."
            ),
        )
        if isinstance(model_result, tuple):
            enhanced, model_info = model_result
        else:
            enhanced = model_result
    except Exception as exc:
        return PortfolioEnhancementResult(
            status="fallback",
            fallback_reason=exc.__class__.__name__,
            model_info=model_info,
            summary=summary,
            risk_exposure=risk_exposure,
            concentration_flags=concentration_flags,
            allocation_balance=allocation_balance,
            recommended_next_actions=recommended_next_actions,
        )

    candidate = enhanced.model_dump(mode="json")
    if _contains_forbidden_model_output(candidate):
        return PortfolioEnhancementResult(
            status="fallback",
            fallback_reason="unsafe_model_output",
            model_info=model_info,
            summary=summary,
            risk_exposure=risk_exposure,
            concentration_flags=concentration_flags,
            allocation_balance=allocation_balance,
            recommended_next_actions=recommended_next_actions,
        )

    return PortfolioEnhancementResult(
        status="enhanced",
        model_info=model_info,
        summary=enhanced.summary,
        risk_exposure=_use_if_long_enough(
            enhanced.risk_exposure,
            risk_exposure,
            minimum=1,
        ),
        concentration_flags=_use_if_long_enough(
            enhanced.concentration_flags,
            concentration_flags,
            minimum=1,
        ),
        allocation_balance=_use_if_long_enough(
            enhanced.allocation_balance,
            allocation_balance,
            minimum=1,
        ),
        recommended_next_actions=_use_if_long_enough(
            enhanced.recommended_next_actions,
            recommended_next_actions,
            minimum=2,
        ),
    )


def _model_enhancement_enabled(
    *,
    configured_model_name: str | None = None,
    deepseek_api_key_override: str | None = None,
) -> bool:
    settings = get_settings()
    if settings.agent_mode.strip().lower() == "deterministic":
        return False
    return model_credentials_available(
        configured_model_name or settings.advisor_model,
        settings=settings,
        deepseek_api_key_override=deepseek_api_key_override,
    )


def _run_model_enhancement(
    *,
    output_type: type[T],
    prompt: str,
    instructions: str,
    configured_model_name: str | None = None,
    deepseek_api_key_override: str | None = None,
) -> tuple[T, ModelRuntimeInfo]:
    settings = get_settings()
    model_name = configured_model_name or settings.advisor_model

    async def run() -> tuple[T, ModelRuntimeInfo]:
        output, info = await run_structured_model(
            configured_model_name=model_name,
            output_type=output_type,
            instructions=instructions,
            prompt=prompt,
            timeout_ms=settings.agent_model_timeout_ms,
            settings=settings,
            deepseek_api_key_override=deepseek_api_key_override,
        )
        return output, info

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run())
    raise RuntimeError("event_loop_running")


def _contains_forbidden_model_output(payload: Any) -> bool:
    haystack = json.dumps(payload, ensure_ascii=False).lower()
    return any(term.lower() in haystack for term in FORBIDDEN_MODEL_OUTPUT_TERMS)


def _use_if_long_enough(
    candidate: list[str],
    fallback: list[str],
    *,
    minimum: int,
) -> list[str]:
    normalized = [" ".join(item.strip().split()) for item in candidate if item.strip()]
    if len(normalized) < minimum:
        return list(fallback)
    return normalized
