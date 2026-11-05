from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any

from app.runtime.model_factory import (
    build_model,
    get_model_runtime_info,
    model_credentials_available,
)
from app.schemas.assistant import AdvisorActionTarget
from app.runtime.v2.schemas import PolicyResult, WorkerOutput
from app.schemas.assistant import AdvisorResponse

try:
    from pydantic_ai import Agent, PromptedOutput
except Exception:  # pragma: no cover - optional dependency bootstrap
    Agent = None
    PromptedOutput = None


@dataclass(slots=True)
class ComposeResult:
    response: AdvisorResponse
    metadata: dict[str, Any] = field(default_factory=dict)
    fallback_reason: str | None = None


class ResponseComposer:
    def __init__(
        self,
        *,
        model_name: str,
        agent_mode: str,
        timeout_ms: int,
        risk_notice: str,
        deepseek_api_key_override: str | None = None,
    ) -> None:
        self.model_name = model_name
        self.agent_mode = agent_mode.strip().lower()
        self.timeout_ms = max(1000, timeout_ms)
        self.risk_notice = risk_notice
        self.deepseek_api_key_override = deepseek_api_key_override
        self.model_info = get_model_runtime_info(
            model_name,
            deepseek_api_key_override=deepseek_api_key_override,
        )

    async def compose(
        self,
        *,
        intent: str,
        worker_output: WorkerOutput,
        input_policy: PolicyResult,
        deterministic_response: AdvisorResponse,
    ) -> ComposeResult:
        if input_policy.status in {"block_with_guidance", "runtime_repair"}:
            return ComposeResult(
                response=deterministic_response,
                metadata={
                    "composer_mode": "deterministic",
                    "reason": input_policy.status,
                    **self.model_info.metadata(),
                },
            )

        if self.agent_mode == "deterministic":
            return ComposeResult(
                response=deterministic_response,
                metadata={
                    "composer_mode": "deterministic",
                    "reason": "configured",
                    **self.model_info.metadata(),
                },
            )

        if self.agent_mode not in {"hybrid", "model"}:
            return ComposeResult(
                response=deterministic_response,
                metadata={
                    "composer_mode": "deterministic_fallback",
                    "reason": f"unsupported_agent_mode:{self.agent_mode}",
                    "fallback_reason": f"unsupported_agent_mode:{self.agent_mode}",
                    **self.model_info.metadata(),
                },
                fallback_reason=f"unsupported_agent_mode:{self.agent_mode}",
            )

        if Agent is None or PromptedOutput is None:
            return ComposeResult(
                response=deterministic_response,
                metadata={
                    "composer_mode": "deterministic_fallback",
                    "reason": "pydantic_ai_unavailable",
                    "fallback_reason": "pydantic_ai_unavailable",
                    **self.model_info.metadata(),
                },
                fallback_reason="pydantic_ai_unavailable",
            )

        if self.agent_mode in {"hybrid", "model"} and not model_credentials_available(
            self.model_name,
            deepseek_api_key_override=self.deepseek_api_key_override,
        ):
            return ComposeResult(
                response=self._build_model_not_configured_response(intent=intent),
                metadata={
                    "composer_mode": "model_unconfigured",
                    "reason": "model_not_configured",
                    "fallback_reason": "model_not_configured",
                    **self.model_info.metadata(),
                },
                fallback_reason="model_not_configured",
            )

        try:
            response = await self._run_model_composer(
                intent=intent,
                worker_output=worker_output,
                input_policy=input_policy,
                deterministic_response=deterministic_response,
            )
        except Exception as exc:
            return ComposeResult(
                response=deterministic_response,
                metadata={
                    "composer_mode": "deterministic_fallback",
                    "reason": exc.__class__.__name__,
                    "fallback_reason": str(exc),
                    **self.model_info.metadata(),
                },
                fallback_reason=str(exc),
            )

        direct_fallback_reason = self._direct_answer_fallback_reason(
            worker_output=worker_output,
            response=response,
        )
        if direct_fallback_reason is not None:
            return ComposeResult(
                response=deterministic_response,
                metadata={
                    "composer_mode": "deterministic_fallback",
                    "reason": direct_fallback_reason,
                    "fallback_reason": direct_fallback_reason,
                    **self.model_info.metadata(),
                },
                fallback_reason=direct_fallback_reason,
            )

        news_fallback_reason = self._news_concrete_items_fallback_reason(
            intent=intent,
            worker_output=worker_output,
            response=response,
        )
        if news_fallback_reason is not None:
            return ComposeResult(
                response=deterministic_response,
                metadata={
                    "composer_mode": "deterministic_fallback",
                    "reason": news_fallback_reason,
                    "fallback_reason": news_fallback_reason,
                    **self.model_info.metadata(),
                },
                fallback_reason=news_fallback_reason,
            )

        return ComposeResult(
            response=response,
            metadata={
                "composer_mode": "model",
                "schema_version": "assistant_message_v1",
                **self.model_info.metadata(),
            },
        )

    def _direct_answer_fallback_reason(
        self,
        *,
        worker_output: WorkerOutput,
        response: AdvisorResponse,
    ) -> str | None:
        finding_text = " ".join(worker_output.findings)
        marker = "直接回答："
        if marker not in finding_text:
            return None

        direct_answer = finding_text.split(marker, 1)[1].split("。", 1)[0]
        anchors = [
            value.strip(" “”《》")
            for value in re.split(r"[；，、:：()（）/]", direct_answer)
            if len(value.strip(" “”《》")) >= 3
        ]
        if not anchors:
            return None

        normalized_answer = response.answer.casefold()
        if any(anchor.casefold() in normalized_answer for anchor in anchors):
            return None

        return "direct_answer_dropped"

    def _news_concrete_items_fallback_reason(
        self,
        *,
        intent: str,
        worker_output: WorkerOutput,
        response: AdvisorResponse,
    ) -> str | None:
        if intent != "news":
            return None

        finding_text = " ".join(worker_output.findings)
        quoted_titles = [
            title.strip()
            for title in re.findall(r"“([^”]{6,180})”", finding_text)
            if title.strip()
        ]
        if not quoted_titles:
            return None

        normalized_answer = response.answer.casefold()
        if any(title.casefold() in normalized_answer for title in quoted_titles):
            return None

        return "news_concrete_items_dropped"

    async def _run_model_composer(
        self,
        *,
        intent: str,
        worker_output: WorkerOutput,
        input_policy: PolicyResult,
        deterministic_response: AdvisorResponse,
    ) -> AdvisorResponse:
        if Agent is None or PromptedOutput is None:  # pragma: no cover - guarded above
            raise RuntimeError("pydantic_ai_unavailable")

        model, _info = build_model(
            self.model_name,
            deepseek_api_key_override=self.deepseek_api_key_override,
        )
        agent = Agent(
            model,
            output_type=PromptedOutput(
                AdvisorResponse,
                template=(
                    "Always respond with one valid JSON object compatible with this schema. "
                    "Do not include Markdown fences, comments, or surrounding prose.\n\n{schema}"
                ),
            ),
            instructions=(
                "You are FundGene's final response composer for beginner fund-investing education. "
                "Use only the provided worker findings and evidence. Do not add trade execution, "
                "return promises, broker/account connection instructions, or unsupported facts. "
                "If the input policy is allow, answer the user's learning question directly; "
                "do not lead with refusal or boundary-only wording. Reserve refusal wording for "
                "block_with_guidance or runtime_repair policy states. "
                "For allowed questions, the answer field must start with the substantive explanation, "
                "not with phrases like '这个问题只能作为...', '不能被理解为...', '不构成...', "
                "'FundGene 提供的是...', or other disclaimers. Keep disclaimer text only in risk_notice. "
                "Keep Chinese answers beginner-friendly, concise, and action-oriented. "
                "If a worker finding contains '直接回答：', preserve that direct answer at "
                "the start of the user-facing answer before adding explanation or safety context. "
                "For news intent, preserve concrete news or policy titles from worker findings; "
                "when the user asks what news exists today or recently, list the available "
                "headlines first before teaching the interpretation method. "
                "Write follow_up_questions as user-voiced prompts the beginner can click and send, "
                "such as '我应该先理解风险等级还是回撤？'. Never write coach-voiced prompts like "
                "'你想...？' or '要不要我...？'."
            ),
        )
        prompt = json.dumps(
            {
                "intent": intent,
                "worker_output": worker_output.model_dump(mode="json"),
                "input_policy": input_policy.model_dump(mode="json"),
                "required_risk_notice": self.risk_notice,
                "fallback_response": deterministic_response.model_dump(mode="json"),
                "output_contract": {
                    "answer": "Chinese beginner-facing explanation grounded in worker findings",
                    "answer_for_allow_policy": (
                        "Start by explaining the requested concept directly. Do not put safety "
                        "boundary language in the answer when input_policy.status is allow."
                    ),
                    "intent": intent,
                    "citations": "reuse evidence citation keys where possible",
                    "risk_notice": self.risk_notice,
                    "recommended_actions": "safe in-product actions only",
                    "follow_up_questions": (
                        "1-2 useful beginner follow-ups phrased from the user's perspective, "
                        "ready to click/send. Use '我...' or '请...' wording, not '你想...' "
                        "or '要不要我...'."
                    ),
                },
            },
            ensure_ascii=False,
        )
        result = await asyncio.wait_for(
            agent.run(
                prompt,
                output_type=PromptedOutput(
                    AdvisorResponse,
                    template=(
                        "Always respond with one valid JSON object compatible with this schema. "
                        "Do not include Markdown fences, comments, or surrounding prose.\n\n{schema}"
                    ),
                ),
            ),
            timeout=self.timeout_ms / 1000,
        )
        raw_output = getattr(result, "output", None)
        if raw_output is None:
            raw_output = getattr(result, "data", None)
        response = AdvisorResponse.model_validate(raw_output)

        citations = response.citations or deterministic_response.citations
        recommended_actions = (
            response.recommended_actions or deterministic_response.recommended_actions
        )
        follow_up_questions = (
            response.follow_up_questions or deterministic_response.follow_up_questions
        )
        return response.model_copy(
            update={
                "intent": intent,
                "citations": citations,
                "risk_notice": self.risk_notice,
                "recommended_actions": recommended_actions,
                "follow_up_questions": follow_up_questions,
            }
        )

    def _build_model_not_configured_response(self, *, intent: str) -> AdvisorResponse:
        action = "到资料中心配置模型 API。"
        return AdvisorResponse(
            answer=(
                "当前还没有接入可用的 LLM API，所以我不能把这次内容伪装成模型回答。"
                "请先到「资料中心 -> 模型设置」配置 DeepSeek API key，或让部署环境配置工作区 key；"
                "配置完成后再发送同一个问题，我会用真实模型回答。"
            ),
            intent=intent,
            citations=[],
            risk_notice=self.risk_notice,
            recommended_actions=[action],
            recommended_action_targets=[
                AdvisorActionTarget(
                    label="打开模型设置",
                    href="/profile?focus=llm-settings",
                    intent="profile",
                    action_id="coach-profile-llm-settings",
                    reason="先补齐模型连接，避免把规则模板误看成真实模型回答。",
                    target_params={"from": "coach", "focus": "llm-settings"},
                    expected_writeback="user_llm_settings",
                    safety_note="模型设置只影响回答生成，不会触发账户操作。",
                )
            ],
            follow_up_questions=["我已经配置好 API key，请重新回答刚才的问题。"],
        )
