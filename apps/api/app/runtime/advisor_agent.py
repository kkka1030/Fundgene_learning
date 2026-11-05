from dataclasses import dataclass, field

from app.runtime.follow_up_prompts import build_user_follow_up_prompts
from app.runtime.action_targets import build_recommended_action_targets
from app.runtime.toolchains import (
    BehaviorToolchain,
    LearningToolchain,
    NewsToolchain,
    PortfolioToolchain,
    SimulationToolchain,
)
from app.runtime.toolchains.base import AdvisorUserContext
from app.schemas.assistant import AdvisorResponse

try:
    from pydantic_ai import Agent  # type: ignore
except Exception:  # pragma: no cover - optional during bootstrap
    Agent = None


@dataclass(slots=True)
class AdvisorAgentRuntime:
    model_name: str
    agent_mode: str = "hybrid"
    model_timeout_ms: int = 8000
    runtime_flags: str = "default"
    max_tool_calls: int = 8
    max_workers: int = 3
    toolchains: dict[str, object] = field(init=False)
    agent: object | None = field(init=False)

    def __post_init__(self) -> None:
        self.toolchains = {
            "learning": LearningToolchain(),
            "portfolio": PortfolioToolchain(),
            "behavior": BehaviorToolchain(),
            "simulation": SimulationToolchain(),
            "news": NewsToolchain(),
        }
        if Agent is None:
            self.agent = None
            return

        try:
            self.agent = Agent(self.model_name)
        except Exception:  # pragma: no cover - optional bootstrap dependency
            self.agent = None

    async def run(
        self,
        *,
        session_id: str,
        message: str,
        user_context: AdvisorUserContext | None = None,
    ) -> "AdvisorRunResult":
        intent = self._detect_intent(message)
        toolchain = self.toolchains[intent]

        try:
            tool_result = await toolchain.summarize(
                session_id=session_id,
                message=message,
                context=user_context,
            )
            response = self._build_response(intent=intent, tool_result=tool_result)
            return AdvisorRunResult(
                response=response,
                intent=intent,
                run_status="completed",
                tool_trace={
                    "intent": intent,
                    "toolchain": toolchain.name,
                    "context": self._serialize_user_context(user_context),
                },
            )
        except Exception as exc:  # pragma: no cover - defensive fallback
            response = self._build_fallback_response(
                message=message, user_context=user_context
            )
            return AdvisorRunResult(
                response=response,
                intent=intent,
                run_status="fallback",
                tool_trace={
                    "intent": intent,
                    "toolchain": toolchain.name,
                    "context": self._serialize_user_context(user_context),
                    "fallback": True,
                },
                fallback_reason=str(exc),
            )

    def _build_response(self, *, intent: str, tool_result: dict) -> AdvisorResponse:
        recommended_actions = tool_result["recommended_actions"]
        return AdvisorResponse(
            answer=tool_result["answer"],
            intent=intent,
            citations=tool_result["citations"],
            risk_notice="这次回答用于帮你理解风险和整理下一步，不代表收益承诺，也不会替你买卖或下单。",
            recommended_actions=recommended_actions,
            recommended_action_targets=build_recommended_action_targets(
                intent=intent,
                actions=recommended_actions,
            ),
            follow_up_questions=tool_result["follow_up_questions"],
        )

    def _build_fallback_response(
        self,
        *,
        message: str,
        user_context: AdvisorUserContext | None = None,
    ) -> AdvisorResponse:
        prefix = ""
        if user_context is not None:
            prefix = f"{user_context.display_name}，先别急着下结论。"

        recommended_actions = [
            "先回到 dashboard，确认当前风险等级和下一步动作。",
            "把你的问题换成一个更具体的场景，再继续提问。",
        ]
        return AdvisorResponse(
            answer=(
                f"{prefix} 这个问题建议先拆成“基金在解释什么风险”“这个风险和你的承受能力是否匹配”两步来看。"
            ).strip(),
            intent="learning",
            citations=["fallback_coach_v1"],
            risk_notice="这次回答用于帮你理解风险和整理下一步，不代表收益承诺，也不会替你买卖或下单。",
            recommended_actions=recommended_actions,
            recommended_action_targets=build_recommended_action_targets(
                intent="learning",
                actions=recommended_actions,
            ),
            follow_up_questions=build_user_follow_up_prompts("learning", message),
        )

    def _serialize_user_context(
        self, user_context: AdvisorUserContext | None
    ) -> dict | None:
        if user_context is None:
            return None
        return {
            "user_id": user_context.user_id,
            "display_name": user_context.display_name,
            "investing_experience": user_context.investing_experience,
            "primary_goal": user_context.primary_goal,
            "risk_level": user_context.risk_level,
            "bias_tags": user_context.bias_tags,
            "learning_progress_percentage": user_context.learning_progress_percentage,
            "learning_completed_courses_count": user_context.learning_completed_courses_count,
            "learning_total_courses": user_context.learning_total_courses,
            "learning_recommended_course_slug": user_context.learning_recommended_course_slug,
            "learning_recommended_course_title": user_context.learning_recommended_course_title,
            "portfolio_has_report": user_context.portfolio_has_report,
            "portfolio_latest_summary": user_context.portfolio_latest_summary,
            "portfolio_latest_snapshot_date": (
                user_context.portfolio_latest_snapshot_date.isoformat()
                if user_context.portfolio_latest_snapshot_date
                else None
            ),
            "behavior_training_focus": user_context.behavior_training_focus,
            "behavior_training_guidance": user_context.behavior_training_guidance,
            "simulation_recommended_scenario_slug": user_context.simulation_recommended_scenario_slug,
            "simulation_recommended_scenario_title": user_context.simulation_recommended_scenario_title,
            "simulation_completed_sessions_count": user_context.simulation_completed_sessions_count,
            "simulation_latest_review_summary": user_context.simulation_latest_review_summary,
            "news_has_analysis": user_context.news_has_analysis,
            "news_latest_analysis_id": user_context.news_latest_analysis_id,
            "news_latest_title": user_context.news_latest_title,
            "news_latest_source_name": user_context.news_latest_source_name,
            "news_latest_beginner_translation": user_context.news_latest_beginner_translation,
            "news_latest_recommended_action": user_context.news_latest_recommended_action,
        }

    def _detect_intent(self, message: str) -> str:
        normalized = message.lower()
        if any(
            keyword in normalized
            for keyword in (
                "portfolio",
                "allocation",
                "holding",
                "holdings",
                "仓位",
                "持仓",
                "配置",
                "集中",
            )
        ):
            return "portfolio"
        if any(
            keyword in normalized
            for keyword in (
                "behavior",
                "bias",
                "emotion",
                "panic",
                "追涨",
                "偏差",
                "情绪",
                "恐慌",
                "冲动",
            )
        ):
            return "behavior"
        if any(
            keyword in normalized
            for keyword in (
                "simulation",
                "scenario",
                "情景",
                "情境",
                "演练",
                "复盘",
            )
        ):
            return "simulation"
        if any(
            keyword in normalized
            for keyword in (
                "news",
                "policy",
                "macro",
                "新闻",
                "政策",
                "宏观",
                "加息",
            )
        ):
            return "news"
        return "learning"


@dataclass(slots=True)
class AdvisorRunResult:
    response: AdvisorResponse
    intent: str
    run_status: str
    tool_trace: dict | None = None
    fallback_reason: str | None = None
