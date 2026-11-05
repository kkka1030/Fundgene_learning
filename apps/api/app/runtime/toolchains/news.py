from app.runtime.toolchains.base import AdvisorUserContext, Toolchain
from app.runtime.follow_up_prompts import build_user_follow_up_prompts


class NewsToolchain(Toolchain):
    name = "news"

    async def summarize(
        self,
        *,
        session_id: str,
        message: str,
        context: AdvisorUserContext | None = None,
    ) -> dict:
        analysis_hint = ""
        recommended_actions = [
            "先判断这是宏观、政策还是基金本身层面的信息。",
            "先评估影响周期，再决定要不要继续跟踪。",
        ]
        citations = ["news_interpretation_v1"]
        if context is not None and context.news_has_analysis:
            analysis_hint = (
                f" 最近一条真实新闻/政策分析来自“{context.news_latest_title}”："
                f"{context.news_latest_beginner_translation}"
            )
            recommended_actions = [
                context.news_latest_recommended_action
                or "先回到 News，查看最近一份新闻/政策结构化分析。",
                "继续追问这条信息通过什么路径影响你持有或正在学习的基金类型。",
            ]
            if context.news_latest_analysis_id:
                citations = [f"news_analysis:{context.news_latest_analysis_id}"]

        return {
            "answer": (
                "新闻解读先分清事实、影响路径和不确定性，不要把标题情绪直接当成投资结论。"
                f"{analysis_hint}"
            ),
            "citations": citations,
            "recommended_actions": recommended_actions,
            "follow_up_questions": build_user_follow_up_prompts("news", message),
        }
