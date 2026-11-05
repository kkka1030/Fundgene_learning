from app.runtime.toolchains.base import AdvisorUserContext, Toolchain
from app.runtime.follow_up_prompts import build_user_follow_up_prompts


class PortfolioToolchain(Toolchain):
    name = "portfolio"

    async def summarize(
        self,
        *,
        session_id: str,
        message: str,
        context: AdvisorUserContext | None = None,
    ) -> dict:
        goal_hint = ""
        if context is not None and context.primary_goal:
            goal_hint = f" 你的当前目标是“{context.primary_goal}”，组合解释也要围绕这个目标来判断。"
        report_hint = ""
        recommended_actions = [
            "先手工录入当前持仓快照，不要跳过原始输入。",
            "先检查集中度和风险桶分布，再讨论是否需要调整。",
        ]
        if context is not None and context.portfolio_has_report:
            report_hint = (
                f" 你最近一份组合报告显示：{context.portfolio_latest_summary}"
            )
            recommended_actions = [
                "先回到 Portfolio 查看最近一份组合报告，再围绕它继续提问。",
                "优先检查最大仓位和前两大持仓是否承担了重复风险。",
            ]
        return {
            "answer": (
                "组合分析先看三件事：是否过度集中、不同基金是否承担了重复风险、以及整体风险是否和你的目标匹配。"
                f"{goal_hint}{report_hint}"
            ),
            "citations": ["portfolio_principles_v1"],
            "recommended_actions": recommended_actions,
            "follow_up_questions": build_user_follow_up_prompts("portfolio", message),
        }
