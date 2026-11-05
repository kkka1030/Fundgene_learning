from app.runtime.toolchains.base import AdvisorUserContext, Toolchain
from app.runtime.follow_up_prompts import build_user_follow_up_prompts


class SimulationToolchain(Toolchain):
    name = "simulation"

    async def summarize(
        self,
        *,
        session_id: str,
        message: str,
        context: AdvisorUserContext | None = None,
    ) -> dict:
        scenario_hint = ""
        if context is not None and context.simulation_recommended_scenario_title:
            scenario_hint = (
                f" 你当前最适合先练的情境是“{context.simulation_recommended_scenario_title}”。"
            )
            if context.simulation_latest_review_summary:
                scenario_hint += f" 最近一次训练复盘提示：{context.simulation_latest_review_summary}"
        return {
            "answer": (
                "历史情境演练的价值不在于猜对涨跌，而在于让你在可解释的市场背景里练习判断和复盘。"
                f"{scenario_hint}"
            ),
            "citations": ["simulation_framework_v1"],
            "recommended_actions": [
                (
                    f"先进入 Simulation，开始“{context.simulation_recommended_scenario_title}”。"
                    if context is not None and context.simulation_recommended_scenario_title
                    else "先从一个波动较容易理解的新手情境开始。"
                ),
                "每完成一步，都把当时的判断理由写下来，便于复盘。",
            ],
            "follow_up_questions": build_user_follow_up_prompts("simulation", message),
        }
