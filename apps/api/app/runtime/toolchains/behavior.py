from app.runtime.toolchains.base import AdvisorUserContext, Toolchain
from app.runtime.follow_up_prompts import build_user_follow_up_prompts


class BehaviorToolchain(Toolchain):
    name = "behavior"

    async def summarize(
        self,
        *,
        session_id: str,
        message: str,
        context: AdvisorUserContext | None = None,
    ) -> dict:
        bias_hint = ""
        if context is not None and context.bias_tags:
            bias_hint = f" 你当前画像里已经出现了“{context.bias_tags[0]}”这个信号，可以把它当成优先复盘对象。"
        training_hint = ""
        recommended_actions = [
            "先记录最近一次想追涨或想止损的情境，写下当时最强烈的情绪。",
            "把这个问题带回 dashboard，看看当前偏差标签和建议动作是否对得上。",
        ]
        if context is not None and context.behavior_training_focus:
            training_hint = f" 当前更适合你的训练方向是“{context.behavior_training_focus}”。"
            if context.simulation_latest_review_summary:
                training_hint += f" 最近一次情境训练提示：{context.simulation_latest_review_summary}"
            recommended_actions[0] = (
                f"进入 Simulation，优先完成“{context.behavior_training_focus}”方向的训练。"
            )

        answer = (
            "行为陪练先不急着下结论，而是先识别你在什么情境下容易被情绪推着走，"
            f"再把这种情绪和投资动作拆开来看。{bias_hint}{training_hint}"
        )
        if "追涨" in message or "chase" in message.lower():
            answer = (
                "追涨常见的问题不是‘看好市场’本身，而是把短期涨幅误当成长期确定性。"
                f" 更稳妥的做法是先问自己：这次冲动来自计划，还是来自害怕错过。{bias_hint}{training_hint}"
            )

        return {
            "answer": answer,
            "citations": ["behavior_bias_v1"],
            "recommended_actions": recommended_actions,
            "follow_up_questions": build_user_follow_up_prompts("behavior", message),
        }
