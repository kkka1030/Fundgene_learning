from app.runtime.toolchains.base import AdvisorUserContext, Toolchain
from app.runtime.follow_up_prompts import build_user_follow_up_prompts


class LearningToolchain(Toolchain):
    name = "learning"

    async def summarize(
        self,
        *,
        session_id: str,
        message: str,
        context: AdvisorUserContext | None = None,
    ) -> dict:
        prefix = ""
        if context is not None:
            if context.risk_level == "growth":
                prefix = "结合你目前偏进取的风险承受度，"
            elif context.risk_level == "balanced":
                prefix = "结合你目前偏平衡的风险承受度，"
            elif context.risk_level == "conservative":
                prefix = "结合你目前偏稳健的风险承受度，"

        learning_hint = ""
        recommended_actions = [
            "回到 dashboard，看一下当前风险等级和下一步动作，再带着问题继续问 coach。",
            "把今天最关键的一个概念写成自己的话，确认你真的理解了。",
        ]
        if (
            context is not None
            and context.learning_recommended_course_title
            and context.learning_total_courses
        ):
            learning_hint = (
                f" 你当前学习路径已完成 {context.learning_completed_courses_count or 0}/"
                f"{context.learning_total_courses} 门课，下一步最适合先完成《{context.learning_recommended_course_title}》。"
            )
            recommended_actions[0] = (
                f"进入 Learning，先完成《{context.learning_recommended_course_title}》的下一节内容。"
            )

        answer = (
            f"{prefix}先看清楚基金的三件事：它买什么、波动主要来自哪里、费用会怎样影响长期结果。"
            " 如果你刚开始接触基金，先把风险等级理解成“你能承受多大波动”，而不是收益承诺。"
            f"{learning_hint}"
        )
        follow_up_questions = build_user_follow_up_prompts("learning", message)

        if "回撤" in message or "drawdown" in message.lower():
            answer = (
                "回撤可以理解为“从一段时间高点回落了多少”。它不会直接告诉你基金好坏，"
                "但能帮助你判断自己能否承受账户短期下跌。"
            )
            follow_up_questions = build_user_follow_up_prompts("learning", "回撤")

        return {
            "answer": answer,
            "citations": ["learning_basics_v1"],
            "recommended_actions": recommended_actions,
            "follow_up_questions": follow_up_questions,
        }
