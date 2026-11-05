from __future__ import annotations


def build_user_follow_up_prompts(intent: str, message: str = "") -> list[str]:
    """Return prompt chips as the user's next message, not the coach's question."""
    normalized_intent = intent.strip().lower()
    normalized_message = message.lower()

    if normalized_intent == "portfolio":
        return [
            "帮我检查组合里最需要先看的风险。",
            "帮我判断我的持仓是不是太集中。",
        ]

    if normalized_intent == "behavior":
        return [
            "帮我拆解我追涨时的触发点。",
            "帮我做一个本周可执行的行为复盘。",
        ]

    if normalized_intent == "simulation":
        return [
            "帮我选择一个适合新手的情境训练。",
            "把这次训练复盘成下一次检查清单。",
        ]

    if normalized_intent == "news":
        return [
            "把这条信息拆成事实、影响路径和不确定性。",
            "帮我判断它和我的基金类型有什么关系。",
        ]

    if "回撤" in normalized_message or "drawdown" in normalized_message:
        return [
            "我想用一个数字例子理解回撤。",
            "帮我判断我能承受多大回撤。",
        ]

    if "风险等级" in normalized_message or "risk" in normalized_message:
        return [
            "帮我把风险等级翻译成新手能懂的话。",
            "帮我判断这个风险等级和我的承受能力是否匹配。",
        ]

    return [
        "帮我把这个概念讲成一个新手例子。",
        "我学完这个概念后下一步该练什么？",
    ]
