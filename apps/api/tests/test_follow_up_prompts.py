from app.runtime.follow_up_prompts import build_user_follow_up_prompts


def test_learning_drawdown_follow_ups_are_user_prompts() -> None:
    prompts = build_user_follow_up_prompts("learning", "怎么理解风险等级和回撤？")

    assert prompts == [
        "我想用一个数字例子理解回撤。",
        "帮我判断我能承受多大回撤。",
    ]


def test_domain_follow_ups_are_actionable_user_prompts() -> None:
    prompts = build_user_follow_up_prompts("portfolio")

    assert prompts == [
        "帮我检查组合里最需要先看的风险。",
        "帮我判断我的持仓是不是太集中。",
    ]


def test_follow_ups_do_not_use_coach_question_voice() -> None:
    prompts = [
        prompt
        for intent in ("learning", "portfolio", "behavior", "simulation", "news")
        for prompt in build_user_follow_up_prompts(intent, "风险等级和回撤")
    ]

    blocked_prefixes = ("你想", "你希望我", "要不要我", "要不要把", "要不要在")
    assert all(not prompt.startswith(blocked_prefixes) for prompt in prompts)
