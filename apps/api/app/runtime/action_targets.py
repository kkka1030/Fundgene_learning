from __future__ import annotations

from urllib.parse import urlencode

from app.schemas.assistant import AdvisorActionTarget


INTERNAL_ACTION_ROUTES = {
    "dashboard": "/dashboard",
    "learning": "/learning",
    "portfolio": "/portfolio",
    "simulation": "/simulation",
    "news": "/news",
    "coach": "/coach",
}


def build_recommended_action_targets(
    *,
    intent: str,
    actions: list[str],
) -> list[AdvisorActionTarget]:
    targets: list[AdvisorActionTarget] = []
    for action in actions:
        surface = _surface_for_action(action, fallback_intent=intent)
        if surface is None:
            continue

        targets.append(
            AdvisorActionTarget(
                label=_label_for_surface(surface),
                href=_href_for_surface(surface, action=action),
                intent=surface,
                action_id=f"coach-{surface}",
                reason=_reason_for_surface(surface),
                target_params=_params_for_surface(surface, action=action),
                expected_writeback=_writeback_for_surface(surface),
                safety_note=_safety_note_for_surface(surface),
            )
        )

    return targets


def _surface_for_action(action: str, *, fallback_intent: str) -> str | None:
    normalized = action.lower()
    if "dashboard" in normalized or "工作台" in action:
        return "dashboard"
    if "learning" in normalized or "学习" in action or "课程" in action:
        return "learning"
    if "portfolio" in normalized or "组合" in action or "持仓" in action:
        return "portfolio"
    if "simulation" in normalized or "情境" in action or "训练" in action:
        return "simulation"
    if "news" in normalized or "资讯" in action or "新闻" in action or "政策" in action:
        return "news"
    if "coach" in normalized or "教练" in action or "继续提问" in action:
        return "coach"
    if fallback_intent in INTERNAL_ACTION_ROUTES:
        return fallback_intent
    return "dashboard"


def _label_for_surface(surface: str) -> str:
    labels = {
        "dashboard": "回到工作台",
        "learning": "进入学习中心",
        "portfolio": "查看组合体检",
        "simulation": "开始情境训练",
        "news": "查看资讯解读",
        "coach": "继续追问教练",
    }
    return labels.get(surface, "打开下一步")


def _params_for_surface(surface: str, *, action: str) -> dict[str, str]:
    params = {"from": "coach"}
    if surface == "portfolio":
        params["focus"] = "concentration"
    elif surface == "news":
        params["focus"] = "impact-path"
    elif surface == "simulation":
        params["focus"] = "drawdown-discipline"
    elif surface == "learning":
        params["focus"] = "risk-basics"
    elif surface == "coach":
        params["focus"] = "follow-up"
    elif surface == "dashboard":
        params["focus"] = "daily-brief"

    if "daily-brief" in action.lower() or "简报" in action:
        params["focus"] = "daily-brief"
    return params


def _href_for_surface(surface: str, *, action: str) -> str:
    route = INTERNAL_ACTION_ROUTES[surface]
    params = _params_for_surface(surface, action=action)
    query = urlencode(params)
    return f"{route}?{query}" if query else route


def _reason_for_surface(surface: str) -> str:
    reasons = {
        "dashboard": "回到今日教练简报，把当前回答放回整体判断里看。",
        "learning": "用一节基础内容先补足理解，再回到组合或新闻解释。",
        "portfolio": "先检查组合风险来源，而不是直接把回答理解成账户动作。",
        "simulation": "用历史情境训练判断顺序，观察冲动触发点。",
        "news": "先看新闻如何触达你的组合、画像或学习缺口。",
        "coach": "带着当前上下文继续追问，避免重新描述背景。",
    }
    return reasons.get(surface, "继续完成一个低风险、可解释的站内动作。")


def _writeback_for_surface(surface: str) -> str:
    writebacks = {
        "dashboard": "daily_brief_context",
        "learning": "user_course_progress",
        "portfolio": "portfolio_snapshots/portfolio_analyses",
        "simulation": "simulation_sessions/simulation_reviews",
        "news": "news_analyses/agent_citations",
        "coach": "chat_messages/agent_runs",
    }
    return writebacks.get(surface, "agent_trace")


def _safety_note_for_surface(surface: str) -> str:
    notes = {
        "dashboard": "工作台只组织理解顺序，不给账户操作指令。",
        "learning": "学习动作帮助理解概念，不代表投资建议。",
        "portfolio": "组合体检用于解释风险来源，不输出买卖或仓位指令。",
        "simulation": "情境训练是练习，不预测未来市场。",
        "news": "新闻解读用于理解影响路径，不构成交易信号。",
        "coach": "继续追问只用于解释证据和边界，不触发账户操作。",
    }
    return notes.get(surface, "该动作只用于理解、检查或训练。")
