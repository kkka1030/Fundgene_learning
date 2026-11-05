from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.chat_message import ChatMessage
from app.models.chat_session import ChatSession
from app.models.user import UserProfile
from app.schemas.dashboard import (
    DashboardCoachActivity,
    DashboardDailyBrief,
    DashboardEvidence,
    DashboardLearningStatus,
    DashboardNewsStatus,
    DashboardPortfolioStatus,
    DashboardResponse,
    DashboardSimulationStatus,
    DashboardSummaryCard,
    SafeNextAction,
)
from app.services.behavior import (
    get_behavior_profile,
    get_behavior_training_plan,
    get_latest_questionnaire,
)
from app.services.learning import get_learning_overview
from app.services.news import get_news_overview
from app.services.portfolio import get_portfolio_overview
from app.services.simulation import get_latest_simulation_overview


def _truncate(text: str, *, limit: int = 96) -> str:
    normalized = " ".join(text.strip().split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit].rstrip()}..."


def _safe_action(
    *,
    action_id: str,
    action_type: str,
    label: str,
    reason: str,
    target_route: str,
    expected_writeback: str,
    safety_note: str,
    target_params: dict[str, str] | None = None,
) -> SafeNextAction:
    return SafeNextAction(
        id=action_id,
        type=action_type,  # type: ignore[arg-type]
        label=label,
        reason=reason,
        target_route=target_route,
        target_params=target_params or {},
        expected_writeback=expected_writeback,
        safety_note=safety_note,
    )


def _base_source_coverage() -> dict[str, bool]:
    return {
        "profile": False,
        "portfolio": False,
        "news_policy": False,
        "learning": False,
        "simulation": False,
        "behavior": False,
        "coach_history": False,
    }


def _first_action_by_route(route: str, *, actions: list[SafeNextAction]) -> SafeNextAction:
    for action in actions:
        if action.target_route == route:
            return action
    return actions[0]


def _prioritize_evidence(
    evidence: list[DashboardEvidence],
    *,
    primary_source: str,
) -> list[DashboardEvidence]:
    source_order = {
        primary_source: 0,
        "portfolio": 1,
        "news_policy": 2,
        "profile": 3,
        "behavior": 4,
        "learning": 5,
        "simulation": 6,
        "coach_history": 7,
    }
    return sorted(
        evidence,
        key=lambda item: (
            source_order.get(item.source_type, 99),
            evidence.index(item),
        ),
    )[:3]


def build_daily_brief(
    *,
    user: UserProfile,
    risk_level: str | None,
    bias_tags: list[str],
    latest_questionnaire,
    learning_overview: DashboardLearningStatus,
    portfolio_overview: DashboardPortfolioStatus,
    simulation_overview: DashboardSimulationStatus,
    news_overview: DashboardNewsStatus | None,
    latest_coach_activity: DashboardCoachActivity | None,
) -> DashboardDailyBrief:
    coverage = _base_source_coverage()
    evidence: list[DashboardEvidence] = []
    actions = [
        _safe_action(
            action_id="learn-risk-basics",
            action_type="learn",
            label="先补一节风险基础",
            reason="先把回撤、波动和风险承受说清楚，后续判断会更稳。",
            target_route="/learning",
            target_params={"from": "dashboard", "focus": "risk-basics"},
            expected_writeback="user_course_progress",
            safety_note="学习动作只帮助理解风险，不代表任何账户操作指令。",
        ),
        _safe_action(
            action_id="inspect-portfolio-concentration",
            action_type="inspect_portfolio",
            label="检查组合集中度",
            reason="组合结构会决定新闻和波动与你的关系，先看风险来源。",
            target_route="/portfolio",
            target_params={"from": "dashboard", "focus": "concentration"},
            expected_writeback="portfolio_snapshots/portfolio_analyses",
            safety_note="组合体检用于解释风险来源，不输出账户操作指令。",
        ),
        _safe_action(
            action_id="run-drawdown-simulation",
            action_type="run_simulation",
            label="做一次回撤纪律训练",
            reason="历史情境能观察你在波动前的判断顺序，而不是只看结果。",
            target_route="/simulation",
            target_params={"from": "dashboard", "focus": "drawdown-discipline"},
            expected_writeback="simulation_sessions/simulation_reviews",
            safety_note="情境训练是练习，不是对未来市场的预测。",
        ),
        _safe_action(
            action_id="ask-coach-brief",
            action_type="ask_coach",
            label="追问教练当前判断",
            reason="如果证据还不清楚，先让教练用你的上下文解释。",
            target_route="/coach",
            target_params={"from": "dashboard", "focus": "daily-brief"},
            expected_writeback="chat_messages/agent_runs",
            safety_note="追问用于理解证据和边界，不会触发账户操作。",
        ),
    ]

    if not user.onboarding_completed or latest_questionnaire is None:
        profile_action = _safe_action(
            action_id="complete-profile-baseline",
            action_type="record_behavior",
            label="先建立画像",
            reason="没有画像时，教练不能安全判断新闻、组合和行为证据与你的关系。",
            target_route="/onboarding",
            target_params={"from": "dashboard", "focus": "profile-baseline"},
            expected_writeback="user_profiles/risk_questionnaires/behavior_profiles",
            safety_note="建档只建立解释上下文，不会生成账户操作建议。",
        )
        return DashboardDailyBrief(
            brief_id=f"daily-brief:{user.id}:profile-missing",
            as_of=user.updated_at,
            status="missing_profile",
            priority_level="learning",
            headline="先建立画像，教练才判断什么与你有关。",
            beginner_explanation=(
                "当前缺少风险承受、投资经验和行为问卷。FundGene 不会在缺少这些"
                "上下文时伪造个性化判断。"
            ),
            evidence=[
                DashboardEvidence(
                    id="evidence:profile-missing",
                    source_type="profile",
                    source_id=user.id,
                    claim="基础画像和问卷尚未完成。",
                    beginner_translation="我还不知道你能承受怎样的波动，也不知道解释应从哪里开始。",
                    support_level="strong",
                    freshness_label="当前会话",
                    risk_boundary="不能据此判断你的组合或新闻影响。",
                )
            ],
            primary_action=profile_action,
            secondary_actions=[],
            do_not_do="资料不足时只展示通用学习方向，个人化判断会等资料补齐后生成。",
            source_coverage={**coverage, "profile": False},
            trace_id=None,
        )

    coverage["profile"] = True
    if risk_level is not None:
        evidence.append(
            DashboardEvidence(
                id="evidence:profile-risk",
                source_type="profile",
                source_id=user.id,
                claim=f"风险画像已建立，当前风险等级为 {risk_level}。",
                beginner_translation="这会影响教练解释波动时的语气、深度和下一步优先级。",
                support_level="strong",
                freshness_label="最新问卷",
                risk_boundary="只用于调整解释方式。",
            )
        )

    if bias_tags:
        coverage["behavior"] = True
        evidence.append(
            DashboardEvidence(
                id="evidence:behavior-bias",
                source_type="behavior",
                source_id=user.id,
                claim=f"当前可观察行为焦点包括：{'、'.join(bias_tags[:2])}。",
                beginner_translation="教练会优先帮你识别容易被波动或热点带偏的判断环节。",
                support_level="medium",
                freshness_label="最新行为画像",
                risk_boundary="只是训练线索，不是固定评价。",
            )
        )

    if portfolio_overview.has_report:
        coverage["portfolio"] = True
        evidence.append(
            DashboardEvidence(
                id="evidence:portfolio-latest",
                source_type="portfolio",
                source_id=portfolio_overview.latest_snapshot_id,
                claim=_truncate(portfolio_overview.summary or "最近组合报告已生成。"),
                beginner_translation="组合结构会决定市场信息更容易影响哪类风险和情绪。",
                support_level="strong",
                freshness_label=(
                    f"{portfolio_overview.latest_snapshot_date} 快照"
                    if portfolio_overview.latest_snapshot_date
                    else "最近组合快照"
                ),
                risk_boundary="只用于解释组合结构。",
            )
        )

    if learning_overview.total_courses > 0:
        coverage["learning"] = True
        learning_claim = (
            f"学习进度为 {learning_overview.overall_progress_percentage}%，"
            f"推荐继续《{learning_overview.recommended_course_title}》。"
            if learning_overview.recommended_course_title
            else f"学习进度为 {learning_overview.overall_progress_percentage}%。"
        )
        evidence.append(
            DashboardEvidence(
                id="evidence:learning-progress",
                source_type="learning",
                source_id=learning_overview.recommended_course_slug,
                claim=learning_claim,
                beginner_translation="知识缺口会影响你理解组合波动和新闻影响路径的速度。",
                support_level="medium",
                freshness_label="当前学习进度",
                risk_boundary="只是学习顺序。",
            )
        )

    if simulation_overview.completed_sessions_count > 0:
        coverage["simulation"] = True
        evidence.append(
            DashboardEvidence(
                id="evidence:simulation-latest",
                source_type="simulation",
                source_id=simulation_overview.latest_session_id,
                claim=_truncate(
                    simulation_overview.latest_review_summary
                    or "最近一次情境训练已完成。"
                ),
                beginner_translation="训练复盘能帮助教练判断你在压力情境下先看什么。",
                support_level="medium",
                freshness_label="最近情境复盘",
                risk_boundary="一次训练只作为复盘线索。",
            )
        )

    if news_overview is not None and news_overview.has_analysis:
        coverage["news_policy"] = True
        news_summary = news_overview.latest_summary
        news_agent_read = (
            news_overview.beginner_translation
            or "先判断它通过什么路径影响组合和行为，再决定是否需要追问。"
        )
        news_translation_parts = []
        if news_summary:
            news_translation_parts.append(f"新闻概括：{news_summary}")
        if news_agent_read:
            news_translation_parts.append(f"简要解读：{news_agent_read}")
        evidence.append(
            DashboardEvidence(
                id="evidence:news-latest",
                source_type="news_policy",
                source_id=news_overview.latest_analysis_id,
                claim=_truncate(
                    news_overview.latest_title or "最近新闻/政策解读已生成。"
                ),
                beginner_translation=_truncate(
                    " ".join(news_translation_parts)
                    or "最近已有一条新闻/政策解读，可进入资讯页查看影响路径。",
                    limit=220,
                ),
                support_level="medium",
                freshness_label="最近新闻/政策分析",
                risk_boundary="只用于理解影响路径。",
            )
        )

    if latest_coach_activity is not None:
        coverage["coach_history"] = True
        evidence.append(
            DashboardEvidence(
                id="evidence:coach-latest",
                source_type="coach_history",
                source_id=latest_coach_activity.session_id,
                claim=_truncate(latest_coach_activity.answer_focus),
                beginner_translation="最近问答会影响今天先解释哪个概念或风险边界。",
                support_level="weak",
                freshness_label="最近 Coach 问答",
                risk_boundary="一次问答只作为追问线索。",
            )
        )

    source_count = sum(1 for covered in coverage.values() if covered)
    primary_source = "learning"

    if not portfolio_overview.has_report:
        status = "missing_portfolio"
        priority = "attention"
        primary_source = "portfolio"
        headline = "今天先录入组合快照，再判断波动来自哪里。"
        explanation = (
            "画像已经建立，但还缺少组合结构。没有持仓快照时，教练只能给学习"
            "和训练建议，不能解释新闻或风险与你的真实关系。"
        )
        primary_action = _first_action_by_route("/portfolio", actions=actions)
    elif news_overview is not None and news_overview.has_analysis:
        status = "ready"
        priority = "attention"
        primary_source = "news_policy"
        headline = "今天先看新闻如何触达你的组合，不急着做账户动作。"
        explanation = (
            "最近的新闻和政策已经有了解读。今天更适合先看它可能影响哪些"
            "持仓和风险感受，再决定是否需要继续追问。"
        )
        primary_action = _safe_action(
            action_id="read-news-impact-path",
            action_type="read_news_context",
            label="查看新闻影响路径",
            reason="最近已有新闻/政策分析，先看它和你的组合及行为风险的关系。",
            target_route="/news",
            target_params={
                "from": "dashboard",
                "focus": "impact-path",
                "source_id": news_overview.latest_analysis_id or "",
            },
            expected_writeback="news_analyses/agent_citations",
            safety_note="新闻解读用于理解影响路径，不构成账户操作指令。",
        )
    elif simulation_overview.completed_sessions_count == 0:
        status = "starter" if source_count < 3 else "ready"
        priority = "learning"
        primary_source = "simulation"
        headline = "今天先做一次历史情境训练，验证你的判断顺序。"
        explanation = (
            "组合和画像已经能提供基本上下文，但行为证据还比较少。先用历史"
            "情境观察自己面对波动时会先看证据还是先被情绪带走。"
        )
        primary_action = _first_action_by_route("/simulation", actions=actions)
    else:
        status = "ready"
        priority = "stable"
        primary_source = "learning"
        headline = "今天先巩固一个风险概念，再回看组合和训练证据。"
        explanation = (
            "当前已有多类来源，今天不需要追逐更多信息。把一个概念学清楚，"
            "再用它解释组合和训练记录，会更适合新手形成稳定判断。"
        )
        primary_action = _first_action_by_route("/learning", actions=actions)

    secondary_actions = [action for action in actions if action.id != primary_action.id][:2]
    visible_evidence = _prioritize_evidence(evidence, primary_source=primary_source)

    return DashboardDailyBrief(
        brief_id=f"daily-brief:{user.id}:{status}",
        as_of=user.updated_at,
        status=status,
        priority_level=priority,
        headline=headline,
        beginner_explanation=explanation,
        evidence=visible_evidence,
        primary_action=primary_action,
        secondary_actions=secondary_actions,
        do_not_do="先把今天的判断用于理解和检查，需要保存的变化会再请你确认。",
        source_coverage=coverage,  # type: ignore[arg-type]
        trace_id=None,
    )


def get_latest_coach_activity(
    db: Session, *, user_id: str
) -> DashboardCoachActivity | None:
    latest_response = db.execute(
        select(ChatSession, ChatMessage)
        .join(ChatMessage, ChatMessage.session_id == ChatSession.id)
        .where(
            ChatSession.user_id == user_id,
            ChatSession.context_type == "coach",
            ChatMessage.role == "assistant",
        )
        .order_by(ChatMessage.created_at.desc(), ChatMessage.id.desc())
    ).first()

    if latest_response is None:
        return None

    session, assistant_message = latest_response
    latest_question = db.scalar(
        select(ChatMessage.content)
        .where(
            ChatMessage.session_id == session.id,
            ChatMessage.role == "user",
            ChatMessage.created_at <= assistant_message.created_at,
        )
        .order_by(ChatMessage.created_at.desc(), ChatMessage.id.desc())
    )
    payload = assistant_message.structured_payload or {}
    recommended_actions = payload.get("recommended_actions")
    intent = payload.get("intent")

    return DashboardCoachActivity(
        session_id=session.id,
        topic=session.topic,
        intent=intent if isinstance(intent, str) else None,
        question=latest_question or session.topic,
        answer_focus=_truncate(assistant_message.content),
        recommended_action=(
            recommended_actions[0]
            if isinstance(recommended_actions, list)
            and recommended_actions
            and isinstance(recommended_actions[0], str)
            else None
        ),
        updated_at=assistant_message.created_at,
    )


def build_dashboard_state(db: Session, *, user: UserProfile) -> DashboardResponse:
    behavior_profile = get_behavior_profile(db, user_id=user.id)
    latest_questionnaire = get_latest_questionnaire(db, user_id=user.id)
    latest_coach_activity = get_latest_coach_activity(db, user_id=user.id)
    learning_overview = (
        get_learning_overview(db, user_id=user.id)
        if user.onboarding_completed
        else DashboardLearningStatus(
            overall_progress_percentage=0,
            completed_courses_count=0,
            total_courses=0,
        )
    )
    portfolio_overview = (
        get_portfolio_overview(db, user_id=user.id)
        if user.onboarding_completed
        else DashboardPortfolioStatus(has_report=False)
    )
    simulation_overview = (
        get_latest_simulation_overview(db, user_id=user.id)
        if user.onboarding_completed
        else DashboardSimulationStatus(completed_sessions_count=0)
    )
    news_overview = (
        get_news_overview(db, user_id=user.id)
        if user.onboarding_completed
        else None
    )
    behavior_training_plan = (
        get_behavior_training_plan(db, user=user)
        if user.onboarding_completed
        else None
    )

    risk_level = behavior_profile.risk_level if behavior_profile else None
    bias_tags = behavior_profile.bias_tags if behavior_profile else []

    if user.onboarding_completed and latest_questionnaire is not None:
        next_actions = [
            (
                f"进入 Simulation，优先完成“{behavior_training_plan.focus_title}”方向的情境训练。"
                if behavior_training_plan is not None
                else "回看当前偏差标签，先挑一个最容易失守的行为作为本周训练重点。"
            ),
            (
                f"进入 Learning，先完成《{learning_overview.recommended_course_title}》这门课。"
                if learning_overview.recommended_course_title
                else "进入 Learning，继续巩固你的基金基础课。"
            ),
            (
                "进入 Portfolio，查看最近一份组合体检报告。"
                if portfolio_overview.has_report
                else "进入 Portfolio，录入第一份手工持仓快照。"
            ),
            (
                "回看最近一次情境训练复盘，把失守节点写成一句固定检查语。"
                if simulation_overview.latest_review_summary
                else "完成第一条情境训练后，这里会出现更具体的行为复盘动作。"
            ),
        ]
        questionnaire_value = "已完成"
    else:
        next_actions = [
            "先完善基础画像，让 dashboard 具备最小上下文。",
            "完成第一版风险与行为问卷，建立风险等级与偏差基线。",
            "回到 dashboard 查看个性化动作建议，再进入下一条产品路径。",
        ]
        questionnaire_value = "待完成"

    if latest_coach_activity is not None:
        coach_next_action = (
            latest_coach_activity.recommended_action
            or "回到 Coach，继续追问你最近一次基金基础问题。"
        )
        next_actions = [
            f"延续最近一次 Coach 主题“{latest_coach_activity.topic}”：{coach_next_action}",
            *next_actions[:2],
        ]

    if news_overview is not None and news_overview.has_analysis:
        next_actions = [
            f"回看最近新闻/政策分析“{news_overview.latest_title}”：{news_overview.recommended_action}",
            *next_actions[:3],
        ]

    summary_cards = [
        DashboardSummaryCard(
            label="问卷状态",
            value=questionnaire_value,
            detail="风险与行为基线来自最新一次问卷提交。",
        ),
        DashboardSummaryCard(
            label="风险等级",
            value=risk_level or "待评估",
            detail="结合波动承受、回撤反应与投资期限得出。",
        ),
        DashboardSummaryCard(
            label="行为焦点",
            value=bias_tags[0] if bias_tags else "待建立画像",
            detail="偏差标签会影响 coaching 与 dashboard 的下一步建议。",
        ),
        DashboardSummaryCard(
            label="学习进度",
            value=(
                f"{learning_overview.overall_progress_percentage}%"
                if learning_overview.total_courses > 0
                else "待开始"
            ),
            detail=(
                f"已完成 {learning_overview.completed_courses_count}/{learning_overview.total_courses} 门基础课。"
                if learning_overview.total_courses > 0
                else "学习路径会在进入 Learning 后形成真实进度。"
            ),
        ),
        DashboardSummaryCard(
            label="组合体检",
            value=(
                "已有报告"
                if portfolio_overview.has_report
                else "待录入"
            ),
            detail=(
                portfolio_overview.summary
                if portfolio_overview.summary
                else "录入第一份持仓快照后，这里会显示最近一份组合解释。"
            ),
        ),
        DashboardSummaryCard(
            label="情境训练",
            value=(
                f"{simulation_overview.completed_sessions_count} 次"
                if simulation_overview.completed_sessions_count > 0
                else "待开始"
            ),
            detail=(
                simulation_overview.latest_review_summary
                if simulation_overview.latest_review_summary
                else (
                    f"建议先从《{simulation_overview.recommended_scenario_title}》开始。"
                    if simulation_overview.recommended_scenario_title
                    else "开始第一条情境训练后，这里会显示最近一次复盘结果。"
                )
            ),
        ),
        DashboardSummaryCard(
            label="新闻政策",
            value=(
                "已有解读"
                if news_overview is not None and news_overview.has_analysis
                else "待解读"
            ),
            detail=(
                news_overview.beginner_translation
                if news_overview is not None
                and news_overview.has_analysis
                and news_overview.beginner_translation
                else "完成第一条新闻/政策分析后，这里会显示事实、影响路径和不确定性。"
            ),
        ),
        DashboardSummaryCard(
            label="最近 Coach",
            value=latest_coach_activity.topic if latest_coach_activity else "尚无记录",
            detail=(
                "最近一次 coach 交互会回写到 dashboard，形成可回看的训练痕迹。"
                if latest_coach_activity
                else "完成第一条 coach 问答后，这里会出现最近一次指导痕迹。"
            ),
        ),
    ]
    daily_brief = build_daily_brief(
        user=user,
        risk_level=risk_level,
        bias_tags=bias_tags,
        latest_questionnaire=latest_questionnaire,
        learning_overview=learning_overview,
        portfolio_overview=portfolio_overview,
        simulation_overview=simulation_overview,
        news_overview=news_overview,
        latest_coach_activity=latest_coach_activity,
    )

    return DashboardResponse(
        user_id=user.id,
        onboarding_completed=user.onboarding_completed,
        risk_level=risk_level,
        bias_tags=bias_tags,
        latest_risk_score=(
            latest_questionnaire.risk_score if latest_questionnaire else None
        ),
        daily_brief=daily_brief,
        next_actions=next_actions,
        summary_cards=summary_cards,
        learning_status=learning_overview,
        portfolio_status=portfolio_overview,
        simulation_status=simulation_overview,
        news_status=news_overview,
        latest_coach_activity=latest_coach_activity,
    )
