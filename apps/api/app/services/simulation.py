from collections import Counter
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.behavior_profile import BehaviorProfile
from app.models.scenario import Scenario
from app.models.scenario_event import ScenarioEvent
from app.models.simulation_action import SimulationAction
from app.models.simulation_review import SimulationReview
from app.models.simulation_session import SimulationSession
from app.models.user import UserProfile
from app.schemas.dashboard import DashboardSimulationStatus
from app.schemas.simulation import (
    ActiveScenarioEvent,
    BehaviorEvidenceCandidate,
    ScenarioCatalogResponse,
    ScenarioChoice,
    ScenarioSummary,
    SimulationActionSubmitRequest,
    SimulationActionSummary,
    SimulationReviewResponse,
    SimulationSessionResponse,
    SimulationSessionStartRequest,
)

SCENARIO_FIXTURES = [
    {
        "slug": "covid-volatility-discipline",
        "title": "疫情冲击下的回撤纪律",
        "summary": "在连续大跌、情绪失控和初步反弹中，练习如何把恐慌和长期计划拆开。",
        "bias_focus": "panic_selling_risk",
        "difficulty": 1,
        "estimated_duration_minutes": 12,
        "events": [
            {
                "step_index": 1,
                "date_label": "2020-02-03",
                "title": "开盘急跌",
                "narrative": "你看到基金净值在假期后明显下挫，社交平台上到处都是“先跑为敬”的声音。",
                "prompt": "作为新手投资者，这时最稳妥的第一反应应该是什么？",
                "recommended_choice_key": "pause_and_review",
                "choices": [
                    {
                        "key": "panic_redeem",
                        "label": "先赎回大部分仓位，避免继续亏损",
                        "description": "让情绪先接管动作，优先止住眼前的不安。",
                        "signals": ["panic_selling_risk"],
                    },
                    {
                        "key": "pause_and_review",
                        "label": "先暂停动作，核对自己的期限、现金需求和原计划",
                        "description": "先把情绪和决策拆开，再判断是不是计划真的失效。",
                        "signals": [],
                    },
                    {
                        "key": "all_in_bottom",
                        "label": "把手里现金一次性全补进去",
                        "description": "把下跌直接当成“便宜了”的信号，忽略后续不确定性。",
                        "signals": ["impulsive_market_timing"],
                    },
                ],
            },
            {
                "step_index": 2,
                "date_label": "2020-03-16",
                "title": "恐慌加剧",
                "narrative": "全球市场继续大幅波动，你的账户回撤比想象中更深，身边有人已经全部赎回。",
                "prompt": "如果你的长期目标和现金需求没有变化，哪种动作更能体现纪律？",
                "recommended_choice_key": "continue_plan_small",
                "choices": [
                    {
                        "key": "redeem_everything",
                        "label": "全部赎回，等市场稳定了再回来",
                        "description": "把“暂时舒服一点”放在长期计划前面。",
                        "signals": ["panic_selling_risk"],
                    },
                    {
                        "key": "continue_plan_small",
                        "label": "按原计划小步执行，并记录自己此刻的情绪",
                        "description": "承认压力存在，但不让它直接替代投资纪律。",
                        "signals": [],
                    },
                    {
                        "key": "switch_hot_theme",
                        "label": "改去追更抗跌或更热门的主题基金",
                        "description": "在恐慌中切换赛道，希望用热点掩盖回撤焦虑。",
                        "signals": ["performance_chasing_risk"],
                    },
                ],
            },
            {
                "step_index": 3,
                "date_label": "2020-04-30",
                "title": "市场回暖",
                "narrative": "市场开始修复，你也慢慢从最恐慌的时候缓过来，但你发现自己已经忘了当时为什么想卖。",
                "prompt": "情境结束前，最值得做的收尾动作是什么？",
                "recommended_choice_key": "review_journal",
                "choices": [
                    {
                        "key": "chase_rebound",
                        "label": "既然反弹了，赶紧把之前卖掉的部分追回来",
                        "description": "用追涨去弥补之前的慌张，没有回头复盘。",
                        "signals": ["performance_chasing_risk"],
                    },
                    {
                        "key": "review_journal",
                        "label": "回看自己最慌的那一刻，记录触发点和下次的应对句子",
                        "description": "把这次情绪波动转成下一次可复用的训练素材。",
                        "signals": [],
                    },
                    {
                        "key": "ignore_emotion",
                        "label": "反正已经过去了，就不复盘当时为什么难受",
                        "description": "跳过复盘，意味着同样的压力下次还会重复出现。",
                        "signals": ["reflection_gap"],
                    },
                ],
            },
        ],
    },
    {
        "slug": "theme-fund-fomo",
        "title": "热门赛道狂热期的追涨冲动",
        "summary": "在朋友都在晒收益、单一主题基金连涨和随后的波动中，练习如何克制追热点。",
        "bias_focus": "performance_chasing_risk",
        "difficulty": 1,
        "estimated_duration_minutes": 12,
        "events": [
            {
                "step_index": 1,
                "date_label": "某季度第 1 周",
                "title": "热门榜单刷屏",
                "narrative": "你看到某类主题基金最近三个月涨幅很高，朋友群里都在讨论“再不上车就错过了”。",
                "prompt": "作为第一反应，什么动作最能帮你避免被 FOMO 直接推着走？",
                "recommended_choice_key": "check_fit_first",
                "choices": [
                    {
                        "key": "buy_now",
                        "label": "先买一笔再说，晚一点可能更贵",
                        "description": "把短期涨幅当成长期确定性。",
                        "signals": ["performance_chasing_risk"],
                    },
                    {
                        "key": "check_fit_first",
                        "label": "先核对这只基金是否真的符合自己的风险和目标",
                        "description": "先确认适不适合自己，而不是先确认别人赚了多少。",
                        "signals": [],
                    },
                    {
                        "key": "sell_old_for_hot",
                        "label": "卖掉原有基金，集中换到这条热门赛道",
                        "description": "为了追热点而牺牲已有配置纪律。",
                        "signals": ["performance_chasing_risk", "concentration_risk"],
                    },
                ],
            },
            {
                "step_index": 2,
                "date_label": "某季度第 5 周",
                "title": "收益诱惑升级",
                "narrative": "主题基金继续上涨，你开始怀疑自己是不是太保守，甚至想把组合里其他持仓都换过去。",
                "prompt": "这时最值得先做哪件事？",
                "recommended_choice_key": "inspect_concentration",
                "choices": [
                    {
                        "key": "all_in_theme",
                        "label": "直接把大部分仓位集中到这条赛道",
                        "description": "把短期强势等同于未来长期最优解。",
                        "signals": ["concentration_risk", "performance_chasing_risk"],
                    },
                    {
                        "key": "inspect_concentration",
                        "label": "先检查现有组合是否已经承担了重复主题风险",
                        "description": "先搞清楚你是不是已经暴露在同样的波动来源里。",
                        "signals": [],
                    },
                    {
                        "key": "copy_friend",
                        "label": "直接照着收益最高的朋友配置去做",
                        "description": "把别人的结果误当成自己的计划。",
                        "signals": ["performance_chasing_risk"],
                    },
                ],
            },
            {
                "step_index": 3,
                "date_label": "某季度第 9 周",
                "title": "回撤开始出现",
                "narrative": "主题基金出现明显回撤，你一边后悔没有更早买，一边又担心现在买进去会被套。",
                "prompt": "情境结束前，最有价值的复盘动作是什么？",
                "recommended_choice_key": "write_checklist",
                "choices": [
                    {
                        "key": "double_down_hot",
                        "label": "继续追进去，赌它很快反弹",
                        "description": "仍然让涨跌节奏替代自己的配置原则。",
                        "signals": ["performance_chasing_risk"],
                    },
                    {
                        "key": "write_checklist",
                        "label": "写下一份“热门基金上车前检查清单”",
                        "description": "把这次冲动整理成下一次可执行的防呆动作。",
                        "signals": [],
                    },
                    {
                        "key": "avoid_review",
                        "label": "以后少看账户就行，不用细想这次为什么心动",
                        "description": "回避复盘只会让同样的冲动重复出现。",
                        "signals": ["reflection_gap"],
                    },
                ],
            },
        ],
    },
]

BEHAVIOR_FOCUS_COPY = {
    "panic_selling_risk": {
        "title": "回撤压力下的纪律训练",
        "guidance": "优先练习“先停一下、先核对计划、再决定动作”，不要让回撤直接触发赎回。",
        "scenario_slug": "covid-volatility-discipline",
    },
    "performance_chasing_risk": {
        "title": "追热点克制训练",
        "guidance": "优先练习“先看适配度和集中度，再决定是否调整”，不要把短期强势当成长期确定性。",
        "scenario_slug": "theme-fund-fomo",
    },
    "concentration_risk": {
        "title": "集中度识别训练",
        "guidance": "优先练习检查已有组合是否承担了重复风险，再决定是否追加到单一主题。",
        "scenario_slug": "theme-fund-fomo",
    },
}


def ensure_scenario_catalog(db: Session) -> None:
    existing = db.scalar(select(Scenario.id).limit(1))
    if existing is not None:
        return

    for scenario_payload in SCENARIO_FIXTURES:
        scenario = Scenario(
            slug=scenario_payload["slug"],
            title=scenario_payload["title"],
            summary=scenario_payload["summary"],
            bias_focus=scenario_payload["bias_focus"],
            difficulty=scenario_payload["difficulty"],
            estimated_duration_minutes=scenario_payload["estimated_duration_minutes"],
        )
        db.add(scenario)
        db.flush()

        for event_payload in scenario_payload["events"]:
            db.add(
                ScenarioEvent(
                    scenario_id=scenario.id,
                    step_index=event_payload["step_index"],
                    date_label=event_payload["date_label"],
                    title=event_payload["title"],
                    narrative=event_payload["narrative"],
                    prompt=event_payload["prompt"],
                    choices=event_payload["choices"],
                    recommended_choice_key=event_payload["recommended_choice_key"],
                )
            )

    db.commit()


def _serialize_action(action: SimulationAction) -> SimulationActionSummary:
    return SimulationActionSummary(
        event_id=action.event_id,
        step_index=action.step_index,
        choice_key=action.choice_key,
        choice_label=action.choice_label,
        reflection=action.reflection,
        is_recommended=action.is_recommended,
        created_at=action.created_at,
    )


def _serialize_active_event(event: ScenarioEvent | None) -> ActiveScenarioEvent | None:
    if event is None:
        return None

    return ActiveScenarioEvent(
        event_id=event.id,
        step_index=event.step_index,
        date_label=event.date_label,
        title=event.title,
        narrative=event.narrative,
        prompt=event.prompt,
        choices=[
            ScenarioChoice(
                key=str(choice["key"]),
                label=str(choice["label"]),
                description=str(choice["description"]),
            )
            for choice in event.choices
        ],
    )


def _get_scenario(db: Session, *, slug: str) -> Scenario:
    scenario = db.scalar(select(Scenario).where(Scenario.slug == slug))
    if scenario is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested simulation scenario does not exist.",
        )
    return scenario


def _list_scenario_events(db: Session, *, scenario_id: str) -> list[ScenarioEvent]:
    return list(
        db.scalars(
            select(ScenarioEvent)
            .where(ScenarioEvent.scenario_id == scenario_id)
            .order_by(ScenarioEvent.step_index.asc(), ScenarioEvent.created_at.asc())
        )
    )


def _list_session_actions(db: Session, *, session_id: str) -> list[SimulationAction]:
    return list(
        db.scalars(
            select(SimulationAction)
            .where(SimulationAction.session_id == session_id)
            .order_by(SimulationAction.step_index.asc(), SimulationAction.created_at.asc())
        )
    )


def _get_review(db: Session, *, session_id: str) -> SimulationReview | None:
    return db.scalar(
        select(SimulationReview).where(SimulationReview.session_id == session_id)
    )


def _get_active_event(
    db: Session,
    *,
    scenario_id: str,
    current_step: int,
) -> ScenarioEvent | None:
    return db.scalar(
        select(ScenarioEvent).where(
            ScenarioEvent.scenario_id == scenario_id,
            ScenarioEvent.step_index == current_step,
        )
    )


def _serialize_review(
    db: Session,
    *,
    session: SimulationSession,
    review: SimulationReview,
) -> SimulationReviewResponse:
    scenario = db.get(Scenario, session.scenario_id)
    actions = _list_session_actions(db, session_id=session.id)
    recommended_count = sum(1 for action in actions if action.is_recommended)
    all_recommended = bool(actions) and recommended_count == len(actions)
    strengths = list(review.bias_observations) if all_recommended else []
    improvement_areas = [] if all_recommended else list(review.bias_observations)
    reflection_questions = (
        [
            "这次哪一个检查动作最能帮你稳住情绪？",
            "下次遇到类似波动或热点刺激时，你准备先对自己说哪一句提醒？",
        ]
        if all_recommended
        else [
            "这次哪个节点最容易让你被情绪或热点牵着走？",
            "下次行动前，你需要先检查哪一条计划边界？",
        ]
    )
    behavior_candidates = _build_behavior_evidence_candidates(
        db,
        session=session,
        actions=actions,
        scenario=scenario,
    )
    pending_state_proposal = (
        {
            "status": "pending",
            "target_type": "behavior_profile",
            "reason": "单次情境训练只能作为行为证据候选，需要后续训练或用户确认后再更新画像。",
        }
        if behavior_candidates
        else None
    )
    return SimulationReviewResponse(
        session_id=session.id,
        scenario_slug=scenario.slug if scenario else "unknown-scenario",
        scenario_title=scenario.title if scenario else "Unknown scenario",
        bias_focus=scenario.bias_focus if scenario else "general_discipline",
        decision_summary=review.decision_summary,
        bias_observations=list(review.bias_observations),
        strengths=strengths,
        improvement_areas=improvement_areas,
        coach_feedback=review.coach_feedback,
        recommended_next_actions=list(review.recommended_next_actions),
        reflection_questions=reflection_questions,
        behavior_evidence_candidates=behavior_candidates,
        pending_state_proposal=pending_state_proposal,
        generated_at=review.generated_at,
        actions=[_serialize_action(action) for action in actions],
    )


def _build_behavior_evidence_candidates(
    db: Session,
    *,
    session: SimulationSession,
    actions: list[SimulationAction],
    scenario: Scenario | None,
) -> list[BehaviorEvidenceCandidate]:
    candidates: list[BehaviorEvidenceCandidate] = []
    seen: set[str] = set()
    scenario_title = scenario.title if scenario else "Unknown scenario"

    for action in actions:
        event = db.get(ScenarioEvent, action.event_id)
        if event is None:
            continue
        choice = _choice_payload(event, choice_key=action.choice_key)
        for raw_signal in choice.get("signals", []):
            signal = str(raw_signal)
            if signal not in BEHAVIOR_FOCUS_COPY and signal != "reflection_gap":
                continue
            if signal in seen:
                continue
            seen.add(signal)
            candidates.append(
                BehaviorEvidenceCandidate(
                    behavior_evidence_id=(
                        f"behavior-evidence:{session.id}:{action.event_id}:{signal}"
                    ),
                    bias_type=signal,
                    observed_signal=(
                        f"在“{event.title}”节点选择了“{action.choice_label}”。"
                    ),
                    source_event=f"{scenario_title} / step {event.step_index}",
                    confidence="medium" if signal != "reflection_gap" else "low",
                    pending_state_proposal_id=None,
                )
            )

    return candidates[:3]


def _serialize_session(
    db: Session,
    *,
    session: SimulationSession,
) -> SimulationSessionResponse:
    scenario = db.get(Scenario, session.scenario_id)
    events = _list_scenario_events(db, scenario_id=session.scenario_id)
    actions = _list_session_actions(db, session_id=session.id)
    review = _get_review(db, session_id=session.id)
    active_event = (
        _get_active_event(db, scenario_id=session.scenario_id, current_step=session.current_step)
        if session.status == "in_progress"
        else None
    )

    return SimulationSessionResponse(
        session_id=session.id,
        scenario_slug=scenario.slug if scenario else "unknown-scenario",
        scenario_title=scenario.title if scenario else "Unknown scenario",
        bias_focus=scenario.bias_focus if scenario else "general_discipline",
        status="completed" if session.status == "completed" else "in_progress",
        current_step=session.current_step,
        total_steps=len(events),
        started_at=session.started_at,
        active_event=_serialize_active_event(active_event),
        actions=[_serialize_action(action) for action in actions],
        completed_at=session.completed_at,
        review=(
            _serialize_review(db, session=session, review=review)
            if review is not None
            else None
        ),
    )


def _choice_payload(event: ScenarioEvent, *, choice_key: str) -> dict:
    for choice in event.choices:
        if str(choice["key"]) == choice_key:
            return choice
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Submitted simulation choice does not exist for this event.",
    )


def _build_review(
    db: Session,
    *,
    session: SimulationSession,
) -> SimulationReview:
    scenario = db.get(Scenario, session.scenario_id)
    actions = _list_session_actions(db, session_id=session.id)
    risky_signals: list[str] = []
    recommended_count = 0

    for action in actions:
        event = db.get(ScenarioEvent, action.event_id)
        if event is None:
            continue
        choice = _choice_payload(event, choice_key=action.choice_key)
        if action.is_recommended:
            recommended_count += 1
        risky_signals.extend(
            [
                str(signal)
                for signal in choice.get("signals", [])
                if str(signal) in BEHAVIOR_FOCUS_COPY or str(signal) == "reflection_gap"
            ]
        )

    signal_counter = Counter(risky_signals)
    total_steps = len(actions)
    strongest_signal = signal_counter.most_common(1)[0][0] if signal_counter else None

    if recommended_count == total_steps:
        decision_summary = "这次情境训练中，你基本能把情绪和动作拆开，没有被单次波动直接带着走。"
        bias_observations = ["你在关键节点都先做了计划检查，而不是让情绪先出手。"]
        coach_feedback = (
            "这次你表现出不错的纪律感。下一步别只满足于“做对了”，要继续总结自己是靠什么句子和检查动作稳住的。"
        )
    else:
        decision_summary = (
            f"这次情境训练里，你在 {total_steps - recommended_count}/{total_steps} 个关键节点被情绪或热点牵动，"
            "说明你的判断框架还需要一层更稳定的防呆动作。"
        )
        bias_observations = []
        if strongest_signal == "panic_selling_risk":
            bias_observations.append("回撤一扩大，你更容易优先追求“马上舒服一点”，而不是先核对长期计划。")
        if strongest_signal == "performance_chasing_risk":
            bias_observations.append("当热门资产持续上涨时，你更容易把短期涨幅误当成长期确定性。")
        if "concentration_risk" in signal_counter:
            bias_observations.append("你在热点面前容易忽略组合已经承担的重复风险。")
        if "reflection_gap" in signal_counter:
            bias_observations.append("你有跳过复盘的倾向，这会让同样的情绪触发点反复出现。")
        if not bias_observations:
            bias_observations.append("你在高压节点还缺少一个固定的“先停一下再决定”的动作。")
        coach_feedback = (
            "别把这次训练当作猜对猜错，而要把它当作情绪暴露测试。下次遇到类似场景时，先复用这次最缺失的检查动作。"
        )

    recommended_scenario = behavior_focus_guidance(strongest_signal or scenario.bias_focus)
    recommended_next_actions = [
        "回到 Dashboard，对照新的行为线索看下一步动作是否已经变化。",
        "把这次最容易失守的触发点带回 Coach，用自己的真实经历继续拆解。",
        (
            f"下次优先继续《{recommended_scenario['title']}》方向的训练。"
            if recommended_scenario
            else "继续完成下一次情境训练，把这次情绪触发点变成固定检查动作。"
        ),
    ]

    review = SimulationReview(
        session_id=session.id,
        decision_summary=decision_summary,
        bias_observations=bias_observations,
        coach_feedback=coach_feedback,
        recommended_next_actions=recommended_next_actions,
        generated_at=datetime.now(timezone.utc),
    )
    db.add(review)

    return review


def _compose_action_reflection(payload: SimulationActionSubmitRequest) -> str | None:
    if payload.rationale is None and payload.reflection is None:
        return None

    parts: list[str] = []
    rationale = payload.rationale or payload.reflection
    if rationale:
        parts.append(f"判断理由：{rationale}")
    if payload.worry:
        parts.append(f"担心点：{payload.worry}")
    if payload.impulse_control_plan:
        parts.append(f"冲动控制计划：{payload.impulse_control_plan}")
    return "\n".join(parts) if parts else None


def behavior_focus_guidance(bias_tag: str | None) -> dict | None:
    if bias_tag is None:
        return None
    return BEHAVIOR_FOCUS_COPY.get(bias_tag)


def list_scenarios(db: Session, *, user: UserProfile) -> ScenarioCatalogResponse:
    ensure_scenario_catalog(db)
    scenarios = list(
        db.scalars(select(Scenario).order_by(Scenario.difficulty.asc(), Scenario.created_at.asc()))
    )
    behavior_profile = db.scalar(
        select(BehaviorProfile).where(BehaviorProfile.user_id == user.id)
    )
    recommended_focus = None
    if behavior_profile is not None:
        for tag in behavior_profile.bias_tags:
            if tag in BEHAVIOR_FOCUS_COPY:
                recommended_focus = tag
                break

    items: list[ScenarioSummary] = []
    recommended_scenario_slug = None
    recommended_scenario_title = None

    for scenario in scenarios:
        event_count = db.scalar(
            select(func.count())
            .select_from(ScenarioEvent)
            .where(ScenarioEvent.scenario_id == scenario.id)
        )
        active_session = db.scalar(
            select(SimulationSession)
            .where(
                SimulationSession.user_id == user.id,
                SimulationSession.scenario_id == scenario.id,
                SimulationSession.status == "in_progress",
            )
            .order_by(SimulationSession.started_at.desc())
        )
        latest_completed = db.scalar(
            select(SimulationSession)
            .where(
                SimulationSession.user_id == user.id,
                SimulationSession.scenario_id == scenario.id,
                SimulationSession.status == "completed",
            )
            .order_by(SimulationSession.completed_at.desc(), SimulationSession.started_at.desc())
        )
        status_value: str = "not_started"
        active_session_id = None
        last_completed_at = None
        if active_session is not None:
            status_value = "in_progress"
            active_session_id = active_session.id
        elif latest_completed is not None:
            status_value = "completed"
            last_completed_at = latest_completed.completed_at

        if (
            recommended_scenario_slug is None
            and (
                scenario.bias_focus == recommended_focus
                or (recommended_focus is None and status_value != "completed")
            )
        ):
            recommended_scenario_slug = scenario.slug
            recommended_scenario_title = scenario.title

        items.append(
            ScenarioSummary(
                slug=scenario.slug,
                title=scenario.title,
                summary=scenario.summary,
                bias_focus=scenario.bias_focus,
                difficulty=scenario.difficulty,
                estimated_duration_minutes=scenario.estimated_duration_minutes,
                event_count=int(event_count or 0),
                status=status_value,  # type: ignore[arg-type]
                active_session_id=active_session_id,
                last_completed_at=last_completed_at,
            )
        )

    if recommended_scenario_slug is None and items:
        recommended_scenario_slug = items[0].slug
        recommended_scenario_title = items[0].title

    return ScenarioCatalogResponse(
        recommended_scenario_slug=recommended_scenario_slug,
        recommended_scenario_title=recommended_scenario_title,
        items=items,
    )


def start_session(
    db: Session,
    *,
    user: UserProfile,
    payload: SimulationSessionStartRequest,
) -> SimulationSessionResponse:
    ensure_scenario_catalog(db)
    scenario = _get_scenario(db, slug=payload.scenario_slug)
    active_session = _get_active_session_for_scenario(
        db,
        user_id=user.id,
        scenario_id=scenario.id,
    )
    if active_session is not None:
        return _serialize_session(db, session=active_session)

    session = SimulationSession(
        user_id=user.id,
        scenario_id=scenario.id,
        status="in_progress",
        current_step=1,
        started_at=datetime.now(timezone.utc),
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return _serialize_session(db, session=session)


def _get_active_session_for_scenario(
    db: Session,
    *,
    user_id: str,
    scenario_id: str,
) -> SimulationSession | None:
    return db.scalar(
        select(SimulationSession)
        .where(
            SimulationSession.user_id == user_id,
            SimulationSession.scenario_id == scenario_id,
            SimulationSession.status == "in_progress",
        )
        .order_by(SimulationSession.started_at.desc())
    )


def get_session_state(
    db: Session,
    *,
    user: UserProfile,
    session_id: str,
) -> SimulationSessionResponse:
    session = db.scalar(
        select(SimulationSession).where(
            SimulationSession.id == session_id,
            SimulationSession.user_id == user.id,
        )
    )
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested simulation session does not exist for the current user.",
        )
    return _serialize_session(db, session=session)


def submit_action(
    db: Session,
    *,
    user: UserProfile,
    payload: SimulationActionSubmitRequest,
) -> SimulationSessionResponse:
    session = db.scalar(
        select(SimulationSession).where(
            SimulationSession.id == payload.session_id,
            SimulationSession.user_id == user.id,
        )
    )
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested simulation session does not exist for the current user.",
        )
    if session.status != "in_progress":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Simulation session is already completed.",
        )

    event = db.scalar(
        select(ScenarioEvent).where(
            ScenarioEvent.id == payload.event_id,
            ScenarioEvent.scenario_id == session.scenario_id,
            ScenarioEvent.step_index == session.current_step,
        )
    )
    if event is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Submitted event does not match the current simulation step.",
        )

    existing = db.scalar(
        select(SimulationAction).where(
            SimulationAction.session_id == session.id,
            SimulationAction.event_id == event.id,
        )
    )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current simulation step already has a recorded action.",
        )

    choice = _choice_payload(event, choice_key=payload.choice_key)
    action = SimulationAction(
        session_id=session.id,
        event_id=event.id,
        step_index=event.step_index,
        choice_key=str(choice["key"]),
        choice_label=str(choice["label"]),
        reflection=_compose_action_reflection(payload),
        is_recommended=str(choice["key"]) == event.recommended_choice_key,
        created_at=datetime.now(timezone.utc),
    )
    db.add(action)

    total_steps = db.scalar(
        select(func.count())
        .select_from(ScenarioEvent)
        .where(ScenarioEvent.scenario_id == session.scenario_id)
    ) or 0
    if session.current_step >= total_steps:
        session.status = "completed"
        session.completed_at = datetime.now(timezone.utc)
        db.add(session)
        db.flush()
        _build_review(db, session=session)
    else:
        session.current_step += 1
        db.add(session)

    db.commit()
    db.refresh(session)
    return _serialize_session(db, session=session)


def get_review(
    db: Session,
    *,
    user: UserProfile,
    session_id: str,
) -> SimulationReviewResponse:
    session = db.scalar(
        select(SimulationSession).where(
            SimulationSession.id == session_id,
            SimulationSession.user_id == user.id,
        )
    )
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested simulation session does not exist for the current user.",
        )
    review = _get_review(db, session_id=session.id)
    if review is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Simulation review is not available yet.",
        )
    return _serialize_review(db, session=session, review=review)


def get_latest_simulation_overview(
    db: Session,
    *,
    user_id: str,
) -> DashboardSimulationStatus:
    ensure_scenario_catalog(db)
    latest_completed_row = db.execute(
        select(SimulationSession, SimulationReview, Scenario)
        .join(SimulationReview, SimulationReview.session_id == SimulationSession.id)
        .join(Scenario, Scenario.id == SimulationSession.scenario_id)
        .where(SimulationSession.user_id == user_id)
        .order_by(
            SimulationReview.generated_at.desc(),
            SimulationSession.completed_at.desc(),
        )
    ).first()
    completed_sessions_count = db.scalar(
        select(func.count())
        .select_from(SimulationSession)
        .where(
            SimulationSession.user_id == user_id,
            SimulationSession.status == "completed",
        )
    ) or 0

    recommended_scenario = None
    behavior_profile = db.scalar(
        select(BehaviorProfile).where(BehaviorProfile.user_id == user_id)
    )
    if behavior_profile is not None:
        for tag in behavior_profile.bias_tags:
            recommended_scenario = behavior_focus_guidance(tag)
            if recommended_scenario is not None:
                break

    if latest_completed_row is None:
        return DashboardSimulationStatus(
            completed_sessions_count=int(completed_sessions_count),
            recommended_scenario_slug=(
                recommended_scenario["scenario_slug"]
                if recommended_scenario
                else SCENARIO_FIXTURES[0]["slug"]
            ),
            recommended_scenario_title=(
                recommended_scenario["title"]
                if recommended_scenario
                else SCENARIO_FIXTURES[0]["title"]
            ),
        )

    session, review, scenario = latest_completed_row
    return DashboardSimulationStatus(
        completed_sessions_count=int(completed_sessions_count),
        recommended_scenario_slug=(
            recommended_scenario["scenario_slug"] if recommended_scenario else scenario.slug
        ),
        recommended_scenario_title=(
            recommended_scenario["title"] if recommended_scenario else scenario.title
        ),
        latest_session_id=session.id,
        latest_scenario_slug=scenario.slug,
        latest_scenario_title=scenario.title,
        latest_review_summary=review.decision_summary,
        latest_completed_at=session.completed_at,
    )
