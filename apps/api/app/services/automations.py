from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import logging
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.agent_run import AgentRun
from app.models.agent_step import AgentStep
from app.models.agent_state_update_proposal import AgentStateUpdateProposal
from app.models.automation_notification import AutomationNotification
from app.models.automation_run import AutomationRun
from app.models.automation_setting import AutomationSetting
from app.models.daily_brief_preference import DailyBriefPreference
from app.models.user import UserProfile
from app.schemas.automations import (
    AutomationCadenceOption,
    AutomationDailyBriefSummary,
    AutomationItemResponse,
    AutomationKey,
    AutomationListResponse,
    AutomationNotificationResponse,
    AutomationQueueItem,
    AutomationRunResponse,
    AutomationRunSummary,
    AutomationRunStep,
    AutomationUpdateRequest,
)
from app.services.dashboard import build_dashboard_state
from app.services.news import refresh_news_feeds_sync

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AutomationDefinition:
    key: AutomationKey
    title: str
    summary: str
    default_enabled: bool
    default_cadence_key: str
    read_scope: tuple[str, ...]
    output_scope: tuple[str, ...]
    confirmation_boundary: str
    safety_boundary: str
    cadence_options: tuple[tuple[str, str], ...]


@dataclass
class AutomationSchedulerResult:
    scanned_count: int
    ran_count: int
    failed_count: int
    run_ids: list[str] = field(default_factory=list)
    created_pending_proposal_ids: list[str] = field(default_factory=list)
    created_notification_ids: list[str] = field(default_factory=list)


AUTOMATION_DEFINITIONS: tuple[AutomationDefinition, ...] = (
    AutomationDefinition(
        key="daily_brief",
        title="每日简报",
        summary="每天自动整理画像、组合、资讯和训练状态，生成 Today 的第一条判断。",
        default_enabled=True,
        default_cadence_key="daily_0830",
        read_scope=("风险画像", "最近组合报告", "资讯/政策分析", "学习与训练状态"),
        output_scope=("今日判断", "最多三条证据", "安全下一步", "确认边界"),
        confirmation_boundary="画像、计划或长期偏好写回前必须确认。",
        safety_boundary="不会生成买卖、清仓、满仓或收益确定语言。",
        cadence_options=(
            ("daily_0830", "每天 08:30"),
            ("daily_1200", "每天 12:00"),
            ("workday_0830", "工作日 08:30"),
        ),
    ),
    AutomationDefinition(
        key="weekly_portfolio",
        title="每周组合巡检",
        summary="每周检查集中度、现金比例和资产暴露，把问题变成可追问的任务。",
        default_enabled=False,
        default_cadence_key="weekly_monday_0900",
        read_scope=("最近组合快照", "持仓权重", "现金比例", "风险问卷"),
        output_scope=("集中度提醒", "组合健康摘要", "建议追问任务"),
        confirmation_boundary="只准备检查建议，不自动改组合和计划。",
        safety_boundary="只解释风险来源，不输出账户操作指令。",
        cadence_options=(
            ("weekly_monday_0900", "每周一 09:00"),
            ("weekly_friday_1800", "每周五 18:00"),
        ),
    ),
    AutomationDefinition(
        key="news_watch",
        title="资讯影响观察",
        summary="只观察和你的画像、持仓、风险敏感点有关的资讯，不堆新闻列表。",
        default_enabled=False,
        default_cadence_key="workday_1600",
        read_scope=("新闻/政策项", "持仓类型", "风险敏感点", "最近简报证据"),
        output_scope=("影响路径", "不确定性", "是否值得追问"),
        confirmation_boundary="加入长期观察或偏好前必须确认。",
        safety_boundary="新闻相关性不是交易信号。",
        cadence_options=(
            ("workday_1600", "工作日 16:00"),
            ("daily_1800", "每天 18:00"),
        ),
    ),
    AutomationDefinition(
        key="behavior_observation",
        title="行为偏差观察",
        summary="从对话和训练理由里发现追热点、回撤焦虑等信号，但只生成待确认证据。",
        default_enabled=False,
        default_cadence_key="weekly_friday_1800",
        read_scope=("用户提问", "模拟理由", "已确认行为证据", "训练复盘"),
        output_scope=("待确认行为证据", "训练建议", "冲动约束提醒"),
        confirmation_boundary="弱证据只进入待确认，不直接改长期画像。",
        safety_boundary="一次行为不会被写成永久标签。",
        cadence_options=(
            ("weekly_friday_1800", "每周五 18:00"),
            ("weekly_monday_0900", "每周一 09:00"),
        ),
    ),
)

AUTOMATION_BY_KEY = {definition.key: definition for definition in AUTOMATION_DEFINITIONS}
DEFAULT_INCLUDE_SOURCES = [
    "profile",
    "portfolio",
    "news_policy",
    "learning",
    "simulation",
    "behavior",
    "coach_history",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timezone_or_default(timezone_name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name or "Asia/Shanghai")
    except ZoneInfoNotFoundError:
        return ZoneInfo("Asia/Shanghai")


def _cadence_label(definition: AutomationDefinition, cadence_key: str) -> str:
    labels = dict(definition.cadence_options)
    return labels.get(cadence_key, cadence_key)


def _cadence_parts(cadence_key: str) -> tuple[str, int | None, int, int] | None:
    mapping = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
    }
    parts = cadence_key.split("_")
    if len(parts) == 2 and parts[0] in {"daily", "workday"}:
        time_part = parts[1]
        return (parts[0], None, int(time_part[:2]), int(time_part[2:]))
    if len(parts) == 3 and parts[0] == "weekly" and parts[1] in mapping:
        time_part = parts[2]
        return ("weekly", mapping[parts[1]], int(time_part[:2]), int(time_part[2:]))
    return None


def _next_run_at(
    cadence_key: str,
    *,
    from_time: datetime | None = None,
    timezone_name: str | None = None,
) -> datetime:
    base = _as_aware_utc(from_time or _utc_now())
    zone = _timezone_or_default(timezone_name)
    local_base = base.astimezone(zone)
    parts = _cadence_parts(cadence_key)
    if parts is None:
        return base + timedelta(days=7 if cadence_key.startswith("weekly") else 1)

    cadence_type, target_weekday, hour, minute = parts
    candidate = local_base.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if cadence_type == "weekly" and target_weekday is not None:
        days_ahead = (target_weekday - local_base.weekday()) % 7
        candidate = candidate + timedelta(days=days_ahead)
        if candidate <= local_base:
            candidate = candidate + timedelta(days=7)
    else:
        if candidate <= local_base:
            candidate = candidate + timedelta(days=1)
        if cadence_type == "workday":
            while candidate.weekday() >= 5:
                candidate = candidate + timedelta(days=1)

    return candidate.astimezone(timezone.utc)


def _definition_or_404(automation_key: str) -> AutomationDefinition:
    definition = AUTOMATION_BY_KEY.get(automation_key)
    if definition is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Automation does not exist.",
        )
    return definition


def _ensure_daily_brief_preference(
    db: Session,
    *,
    user: UserProfile,
) -> DailyBriefPreference:
    preference = db.scalar(
        select(DailyBriefPreference).where(DailyBriefPreference.user_id == user.id)
    )
    if preference is not None:
        return preference

    preference = DailyBriefPreference(
        user_id=user.id,
        enabled=True,
        cadence_key="daily_0830",
        timezone="Asia/Shanghai",
        include_sources=DEFAULT_INCLUDE_SOURCES,
    )
    db.add(preference)
    db.flush()
    return preference


def _automation_timezone(
    db: Session,
    *,
    user: UserProfile,
    definition: AutomationDefinition,
) -> str:
    if definition.key != "daily_brief":
        return "Asia/Shanghai"
    preference = _ensure_daily_brief_preference(db, user=user)
    return preference.timezone


def ensure_automation_settings(
    db: Session,
    *,
    user: UserProfile,
) -> list[AutomationSetting]:
    existing = {
        setting.automation_key: setting
        for setting in db.scalars(
            select(AutomationSetting).where(AutomationSetting.user_id == user.id)
        )
    }
    settings: list[AutomationSetting] = []
    for definition in AUTOMATION_DEFINITIONS:
        setting = existing.get(definition.key)
        if setting is None:
            setting = AutomationSetting(
                user_id=user.id,
                automation_key=definition.key,
                enabled=definition.default_enabled,
                frequency=definition.default_cadence_key,
                read_scope=list(definition.read_scope),
                produces=list(definition.output_scope),
                requires_confirmation_for=[definition.confirmation_boundary],
                safety_boundary=definition.safety_boundary,
                next_run_at=(
                    _next_run_at(
                        definition.default_cadence_key,
                        timezone_name="Asia/Shanghai",
                    )
                    if definition.default_enabled
                    else None
                ),
            )
            db.add(setting)
            db.flush()
        settings.append(setting)

    _ensure_daily_brief_preference(db, user=user)
    db.commit()
    for setting in settings:
        db.refresh(setting)
    return sorted(
        settings,
        key=lambda item: list(AUTOMATION_BY_KEY).index(item.automation_key),
    )


def _latest_run_by_key(db: Session, *, user_id: str) -> dict[str, AutomationRun]:
    rows = list(
        db.scalars(
            select(AutomationRun)
            .where(AutomationRun.user_id == user_id)
            .order_by(AutomationRun.started_at.desc(), AutomationRun.id.desc())
        )
    )
    latest: dict[str, AutomationRun] = {}
    for row in rows:
        latest.setdefault(row.automation_key, row)
    return latest


def _recent_notifications(
    db: Session,
    *,
    user_id: str,
    limit: int = 5,
) -> list[AutomationNotification]:
    return list(
        db.scalars(
            select(AutomationNotification)
            .where(AutomationNotification.user_id == user_id)
            .order_by(
                AutomationNotification.created_at.desc(),
                AutomationNotification.id.desc(),
            )
            .limit(limit)
        )
    )


def _serialize_notification(
    notification: AutomationNotification,
) -> AutomationNotificationResponse:
    return AutomationNotificationResponse(
        id=notification.id,
        automation_key=notification.automation_key,  # type: ignore[arg-type]
        title=notification.title,
        message=notification.message,
        action_label=notification.action_label,
        action_route=notification.action_route,
        created_at=notification.created_at,
        read_at=notification.read_at,
    )


def _serialize_item(
    setting: AutomationSetting,
    *,
    latest_run: AutomationRun | None,
    user: UserProfile,
) -> AutomationItemResponse:
    definition = _definition_or_404(setting.automation_key)
    disabled_reason = None
    item_status = "ready"
    if not user.onboarding_completed:
        item_status = "needs_profile"
        disabled_reason = "完成画像和风险问卷后才能安全运行自动任务。"
    elif not setting.enabled:
        item_status = "disabled"

    return AutomationItemResponse(
        key=definition.key,
        title=definition.title,
        summary=definition.summary,
        enabled=setting.enabled,
        default_enabled=definition.default_enabled,
        cadence_key=setting.frequency,
        cadence_label=_cadence_label(definition, setting.frequency),
        cadence_options=[
            AutomationCadenceOption(key=key, label=label)
            for key, label in definition.cadence_options
        ],
        read_scope=setting.read_scope,
        output_scope=setting.produces,
        confirmation_boundary=setting.requires_confirmation_for[0],
        safety_boundary=setting.safety_boundary,
        status=item_status,  # type: ignore[arg-type]
        disabled_reason=disabled_reason,
        last_run=(
            AutomationRunSummary(
                run_id=latest_run.id,
                agent_run_id=latest_run.agent_run_id,
                status=latest_run.status,  # type: ignore[arg-type]
                started_at=latest_run.started_at,
                completed_at=latest_run.completed_at,
                summary=latest_run.summary,
                output_ref=latest_run.output_ref,
            )
            if latest_run is not None
            else None
        ),
        next_run_at=setting.next_run_at,
        can_run_now=user.onboarding_completed and setting.enabled,
    )


def get_automations_state(
    db: Session,
    *,
    user: UserProfile,
) -> AutomationListResponse:
    settings = ensure_automation_settings(db, user=user)
    latest_runs = _latest_run_by_key(db, user_id=user.id)
    dashboard = build_dashboard_state(db, user=user)
    items = [
        _serialize_item(
            setting,
            latest_run=latest_runs.get(setting.automation_key),
            user=user,
        )
        for setting in settings
    ]
    active_items = [item for item in items if item.enabled]
    next_queue = [
        AutomationQueueItem(
            automation_key=item.key,
            title=item.title,
            next_run_at=item.next_run_at,
            cadence_label=item.cadence_label,
        )
        for item in active_items
    ]
    next_queue.sort(
        key=lambda item: item.next_run_at
        or datetime.max.replace(tzinfo=timezone.utc)
    )

    brief = dashboard.daily_brief
    return AutomationListResponse(
        user_id=user.id,
        updated_at=_utc_now(),
        active_count=len(active_items),
        total_count=len(items),
        automations=items,
        next_queue=next_queue,
        daily_brief_summary=AutomationDailyBriefSummary(
            brief_id=brief.brief_id,
            headline=brief.headline,
            beginner_explanation=brief.beginner_explanation,
            as_of=brief.as_of,
            source_coverage={
                key: bool(value) for key, value in brief.source_coverage.items()
            },
        ),
        recent_notifications=[
            _serialize_notification(notification)
            for notification in _recent_notifications(db, user_id=user.id)
        ],
    )


def update_automation_setting(
    db: Session,
    *,
    user: UserProfile,
    automation_key: str,
    payload: AutomationUpdateRequest,
) -> AutomationItemResponse:
    definition = _definition_or_404(automation_key)
    settings = ensure_automation_settings(db, user=user)
    setting = next(item for item in settings if item.automation_key == definition.key)

    if payload.cadence_key is not None:
        valid_cadences = {key for key, _label in definition.cadence_options}
        if payload.cadence_key not in valid_cadences:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="cadence_key is not valid for this automation.",
            )
        setting.frequency = payload.cadence_key

    if payload.enabled is not None:
        setting.enabled = payload.enabled

    if definition.key == "daily_brief":
        preference = _ensure_daily_brief_preference(db, user=user)
        preference.enabled = setting.enabled
        preference.cadence_key = setting.frequency
        if payload.timezone is not None:
            preference.timezone = payload.timezone
        preference.updated_at = _utc_now()

    setting.next_run_at = (
        _next_run_at(
            setting.frequency,
            timezone_name=_automation_timezone(db, user=user, definition=definition),
        )
        if setting.enabled
        else None
    )

    setting.updated_at = _utc_now()
    db.commit()
    db.refresh(setting)
    latest_run = _latest_run_by_key(db, user_id=user.id).get(definition.key)
    return _serialize_item(setting, latest_run=latest_run, user=user)


def run_automation_now(
    db: Session,
    *,
    user: UserProfile,
    automation_key: str,
) -> AutomationRunResponse:
    definition = _definition_or_404(automation_key)
    settings = ensure_automation_settings(db, user=user)
    setting = next(item for item in settings if item.automation_key == definition.key)
    if not setting.enabled:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Automation is disabled.",
        )
    if not user.onboarding_completed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Complete onboarding before running automation.",
        )

    return _execute_automation(
        db,
        user=user,
        setting=setting,
        definition=definition,
        trigger="manual",
    )


def run_due_automations(
    db: Session,
    *,
    now: datetime | None = None,
    limit: int = 25,
    retry_delay_minutes: int = 30,
) -> AutomationSchedulerResult:
    effective_now = _as_aware_utc(now or _utc_now())
    due_settings = list(
        db.scalars(
            select(AutomationSetting)
            .join(UserProfile, UserProfile.id == AutomationSetting.user_id)
            .where(
                AutomationSetting.enabled.is_(True),
                AutomationSetting.next_run_at.is_not(None),
                AutomationSetting.next_run_at <= effective_now,
                UserProfile.onboarding_completed.is_(True),
            )
            .order_by(AutomationSetting.next_run_at.asc(), AutomationSetting.id.asc())
            .with_for_update(skip_locked=True)
            .limit(limit)
        )
    )
    result = AutomationSchedulerResult(
        scanned_count=len(due_settings),
        ran_count=0,
        failed_count=0,
    )
    for setting in due_settings:
        definition = AUTOMATION_BY_KEY.get(setting.automation_key)
        user = db.get(UserProfile, setting.user_id)
        if definition is None or user is None:
            result.failed_count += 1
            continue
        try:
            run = _execute_automation(
                db,
                user=user,
                setting=setting,
                definition=definition,
                trigger="scheduled",
                now=effective_now,
            )
        except Exception:
            logger.exception(
                "scheduled automation failed",
                extra={"automation_key": setting.automation_key, "user_id": user.id},
            )
            db.rollback()
            _record_failed_automation_run(
                db,
                user=user,
                setting=setting,
                definition=definition,
                trigger="scheduled",
                due_at=effective_now,
                error_message="Scheduled automation failed before producing output.",
                retry_delay_minutes=retry_delay_minutes,
            )
            result.failed_count += 1
            continue
        result.ran_count += 1
        result.run_ids.append(run.run_id)
        result.created_pending_proposal_ids.extend(run.created_pending_proposal_ids)
        notification_ids = run.output_payload.get("created_notification_ids", [])
        if isinstance(notification_ids, list):
            result.created_notification_ids.extend(
                item for item in notification_ids if isinstance(item, str)
            )
    return result


def _automation_run_steps(
    *,
    definition: AutomationDefinition,
    status_value: str,
    refreshed_news_count: int = 0,
    proposal_count: int = 0,
) -> list[AutomationRunStep]:
    final_status = "failed" if status_value == "failed" else "completed"
    news_detail = (
        f"已同步 {refreshed_news_count} 条真实资讯，再筛选与用户上下文有关的信号。"
        if definition.key == "news_watch"
        else "已读取画像、组合、学习、模拟和最近简报上下文。"
    )
    proposal_detail = (
        f"已生成 {proposal_count} 条待确认建议。"
        if proposal_count > 0
        else "本轮没有新增待确认写回；只保留可读结果和通知。"
    )
    return [
        AutomationRunStep(
            key="submit_run",
            label="接收运行请求",
            status=final_status,  # type: ignore[arg-type]
            detail=f"{definition.title} 已进入本轮手动/自动执行。",
        ),
        AutomationRunStep(
            key="read_context",
            label="读取授权上下文",
            status=final_status,  # type: ignore[arg-type]
            detail=news_detail,
        ),
        AutomationRunStep(
            key="build_brief",
            label="整理今日判断和安全边界",
            status=final_status,  # type: ignore[arg-type]
            detail="已把证据压缩成一个判断、一个安全下一步和明确的不要做边界。",
        ),
        AutomationRunStep(
            key="prepare_writeback",
            label="准备待确认建议",
            status=final_status,  # type: ignore[arg-type]
            detail=proposal_detail,
        ),
        AutomationRunStep(
            key="notify_user",
            label="写入通知和运行记录",
            status=final_status,  # type: ignore[arg-type]
            detail="已保存运行记录，通知会出现在自动任务页，关键写回仍需用户确认。",
        ),
    ]


def _persist_automation_steps(
    db: Session,
    *,
    agent_run: AgentRun,
    steps: list[AutomationRunStep],
    started_at: datetime,
    completed_at: datetime | None,
) -> None:
    for index, step in enumerate(steps, start=1):
        db.add(
            AgentStep(
                run_id=agent_run.id,
                step_name=step.key,
                sequence=index,
                status=step.status,
                input_payload={"display_label": step.label},
                output_payload={"display_detail": step.detail},
                started_at=started_at,
                completed_at=completed_at,
            )
        )


def _execute_automation(
    db: Session,
    *,
    user: UserProfile,
    setting: AutomationSetting,
    definition: AutomationDefinition,
    trigger: str,
    now: datetime | None = None,
) -> AutomationRunResponse:
    started_at = _as_aware_utc(now or _utc_now())
    refreshed_news_count = 0
    if definition.key == "news_watch":
        refreshed_news_count = len(refresh_news_feeds_sync(db, limit_per_feed=10))
    dashboard = build_dashboard_state(db, user=user)
    brief = dashboard.daily_brief
    summary = _run_summary(definition.key, headline=brief.headline)
    output_payload = {
        "automation_key": definition.key,
        "headline": brief.headline,
        "source_coverage": {
            key: bool(value) for key, value in brief.source_coverage.items()
        },
        "primary_action": brief.primary_action.model_dump(mode="json"),
        "safety_boundary": definition.safety_boundary,
        "trigger_type": trigger,
        "refreshed_news_count": refreshed_news_count,
    }
    completed_at = _utc_now()
    run = AutomationRun(
        setting_id=setting.id,
        user_id=user.id,
        automation_key=definition.key,
        status="succeeded",
        trigger_type=trigger,
        summary=summary,
        output_ref=brief.brief_id if definition.key == "daily_brief" else None,
        output_payload=output_payload,
        safety_boundary=definition.safety_boundary,
        due_at=started_at if trigger == "scheduled" else None,
        started_at=started_at,
        completed_at=completed_at,
    )
    db.add(run)
    db.flush()
    agent_run = _create_automation_agent_run(
        db,
        user=user,
        run=run,
        setting=setting,
        definition=definition,
        trigger=trigger,
        output_payload=output_payload,
    )
    run.agent_run_id = agent_run.id
    proposal_ids = _create_pending_proposals(
        db,
        user=user,
        agent_run=agent_run,
        definition=definition,
        brief_headline=brief.headline,
    )
    steps = _automation_run_steps(
        definition=definition,
        status_value="succeeded",
        refreshed_news_count=refreshed_news_count,
        proposal_count=len(proposal_ids),
    )
    _persist_automation_steps(
        db,
        agent_run=agent_run,
        steps=steps,
        started_at=started_at,
        completed_at=completed_at,
    )
    notification = _create_notification(
        db,
        user=user,
        run=run,
        definition=definition,
        summary=summary,
    )
    output_payload = {
        **output_payload,
        "created_pending_proposal_ids": proposal_ids,
        "created_notification_ids": [notification.id],
    }
    run.output_payload = output_payload
    agent_run.output_payload = {**agent_run.output_payload, **output_payload}
    setting.last_run_at = completed_at
    setting.next_run_at = _next_run_at(
        setting.frequency,
        from_time=completed_at,
        timezone_name=_automation_timezone(db, user=user, definition=definition),
    )
    setting.updated_at = completed_at
    db.commit()
    db.refresh(run)
    db.refresh(setting)

    return AutomationRunResponse(
        run_id=run.id,
        agent_run_id=run.agent_run_id,
        automation_key=definition.key,
        status=run.status,  # type: ignore[arg-type]
        trigger_type=run.trigger_type,  # type: ignore[arg-type]
        due_at=run.due_at,
        started_at=run.started_at,
        completed_at=run.completed_at,
        summary=run.summary,
        output_ref=run.output_ref,
        error_message=run.error_message,
        created_pending_proposal_ids=proposal_ids,
        steps=steps,
        output_payload=run.output_payload,
    )


def _record_failed_automation_run(
    db: Session,
    *,
    user: UserProfile,
    setting: AutomationSetting,
    definition: AutomationDefinition,
    trigger: str,
    due_at: datetime,
    error_message: str,
    retry_delay_minutes: int,
) -> None:
    now = _utc_now()
    run = AutomationRun(
        setting_id=setting.id,
        user_id=user.id,
        automation_key=definition.key,
        status="failed",
        trigger_type=trigger,
        summary=f"{definition.title}本轮自动运行失败，已安排稍后重试。",
        output_ref=None,
        output_payload={
            "automation_key": definition.key,
            "trigger_type": trigger,
            "failure": "runtime_error",
        },
        safety_boundary=definition.safety_boundary,
        due_at=due_at,
        error_message=error_message,
        started_at=now,
        completed_at=now,
    )
    db.add(run)
    setting.next_run_at = now + timedelta(minutes=max(5, retry_delay_minutes))
    setting.updated_at = now
    db.commit()


def _create_automation_agent_run(
    db: Session,
    *,
    user: UserProfile,
    run: AutomationRun,
    setting: AutomationSetting,
    definition: AutomationDefinition,
    trigger: str,
    output_payload: dict,
) -> AgentRun:
    agent_run = AgentRun(
        session_id=f"automation:{setting.id}",
        user_id=user.id,
        model_name="deterministic:automation-scheduler",
        schema_version="v1",
        run_status="completed",
        run_type="automation_run",
        intent=definition.key,
        orchestrator_version="automation-scheduler-v1",
        policy_status="passed",
        latency_ms=0,
        context_snapshot={
            "automation_key": definition.key,
            "setting_id": setting.id,
            "automation_run_id": run.id,
            "trigger_type": trigger,
        },
        context_snapshot_version="automation_context_v1",
        input_payload={
            "automation_key": definition.key,
            "frequency": setting.frequency,
            "trigger_type": trigger,
        },
        output_payload={**output_payload, "automation_run_id": run.id},
        tool_trace={
            "worker": "automation_scheduler",
            "requires_confirmation_for": setting.requires_confirmation_for,
        },
        completed_at=run.completed_at,
        created_at=run.started_at,
    )
    db.add(agent_run)
    db.flush()
    return agent_run


def _create_pending_proposals(
    db: Session,
    *,
    user: UserProfile,
    agent_run: AgentRun,
    definition: AutomationDefinition,
    brief_headline: str,
) -> list[str]:
    proposal = _proposal_for_automation(
        user=user,
        agent_run=agent_run,
        definition=definition,
        brief_headline=brief_headline,
    )
    if proposal is None:
        return []
    duplicate = db.scalar(
        select(AgentStateUpdateProposal.id)
        .join(AgentRun, AgentRun.id == AgentStateUpdateProposal.run_id)
        .where(
            AgentRun.user_id == user.id,
            AgentStateUpdateProposal.target_type == proposal.target_type,
            AgentStateUpdateProposal.target_id == proposal.target_id,
            AgentStateUpdateProposal.user_decision_status == "pending",
        )
        .limit(1)
    )
    if duplicate is not None:
        return []
    db.add(proposal)
    db.flush()
    return [proposal.id]


def _proposal_for_automation(
    *,
    user: UserProfile,
    agent_run: AgentRun,
    definition: AutomationDefinition,
    brief_headline: str,
) -> AgentStateUpdateProposal | None:
    if definition.key == "daily_brief":
        return None
    if definition.key == "weekly_portfolio":
        return AgentStateUpdateProposal(
            run_id=agent_run.id,
            target_type="safe_next_action",
            target_id="portfolio-concentration-review",
            patch_payload={
                "automation_key": definition.key,
                "action_label": "查看组合集中度",
                "target_route": "/portfolio?from=automation&focus=concentration",
                "message_excerpt": "本周组合巡检已完成，建议先查看集中度和单一主题暴露。",
            },
            reason="自动任务已准备组合巡检结果，等待用户决定是否查看和采纳后续训练。",
            validator_status="passed",
            validator_message="不包含交易执行或仓位指令。",
        )
    if definition.key == "news_watch":
        return AgentStateUpdateProposal(
            run_id=agent_run.id,
            target_type="safe_next_action",
            target_id="news-impact-review",
            patch_payload={
                "automation_key": definition.key,
                "action_label": "查看资讯影响路径",
                "target_route": "/news?from=automation&focus=impact",
                "message_excerpt": "资讯影响观察已筛出一条与用户上下文有关的影响路径。",
            },
            reason="自动任务已准备资讯影响观察，等待用户确认是否进入详情或持续观察。",
            validator_status="passed",
            validator_message="新闻相关性不被写成交易信号。",
        )
    if definition.key == "behavior_observation":
        return AgentStateUpdateProposal(
            run_id=agent_run.id,
            target_type="behavior_profile_note",
            target_id=user.id,
            patch_payload={
                "automation_key": definition.key,
                "observed_trigger": "自动行为观察提示用户需要继续关注冲动决策线索。",
                "message_excerpt": f"围绕“{brief_headline}”的自动观察生成一条待确认行为训练线索。",
                "suggested_training_focus": "先记录理由，再决定是否行动。",
            },
            reason="行为偏差观察只能生成待确认线索，不能直接改写长期画像。",
            validator_status="passed",
            validator_message="确认后只追加行为证据，不触发账户动作。",
        )
    return None


def _create_notification(
    db: Session,
    *,
    user: UserProfile,
    run: AutomationRun,
    definition: AutomationDefinition,
    summary: str,
) -> AutomationNotification:
    action_label, action_route = _notification_action(definition.key)
    notification = AutomationNotification(
        user_id=user.id,
        run_id=run.id,
        automation_key=definition.key,
        title=f"{definition.title}已完成",
        message=summary,
        action_label=action_label,
        action_route=action_route,
        created_at=run.completed_at or _utc_now(),
    )
    db.add(notification)
    db.flush()
    return notification


def _notification_action(key: AutomationKey) -> tuple[str, str]:
    if key == "daily_brief":
        return "查看今日简报", "/today"
    if key == "weekly_portfolio":
        return "查看组合巡检", "/portfolio?from=automation&focus=concentration"
    if key == "news_watch":
        return "查看影响路径", "/news?from=automation&focus=impact"
    return "确认行为线索", "/profile"


def _run_summary(key: AutomationKey, *, headline: str) -> str:
    if key == "daily_brief":
        return f"已生成今日简报：{headline}"
    if key == "weekly_portfolio":
        return "已完成本周组合巡检草稿，重点仍是理解集中度和风险来源。"
    if key == "news_watch":
        return "已完成资讯影响观察，本轮只保留与用户上下文有关的影响路径。"
    return "已完成行为偏差观察，本轮只生成需要用户确认的训练线索。"
