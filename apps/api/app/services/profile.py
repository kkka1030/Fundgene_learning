from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.agent_run import AgentRun
from app.models.agent_state_update_proposal import AgentStateUpdateProposal
from app.models.user_llm_setting import UserLlmSetting
from app.models.user import UserProfile
from app.core.config import get_settings
from app.schemas.profile import (
    ProfileAuthorizationScopeItem,
    ProfileAutomationAuthorization,
    ProfileBehaviorProfile,
    ProfileContextReadiness,
    ProfileContextResponse,
    ProfileLearningContext,
    ProfilePendingProposalResponse,
    ProfilePendingProposalsResponse,
    ProfilePortfolioContext,
    ProfileProposalDecisionResponse,
    ProfileReadinessItem,
    ProfileRiskProfile,
    ProfileSimulationContext,
    ProfileLlmSettingsResponse,
    ProfileLlmSettingsUpdateRequest,
    ProfileLlmSettingsUpdateResponse,
)
from app.services.automations import (
    AUTOMATION_BY_KEY,
    _cadence_label,
    ensure_automation_settings,
)
from app.services.behavior import (
    get_behavior_profile,
    get_latest_questionnaire,
)
from app.services.dashboard import build_dashboard_state


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _mask_api_key(value: str | None) -> str | None:
    if not value:
        return None
    stripped = value.strip()
    if len(stripped) <= 10:
        return f"{stripped[:2]}...{stripped[-2:]}"
    return f"{stripped[:6]}...{stripped[-4:]}"


def _get_user_llm_setting(
    db: Session,
    *,
    user_id: str,
) -> UserLlmSetting | None:
    return db.get(UserLlmSetting, user_id)


def _serialize_llm_settings(
    setting: UserLlmSetting | None,
) -> ProfileLlmSettingsResponse:
    settings = get_settings()
    user_key = (
        setting.api_key.strip()
        if setting is not None and setting.enabled and setting.api_key
        else None
    )
    workspace_key = settings.resolved_deepseek_api_key
    source = "user" if user_key else "workspace" if workspace_key else "none"
    configured = source != "none"
    model_name = setting.model_name if setting is not None else settings.advisor_model
    warning = None
    if not configured:
        warning = "当前没有可用的 LLM API，Agent 会明确提示模型未配置，不会伪装成模型回答。"

    return ProfileLlmSettingsResponse(
        provider="deepseek",
        model_name=model_name,
        configured=configured,
        enabled=setting.enabled if setting is not None else bool(workspace_key),
        source=source,  # type: ignore[arg-type]
        masked_api_key=(
            _mask_api_key(setting.api_key)
            if setting is not None and setting.api_key
            else ("工作区已配置" if workspace_key else None)
        ),
        updated_at=setting.updated_at if setting is not None else None,
        warning=warning,
    )


def get_profile_llm_settings(
    db: Session,
    *,
    user: UserProfile,
) -> ProfileLlmSettingsResponse:
    return _serialize_llm_settings(_get_user_llm_setting(db, user_id=user.id))


def get_effective_llm_config(
    db: Session,
    *,
    user: UserProfile,
) -> tuple[str, str | None, str]:
    settings = get_settings()
    setting = _get_user_llm_setting(db, user_id=user.id)
    if setting is not None and setting.enabled and setting.api_key:
        api_key = setting.api_key.strip()
        if api_key:
            return setting.model_name, api_key, "user"
    if settings.resolved_deepseek_api_key:
        return settings.advisor_model, None, "workspace"
    return (
        setting.model_name if setting is not None else settings.advisor_model,
        None,
        "none",
    )


def update_profile_llm_settings(
    db: Session,
    *,
    user: UserProfile,
    payload: ProfileLlmSettingsUpdateRequest,
) -> ProfileLlmSettingsUpdateResponse:
    model_name = payload.model_name.strip() or "deepseek:deepseek-v4-pro"
    if not model_name.startswith("deepseek:"):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="FundGene demo currently supports DeepSeek model names only.",
        )

    setting = _get_user_llm_setting(db, user_id=user.id)
    now = _utc_now()
    api_key = payload.api_key.strip() if payload.api_key else None
    if setting is None:
        setting = UserLlmSetting(
            user_id=user.id,
            provider=payload.provider,
            model_name=model_name,
            api_key=api_key,
            enabled=payload.enabled,
            created_at=now,
            updated_at=now,
        )
    else:
        setting.provider = payload.provider
        setting.model_name = model_name
        if api_key:
            setting.api_key = api_key
        setting.enabled = payload.enabled
        setting.updated_at = now
    db.add(setting)
    db.commit()
    db.refresh(setting)
    return ProfileLlmSettingsUpdateResponse(settings=_serialize_llm_settings(setting))


def _proposal_status(proposal: AgentStateUpdateProposal) -> str:
    return proposal.user_decision_status or "pending"


def _proposal_title(proposal: AgentStateUpdateProposal) -> str:
    if proposal.target_type == "behavior_profile_note":
        return "确认一条行为观察证据"
    if proposal.target_type == "automation_preference":
        return "确认自动任务偏好"
    return "确认 Agent 写回建议"


def _writeback_label(proposal: AgentStateUpdateProposal) -> str:
    if proposal.target_type == "behavior_profile_note":
        return "行为证据，不是永久标签"
    if proposal.target_type == "automation_preference":
        return "自动任务偏好"
    return proposal.target_type.replace("_", " ")


def _evidence_summary(proposal: AgentStateUpdateProposal) -> str:
    payload = proposal.patch_payload or {}
    excerpt = payload.get("message_excerpt")
    if isinstance(excerpt, str) and excerpt.strip():
        return excerpt.strip()
    trigger = payload.get("observed_trigger")
    if isinstance(trigger, str) and trigger.strip():
        return trigger.strip()
    return proposal.reason


def _serialize_proposal(
    proposal: AgentStateUpdateProposal,
) -> ProfilePendingProposalResponse:
    status_value = _proposal_status(proposal)
    safety_note = (
        "确认后只记录为训练线索，不会生成交易动作或直接改写仓位。"
        if proposal.target_type == "behavior_profile_note"
        else "确认后也只会进入 FundGene 内部偏好或资料，不会触发账户操作。"
    )
    return ProfilePendingProposalResponse(
        id=proposal.id,
        title=_proposal_title(proposal),
        source_label="Agent Workspace",
        evidence_summary=_evidence_summary(proposal),
        writeback_label=_writeback_label(proposal),
        target_type=proposal.target_type,
        target_id=proposal.target_id,
        patch_preview=proposal.patch_payload,
        reason=proposal.reason,
        validator_status=proposal.validator_status,
        validator_message=proposal.validator_message,
        status=status_value,  # type: ignore[arg-type]
        safety_note=safety_note,
        created_at=proposal.created_at,
        run_id=proposal.run_id,
    )


def _select_user_proposals(
    db: Session,
    *,
    user_id: str,
    only_pending: bool,
) -> list[AgentStateUpdateProposal]:
    query = (
        select(AgentStateUpdateProposal)
        .join(AgentRun, AgentRun.id == AgentStateUpdateProposal.run_id)
        .where(AgentRun.user_id == user_id)
        .order_by(
            AgentStateUpdateProposal.created_at.desc(),
            AgentStateUpdateProposal.id.desc(),
        )
    )
    if only_pending:
        query = query.where(AgentStateUpdateProposal.user_decision_status == "pending")
    return list(db.scalars(query))


def get_profile_pending_proposals(
    db: Session,
    *,
    user: UserProfile,
    only_pending: bool = True,
) -> ProfilePendingProposalsResponse:
    proposals = _select_user_proposals(db, user_id=user.id, only_pending=only_pending)
    resolved_count = db.scalar(
        select(func.count())
        .select_from(AgentStateUpdateProposal)
        .join(AgentRun, AgentRun.id == AgentStateUpdateProposal.run_id)
        .where(
            AgentRun.user_id == user.id,
            AgentStateUpdateProposal.user_decision_status != "pending",
        )
    )
    return ProfilePendingProposalsResponse(
        pending_count=sum(
            1 for proposal in proposals if _proposal_status(proposal) == "pending"
        ),
        resolved_count=int(resolved_count or 0),
        proposals=[_serialize_proposal(proposal) for proposal in proposals],
    )


def get_profile_context(
    db: Session,
    *,
    user: UserProfile,
) -> ProfileContextResponse:
    dashboard = build_dashboard_state(db, user=user)
    behavior_profile = get_behavior_profile(db, user_id=user.id)
    latest_questionnaire = get_latest_questionnaire(db, user_id=user.id)
    automation_settings = ensure_automation_settings(db, user=user)
    pending = get_profile_pending_proposals(db, user=user)

    readiness_items = [
        ProfileReadinessItem(
            key="profile",
            label="基础画像",
            ready=user.onboarding_completed,
            last_updated_at=user.updated_at,
            missing_action_route="/onboarding",
        ),
        ProfileReadinessItem(
            key="risk",
            label="风险问卷",
            ready=latest_questionnaire is not None,
            last_updated_at=(
                latest_questionnaire.submitted_at if latest_questionnaire else None
            ),
            missing_action_route="/onboarding",
        ),
        ProfileReadinessItem(
            key="portfolio",
            label="组合报告",
            ready=bool(dashboard.portfolio_status and dashboard.portfolio_status.has_report),
            last_updated_at=(
                dashboard.portfolio_status.latest_snapshot_date
                if dashboard.portfolio_status
                else None
            ),
            missing_action_route="/portfolio",
        ),
        ProfileReadinessItem(
            key="news",
            label="资讯分析",
            ready=bool(dashboard.news_status and dashboard.news_status.has_analysis),
            last_updated_at=(
                dashboard.news_status.generated_at if dashboard.news_status else None
            ),
            missing_action_route="/news",
        ),
        ProfileReadinessItem(
            key="learning",
            label="学习状态",
            ready=dashboard.learning_status is not None,
            last_updated_at=None,
            missing_action_route="/learning",
        ),
        ProfileReadinessItem(
            key="simulation",
            label="训练复盘",
            ready=bool(
                dashboard.simulation_status
                and dashboard.simulation_status.latest_review_summary
            ),
            last_updated_at=(
                dashboard.simulation_status.latest_completed_at
                if dashboard.simulation_status
                else None
            ),
            missing_action_route="/simulation",
        ),
        ProfileReadinessItem(
            key="behavior",
            label="行为证据",
            ready=behavior_profile is not None,
            last_updated_at=behavior_profile.updated_at if behavior_profile else None,
            missing_action_route="/onboarding",
        ),
        ProfileReadinessItem(
            key="automations",
            label="自动任务授权",
            ready=any(setting.enabled for setting in automation_settings),
            last_updated_at=max(setting.updated_at for setting in automation_settings),
            missing_action_route="/automations",
        ),
    ]
    ready_count = sum(1 for item in readiness_items if item.ready)

    return ProfileContextResponse(
        user_id=user.id,
        display_name=user.display_name,
        context_readiness=ProfileContextReadiness(
            ready_count=ready_count,
            total_count=len(readiness_items),
            items=readiness_items,
        ),
        risk_profile=ProfileRiskProfile(
            risk_level=dashboard.risk_level,
            latest_risk_score=dashboard.latest_risk_score,
            updated_at=latest_questionnaire.submitted_at if latest_questionnaire else None,
        ),
        behavior_profile=ProfileBehaviorProfile(
            bias_tags=dashboard.bias_tags,
            evidence=behavior_profile.evidence if behavior_profile else [],
            updated_at=behavior_profile.updated_at if behavior_profile else None,
        ),
        portfolio_context=ProfilePortfolioContext(
            has_report=(
                dashboard.portfolio_status.has_report
                if dashboard.portfolio_status is not None
                else False
            ),
            latest_snapshot_date=(
                dashboard.portfolio_status.latest_snapshot_date
                if dashboard.portfolio_status is not None
                else None
            ),
            total_value=(
                dashboard.portfolio_status.total_value
                if dashboard.portfolio_status is not None
                else None
            ),
            summary=(
                dashboard.portfolio_status.summary
                if dashboard.portfolio_status is not None
                else None
            ),
        ),
        learning_context=ProfileLearningContext(
            overall_progress_percentage=(
                dashboard.learning_status.overall_progress_percentage
                if dashboard.learning_status is not None
                else 0
            ),
            recommended_course_title=(
                dashboard.learning_status.recommended_course_title
                if dashboard.learning_status is not None
                else None
            ),
        ),
        simulation_context=ProfileSimulationContext(
            latest_review_summary=(
                dashboard.simulation_status.latest_review_summary
                if dashboard.simulation_status is not None
                else None
            ),
            latest_completed_at=(
                dashboard.simulation_status.latest_completed_at
                if dashboard.simulation_status is not None
                else None
            ),
        ),
        automation_authorizations=[
            ProfileAutomationAuthorization(
                automation_key=setting.automation_key,
                enabled=setting.enabled,
                cadence_label=_cadence_label(
                    AUTOMATION_BY_KEY[setting.automation_key],
                    setting.frequency,
                ),
            )
            for setting in automation_settings
        ],
        authorization_scope=[
            ProfileAuthorizationScopeItem(
                key="profile",
                label="基础画像、风险问卷和行为证据",
                readable=user.onboarding_completed,
            ),
            ProfileAuthorizationScopeItem(
                key="portfolio",
                label="最近组合快照和组合报告",
                readable=bool(
                    dashboard.portfolio_status and dashboard.portfolio_status.has_report
                ),
            ),
            ProfileAuthorizationScopeItem(
                key="agent_runs",
                label="Agent Workspace 对话和待确认写回",
                readable=True,
            ),
            ProfileAuthorizationScopeItem(
                key="automations",
                label="自动任务授权和运行记录",
                readable=True,
            ),
        ],
        pending_proposal_count=pending.pending_count,
    )


def _get_owned_proposal(
    db: Session,
    *,
    user: UserProfile,
    proposal_id: str,
) -> AgentStateUpdateProposal:
    proposal = db.scalar(
        select(AgentStateUpdateProposal)
        .join(AgentRun, AgentRun.id == AgentStateUpdateProposal.run_id)
        .where(
            AgentStateUpdateProposal.id == proposal_id,
            AgentRun.user_id == user.id,
        )
    )
    if proposal is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pending proposal does not exist for the current user.",
        )
    return proposal


def accept_profile_pending_proposal(
    db: Session,
    *,
    user: UserProfile,
    proposal_id: str,
    decision_note: str | None = None,
) -> ProfileProposalDecisionResponse:
    proposal = _get_owned_proposal(db, user=user, proposal_id=proposal_id)
    applied = False
    if proposal.user_decision_status in {"accepted", "applied"}:
        return ProfileProposalDecisionResponse(
            proposal=_serialize_proposal(proposal),
            applied_writeback=proposal.user_decision_status == "applied",
        )
    if proposal.user_decision_status == "rejected":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Rejected proposal cannot be accepted again.",
        )

    proposal.user_decision_status = "accepted"
    proposal.decision_note = decision_note
    proposal.decided_at = _utc_now()
    if proposal.target_type == "behavior_profile_note":
        applied = _apply_behavior_profile_note(db, user=user, proposal=proposal)
        if applied:
            proposal.user_decision_status = "applied"
            proposal.applied_at = _utc_now()

    db.commit()
    db.refresh(proposal)
    return ProfileProposalDecisionResponse(
        proposal=_serialize_proposal(proposal),
        applied_writeback=applied,
    )


def reject_profile_pending_proposal(
    db: Session,
    *,
    user: UserProfile,
    proposal_id: str,
    decision_note: str | None = None,
) -> ProfileProposalDecisionResponse:
    proposal = _get_owned_proposal(db, user=user, proposal_id=proposal_id)
    if proposal.user_decision_status in {"accepted", "applied"}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Accepted proposal cannot be rejected.",
        )
    proposal.user_decision_status = "rejected"
    proposal.decision_note = decision_note
    proposal.decided_at = _utc_now()
    db.commit()
    db.refresh(proposal)
    return ProfileProposalDecisionResponse(
        proposal=_serialize_proposal(proposal),
        applied_writeback=False,
    )


def _apply_behavior_profile_note(
    db: Session,
    *,
    user: UserProfile,
    proposal: AgentStateUpdateProposal,
) -> bool:
    behavior_profile = get_behavior_profile(db, user_id=user.id)
    if behavior_profile is None:
        return False

    note = _behavior_note_from_payload(proposal.patch_payload)
    evidence = list(behavior_profile.evidence)
    if note not in evidence:
        evidence.append(note)
    behavior_profile.evidence = evidence
    behavior_profile.updated_at = _utc_now()
    db.add(behavior_profile)
    return True


def _behavior_note_from_payload(payload: dict[str, Any]) -> str:
    excerpt = payload.get("message_excerpt")
    focus = payload.get("suggested_training_focus")
    if isinstance(excerpt, str) and excerpt.strip():
        return f"用户确认行为线索：{excerpt.strip()[:120]}"
    if isinstance(focus, str) and focus.strip():
        return f"用户确认训练线索：{focus.strip()[:120]}"
    return "用户确认了一条 Agent 生成的行为观察线索。"
