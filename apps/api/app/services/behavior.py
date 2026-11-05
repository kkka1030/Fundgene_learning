from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.behavior_profile import BehaviorProfile
from app.models.risk_questionnaire import RiskQuestionnaire
from app.models.scenario import Scenario
from app.models.simulation_review import SimulationReview
from app.models.simulation_session import SimulationSession
from app.models.user import UserProfile
from app.schemas.behavior import (
    BehaviorQuestionnaireSubmitRequest,
    BehaviorTrainingPlanResponse,
    RiskLevel,
)
from app.services.simulation import behavior_focus_guidance

SUPPORTED_QUESTIONNAIRE_VERSION = "v1"


@dataclass
class QuestionnaireAssessment:
    risk_score: int
    risk_level: RiskLevel
    bias_tags: list[str]
    evidence: list[str]


def assess_questionnaire(answers: dict[str, int]) -> QuestionnaireAssessment:
    normalized = {key: max(1, min(5, int(value))) for key, value in answers.items()}

    risk_inputs = [
        normalized.get("volatility_comfort", 3),
        normalized.get("drawdown_reaction", 3),
        normalized.get("investment_horizon", 3),
    ]
    average_score = sum(risk_inputs) / len(risk_inputs)
    risk_score = round(((average_score - 1) / 4) * 100)

    if risk_score >= 67:
        risk_level: RiskLevel = "growth"
    elif risk_score >= 34:
        risk_level = "balanced"
    else:
        risk_level = "conservative"

    bias_tags: list[str] = []
    evidence: list[str] = []

    if normalized.get("panic_sell_impulse", 1) >= 4:
        bias_tags.append("panic_selling_risk")
        evidence.append("High impulse to sell during market drawdowns.")
    if normalized.get("chase_hot_funds", 1) >= 4:
        bias_tags.append("performance_chasing_risk")
        evidence.append("Easily pulled by short-term fund popularity or rankings.")
    if normalized.get("diversification_habit", 5) <= 2:
        bias_tags.append("concentration_risk")
        evidence.append("Diversification habit is still weak and needs structure.")

    if not bias_tags:
        bias_tags = ["no_major_bias_detected"]
        evidence = ["No major high-risk behavior signal was triggered in this pass."]

    return QuestionnaireAssessment(
        risk_score=risk_score,
        risk_level=risk_level,
        bias_tags=bias_tags,
        evidence=evidence,
    )


def submit_questionnaire(
    db: Session,
    *,
    user: UserProfile,
    payload: BehaviorQuestionnaireSubmitRequest,
) -> tuple[RiskQuestionnaire, BehaviorProfile]:
    if payload.questionnaire_version != SUPPORTED_QUESTIONNAIRE_VERSION:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported questionnaire version.",
        )

    assessment = assess_questionnaire(payload.answers)

    questionnaire = RiskQuestionnaire(
        user_id=user.id,
        questionnaire_version=payload.questionnaire_version,
        answers=payload.answers,
        risk_score=assessment.risk_score,
        risk_level=assessment.risk_level,
    )
    db.add(questionnaire)

    behavior_profile = db.scalar(
        select(BehaviorProfile).where(BehaviorProfile.user_id == user.id)
    )
    if behavior_profile is None:
        behavior_profile = BehaviorProfile(
            user_id=user.id,
            risk_level=assessment.risk_level,
            bias_tags=assessment.bias_tags,
            evidence=assessment.evidence,
            updated_at=datetime.now(timezone.utc),
        )
        db.add(behavior_profile)
    else:
        behavior_profile.risk_level = assessment.risk_level
        behavior_profile.bias_tags = assessment.bias_tags
        behavior_profile.evidence = assessment.evidence
        behavior_profile.updated_at = datetime.now(timezone.utc)

    user.onboarding_completed = True
    user.updated_at = datetime.now(timezone.utc)

    db.commit()
    db.refresh(questionnaire)
    db.refresh(behavior_profile)
    db.refresh(user)
    return questionnaire, behavior_profile


def get_behavior_profile(db: Session, *, user_id: str) -> BehaviorProfile | None:
    return db.scalar(select(BehaviorProfile).where(BehaviorProfile.user_id == user_id))


def get_latest_questionnaire(db: Session, *, user_id: str) -> RiskQuestionnaire | None:
    return db.scalar(
        select(RiskQuestionnaire)
        .where(RiskQuestionnaire.user_id == user_id)
        .order_by(RiskQuestionnaire.submitted_at.desc())
    )


def get_behavior_training_plan(
    db: Session, *, user: UserProfile
) -> BehaviorTrainingPlanResponse:
    behavior_profile = get_behavior_profile(db, user_id=user.id)
    focus_bias_tag = None
    if behavior_profile is not None:
        for tag in behavior_profile.bias_tags:
            if tag in {"panic_selling_risk", "performance_chasing_risk", "concentration_risk"}:
                focus_bias_tag = tag
                break

    focus = behavior_focus_guidance(focus_bias_tag)
    latest_review_row = db.execute(
        select(SimulationReview, SimulationSession, Scenario)
        .join(SimulationSession, SimulationSession.id == SimulationReview.session_id)
        .join(Scenario, Scenario.id == SimulationSession.scenario_id)
        .where(SimulationSession.user_id == user.id)
        .order_by(SimulationReview.generated_at.desc())
    ).first()
    recent_review_summary = (
        latest_review_row[0].decision_summary if latest_review_row is not None else None
    )

    if focus is None:
        focus_title = "基础情境训练"
        guidance = "先完成一条最基础的历史情境，把“先停一下再决定”练成固定动作。"
        recommended_scenario_slug = "covid-volatility-discipline"
    else:
        focus_title = focus["title"]
        guidance = focus["guidance"]
        recommended_scenario_slug = focus["scenario_slug"]

    next_actions = [
        f"进入 Simulation，优先完成与“{focus_title}”对应的情境训练。",
        "完成后回到 Dashboard，看新的行为线索是否改变了下一步动作。",
        "把训练里最容易失守的节点带回 Coach，用自己的真实例子继续拆解。",
    ]
    if recent_review_summary is not None:
        next_actions[1] = "回看最近一次情境训练复盘，把最容易失守的触发点写成一句防呆提示。"

    return BehaviorTrainingPlanResponse(
        user_id=user.id,
        focus_bias_tag=focus_bias_tag,
        focus_title=focus_title,
        guidance=guidance,
        recommended_scenario_slug=recommended_scenario_slug,
        recent_review_summary=recent_review_summary,
        next_actions=next_actions,
    )
