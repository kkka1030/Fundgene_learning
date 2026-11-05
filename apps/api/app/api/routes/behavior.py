from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db_session
from app.models.user import UserProfile
from app.schemas.behavior import (
    BehaviorProfileResponse,
    BehaviorQuestionnaireSubmitRequest,
    BehaviorQuestionnaireSubmitResponse,
    BehaviorTrainingPlanResponse,
)
from app.services.behavior import (
    get_behavior_profile,
    get_behavior_training_plan,
    submit_questionnaire,
)

router = APIRouter(prefix="/behavior", tags=["behavior"])


@router.post("/questionnaires", response_model=BehaviorQuestionnaireSubmitResponse)
def submit_behavior_questionnaire(
    payload: BehaviorQuestionnaireSubmitRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> BehaviorQuestionnaireSubmitResponse:
    questionnaire, behavior_profile = submit_questionnaire(
        db, user=user, payload=payload
    )
    return BehaviorQuestionnaireSubmitResponse(
        user_id=user.id,
        risk_score=questionnaire.risk_score,
        risk_level=questionnaire.risk_level,
        bias_tags=behavior_profile.bias_tags,
        evidence=behavior_profile.evidence,
        submitted_at=questionnaire.submitted_at,
    )


@router.get("/profile", response_model=BehaviorProfileResponse)
def read_behavior_profile(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> BehaviorProfileResponse:
    behavior_profile = get_behavior_profile(db, user_id=user.id)
    if behavior_profile is None:
        return BehaviorProfileResponse(
            user_id=user.id,
            risk_level=None,
            bias_tags=[],
            evidence=[],
            updated_at=None,
        )

    return BehaviorProfileResponse(
        user_id=user.id,
        risk_level=behavior_profile.risk_level,
        bias_tags=behavior_profile.bias_tags,
        evidence=behavior_profile.evidence,
        updated_at=behavior_profile.updated_at,
    )


@router.get("/training-plan", response_model=BehaviorTrainingPlanResponse)
def read_behavior_training_plan(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> BehaviorTrainingPlanResponse:
    return get_behavior_training_plan(db, user=user)
