from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_auth_user
from app.core.database import get_db_session
from app.models.auth_user import AuthUser
from app.schemas.onboarding import (
    OnboardingProfileUpsertRequest,
    OnboardingProfileUpsertResponse,
)
from app.schemas.user import UserMeResponse
from app.services.onboarding import upsert_user_profile

router = APIRouter(prefix="/onboarding", tags=["onboarding"])


@router.post("/profile", response_model=OnboardingProfileUpsertResponse)
def upsert_profile(
    payload: OnboardingProfileUpsertRequest,
    db: Annotated[Session, Depends(get_db_session)],
    auth_user: Annotated[AuthUser, Depends(get_current_auth_user)],
) -> OnboardingProfileUpsertResponse:
    user = upsert_user_profile(db, auth_user=auth_user, payload=payload)
    return OnboardingProfileUpsertResponse(user=UserMeResponse.model_validate(user))
