from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models.auth_user import AuthUser
from app.models.user import UserProfile
from app.schemas.onboarding import OnboardingProfileUpsertRequest


def get_user_by_id(db: Session, user_id: str) -> UserProfile | None:
    return db.get(UserProfile, user_id)


def upsert_user_profile(
    db: Session,
    *,
    auth_user: AuthUser,
    payload: OnboardingProfileUpsertRequest,
) -> UserProfile:
    profile_id = auth_user.profile_id or auth_user.id
    user = db.get(UserProfile, profile_id)
    now = datetime.now(timezone.utc)

    if user is None:
        user = UserProfile(
            id=profile_id,
            display_name=payload.display_name,
            investing_experience=payload.investing_experience,
            monthly_contribution_band=payload.monthly_contribution_band,
            primary_goal=payload.primary_goal,
            onboarding_completed=False,
            created_at=now,
            updated_at=now,
        )
        db.add(user)
        db.flush()
    else:
        user.display_name = payload.display_name
        user.investing_experience = payload.investing_experience
        user.monthly_contribution_band = payload.monthly_contribution_band
        user.primary_goal = payload.primary_goal
        user.updated_at = now

    auth_user.profile_id = user.id
    auth_user.updated_at = now

    db.commit()
    db.refresh(user)
    return user
