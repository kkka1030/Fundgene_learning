from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db_session
from app.models.user import UserProfile
from app.schemas.automations import (
    AutomationItemResponse,
    AutomationListResponse,
    AutomationRunResponse,
    AutomationUpdateRequest,
)
from app.services.automations import (
    get_automations_state,
    run_automation_now,
    update_automation_setting,
)

router = APIRouter(prefix="/automations", tags=["automations"])


@router.get("", response_model=AutomationListResponse)
def read_automations(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AutomationListResponse:
    return get_automations_state(db, user=user)


@router.patch("/{automation_key}", response_model=AutomationItemResponse)
def patch_automation(
    automation_key: str,
    payload: AutomationUpdateRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AutomationItemResponse:
    return update_automation_setting(
        db,
        user=user,
        automation_key=automation_key,
        payload=payload,
    )


@router.post("/{automation_key}/run", response_model=AutomationRunResponse)
def run_automation(
    automation_key: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> AutomationRunResponse:
    return run_automation_now(db, user=user, automation_key=automation_key)
