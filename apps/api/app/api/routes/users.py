from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.deps import get_current_user
from app.models.user import UserProfile
from app.schemas.user import UserMeResponse

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserMeResponse)
def read_current_user(
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> UserMeResponse:
    return UserMeResponse.model_validate(user)
