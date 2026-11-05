from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db_session
from app.models.user import UserProfile
from app.schemas.learning import (
    LearningCourseDetailResponse,
    LearningPathResponse,
    LearningProgressUpdateRequest,
    LearningProgressUpdateResponse,
)
from app.services.learning import (
    get_learning_course_detail,
    get_learning_path,
    update_learning_progress,
)

router = APIRouter(prefix="/learning", tags=["learning"])


@router.get("/path", response_model=LearningPathResponse)
def read_learning_path(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> LearningPathResponse:
    return get_learning_path(db, user=user)


@router.get("/courses/{course_slug}", response_model=LearningCourseDetailResponse)
def read_learning_course(
    course_slug: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> LearningCourseDetailResponse:
    return get_learning_course_detail(db, user=user, course_slug=course_slug)


@router.post("/progress", response_model=LearningProgressUpdateResponse)
def post_learning_progress(
    payload: LearningProgressUpdateRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> LearningProgressUpdateResponse:
    return update_learning_progress(db, user=user, payload=payload)
