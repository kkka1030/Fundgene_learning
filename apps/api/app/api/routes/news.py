from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db_session
from app.models.user import UserProfile
from app.schemas.news import (
    NewsAnalysisResponse,
    NewsAnalyzeRequest,
    NewsItemDetailResponse,
    NewsListResponse,
)
from app.services.news import (
    create_news_analysis,
    get_news_analysis,
    get_news_item_detail,
    list_news_items,
    refresh_news_feeds,
)

router = APIRouter(prefix="/news", tags=["news"])


@router.get("", response_model=NewsListResponse)
async def read_news_items(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
    refresh: Annotated[bool, Query()] = False,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> NewsListResponse:
    if refresh:
        await refresh_news_feeds(db, limit_per_feed=limit)
    return list_news_items(db, user_id=user.id, limit=limit)


@router.post("/analyze", response_model=NewsAnalysisResponse)
def post_news_analysis(
    payload: NewsAnalyzeRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> NewsAnalysisResponse:
    return create_news_analysis(db, user=user, payload=payload)


@router.get("/analyses/{analysis_id}", response_model=NewsAnalysisResponse)
def read_news_analysis(
    analysis_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> NewsAnalysisResponse:
    return get_news_analysis(db, user=user, analysis_id=analysis_id)


@router.get("/{item_id}", response_model=NewsItemDetailResponse)
def read_news_item(
    item_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> NewsItemDetailResponse:
    return get_news_item_detail(db, user_id=user.id, item_id=item_id)
