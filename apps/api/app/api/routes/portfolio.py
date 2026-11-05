from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db_session
from app.models.user import UserProfile
from app.schemas.portfolio import (
    PortfolioHistoryResponse,
    PortfolioLatestResponse,
    PortfolioReportResponse,
    PortfolioSnapshotCreateRequest,
)
from app.services.portfolio import (
    create_portfolio_snapshot,
    get_latest_portfolio_report,
    get_portfolio_history,
)

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.post("/snapshots", response_model=PortfolioReportResponse)
def post_portfolio_snapshot(
    payload: PortfolioSnapshotCreateRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> PortfolioReportResponse:
    return create_portfolio_snapshot(db, user=user, payload=payload)


@router.get("/latest", response_model=PortfolioLatestResponse)
def read_latest_portfolio(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> PortfolioLatestResponse:
    return PortfolioLatestResponse(report=get_latest_portfolio_report(db, user_id=user.id))


@router.get("/history", response_model=PortfolioHistoryResponse)
def read_portfolio_history(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> PortfolioHistoryResponse:
    return get_portfolio_history(db, user_id=user.id)
