from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.core.database import get_db_session
from app.models.user import UserProfile
from app.schemas.simulation import (
    ScenarioCatalogResponse,
    SimulationActionSubmitRequest,
    SimulationReviewResponse,
    SimulationSessionResponse,
    SimulationSessionStartRequest,
)
from app.services.simulation import (
    get_review,
    get_session_state,
    list_scenarios,
    start_session,
    submit_action,
)

router = APIRouter(prefix="/simulations", tags=["simulations"])


@router.get("/scenarios", response_model=ScenarioCatalogResponse)
def read_scenarios(
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> ScenarioCatalogResponse:
    return list_scenarios(db, user=user)


@router.post("/sessions", response_model=SimulationSessionResponse)
def post_session(
    payload: SimulationSessionStartRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> SimulationSessionResponse:
    return start_session(db, user=user, payload=payload)


@router.get("/sessions/{session_id}", response_model=SimulationSessionResponse)
def read_session(
    session_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> SimulationSessionResponse:
    return get_session_state(db, user=user, session_id=session_id)


@router.post("/actions", response_model=SimulationSessionResponse)
def post_action(
    payload: SimulationActionSubmitRequest,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> SimulationSessionResponse:
    return submit_action(db, user=user, payload=payload)


@router.get("/review/{session_id}", response_model=SimulationReviewResponse)
def read_review(
    session_id: str,
    db: Annotated[Session, Depends(get_db_session)],
    user: Annotated[UserProfile, Depends(get_current_user)],
) -> SimulationReviewResponse:
    return get_review(db, user=user, session_id=session_id)
