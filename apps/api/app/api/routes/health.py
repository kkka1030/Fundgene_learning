from datetime import datetime, timezone

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.core.config import get_settings
from app.schemas.health import HealthResponse, ReadyResponse

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        service=settings.app_name,
        environment=settings.app_env,
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/ready", response_model=ReadyResponse)
def readiness_check(
    db: Annotated[Session, Depends(get_db_session)],
) -> ReadyResponse:
    settings = get_settings()
    try:
        db.execute(text("select 1"))
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is not ready.",
        ) from exc

    return ReadyResponse(
        status="ready",
        service=settings.app_name,
        environment=settings.app_env,
        database="ok",
        timestamp=datetime.now(timezone.utc),
    )
