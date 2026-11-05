from typing import Annotated

from fastapi import Depends, HTTPException, Request, Response, status
from sqlalchemy.orm import Session

from app.core.database import get_db_session
from app.core.config import get_settings
from app.models.auth_session import AuthSession
from app.models.auth_user import AuthUser
from app.models.user import UserProfile
from app.services.auth import get_session_by_token
from app.services.onboarding import get_user_by_id


def get_session_cookie_name() -> str:
    return get_settings().auth_session_cookie_name


def set_auth_session_cookie(response: Response, *, session_token: str) -> None:
    settings = get_settings()
    response.set_cookie(
        key=settings.auth_session_cookie_name,
        value=session_token,
        httponly=True,
        secure=settings.auth_session_secure,
        samesite=settings.auth_session_samesite,
        max_age=settings.auth_session_ttl_hours * 60 * 60,
        path="/",
    )


def clear_auth_session_cookie(response: Response) -> None:
    settings = get_settings()
    response.delete_cookie(
        key=settings.auth_session_cookie_name,
        path="/",
        samesite=settings.auth_session_samesite,
    )


def get_current_auth_session(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> AuthSession:
    session_token = request.cookies.get(get_session_cookie_name())
    if session_token is None or not session_token.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
        )

    auth_session = get_session_by_token(db, session_token=session_token)
    if auth_session is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session is invalid or expired.",
        )

    return auth_session


def get_optional_auth_session(
    request: Request,
    db: Annotated[Session, Depends(get_db_session)],
) -> AuthSession | None:
    session_token = request.cookies.get(get_session_cookie_name())
    if session_token is None or not session_token.strip():
        return None

    return get_session_by_token(db, session_token=session_token)


def get_current_auth_user(
    db: Annotated[Session, Depends(get_db_session)],
    auth_session: Annotated[AuthSession, Depends(get_current_auth_session)],
) -> AuthUser:
    auth_user = db.get(AuthUser, auth_session.user_id)
    if auth_user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authenticated user no longer exists.",
        )
    return auth_user


def get_current_user(
    db: Annotated[Session, Depends(get_db_session)],
    auth_user: Annotated[AuthUser, Depends(get_current_auth_user)],
) -> UserProfile:
    if auth_user.profile_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Current user has not completed onboarding yet.",
        )

    user = get_user_by_id(db, auth_user.profile_id)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Current user is not onboarded yet.",
        )

    return user
