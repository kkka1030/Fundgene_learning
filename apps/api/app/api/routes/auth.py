from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.orm import Session

from app.api.deps import (
    clear_auth_session_cookie,
    get_current_auth_session,
    get_current_auth_user,
    get_db_session,
    get_optional_auth_session,
    set_auth_session_cookie,
)
from app.core.config import get_settings
from app.models.auth_session import AuthSession
from app.models.auth_user import AuthUser
from app.schemas.auth import (
    AuthCredentialsRequest,
    AuthLogoutResponse,
    AuthSessionInfoResponse,
    AuthSessionResponse,
    AuthSessionUserResponse,
)
from app.schemas.user import UserMeResponse
from app.services.auth import (
    authenticate_auth_user,
    create_auth_user,
    create_persisted_session,
    get_user_profile_for_auth_user,
    revoke_session,
)

router = APIRouter(prefix="/auth", tags=["auth"])


def serialize_auth_user(
    auth_user: AuthUser,
    *,
    profile: UserMeResponse | None,
) -> AuthSessionUserResponse:
    return AuthSessionUserResponse(
        id=auth_user.id,
        email=auth_user.email,
        profile_id=auth_user.profile_id,
        onboarding_completed=profile.onboarding_completed if profile is not None else False,
        profile=profile,
    )


@router.post(
    "/register",
    response_model=AuthSessionResponse,
    status_code=status.HTTP_201_CREATED,
)
def register(
    payload: AuthCredentialsRequest,
    response: Response,
    db: Annotated[Session, Depends(get_db_session)],
) -> AuthSessionResponse:
    settings = get_settings()
    auth_user = create_auth_user(db, email=payload.email, password=payload.password)
    session_token, _persisted_session = create_persisted_session(
        db,
        auth_user=auth_user,
        ttl_hours=settings.auth_session_ttl_hours,
    )
    set_auth_session_cookie(response, session_token=session_token)
    return AuthSessionResponse(
        user=serialize_auth_user(auth_user, profile=None)
    )


@router.post("/login", response_model=AuthSessionResponse)
def login(
    payload: AuthCredentialsRequest,
    response: Response,
    db: Annotated[Session, Depends(get_db_session)],
) -> AuthSessionResponse:
    settings = get_settings()
    auth_user = authenticate_auth_user(db, email=payload.email, password=payload.password)
    session_token, _persisted_session = create_persisted_session(
        db,
        auth_user=auth_user,
        ttl_hours=settings.auth_session_ttl_hours,
    )
    profile_model = get_user_profile_for_auth_user(db, auth_user=auth_user)
    profile = (
        UserMeResponse.model_validate(profile_model) if profile_model is not None else None
    )
    set_auth_session_cookie(response, session_token=session_token)
    return AuthSessionResponse(user=serialize_auth_user(auth_user, profile=profile))


def _build_session_info_response(
    auth_session: AuthSession,
    auth_user: AuthUser,
    db: Session,
) -> AuthSessionInfoResponse:
    profile_model = get_user_profile_for_auth_user(db, auth_user=auth_user)
    profile = (
        UserMeResponse.model_validate(profile_model) if profile_model is not None else None
    )
    return AuthSessionInfoResponse(
        session_expires_at=auth_session.expires_at,
        user=serialize_auth_user(auth_user, profile=profile),
    )


@router.get("/me", response_model=AuthSessionInfoResponse)
def read_current_session_me(
    auth_session: Annotated[AuthSession, Depends(get_current_auth_session)],
    auth_user: Annotated[AuthUser, Depends(get_current_auth_user)],
    db: Annotated[Session, Depends(get_db_session)],
) -> AuthSessionInfoResponse:
    return _build_session_info_response(auth_session, auth_user, db)


@router.get("/session", response_model=AuthSessionInfoResponse)
def read_current_session_compat(
    auth_session: Annotated[AuthSession, Depends(get_current_auth_session)],
    auth_user: Annotated[AuthUser, Depends(get_current_auth_user)],
    db: Annotated[Session, Depends(get_db_session)],
) -> AuthSessionInfoResponse:
    return _build_session_info_response(auth_session, auth_user, db)


@router.post("/logout", response_model=AuthLogoutResponse)
def logout(
    response: Response,
    maybe_session: Annotated[AuthSession | None, Depends(get_optional_auth_session)],
    db: Annotated[Session, Depends(get_db_session)],
) -> AuthLogoutResponse:
    if maybe_session is not None:
        revoke_session(db, persisted_session=maybe_session)
    clear_auth_session_cookie(response)
    return AuthLogoutResponse()
