from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.security import (
    create_session_token,
    hash_password,
    hash_session_token,
    verify_password,
)
from app.models.auth_session import AuthSession
from app.models.auth_user import AuthUser
from app.models.user import UserProfile


@dataclass(slots=True)
class AuthenticatedSession:
    auth_user: AuthUser
    session: AuthSession
    profile: UserProfile | None


def get_auth_user_by_id(db: Session, user_id: str) -> AuthUser | None:
    return db.get(AuthUser, user_id)


def get_auth_user_by_email(db: Session, email: str) -> AuthUser | None:
    return db.scalar(select(AuthUser).where(AuthUser.email == email))


def create_auth_user(db: Session, *, email: str, password: str) -> AuthUser:
    existing = get_auth_user_by_email(db, email)
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An account with this email already exists.",
        )

    now = datetime.now(timezone.utc)
    auth_user = AuthUser(
        email=email,
        password_hash=hash_password(password),
        created_at=now,
        updated_at=now,
    )
    db.add(auth_user)
    db.commit()
    db.refresh(auth_user)
    return auth_user


def authenticate_auth_user(db: Session, *, email: str, password: str) -> AuthUser:
    auth_user = get_auth_user_by_email(db, email)
    if auth_user is None or not verify_password(password, auth_user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )

    return auth_user


def create_persisted_session(
    db: Session,
    *,
    auth_user: AuthUser,
    ttl_hours: int,
) -> tuple[str, AuthSession]:
    now = datetime.now(timezone.utc)
    session_token = create_session_token()
    persisted_session = AuthSession(
        user_id=auth_user.id,
        session_token_hash=hash_session_token(session_token),
        expires_at=now + timedelta(hours=ttl_hours),
        created_at=now,
    )
    auth_user.last_login_at = now
    auth_user.updated_at = now
    db.add(persisted_session)
    db.add(auth_user)
    db.commit()
    db.refresh(persisted_session)
    db.refresh(auth_user)
    return session_token, persisted_session


def get_user_profile_for_auth_user(
    db: Session,
    *,
    auth_user: AuthUser,
) -> UserProfile | None:
    if auth_user.profile_id is None:
        return None

    return db.get(UserProfile, auth_user.profile_id)


def get_session_by_token(db: Session, *, session_token: str) -> AuthSession | None:
    now = datetime.now(timezone.utc)
    persisted_session = db.scalar(
        select(AuthSession).where(
            AuthSession.session_token_hash == hash_session_token(session_token)
        )
    )
    if persisted_session is None:
        return None

    expires_at = persisted_session.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if persisted_session.revoked_at is not None or expires_at <= now:
        return None

    return persisted_session


def build_authenticated_session(
    db: Session,
    *,
    auth_session: AuthSession,
) -> AuthenticatedSession | None:
    auth_user = get_auth_user_by_id(db, auth_session.user_id)
    if auth_user is None:
        return None

    profile = get_user_profile_for_auth_user(db, auth_user=auth_user)
    return AuthenticatedSession(
        auth_user=auth_user,
        session=auth_session,
        profile=profile,
    )


def revoke_session(db: Session, *, persisted_session: AuthSession) -> None:
    persisted_session.revoked_at = datetime.now(timezone.utc)
    db.add(persisted_session)
    db.commit()
