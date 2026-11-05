from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.models.auth_session import AuthSession
from app.models.auth_user import AuthUser


def test_register_login_logout_roundtrip(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "email": "coach@example.com",
            "password": "supersecure123",
        },
    )
    assert register_response.status_code == 201
    assert register_response.json()["user"]["email"] == "coach@example.com"

    session_response = client.get("/api/auth/me")
    assert session_response.status_code == 200
    assert session_response.json()["user"]["email"] == "coach@example.com"
    assert session_response.json()["user"]["onboarding_completed"] is False

    logout_response = client.post("/api/auth/logout")
    assert logout_response.status_code == 200
    assert logout_response.json()["signed_out"] is True

    post_logout_session_response = client.get("/api/auth/me")
    assert post_logout_session_response.status_code == 401

    bad_login_response = client.post(
        "/api/auth/login",
        json={
            "email": "coach@example.com",
            "password": "wrongpass123",
        },
    )
    assert bad_login_response.status_code == 401

    login_response = client.post(
        "/api/auth/login",
        json={
            "email": "coach@example.com",
            "password": "supersecure123",
        },
    )
    assert login_response.status_code == 200
    assert login_response.json()["user"]["email"] == "coach@example.com"

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(AuthUser)) == 1
        assert session.scalar(select(func.count()).select_from(AuthSession)) == 2
