from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.models.behavior_profile import BehaviorProfile
from app.models.auth_session import AuthSession
from app.models.auth_user import AuthUser
from app.models.risk_questionnaire import RiskQuestionnaire
from app.models.user import UserProfile


def test_onboarding_questionnaire_dashboard_flow(
    client: TestClient, session_factory: sessionmaker[Session]
) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "email": "ava@example.com",
            "password": "supersecure123",
        },
    )
    assert register_response.status_code == 201

    session_payload = register_response.json()["user"]
    assert session_payload["email"] == "ava@example.com"
    assert session_payload["profile_id"] is None
    assert session_payload["onboarding_completed"] is False

    missing_user_response = client.get("/api/users/me")
    assert missing_user_response.status_code == 404

    create_profile_response = client.post(
        "/api/onboarding/profile",
        json={
            "display_name": "Ava",
            "investing_experience": "beginner",
            "monthly_contribution_band": "under_3000",
            "primary_goal": "先建立长期可执行的基金投资习惯。",
        },
    )

    assert create_profile_response.status_code == 200
    profile_payload = create_profile_response.json()["user"]
    user_id = profile_payload["id"]
    assert profile_payload["onboarding_completed"] is False

    user_response = client.get("/api/users/me")
    assert user_response.status_code == 200
    assert user_response.json()["display_name"] == "Ava"

    auth_session_response = client.get("/api/auth/session")
    assert auth_session_response.status_code == 200
    assert auth_session_response.json()["user"]["profile_id"] == user_id

    empty_behavior_response = client.get("/api/behavior/profile")
    assert empty_behavior_response.status_code == 200
    assert empty_behavior_response.json()["risk_level"] is None

    pending_dashboard_response = client.get("/api/dashboard")
    assert pending_dashboard_response.status_code == 200
    pending_dashboard_payload = pending_dashboard_response.json()
    assert pending_dashboard_payload["onboarding_completed"] is False
    assert pending_dashboard_payload["risk_level"] is None
    assert pending_dashboard_payload["summary_cards"][0]["value"] == "待完成"
    assert pending_dashboard_payload["daily_brief"]["status"] == "missing_profile"
    assert pending_dashboard_payload["daily_brief"]["primary_action"]["target_route"] == "/onboarding"
    assert "伪造个性化判断" in pending_dashboard_payload["daily_brief"]["beginner_explanation"]

    submit_response = client.post(
        "/api/behavior/questionnaires",
        json={
            "questionnaire_version": "v1",
            "answers": {
                "volatility_comfort": 4,
                "drawdown_reaction": 4,
                "investment_horizon": 5,
                "panic_sell_impulse": 4,
                "chase_hot_funds": 5,
                "diversification_habit": 2,
            },
        },
    )

    assert submit_response.status_code == 200
    submission_payload = submit_response.json()
    assert submission_payload["user_id"] == user_id
    assert submission_payload["risk_level"] == "growth"
    assert submission_payload["risk_score"] == 83
    assert "panic_selling_risk" in submission_payload["bias_tags"]
    assert "performance_chasing_risk" in submission_payload["bias_tags"]
    assert "concentration_risk" in submission_payload["bias_tags"]

    behavior_response = client.get("/api/behavior/profile")
    assert behavior_response.status_code == 200
    behavior_payload = behavior_response.json()
    assert behavior_payload["risk_level"] == "growth"
    assert len(behavior_payload["bias_tags"]) == 3
    assert behavior_payload["updated_at"] is not None

    dashboard_response = client.get("/api/dashboard")
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["onboarding_completed"] is True
    assert dashboard_payload["risk_level"] == "growth"
    assert dashboard_payload["latest_risk_score"] == 83
    assert dashboard_payload["summary_cards"][0]["value"] == "已完成"
    assert dashboard_payload["daily_brief"]["status"] == "missing_portfolio"
    assert dashboard_payload["daily_brief"]["primary_action"]["type"] == "inspect_portfolio"
    assert dashboard_payload["daily_brief"]["primary_action"]["target_route"] == "/portfolio"
    assert len(dashboard_payload["daily_brief"]["evidence"]) <= 3
    assert "理解和检查" in dashboard_payload["daily_brief"]["do_not_do"]
    assert len(dashboard_payload["next_actions"]) == 4
    assert dashboard_payload["simulation_status"]["completed_sessions_count"] == 0
    assert dashboard_payload["next_actions"][0].startswith("进入 Simulation")

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(AuthUser)) == 1
        assert session.scalar(select(func.count()).select_from(AuthSession)) == 1
        assert session.scalar(select(func.count()).select_from(UserProfile)) == 1
        assert session.scalar(select(func.count()).select_from(RiskQuestionnaire)) == 1
        assert session.scalar(select(func.count()).select_from(BehaviorProfile)) == 1

    logout_response = client.post("/api/auth/logout")
    assert logout_response.status_code == 200
    assert logout_response.json()["signed_out"] is True

    expired_session_response = client.get("/api/auth/session")
    assert expired_session_response.status_code == 401
