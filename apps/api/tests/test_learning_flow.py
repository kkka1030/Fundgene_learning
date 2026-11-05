from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.models.course import Course
from app.models.course_section import CourseSection
from app.models.learning_path import LearningPath
from app.models.user_course_progress import UserCourseProgress


def _create_profile(client: TestClient, *, email: str) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "email": email,
            "password": "supersecure123",
        },
    )
    assert register_response.status_code == 201

    profile_response = client.post(
        "/api/onboarding/profile",
        json={
            "display_name": "Ava",
            "investing_experience": "beginner",
            "monthly_contribution_band": "under_3000",
            "primary_goal": "先把基金基础和风险逻辑学明白。",
        },
    )
    assert profile_response.status_code == 200


def _complete_onboarding(client: TestClient, *, email: str) -> None:
    _create_profile(client, email=email)
    questionnaire_response = client.post(
        "/api/behavior/questionnaires",
        json={
            "questionnaire_version": "v1",
            "answers": {
                "volatility_comfort": 3,
                "drawdown_reaction": 3,
                "investment_horizon": 4,
                "panic_sell_impulse": 2,
                "chase_hot_funds": 2,
                "diversification_habit": 3,
            },
        },
    )
    assert questionnaire_response.status_code == 200


def test_learning_path_is_available_after_profile_creation(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _create_profile(client, email="learning-profile@example.com")

    response = client.get("/api/learning/path")
    assert response.status_code == 200
    payload = response.json()

    assert payload["path_slug"] == "beginner-core-path"
    assert payload["overall_progress_percentage"] == 0
    assert payload["recommended_course_slug"] == "fund-basics"
    assert len(payload["courses"]) == 3
    assert payload["courses"][0]["slug"] == "fund-basics"

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(LearningPath)) == 1
        assert session.scalar(select(func.count()).select_from(Course)) == 3
        assert session.scalar(select(func.count()).select_from(CourseSection)) == 9


def test_learning_progress_updates_course_and_dashboard(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="learning-progress@example.com")

    detail_response = client.get("/api/learning/courses/fund-basics")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["progress_percentage"] == 0
    assert detail_payload["section_count"] == 3

    progress_response = client.post(
        "/api/learning/progress",
        json={
            "course_slug": "fund-basics",
            "section_slug": "what-a-fund-owns",
            "status": "completed",
        },
    )
    assert progress_response.status_code == 200
    progress_payload = progress_response.json()
    assert progress_payload["progress_percentage"] == 33
    assert progress_payload["completed_section_count"] == 1
    assert progress_payload["status"] == "in_progress"

    refreshed_detail_response = client.get("/api/learning/courses/fund-basics")
    assert refreshed_detail_response.status_code == 200
    refreshed_detail_payload = refreshed_detail_response.json()
    assert refreshed_detail_payload["progress_percentage"] == 33
    assert refreshed_detail_payload["sections"][0]["completed"] is True

    path_response = client.get("/api/learning/path")
    assert path_response.status_code == 200
    path_payload = path_response.json()
    assert path_payload["overall_progress_percentage"] == 11
    assert path_payload["recommended_course_slug"] == "fund-basics"

    dashboard_response = client.get("/api/dashboard")
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["learning_status"]["overall_progress_percentage"] == 11
    assert dashboard_payload["learning_status"]["completed_courses_count"] == 0
    assert any(
        card["label"] == "学习进度" for card in dashboard_payload["summary_cards"]
    )

    with session_factory() as session:
        assert (
            session.scalar(select(func.count()).select_from(UserCourseProgress)) == 1
        )
