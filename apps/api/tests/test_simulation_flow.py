from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.models.behavior_profile import BehaviorProfile
from app.models.scenario import Scenario
from app.models.scenario_event import ScenarioEvent
from app.models.simulation_action import SimulationAction
from app.models.simulation_review import SimulationReview
from app.models.simulation_session import SimulationSession


def _complete_onboarding(client: TestClient) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "email": "simulation-flow@example.com",
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
            "primary_goal": "先把自己在波动里会不会慌张这件事练明白。",
        },
    )
    assert profile_response.status_code == 200

    questionnaire_response = client.post(
        "/api/behavior/questionnaires",
        json={
            "questionnaire_version": "v1",
            "answers": {
                "volatility_comfort": 3,
                "drawdown_reaction": 2,
                "investment_horizon": 4,
                "panic_sell_impulse": 5,
                "chase_hot_funds": 3,
                "diversification_habit": 3,
            },
        },
    )
    assert questionnaire_response.status_code == 200


def test_simulation_session_review_and_behavior_training_flow(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client)

    training_plan_response = client.get("/api/behavior/training-plan")
    assert training_plan_response.status_code == 200
    training_plan_payload = training_plan_response.json()
    assert training_plan_payload["focus_bias_tag"] == "panic_selling_risk"
    assert training_plan_payload["recommended_scenario_slug"] == "covid-volatility-discipline"
    assert len(training_plan_payload["next_actions"]) == 3

    scenarios_response = client.get("/api/simulations/scenarios")
    assert scenarios_response.status_code == 200
    scenarios_payload = scenarios_response.json()
    assert scenarios_payload["recommended_scenario_slug"] == "covid-volatility-discipline"
    assert len(scenarios_payload["items"]) == 2
    assert scenarios_payload["items"][0]["event_count"] == 3
    assert scenarios_payload["items"][0]["status"] == "not_started"

    session_response = client.post(
        "/api/simulations/sessions",
        json={"scenario_slug": "covid-volatility-discipline"},
    )
    assert session_response.status_code == 200
    session_payload = session_response.json()
    assert session_payload["status"] == "in_progress"
    assert session_payload["current_step"] == 1
    assert session_payload["total_steps"] == 3
    assert session_payload["active_event"]["step_index"] == 1

    session_id = session_payload["session_id"]
    duplicate_session_response = client.post(
        "/api/simulations/sessions",
        json={"scenario_slug": "covid-volatility-discipline"},
    )
    assert duplicate_session_response.status_code == 200
    assert duplicate_session_response.json()["session_id"] == session_id

    recommended_choices = {
        1: "panic_redeem",
        2: "continue_plan_small",
        3: "review_journal",
    }

    for step_index in (1, 2, 3):
        active_event = session_payload["active_event"]
        assert active_event is not None
        assert active_event["step_index"] == step_index

        action_response = client.post(
            "/api/simulations/actions",
            json={
                "session_id": session_id,
                "event_id": active_event["event_id"],
                "choice_key": recommended_choices[step_index],
                "rationale": f"step-{step_index} rationale",
                "worry": "担心账户继续回撤",
                "impulse_control_plan": "先核对期限、现金需求和原计划",
            },
        )
        assert action_response.status_code == 200
        session_payload = action_response.json()

    assert session_payload["status"] == "completed"
    assert session_payload["review"] is not None
    assert len(session_payload["actions"]) == 3

    review_response = client.get(f"/api/simulations/review/{session_id}")
    assert review_response.status_code == 200
    review_payload = review_response.json()
    assert review_payload["session_id"] == session_id
    assert review_payload["scenario_slug"] == "covid-volatility-discipline"
    assert len(review_payload["actions"]) == 3
    assert "1/3 个关键节点" in review_payload["decision_summary"]
    assert review_payload["behavior_evidence_candidates"]
    assert review_payload["behavior_evidence_candidates"][0]["bias_type"] == "panic_selling_risk"
    assert review_payload["pending_state_proposal"]["status"] == "pending"

    dashboard_response = client.get("/api/dashboard")
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["simulation_status"]["completed_sessions_count"] == 1
    assert dashboard_payload["simulation_status"]["latest_session_id"] == session_id
    assert (
        dashboard_payload["simulation_status"]["latest_review_summary"]
        == review_payload["decision_summary"]
    )

    coach_response = client.post(
        "/api/assistant/messages",
        json={"message": "我想做一次情境演练并复盘。"},
    )
    assert coach_response.status_code == 200
    coach_payload = coach_response.json()
    advisor_response = coach_payload["messages"][1]["advisor_response"]
    assert advisor_response["intent"] == "simulation"
    assert "最近一次训练复盘提示" in advisor_response["answer"]
    assert "不需要马上重复同一轮训练" in advisor_response["answer"]
    assert not any(
        action.startswith("进入 Simulation")
        for action in advisor_response["recommended_actions"]
    )

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Scenario)) == 2
        assert session.scalar(select(func.count()).select_from(ScenarioEvent)) == 6
        assert session.scalar(select(func.count()).select_from(SimulationSession)) == 1
        assert session.scalar(select(func.count()).select_from(SimulationAction)) == 3
        assert session.scalar(select(func.count()).select_from(SimulationReview)) == 1

        behavior_profile = session.scalar(select(BehaviorProfile).limit(1))
        assert behavior_profile is not None
        assert not any("Simulation" in evidence for evidence in behavior_profile.evidence)


def test_simulation_action_requires_rationale(client: TestClient) -> None:
    _complete_onboarding(client)

    session_response = client.post(
        "/api/simulations/sessions",
        json={"scenario_slug": "covid-volatility-discipline"},
    )
    assert session_response.status_code == 200
    session_payload = session_response.json()
    active_event = session_payload["active_event"]
    assert active_event is not None

    action_response = client.post(
        "/api/simulations/actions",
        json={
            "session_id": session_payload["session_id"],
            "event_id": active_event["event_id"],
            "choice_key": "pause_and_review",
        },
    )
    assert action_response.status_code == 422
