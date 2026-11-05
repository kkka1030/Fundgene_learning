from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.models.agent_run import AgentRun
from app.models.chat_message import ChatMessage
from app.models.chat_session import ChatSession


def _complete_onboarding(client: TestClient) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "email": "coach-flow@example.com",
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
            "primary_goal": "先把基金风险和回撤理解清楚。",
        },
    )
    assert profile_response.status_code == 200

    questionnaire_response = client.post(
        "/api/behavior/questionnaires",
        json={
            "questionnaire_version": "v1",
            "answers": {
                "volatility_comfort": 4,
                "drawdown_reaction": 3,
                "investment_horizon": 4,
                "panic_sell_impulse": 2,
                "chase_hot_funds": 4,
                "diversification_habit": 3,
            },
        },
    )
    assert questionnaire_response.status_code == 200


def test_coach_message_persists_and_updates_dashboard(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client)

    empty_conversation_response = client.get("/api/assistant/session")
    assert empty_conversation_response.status_code == 200
    assert empty_conversation_response.json() == {"session": None, "messages": []}

    message_response = client.post(
        "/api/assistant/messages",
        json={
            "message": "我刚开始买基金，怎么理解风险等级和回撤？",
            "context": {
                "from_route": "/dashboard",
                "focus": "daily-brief",
                "daily_brief_id": "daily-brief:test",
                "source_ids": {"course_slug": "risk-basics"},
            },
        },
    )
    assert message_response.status_code == 200
    conversation_payload = message_response.json()

    assert conversation_payload["session"] is not None
    assert conversation_payload["session"]["context_type"] == "coach"
    assert len(conversation_payload["messages"]) == 2

    user_message, assistant_message = conversation_payload["messages"]
    assert user_message["role"] == "user"
    assert user_message["message_type"] == "user_prompt"
    assert user_message["content"] == "我刚开始买基金，怎么理解风险等级和回撤？"
    assert assistant_message["role"] == "assistant"
    assert assistant_message["message_type"] == "advisor_response"
    assert assistant_message["agent_run_id"] is not None
    assert assistant_message["advisor_response"]["intent"] == "learning"
    assert "回撤" in assistant_message["advisor_response"]["answer"]
    assert assistant_message["advisor_response"]["recommended_action_targets"]
    assert assistant_message["advisor_response"]["recommended_action_targets"][0][
        "href"
    ].startswith("/")

    persisted_conversation_response = client.get("/api/assistant/session")
    assert persisted_conversation_response.status_code == 200
    persisted_payload = persisted_conversation_response.json()
    assert persisted_payload["session"]["id"] == conversation_payload["session"]["id"]
    assert persisted_payload["session"]["message_count"] == 2
    assert persisted_payload["session"]["last_question"] == (
        "我刚开始买基金，怎么理解风险等级和回撤？"
    )
    assert len(persisted_payload["messages"]) == 2
    persisted_advisor = persisted_payload["messages"][-1]["advisor_response"]
    assert (
        persisted_advisor["recommended_action_targets"]
        == assistant_message["advisor_response"]["recommended_action_targets"]
    )

    dashboard_response = client.get("/api/dashboard")
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["latest_coach_activity"] is not None
    assert dashboard_payload["latest_coach_activity"]["intent"] == "learning"
    assert dashboard_payload["latest_coach_activity"]["question"] == (
        "我刚开始买基金，怎么理解风险等级和回撤？"
    )
    assert dashboard_payload["next_actions"][0].startswith("延续最近一次 Coach 主题")

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(ChatSession)) == 1
        assert session.scalar(select(func.count()).select_from(ChatMessage)) == 2
        assert session.scalar(select(func.count()).select_from(AgentRun)) == 1

        chat_session = session.scalar(select(ChatSession))
        assert chat_session is not None
        assert chat_session.context_type == "coach"
        assert "风险等级和回撤" in chat_session.topic

        agent_run = session.scalar(select(AgentRun))
        assert agent_run is not None
        assert agent_run.user_id == chat_session.user_id
        assert agent_run.session_id == chat_session.id
        assert agent_run.input_payload["page_context"]["from_route"] == "/dashboard"
        assert agent_run.input_payload["page_context"]["focus"] == "daily-brief"
        assert agent_run.tool_trace["page_context"]["source_ids"] == {
            "course_slug": "risk-basics"
        }
        assert agent_run.output_payload["intent"] == "learning"


def test_coach_session_history_can_reload_and_continue_old_conversation(
    client: TestClient,
) -> None:
    _complete_onboarding(client)

    first_response = client.post(
        "/api/assistant/messages",
        json={"message": "第一段对话：基金回撤是什么意思？"},
    )
    assert first_response.status_code == 200
    first_session_id = first_response.json()["session"]["id"]

    second_response = client.post(
        "/api/assistant/messages",
        json={
            "message": "第二段对话：我应该怎么看热门基金？",
            "start_new_session": True,
        },
    )
    assert second_response.status_code == 200
    second_session_id = second_response.json()["session"]["id"]
    assert second_session_id != first_session_id

    fresh_response = client.post(
        "/api/assistant/messages",
        json={
            "message": "新会话：帮我看组合集中度。",
            "start_new_session": True,
        },
    )
    assert fresh_response.status_code == 200
    latest_session_id = fresh_response.json()["session"]["id"]
    assert latest_session_id not in {first_session_id, second_session_id}

    history_response = client.get("/api/assistant/sessions")
    assert history_response.status_code == 200
    history_payload = history_response.json()
    assert [item["id"] for item in history_payload["sessions"]] == [
        latest_session_id,
        second_session_id,
        first_session_id,
    ]

    loaded_response = client.get(f"/api/assistant/sessions/{first_session_id}")
    assert loaded_response.status_code == 200
    loaded_payload = loaded_response.json()
    assert loaded_payload["session"]["id"] == first_session_id
    assert len(loaded_payload["messages"]) == 2

    continue_response = client.post(
        "/api/assistant/messages",
        json={
            "message": "接着刚才那段继续解释。",
            "session_id": first_session_id,
        },
    )
    assert continue_response.status_code == 200
    continued_payload = continue_response.json()
    assert continued_payload["session"]["id"] == first_session_id
    assert len(continued_payload["messages"]) == 4


def test_coach_requires_onboarding_context(client: TestClient) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "email": "not-ready@example.com",
            "password": "supersecure123",
        },
    )
    assert register_response.status_code == 201

    coach_response = client.post(
        "/api/assistant/messages",
        json={
            "message": "基金是什么？",
        },
    )
    assert coach_response.status_code == 404
