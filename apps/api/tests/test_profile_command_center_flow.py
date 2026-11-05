from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.models.agent_state_update_proposal import AgentStateUpdateProposal
from app.models.behavior_profile import BehaviorProfile


def _complete_onboarding(client: TestClient, *, email: str) -> None:
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
            "primary_goal": "先训练自己不要被热点带跑。",
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
                "chase_hot_funds": 5,
                "diversification_habit": 3,
            },
        },
    )
    assert questionnaire_response.status_code == 200


def test_profile_context_and_pending_proposal_acceptance(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="profile-accept@example.com")

    context_response = client.get("/api/profile/context")
    assert context_response.status_code == 200
    context_payload = context_response.json()
    assert context_payload["display_name"] == "Ava"
    assert context_payload["context_readiness"]["total_count"] == 8
    assert any(
        item["key"] == "automations" and item["ready"] is True
        for item in context_payload["context_readiness"]["items"]
    )
    assert context_payload["automation_authorizations"]

    coach_response = client.post(
        "/api/assistant/messages",
        json={"message": "我最近总想追涨热门基金，这个行为要不要记下来？"},
    )
    assert coach_response.status_code == 200

    pending_response = client.get("/api/profile/pending-proposals")
    assert pending_response.status_code == 200
    pending_payload = pending_response.json()
    assert pending_payload["pending_count"] == 1
    proposal = pending_payload["proposals"][0]
    assert proposal["target_type"] == "behavior_profile_note"
    assert proposal["status"] == "pending"
    assert "交易" in proposal["safety_note"] or "账户" in proposal["safety_note"]

    accept_response = client.post(
        f"/api/profile/pending-proposals/{proposal['id']}/accept",
        json={"reason": "这是我确认过的训练线索。"},
    )
    assert accept_response.status_code == 200
    accepted_payload = accept_response.json()
    assert accepted_payload["applied_writeback"] is True
    assert accepted_payload["proposal"]["status"] == "applied"

    pending_after_accept = client.get("/api/profile/pending-proposals")
    assert pending_after_accept.status_code == 200
    assert pending_after_accept.json()["pending_count"] == 0

    with session_factory() as session:
        persisted_proposal = session.get(AgentStateUpdateProposal, proposal["id"])
        assert persisted_proposal is not None
        assert persisted_proposal.user_decision_status == "applied"
        assert persisted_proposal.decision_note == "这是我确认过的训练线索。"
        behavior_profile = session.scalar(select(BehaviorProfile))
        assert behavior_profile is not None
        assert any("追涨热门基金" in item for item in behavior_profile.evidence)


def test_profile_pending_proposal_rejection_is_user_scoped(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="profile-reject@example.com")
    coach_response = client.post(
        "/api/assistant/messages",
        json={"message": "我有点 panic，总想马上赎回。"},
    )
    assert coach_response.status_code == 200

    pending_response = client.get("/api/profile/pending-proposals")
    proposal_id = pending_response.json()["proposals"][0]["id"]

    reject_response = client.post(
        f"/api/profile/pending-proposals/{proposal_id}/reject",
        json={"reason": "这次只是测试，不作为长期证据。"},
    )
    assert reject_response.status_code == 200
    assert reject_response.json()["proposal"]["status"] == "rejected"

    with session_factory() as session:
        proposal = session.get(AgentStateUpdateProposal, proposal_id)
        assert proposal is not None
        assert proposal.user_decision_status == "rejected"
        assert proposal.decision_note == "这次只是测试，不作为长期证据。"
