from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.models.agent_state_update_proposal import AgentStateUpdateProposal
from app.models.automation_notification import AutomationNotification
from app.models.automation_run import AutomationRun
from app.models.automation_setting import AutomationSetting
from app.models.daily_brief_preference import DailyBriefPreference
from app.services.automations import run_due_automations


def _complete_onboarding(client: TestClient, *, email: str = "auto@example.com") -> None:
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
            "primary_goal": "每天先知道哪里需要检查。",
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


def test_automations_get_patch_and_manual_run_are_persisted(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client)

    list_response = client.get("/api/automations")
    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload["total_count"] == 4
    assert payload["active_count"] == 1
    daily_brief = next(
        item for item in payload["automations"] if item["key"] == "daily_brief"
    )
    assert daily_brief["enabled"] is True
    assert daily_brief["can_run_now"] is True
    assert daily_brief["safety_boundary"]
    assert payload["daily_brief_summary"]["headline"]

    patch_response = client.patch(
        "/api/automations/weekly_portfolio",
        json={"enabled": True, "cadence_key": "weekly_friday_1800"},
    )
    assert patch_response.status_code == 200
    patched = patch_response.json()
    assert patched["enabled"] is True
    assert patched["cadence_key"] == "weekly_friday_1800"
    assert patched["next_run_at"] is not None

    run_response = client.post("/api/automations/weekly_portfolio/run")
    assert run_response.status_code == 200
    run_payload = run_response.json()
    assert run_payload["automation_key"] == "weekly_portfolio"
    assert run_payload["status"] == "succeeded"
    assert run_payload["trigger_type"] == "manual"
    assert "组合巡检" in run_payload["summary"]
    assert run_payload["output_payload"]["safety_boundary"]
    assert run_payload["created_pending_proposal_ids"]

    refreshed_response = client.get("/api/automations")
    assert refreshed_response.status_code == 200
    refreshed = refreshed_response.json()
    weekly = next(
        item for item in refreshed["automations"] if item["key"] == "weekly_portfolio"
    )
    assert weekly["last_run"]["run_id"] == run_payload["run_id"]
    assert refreshed["active_count"] == 2
    assert refreshed["recent_notifications"][0]["automation_key"] == "weekly_portfolio"

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(AutomationSetting)) == 4
        assert session.scalar(select(func.count()).select_from(DailyBriefPreference)) == 1
        assert session.scalar(select(func.count()).select_from(AutomationRun)) == 1
        assert (
            session.scalar(select(func.count()).select_from(AutomationNotification))
            == 1
        )
        assert (
            session.scalar(select(func.count()).select_from(AgentStateUpdateProposal))
            == 1
        )


def test_automations_reject_unknown_or_disabled_run(client: TestClient) -> None:
    _complete_onboarding(client, email="auto-disabled@example.com")

    unknown_response = client.patch(
        "/api/automations/not_allowed",
        json={"enabled": True},
    )
    assert unknown_response.status_code == 404

    disabled_run_response = client.post("/api/automations/news_watch/run")
    assert disabled_run_response.status_code == 409


def test_due_scheduler_creates_run_notification_and_pending_proposal(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="auto-scheduler@example.com")

    patch_response = client.patch(
        "/api/automations/behavior_observation",
        json={"enabled": True, "cadence_key": "weekly_friday_1800"},
    )
    assert patch_response.status_code == 200

    now = datetime(2026, 5, 18, 1, 0, tzinfo=timezone.utc)
    with session_factory() as session:
        daily_setting = session.scalar(
            select(AutomationSetting).where(AutomationSetting.automation_key == "daily_brief")
        )
        assert daily_setting is not None
        daily_setting.next_run_at = now + timedelta(hours=1)
        setting = session.scalar(
            select(AutomationSetting).where(
                AutomationSetting.automation_key == "behavior_observation"
            )
        )
        assert setting is not None
        setting.next_run_at = now - timedelta(minutes=5)
        session.commit()

        result = run_due_automations(session, now=now, limit=10)
        assert result.scanned_count == 1
        assert result.ran_count == 1
        assert result.failed_count == 0
        assert len(result.run_ids) == 1
        assert len(result.created_pending_proposal_ids) == 1
        assert len(result.created_notification_ids) == 1

        run = session.get(AutomationRun, result.run_ids[0])
        assert run is not None
        assert run.status == "succeeded"
        assert run.trigger_type == "scheduled"
        assert run.due_at is not None
        assert run.due_at.replace(tzinfo=timezone.utc) == now
        assert run.agent_run_id is not None
        assert run.output_payload["trigger_type"] == "scheduled"

        proposal = session.get(
            AgentStateUpdateProposal,
            result.created_pending_proposal_ids[0],
        )
        assert proposal is not None
        assert proposal.target_type == "behavior_profile_note"
        assert proposal.user_decision_status == "pending"

        notification = session.get(
            AutomationNotification,
            result.created_notification_ids[0],
        )
        assert notification is not None
        assert notification.action_route == "/profile"

        session.refresh(setting)
        assert setting.last_run_at is not None
        assert setting.next_run_at is not None
        assert setting.next_run_at.replace(tzinfo=timezone.utc) > now

    pending_response = client.get("/api/profile/pending-proposals")
    assert pending_response.status_code == 200
    pending_payload = pending_response.json()
    assert pending_payload["pending_count"] == 1
    assert pending_payload["proposals"][0]["target_type"] == "behavior_profile_note"


def test_due_scheduler_skips_disabled_future_and_incomplete_users(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="auto-skip@example.com")
    list_response = client.get("/api/automations")
    assert list_response.status_code == 200
    now = datetime(2026, 5, 18, 1, 0, tzinfo=timezone.utc)

    with session_factory() as session:
        daily_setting = session.scalar(
            select(AutomationSetting).where(AutomationSetting.automation_key == "daily_brief")
        )
        assert daily_setting is not None
        daily_setting.next_run_at = now + timedelta(hours=1)
        session.commit()

        result = run_due_automations(session, now=now, limit=10)
        assert result.scanned_count == 0
        assert result.ran_count == 0
        assert session.scalar(select(func.count()).select_from(AutomationRun)) == 0


def test_news_watch_refreshes_feeds_before_creating_scheduled_run(
    client: TestClient,
    session_factory: sessionmaker[Session],
    monkeypatch,
) -> None:
    _complete_onboarding(client, email="auto-news-watch@example.com")

    refresh_calls: list[int] = []

    def fake_refresh_news_feeds_sync(
        db: Session,
        *,
        feed_urls: list[str] | None = None,
        limit_per_feed: int = 20,
    ) -> list[object]:
        refresh_calls.append(limit_per_feed)
        return [object(), object()]

    monkeypatch.setattr(
        "app.services.automations.refresh_news_feeds_sync",
        fake_refresh_news_feeds_sync,
    )

    patch_response = client.patch(
        "/api/automations/news_watch",
        json={"enabled": True, "cadence_key": "workday_1600"},
    )
    assert patch_response.status_code == 200

    now = datetime(2026, 5, 18, 1, 0, tzinfo=timezone.utc)
    with session_factory() as session:
        daily_setting = session.scalar(
            select(AutomationSetting).where(
                AutomationSetting.automation_key == "daily_brief"
            )
        )
        assert daily_setting is not None
        daily_setting.next_run_at = now + timedelta(hours=1)

        setting = session.scalar(
            select(AutomationSetting).where(
                AutomationSetting.automation_key == "news_watch"
            )
        )
        assert setting is not None
        setting.next_run_at = now - timedelta(minutes=5)
        session.commit()

        result = run_due_automations(session, now=now, limit=10)
        assert result.ran_count == 1
        run = session.get(AutomationRun, result.run_ids[0])
        assert run is not None
        assert run.automation_key == "news_watch"
        assert run.output_payload["refreshed_news_count"] == 2

    assert refresh_calls == [10]
