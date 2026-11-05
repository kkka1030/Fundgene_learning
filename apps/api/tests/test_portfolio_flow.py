from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.models.portfolio_analysis import PortfolioAnalysis
from app.models.portfolio_holding import PortfolioHolding
from app.models.portfolio_snapshot import PortfolioSnapshot


def _complete_onboarding(client: TestClient) -> None:
    register_response = client.post(
        "/api/auth/register",
        json={
            "email": "portfolio-flow@example.com",
            "password": "supersecure123",
        },
    )
    assert register_response.status_code == 201

    profile_response = client.post(
        "/api/onboarding/profile",
        json={
            "display_name": "Ava",
            "investing_experience": "starter",
            "monthly_contribution_band": "3000_10000",
            "primary_goal": "先判断自己的组合是不是太集中。",
        },
    )
    assert profile_response.status_code == 200

    questionnaire_response = client.post(
        "/api/behavior/questionnaires",
        json={
            "questionnaire_version": "v1",
            "answers": {
                "volatility_comfort": 4,
                "drawdown_reaction": 4,
                "investment_horizon": 4,
                "panic_sell_impulse": 2,
                "chase_hot_funds": 3,
                "diversification_habit": 2,
            },
        },
    )
    assert questionnaire_response.status_code == 200


def test_portfolio_snapshot_persists_report_history_and_coach_context(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client)

    empty_latest_response = client.get("/api/portfolio/latest")
    assert empty_latest_response.status_code == 200
    assert empty_latest_response.json() == {"report": None}

    create_response = client.post(
        "/api/portfolio/snapshots",
        json={
            "snapshot_date": "2026-04-26",
            "cash_value": 5000,
            "holdings": [
                {
                    "fund_code": "161725",
                    "fund_name": "白酒指数",
                    "fund_type": "equity",
                    "market_value": 12000,
                },
                {
                    "fund_code": "006327",
                    "fund_name": "中短债基金",
                    "fund_type": "bond",
                    "market_value": 8000,
                },
            ],
        },
    )
    assert create_response.status_code == 200
    report_payload = create_response.json()
    assert report_payload["snapshot_date"] == "2026-04-26"
    assert report_payload["total_value"] == 25000
    assert len(report_payload["holdings"]) == 2
    assert len(report_payload["recommended_next_actions"]) == 3

    latest_response = client.get("/api/portfolio/latest")
    assert latest_response.status_code == 200
    latest_payload = latest_response.json()
    assert latest_payload["report"]["snapshot_id"] == report_payload["snapshot_id"]

    history_response = client.get("/api/portfolio/history")
    assert history_response.status_code == 200
    history_payload = history_response.json()
    assert len(history_payload["items"]) == 1
    assert history_payload["items"][0]["summary"] == report_payload["summary"]

    dashboard_response = client.get("/api/dashboard")
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["portfolio_status"]["has_report"] is True
    assert dashboard_payload["daily_brief"]["source_coverage"]["portfolio"] is True
    assert any(
        evidence["source_type"] == "portfolio"
        for evidence in dashboard_payload["daily_brief"]["evidence"]
    )
    assert len(dashboard_payload["daily_brief"]["evidence"]) <= 3
    assert "买入" not in str(dashboard_payload["daily_brief"])
    assert "卖出" not in str(dashboard_payload["daily_brief"])
    assert any(
        card["label"] == "组合体检" and card["value"] == "已有报告"
        for card in dashboard_payload["summary_cards"]
    )

    coach_response = client.post(
        "/api/assistant/messages",
        json={
            "message": "我现在的持仓是不是太集中？",
        },
    )
    assert coach_response.status_code == 200
    coach_payload = coach_response.json()
    assert coach_payload["messages"][1]["advisor_response"]["intent"] == "portfolio"
    assert "直接回答：" not in coach_payload["messages"][1]["advisor_response"]["answer"]
    assert "当前已有最近组合报告" in coach_payload["messages"][1]["advisor_response"]["answer"]

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(PortfolioSnapshot)) == 1
        assert session.scalar(select(func.count()).select_from(PortfolioHolding)) == 2
        assert session.scalar(select(func.count()).select_from(PortfolioAnalysis)) == 1


def test_portfolio_snapshot_rejects_invalid_fund_type_and_duplicate_code(
    client: TestClient,
) -> None:
    _complete_onboarding(client)

    invalid_type_response = client.post(
        "/api/portfolio/snapshots",
        json={
            "snapshot_date": "2026-04-26",
            "cash_value": 1000,
            "holdings": [
                {
                    "fund_code": "161725",
                    "fund_name": "白酒指数",
                    "fund_type": "crypto",
                    "market_value": 12000,
                },
            ],
        },
    )
    assert invalid_type_response.status_code == 422

    duplicate_code_response = client.post(
        "/api/portfolio/snapshots",
        json={
            "snapshot_date": "2026-04-26",
            "cash_value": 1000,
            "holdings": [
                {
                    "fund_code": "161725",
                    "fund_name": "白酒指数",
                    "fund_type": "equity",
                    "market_value": 12000,
                },
                {
                    "fund_code": " 161725 ",
                    "fund_name": "同代码债券基金",
                    "fund_type": "bond",
                    "market_value": 8000,
                },
            ],
        },
    )
    assert duplicate_code_response.status_code == 422
