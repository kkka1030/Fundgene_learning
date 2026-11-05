from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import DEFAULT_NEWS_FEEDS
from app.models.agent_citation import AgentCitation
from app.models.news_item import NewsItem
from app.models.news_analysis import NewsAnalysis
from app.models.policy_item import PolicyItem
from app.services.news import ingest_feed_document


def _complete_onboarding(
    client: TestClient,
    *,
    email: str = "news-flow@example.com",
) -> None:
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
            "primary_goal": "先学会把政策新闻和自己的基金组合分开判断。",
        },
    )
    assert profile_response.status_code == 200

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


def test_default_news_feeds_include_domestic_policy_and_finance_sources() -> None:
    joined = "\n".join(DEFAULT_NEWS_FEEDS)

    assert "pbc.gov.cn" in joined
    assert "people.com.cn/rss/finance.xml" in joined
    assert "chinanews.com.cn/rss/finance.xml" in joined


def test_news_policy_analysis_updates_dashboard_and_coach_context(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client)

    feed_document = """
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Federal Reserve Policy Feed</title>
      <entry>
        <id>tag:example.com,2026:policy-rate-guidance</id>
        <title>Federal Reserve issues policy rate guidance</title>
        <link href="https://example.com/policy-rate-guidance" />
        <updated>2026-04-25T12:00:00Z</updated>
        <summary>Policy guidance discusses the interest rate path and uncertainty for markets.</summary>
      </entry>
    </feed>
    """
    with session_factory() as session:
        ingest_feed_document(
            session,
            feed_url="https://example.com/feed.xml",
            content=feed_document,
        )

    list_response = client.get("/api/news")
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert len(list_payload["items"]) == 1
    item = list_payload["items"][0]
    assert item["item_type"] == "policy"
    assert item["latest_analysis_id"] is None

    detail_response = client.get(f"/api/news/{item['id']}")
    assert detail_response.status_code == 200
    assert detail_response.json()["item"]["title"] == item["title"]

    analysis_response = client.post(
        "/api/news/analyze",
        json={"item_id": item["id"]},
    )
    assert analysis_response.status_code == 200
    analysis_payload = analysis_response.json()
    analysis_id = analysis_payload["id"]
    assert analysis_payload["item"]["item_type"] == "policy"
    assert len(analysis_payload["facts"]) >= 3
    assert len(analysis_payload["impact_paths"]) >= 2
    assert len(analysis_payload["uncertainty_notes"]) == 3
    assert "不构成收益承诺" in analysis_payload["risk_notice"]
    assert len(analysis_payload["citations"]) == 1

    persisted_analysis_response = client.get(f"/api/news/analyses/{analysis_id}")
    assert persisted_analysis_response.status_code == 200
    assert persisted_analysis_response.json()["id"] == analysis_id

    refreshed_list_response = client.get("/api/news")
    assert refreshed_list_response.status_code == 200
    assert refreshed_list_response.json()["items"][0]["latest_analysis_id"] == analysis_id

    dashboard_response = client.get("/api/dashboard")
    assert dashboard_response.status_code == 200
    dashboard_payload = dashboard_response.json()
    assert dashboard_payload["news_status"]["has_analysis"] is True
    assert dashboard_payload["news_status"]["latest_analysis_id"] == analysis_id
    assert dashboard_payload["news_status"]["latest_title"] == item["title"]
    assert (
        dashboard_payload["news_status"]["latest_summary"]
        == "Policy guidance discusses the interest rate path and uncertainty for markets."
    )
    assert "把这条政策先翻译成一句新手能执行的话" not in dashboard_payload[
        "news_status"
    ]["beginner_translation"]
    assert dashboard_payload["daily_brief"]["primary_action"]["target_route"] == "/portfolio"
    assert dashboard_payload["daily_brief"]["source_coverage"]["news_policy"] is True
    news_evidence = [
        evidence
        for evidence in dashboard_payload["daily_brief"]["evidence"]
        if evidence["source_type"] == "news_policy"
    ]
    assert news_evidence
    assert news_evidence[0]["claim"] == item["title"]
    assert "新闻概括：" in news_evidence[0]["beginner_translation"]
    assert "简要解读：" in news_evidence[0]["beginner_translation"]
    assert any(
        evidence["source_type"] == "news_policy"
        for evidence in dashboard_payload["daily_brief"]["evidence"]
    )
    assert any(
        card["label"] == "新闻政策" and card["value"] == "已有解读"
        for card in dashboard_payload["summary_cards"]
    )

    coach_response = client.post(
        "/api/assistant/messages",
        json={"message": "这条政策新闻会影响我的基金吗？"},
    )
    assert coach_response.status_code == 200
    coach_payload = coach_response.json()
    advisor_response = coach_payload["messages"][1]["advisor_response"]
    assert advisor_response["intent"] == "news"
    assert "最近一条真实新闻/政策分析" in advisor_response["answer"]
    assert advisor_response["citations"] == [f"news_analysis:{analysis_id}"]

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(PolicyItem)) == 1
        assert session.scalar(select(func.count()).select_from(NewsAnalysis)) == 1
        assert session.scalar(select(func.count()).select_from(AgentCitation)) == 1


def test_coach_latest_news_question_lists_raw_feed_items(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="latest-news-coach@example.com")

    feed_document = """
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Policy Feed</title>
      <entry>
        <id>tag:example.com,2026:latest-sec-case</id>
        <title>SEC charges 21 individuals in insider trading case</title>
        <link href="https://example.com/latest-sec-case" />
        <updated>2026-05-06T14:00:00Z</updated>
        <summary>SEC describes an alleged insider trading scheme involving multiple individuals.</summary>
      </entry>
      <entry>
        <id>tag:example.com,2026:retirement-plan-guidance</id>
        <title>SEC staff issues retirement plan guidance for small businesses</title>
        <link href="https://example.com/retirement-plan-guidance" />
        <updated>2026-05-05T12:00:00Z</updated>
        <summary>Staff guidance discusses pooled employer plans and securities law questions.</summary>
      </entry>
    </feed>
    """
    with session_factory() as session:
        ingest_feed_document(
            session,
            feed_url="https://example.com/latest-feed.xml",
            content=feed_document,
        )

    response = client.post(
        "/api/assistant/messages",
        json={"message": "今天有什么新闻？"},
    )
    assert response.status_code == 200
    advisor_response = response.json()["messages"][-1]["advisor_response"]

    assert advisor_response["intent"] == "news"
    assert "当前已同步资讯中最近几条" in advisor_response["answer"]
    assert "SEC charges 21 individuals in insider trading case" in advisor_response["answer"]
    assert (
        "SEC staff issues retirement plan guidance for small businesses"
        in advisor_response["answer"]
    )
    assert "进入 News" in advisor_response["recommended_actions"][0]


def test_manual_news_analysis_is_visible_only_to_current_user(
    client: TestClient,
) -> None:
    _complete_onboarding(client, email="manual-news-owner@example.com")

    analysis_response = client.post(
        "/api/news/analyze",
        json={
            "headline": "长期资金入市政策继续推进",
            "body": "政策强调长期资金和资本市场稳定，但具体节奏、落地方式与基金组合影响仍需要进一步观察。",
        },
    )
    assert analysis_response.status_code == 200
    owner_payload = analysis_response.json()
    assert owner_payload["item"]["source_name"] == "用户粘贴内容"
    assert owner_payload["item"]["url"] == ""

    owner_list_response = client.get("/api/news")
    assert owner_list_response.status_code == 200
    owner_item_ids = {item["id"] for item in owner_list_response.json()["items"]}
    assert owner_payload["item"]["id"] in owner_item_ids

    logout_response = client.post("/api/auth/logout")
    assert logout_response.status_code == 200

    _complete_onboarding(client, email="manual-news-other@example.com")
    other_list_response = client.get("/api/news")
    assert other_list_response.status_code == 200
    other_item_ids = {item["id"] for item in other_list_response.json()["items"]}
    assert owner_payload["item"]["id"] not in other_item_ids


def test_news_list_refreshes_real_feed_sources(
    client: TestClient,
    monkeypatch,
) -> None:
    _complete_onboarding(client, email="refresh-news@example.com")

    async def fake_refresh_news_feeds(
        db: Session,
        *,
        feed_urls: list[str] | None = None,
        limit_per_feed: int = 20,
    ) -> list[NewsItem]:
        item = NewsItem(
            user_id=None,
            source_name="Live Feed",
            source_url="https://example.com/live.xml",
            external_id="live-1",
            title="Live market structure update",
            summary="A real feed item is available for beginner-safe interpretation.",
            url="https://example.com/live-1",
            published_at=None,
            fetched_at=datetime.now(timezone.utc),
        )
        db.add(item)
        db.commit()
        db.refresh(item)
        return [item]

    monkeypatch.setattr(
        "app.api.routes.news.refresh_news_feeds",
        fake_refresh_news_feeds,
    )

    response = client.get("/api/news?refresh=true")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 1
    assert payload["items"][0]["title"] == "Live market structure update"


def test_news_list_only_returns_latest_daily_items(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="daily-news@example.com")

    feed_document = """
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Daily Finance Feed</title>
      <entry>
        <id>tag:example.com,2026:today-market</id>
        <title>Today market policy update</title>
        <link href="https://example.com/today-market" />
        <updated>2026-05-19T08:00:00Z</updated>
        <summary>Latest daily market update.</summary>
      </entry>
      <entry>
        <id>tag:example.com,2026:yesterday-market</id>
        <title>Yesterday market policy update</title>
        <link href="https://example.com/yesterday-market" />
        <updated>2026-05-18T08:00:00Z</updated>
        <summary>Previous daily market update.</summary>
      </entry>
    </feed>
    """
    with session_factory() as session:
        ingest_feed_document(
            session,
            feed_url="https://example.com/daily-feed.xml",
            content=feed_document,
        )

    response = client.get("/api/news")
    assert response.status_code == 200
    titles = [item["title"] for item in response.json()["items"]]

    assert "Today market policy update" in titles
    assert "Yesterday market policy update" not in titles


def test_news_list_prefers_real_items_over_dev_fixture_same_day(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="real-news-priority@example.com")
    fetched_at = datetime(2026, 5, 20, 8, 0, tzinfo=timezone.utc)

    with session_factory() as session:
        session.add_all(
            [
                NewsItem(
                    user_id=None,
                    source_name="FundGene dev fixture",
                    source_url="https://example.com/dev.xml",
                    external_id="fixture-1",
                    title="暂未返回标题",
                    summary=None,
                    url="https://example.com/fixture",
                    published_at=fetched_at,
                    fetched_at=fetched_at,
                ),
                NewsItem(
                    user_id=None,
                    source_name="Live Feed",
                    source_url="https://example.com/live.xml",
                    external_id="live-priority-1",
                    title="Real source market update",
                    summary="Real synced item should be visible first.",
                    url="https://example.com/live-priority-1",
                    published_at=fetched_at,
                    fetched_at=fetched_at,
                ),
            ]
        )
        session.commit()

    response = client.get("/api/news")
    assert response.status_code == 200
    titles = [item["title"] for item in response.json()["items"]]

    assert titles == ["Real source market update"]


def test_news_list_uses_latest_real_synced_day_when_fixture_is_newer(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="saved-real-news@example.com")
    real_fetched_at = datetime(2026, 5, 19, 8, 0, tzinfo=timezone.utc)
    fixture_fetched_at = datetime(2026, 5, 20, 8, 0, tzinfo=timezone.utc)

    with session_factory() as session:
        session.add_all(
            [
                NewsItem(
                    user_id=None,
                    source_name="Live Feed",
                    source_url="https://example.com/live-saved.xml",
                    external_id="live-saved-1",
                    title="Persisted real synced update",
                    summary="A previously synced real item should still be shown.",
                    url="https://example.com/live-saved-1",
                    published_at=real_fetched_at,
                    fetched_at=real_fetched_at,
                ),
                NewsItem(
                    user_id=None,
                    source_name="FundGene dev fixture",
                    source_url="https://example.com/dev-newer.xml",
                    external_id="fixture-newer-1",
                    title="暂未返回标题",
                    summary=None,
                    url="https://example.com/fixture-newer",
                    published_at=fixture_fetched_at,
                    fetched_at=fixture_fetched_at,
                ),
            ]
        )
        session.commit()

    response = client.get("/api/news")
    assert response.status_code == 200
    titles = [item["title"] for item in response.json()["items"]]

    assert titles == ["Persisted real synced update"]


def test_dashboard_news_overview_skips_dev_fixture_analysis(
    client: TestClient,
    session_factory: sessionmaker[Session],
) -> None:
    _complete_onboarding(client, email="dashboard-real-news@example.com")
    user_id = client.get("/api/auth/session").json()["user"]["profile_id"]
    fetched_at = datetime(2026, 5, 20, 8, 0, tzinfo=timezone.utc)

    with session_factory() as session:
        fixture_item = NewsItem(
            user_id=None,
            source_name="FundGene dev fixture",
            source_url="https://example.com/dev-overview.xml",
            external_id="fixture-overview-1",
            title="暂未返回标题",
            summary=None,
            url="https://example.com/fixture-overview",
            published_at=fetched_at,
            fetched_at=fetched_at,
        )
        real_item = NewsItem(
            user_id=None,
            source_name="Live Feed",
            source_url="https://example.com/live-overview.xml",
            external_id="live-overview-1",
            title="Real dashboard policy update",
            summary="Real summary should appear in Today evidence.",
            url="https://example.com/live-overview-1",
            published_at=fetched_at,
            fetched_at=fetched_at,
        )
        session.add_all([fixture_item, real_item])
        session.flush()
        session.add_all(
            [
                NewsAnalysis(
                    user_id=user_id,
                    news_item_id=fixture_item.id,
                    facts=["fixture"],
                    impact_paths=["fixture path"],
                    uncertainty_notes=["fixture uncertainty"],
                    beginner_translation="Fixture analysis should not be shown.",
                    related_learning_topics=[],
                    recommended_next_actions=[],
                    risk_notice="fixture notice",
                    generated_at=datetime(2026, 5, 20, 9, 0, tzinfo=timezone.utc),
                ),
                NewsAnalysis(
                    user_id=user_id,
                    news_item_id=real_item.id,
                    facts=["real"],
                    impact_paths=["real path"],
                    uncertainty_notes=["real uncertainty"],
                    beginner_translation="Real analysis should be shown.",
                    related_learning_topics=[],
                    recommended_next_actions=["核对组合暴露"],
                    risk_notice="real notice",
                    generated_at=datetime(2026, 5, 20, 8, 30, tzinfo=timezone.utc),
                ),
            ]
        )
        session.commit()

    response = client.get("/api/dashboard")
    assert response.status_code == 200
    payload = response.json()

    assert payload["news_status"]["has_analysis"] is True
    assert payload["news_status"]["latest_title"] == "Real dashboard policy update"
    news_evidence = [
        evidence
        for evidence in payload["daily_brief"]["evidence"]
        if evidence["source_type"] == "news_policy"
    ]
    assert news_evidence
    assert "Real summary should appear" in news_evidence[0]["beginner_translation"]
    assert "原始摘要暂不可用" not in news_evidence[0]["beginner_translation"]
