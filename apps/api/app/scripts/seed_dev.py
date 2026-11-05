from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi import HTTPException

from app.core.database import Base, SessionLocal, engine
from app.models.policy_item import PolicyItem
from app.schemas.behavior import BehaviorQuestionnaireSubmitRequest
from app.schemas.news import NewsAnalyzeRequest
from app.schemas.onboarding import OnboardingProfileUpsertRequest
from app.schemas.portfolio import PortfolioHoldingInput, PortfolioSnapshotCreateRequest
from app.services.auth import create_auth_user, get_auth_user_by_email
from app.services.behavior import submit_questionnaire
from app.services.learning import ensure_learning_catalog
from app.services.news import create_news_analysis
from app.services.onboarding import upsert_user_profile
from app.services.portfolio import create_portfolio_snapshot
from app.services.simulation import ensure_scenario_catalog


DEMO_EMAIL = "demo@fundgene.local"
DEMO_PASSWORD = "fundgene-demo-2026"


def main() -> None:
    if str(engine.url).startswith("sqlite"):
        Base.metadata.create_all(bind=engine)

    with SessionLocal() as db:
        auth_user = get_auth_user_by_email(db, DEMO_EMAIL)
        if auth_user is None:
            try:
                auth_user = create_auth_user(
                    db,
                    email=DEMO_EMAIL,
                    password=DEMO_PASSWORD,
                )
            except HTTPException:
                auth_user = get_auth_user_by_email(db, DEMO_EMAIL)
        if auth_user is None:
            raise RuntimeError("Failed to create demo auth user.")

        profile = upsert_user_profile(
            db,
            auth_user=auth_user,
            payload=OnboardingProfileUpsertRequest(
                display_name="FundGene Demo",
                investing_experience="beginner",
                monthly_contribution_band="under_3000",
                primary_goal="建立长期基金投资纪律，先理解风险和组合结构。",
            ),
        )
        submit_questionnaire(
            db,
            user=profile,
            payload=BehaviorQuestionnaireSubmitRequest(
                questionnaire_version="v1",
                answers={
                    "volatility_comfort": 4,
                    "drawdown_reaction": 3,
                    "investment_horizon": 4,
                    "panic_sell_impulse": 2,
                    "chase_hot_funds": 4,
                    "diversification_habit": 3,
                },
            ),
        )
        ensure_learning_catalog(db)
        ensure_scenario_catalog(db)

        if not _has_portfolio(db, user_id=profile.id):
            create_portfolio_snapshot(
                db,
                user=profile,
                payload=PortfolioSnapshotCreateRequest(
                    snapshot_date=date.today(),
                    cash_value=6000,
                    holdings=[
                        PortfolioHoldingInput(
                            fund_code="161725",
                            fund_name="招商中证白酒指数",
                            fund_type="equity",
                            market_value=18000,
                        ),
                        PortfolioHoldingInput(
                            fund_code="110027",
                            fund_name="易方达安心债券",
                            fund_type="bond",
                            market_value=16000,
                        ),
                        PortfolioHoldingInput(
                            fund_code="000071",
                            fund_name="华夏恒生 ETF 联接",
                            fund_type="international",
                            market_value=12000,
                        ),
                    ],
                ),
            )

        policy_item = _ensure_policy_item(db)
        create_news_analysis(
            db,
            user=profile,
            payload=NewsAnalyzeRequest(item_id=policy_item.id, item_type="policy"),
        )

    print("FundGene dev seed complete.")
    print(f"Demo email: {DEMO_EMAIL}")
    print(f"Demo password: {DEMO_PASSWORD}")


def _has_portfolio(db, *, user_id: str) -> bool:
    from sqlalchemy import select

    from app.models.portfolio_snapshot import PortfolioSnapshot

    return db.scalar(select(PortfolioSnapshot).where(PortfolioSnapshot.user_id == user_id)) is not None


def _ensure_policy_item(db) -> PolicyItem:
    from sqlalchemy import select

    existing = db.scalar(
        select(PolicyItem).where(PolicyItem.external_id == "dev-policy-long-term-capital")
    )
    if existing is not None:
        return existing

    now = datetime.now(timezone.utc)
    item = PolicyItem(
        source_name="FundGene dev fixture",
        source_url="fixture://fundgene/policy",
        external_id="dev-policy-long-term-capital",
        title="长期资金入市政策继续推进",
        summary="政策强调长期资金和资本市场稳定，但具体节奏、落地方式与基金组合影响仍需要进一步观察。",
        url="",
        policy_area="capital_market_policy",
        published_at=now,
        fetched_at=now,
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


if __name__ == "__main__":
    main()
