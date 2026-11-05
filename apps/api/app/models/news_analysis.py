from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class NewsAnalysis(Base):
    __tablename__ = "news_analyses"
    __table_args__ = (
        CheckConstraint(
            "(news_item_id IS NOT NULL AND policy_item_id IS NULL) OR "
            "(news_item_id IS NULL AND policy_item_id IS NOT NULL)",
            name="ck_news_analysis_exactly_one_item",
        ),
    )

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    user_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("user_profiles.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    news_item_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("news_items.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    policy_item_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("policy_items.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    analysis_version: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="news_policy_analysis_v1",
    )
    facts: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    impact_paths: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    uncertainty_notes: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    beginner_translation: Mapped[str] = mapped_column(Text, nullable=False)
    related_learning_topics: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    recommended_next_actions: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    risk_notice: Mapped[str] = mapped_column(Text, nullable=False)
    model_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="legacy",
    )
    model_provider: Mapped[str | None] = mapped_column(String(64), nullable=True)
    model_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    fallback_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        index=True,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
