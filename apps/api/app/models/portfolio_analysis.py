from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class PortfolioAnalysis(Base):
    __tablename__ = "portfolio_analyses"

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
    snapshot_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("portfolio_snapshots.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    analysis_version: Mapped[str] = mapped_column(String(32), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    risk_exposure: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    concentration_flags: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    allocation_balance: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    recommended_next_actions: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    total_value: Mapped[float] = mapped_column(Float, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
