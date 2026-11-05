from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class SimulationReview(Base):
    __tablename__ = "simulation_reviews"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    session_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("simulation_sessions.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    decision_summary: Mapped[str] = mapped_column(Text, nullable=False)
    bias_observations: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    coach_feedback: Mapped[str] = mapped_column(Text, nullable=False)
    recommended_next_actions: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
