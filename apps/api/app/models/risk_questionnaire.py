from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class RiskQuestionnaire(Base):
    __tablename__ = "risk_questionnaires"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    user_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("user_profiles.id"),
        index=True,
        nullable=False,
    )
    questionnaire_version: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="v1",
    )
    answers: Mapped[dict[str, int]] = mapped_column(JSON, nullable=False)
    risk_score: Mapped[int] = mapped_column(Integer, nullable=False)
    risk_level: Mapped[str] = mapped_column(String(32), nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
