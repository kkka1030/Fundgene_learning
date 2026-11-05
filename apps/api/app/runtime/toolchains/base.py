from dataclasses import dataclass, field
from datetime import date
from typing import Protocol


@dataclass(slots=True)
class AdvisorUserContext:
    user_id: str
    display_name: str
    investing_experience: str
    primary_goal: str | None = None
    risk_level: str | None = None
    bias_tags: list[str] = field(default_factory=list)
    learning_progress_percentage: int | None = None
    learning_completed_courses_count: int | None = None
    learning_total_courses: int | None = None
    learning_recommended_course_slug: str | None = None
    learning_recommended_course_title: str | None = None
    portfolio_has_report: bool = False
    portfolio_latest_summary: str | None = None
    portfolio_latest_snapshot_date: date | None = None
    behavior_training_focus: str | None = None
    behavior_training_guidance: str | None = None
    simulation_recommended_scenario_slug: str | None = None
    simulation_recommended_scenario_title: str | None = None
    simulation_completed_sessions_count: int | None = None
    simulation_latest_review_summary: str | None = None
    news_has_analysis: bool = False
    news_latest_analysis_id: str | None = None
    news_latest_title: str | None = None
    news_latest_source_name: str | None = None
    news_latest_beginner_translation: str | None = None
    news_latest_recommended_action: str | None = None


class Toolchain(Protocol):
    name: str

    async def summarize(
        self,
        *,
        session_id: str,
        message: str,
        context: AdvisorUserContext | None = None,
    ) -> dict:
        """Produce a structured coaching summary for the selected domain."""
