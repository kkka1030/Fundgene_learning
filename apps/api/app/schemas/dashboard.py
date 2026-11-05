from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from app.schemas.behavior import RiskLevel


DashboardBriefStatus = Literal[
    "ready",
    "starter",
    "missing_profile",
    "missing_portfolio",
    "fallback",
]
DashboardBriefPriority = Literal["urgent", "attention", "learning", "stable"]
DashboardEvidenceSource = Literal[
    "profile",
    "portfolio",
    "news_policy",
    "learning",
    "simulation",
    "behavior",
    "coach_history",
]
DashboardEvidenceSupport = Literal["strong", "medium", "weak"]
SafeNextActionType = Literal[
    "learn",
    "inspect_portfolio",
    "run_simulation",
    "ask_coach",
    "record_behavior",
    "read_news_context",
]


class DashboardEvidence(BaseModel):
    id: str
    source_type: DashboardEvidenceSource
    source_id: str | None = None
    claim: str
    beginner_translation: str
    support_level: DashboardEvidenceSupport
    freshness_label: str
    risk_boundary: str


class SafeNextAction(BaseModel):
    id: str
    type: SafeNextActionType
    label: str
    reason: str
    target_route: str
    target_params: dict[str, str] = Field(default_factory=dict)
    expected_writeback: str
    safety_note: str

    @field_validator("target_route")
    @classmethod
    def validate_target_route(cls, value: str) -> str:
        allowed_routes = {
            "/dashboard",
            "/onboarding",
            "/learning",
            "/portfolio",
            "/simulation",
            "/news",
            "/coach",
        }
        if value not in allowed_routes:
            raise ValueError("Safe Next Action target route is not allow-listed.")
        return value


class DashboardDailyBrief(BaseModel):
    brief_id: str
    as_of: datetime
    status: DashboardBriefStatus
    priority_level: DashboardBriefPriority
    headline: str
    beginner_explanation: str
    evidence: list[DashboardEvidence]
    primary_action: SafeNextAction
    secondary_actions: list[SafeNextAction] = Field(default_factory=list)
    do_not_do: str
    source_coverage: dict[DashboardEvidenceSource, bool]
    trace_id: str | None = None


class DashboardSummaryCard(BaseModel):
    label: str
    value: str
    detail: str


class DashboardCoachActivity(BaseModel):
    session_id: str
    topic: str
    intent: str | None
    question: str
    answer_focus: str
    recommended_action: str | None
    updated_at: datetime


class DashboardLearningStatus(BaseModel):
    overall_progress_percentage: int
    completed_courses_count: int
    total_courses: int
    recommended_course_slug: str | None = None
    recommended_course_title: str | None = None


class DashboardPortfolioStatus(BaseModel):
    has_report: bool
    latest_snapshot_id: str | None = None
    latest_snapshot_date: date | None = None
    total_value: float | None = None
    summary: str | None = None


class DashboardSimulationStatus(BaseModel):
    completed_sessions_count: int
    recommended_scenario_slug: str | None = None
    recommended_scenario_title: str | None = None
    latest_session_id: str | None = None
    latest_scenario_slug: str | None = None
    latest_scenario_title: str | None = None
    latest_review_summary: str | None = None
    latest_completed_at: datetime | None = None


class DashboardNewsStatus(BaseModel):
    has_analysis: bool
    latest_analysis_id: str | None = None
    latest_item_id: str | None = None
    latest_item_type: str | None = None
    latest_title: str | None = None
    latest_summary: str | None = None
    source_name: str | None = None
    beginner_translation: str | None = None
    recommended_action: str | None = None
    generated_at: datetime | None = None


class DashboardResponse(BaseModel):
    user_id: str
    onboarding_completed: bool
    risk_level: RiskLevel | None
    bias_tags: list[str]
    latest_risk_score: int | None
    daily_brief: DashboardDailyBrief
    next_actions: list[str]
    summary_cards: list[DashboardSummaryCard]
    learning_status: DashboardLearningStatus | None = None
    portfolio_status: DashboardPortfolioStatus | None = None
    simulation_status: DashboardSimulationStatus | None = None
    news_status: DashboardNewsStatus | None = None
    latest_coach_activity: DashboardCoachActivity | None = None
