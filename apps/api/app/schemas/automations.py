from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


AutomationKey = Literal[
    "daily_brief",
    "weekly_portfolio",
    "news_watch",
    "behavior_observation",
]
AutomationStatus = Literal["ready", "disabled", "needs_profile", "blocked"]
AutomationRunStatus = Literal["queued", "running", "succeeded", "failed"]


class AutomationCadenceOption(BaseModel):
    key: str
    label: str


class AutomationRunSummary(BaseModel):
    run_id: str
    agent_run_id: str | None = None
    status: AutomationRunStatus
    started_at: datetime | None = None
    completed_at: datetime | None = None
    summary: str | None = None
    output_ref: str | None = None


class AutomationRunStep(BaseModel):
    key: str
    label: str
    status: Literal["queued", "running", "completed", "failed"]
    detail: str


class AutomationItemResponse(BaseModel):
    key: AutomationKey
    title: str
    summary: str
    enabled: bool
    default_enabled: bool
    cadence_key: str
    cadence_label: str
    cadence_options: list[AutomationCadenceOption]
    read_scope: list[str]
    output_scope: list[str]
    confirmation_boundary: str
    safety_boundary: str
    status: AutomationStatus
    disabled_reason: str | None = None
    last_run: AutomationRunSummary | None = None
    next_run_at: datetime | None = None
    can_run_now: bool


class AutomationQueueItem(BaseModel):
    automation_key: AutomationKey
    title: str
    next_run_at: datetime | None = None
    cadence_label: str


class AutomationDailyBriefSummary(BaseModel):
    brief_id: str
    headline: str
    beginner_explanation: str
    as_of: datetime | None = None
    source_coverage: dict[str, bool] = Field(default_factory=dict)


class AutomationNotificationResponse(BaseModel):
    id: str
    automation_key: AutomationKey
    title: str
    message: str
    action_label: str | None = None
    action_route: str | None = None
    created_at: datetime
    read_at: datetime | None = None


class AutomationListResponse(BaseModel):
    user_id: str
    updated_at: datetime
    active_count: int
    total_count: int
    automations: list[AutomationItemResponse]
    next_queue: list[AutomationQueueItem]
    daily_brief_summary: AutomationDailyBriefSummary | None = None
    recent_notifications: list[AutomationNotificationResponse] = Field(
        default_factory=list
    )


class AutomationUpdateRequest(BaseModel):
    enabled: bool | None = None
    cadence_key: str | None = Field(default=None, max_length=64)
    timezone: str | None = Field(default=None, max_length=64)

    @field_validator("cadence_key", "timezone")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class AutomationRunResponse(BaseModel):
    run_id: str
    agent_run_id: str | None = None
    automation_key: AutomationKey
    status: AutomationRunStatus
    trigger_type: Literal["manual", "scheduled"] = "manual"
    due_at: datetime | None = None
    started_at: datetime
    completed_at: datetime | None = None
    summary: str | None = None
    output_ref: str | None = None
    error_message: str | None = None
    created_pending_proposal_ids: list[str] = Field(default_factory=list)
    steps: list[AutomationRunStep] = Field(default_factory=list)
    output_payload: dict[str, Any] = Field(default_factory=dict)
