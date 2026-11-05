from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


ReadinessKey = Literal[
    "profile",
    "risk",
    "portfolio",
    "news",
    "learning",
    "simulation",
    "behavior",
    "automations",
]
ProposalDecisionStatus = Literal["pending", "accepted", "rejected", "applied"]
LlmSettingsSource = Literal["user", "workspace", "none"]


class ProfileReadinessItem(BaseModel):
    key: ReadinessKey
    label: str
    ready: bool
    last_updated_at: datetime | date | None = None
    missing_action_route: str | None = None


class ProfileContextReadiness(BaseModel):
    ready_count: int
    total_count: int
    items: list[ProfileReadinessItem]


class ProfileRiskProfile(BaseModel):
    risk_level: str | None = None
    latest_risk_score: int | None = None
    updated_at: datetime | None = None


class ProfileBehaviorProfile(BaseModel):
    bias_tags: list[str] = Field(default_factory=list)
    evidence: list[str] = Field(default_factory=list)
    updated_at: datetime | None = None


class ProfilePortfolioContext(BaseModel):
    has_report: bool
    latest_snapshot_date: date | None = None
    total_value: float | None = None
    summary: str | None = None


class ProfileLearningContext(BaseModel):
    overall_progress_percentage: int
    recommended_course_title: str | None = None


class ProfileSimulationContext(BaseModel):
    latest_review_summary: str | None = None
    latest_completed_at: datetime | None = None


class ProfileAutomationAuthorization(BaseModel):
    automation_key: str
    enabled: bool
    cadence_label: str


class ProfileAuthorizationScopeItem(BaseModel):
    key: str
    label: str
    readable: bool


class ProfileContextResponse(BaseModel):
    user_id: str
    display_name: str | None = None
    context_readiness: ProfileContextReadiness
    risk_profile: ProfileRiskProfile
    behavior_profile: ProfileBehaviorProfile
    portfolio_context: ProfilePortfolioContext
    learning_context: ProfileLearningContext
    simulation_context: ProfileSimulationContext
    automation_authorizations: list[ProfileAutomationAuthorization]
    authorization_scope: list[ProfileAuthorizationScopeItem]
    pending_proposal_count: int


class ProfilePendingProposalResponse(BaseModel):
    id: str
    title: str
    source_label: str
    evidence_summary: str
    writeback_label: str
    target_type: str
    target_id: str | None = None
    patch_preview: dict[str, Any] = Field(default_factory=dict)
    reason: str
    validator_status: str
    validator_message: str | None = None
    status: ProposalDecisionStatus
    safety_note: str
    created_at: datetime
    run_id: str | None = None


class ProfilePendingProposalsResponse(BaseModel):
    pending_count: int
    resolved_count: int
    proposals: list[ProfilePendingProposalResponse]


class ProfileProposalDecisionRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=500)


class ProfileProposalDecisionResponse(BaseModel):
    proposal: ProfilePendingProposalResponse
    applied_writeback: bool = False


class ProfileLlmSettingsResponse(BaseModel):
    provider: Literal["deepseek"] = "deepseek"
    model_name: str
    configured: bool
    enabled: bool
    source: LlmSettingsSource
    masked_api_key: str | None = None
    updated_at: datetime | None = None
    warning: str | None = None


class ProfileLlmSettingsUpdateRequest(BaseModel):
    provider: Literal["deepseek"] = "deepseek"
    model_name: str = Field(default="deepseek:deepseek-v4-pro", max_length=128)
    api_key: str | None = Field(default=None, max_length=512)
    enabled: bool = True


class ProfileLlmSettingsUpdateResponse(BaseModel):
    settings: ProfileLlmSettingsResponse
