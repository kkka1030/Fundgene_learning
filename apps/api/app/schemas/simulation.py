from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


SimulationSessionStatus = Literal["in_progress", "completed"]


class ScenarioSummary(BaseModel):
    slug: str
    title: str
    summary: str
    bias_focus: str
    difficulty: int
    estimated_duration_minutes: int
    event_count: int
    status: Literal["not_started", "in_progress", "completed"]
    active_session_id: str | None = None
    last_completed_at: datetime | None = None


class ScenarioCatalogResponse(BaseModel):
    recommended_scenario_slug: str | None = None
    recommended_scenario_title: str | None = None
    items: list[ScenarioSummary]


class ScenarioChoice(BaseModel):
    key: str
    label: str
    description: str


class ActiveScenarioEvent(BaseModel):
    event_id: str
    step_index: int
    date_label: str
    title: str
    narrative: str
    prompt: str
    choices: list[ScenarioChoice]


class SimulationActionSummary(BaseModel):
    event_id: str
    step_index: int
    choice_key: str
    choice_label: str
    reflection: str | None = None
    is_recommended: bool
    created_at: datetime


class BehaviorEvidenceCandidate(BaseModel):
    behavior_evidence_id: str
    bias_type: str
    observed_signal: str
    source_event: str
    confidence: Literal["low", "medium", "high"]
    pending_state_proposal_id: str | None = None


class SimulationReviewResponse(BaseModel):
    session_id: str
    scenario_slug: str
    scenario_title: str
    bias_focus: str
    decision_summary: str
    bias_observations: list[str]
    strengths: list[str] = Field(default_factory=list)
    improvement_areas: list[str] = Field(default_factory=list)
    coach_feedback: str
    recommended_next_actions: list[str]
    reflection_questions: list[str] = Field(default_factory=list)
    behavior_evidence_candidates: list[BehaviorEvidenceCandidate] = Field(
        default_factory=list
    )
    pending_state_proposal: dict[str, str] | None = None
    generated_at: datetime
    actions: list[SimulationActionSummary]


class SimulationSessionResponse(BaseModel):
    session_id: str
    scenario_slug: str
    scenario_title: str
    bias_focus: str
    status: SimulationSessionStatus
    current_step: int
    total_steps: int
    started_at: datetime
    active_event: ActiveScenarioEvent | None = None
    actions: list[SimulationActionSummary]
    completed_at: datetime | None = None
    review: SimulationReviewResponse | None = None


class SimulationSessionStartRequest(BaseModel):
    scenario_slug: str = Field(min_length=1, max_length=64)

    @field_validator("scenario_slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Scenario slug cannot be empty.")
        return normalized


class SimulationActionSubmitRequest(BaseModel):
    session_id: str = Field(min_length=1, max_length=36)
    event_id: str = Field(min_length=1, max_length=36)
    choice_key: str = Field(min_length=1, max_length=64)
    rationale: str | None = Field(default=None, max_length=500)
    worry: str | None = Field(default=None, max_length=500)
    impulse_control_plan: str | None = Field(default=None, max_length=500)
    reflection: str | None = Field(default=None, max_length=500)

    @field_validator("session_id", "event_id", "choice_key")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Field cannot be empty.")
        return normalized

    @field_validator("rationale", "worry", "impulse_control_plan", "reflection")
    @classmethod
    def normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def require_rationale_or_reflection(self) -> "SimulationActionSubmitRequest":
        if self.rationale is None and self.reflection is None:
            raise ValueError("Simulation action needs a rationale before submission.")
        return self
