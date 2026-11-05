from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.schemas.assistant import AdvisorResponse


Intent = Literal["learning", "portfolio", "behavior", "simulation", "news"]
PolicyStatus = Literal["allow", "revise", "block_with_guidance", "runtime_repair"]
WorkerName = Literal[
    "LearningWorker",
    "PortfolioWorker",
    "BehaviorWorker",
    "SimulationWorker",
    "NewsWorker",
    "SafetyPolicyWorker",
]


class EvidenceRef(BaseModel):
    worker_name: str
    source_type: str
    source_id: str | None = None
    source_version: str | None = None
    quote_or_summary: str
    claim: str
    support_summary: str | None = None


class StateUpdateProposal(BaseModel):
    target_type: str
    target_id: str | None = None
    patch_payload: dict[str, Any] = Field(default_factory=dict)
    reason: str
    validator_status: str = "pending"
    validator_message: str | None = None


class ContextSnapshot(BaseModel):
    schema_version: str = "context_snapshot_v1"
    user_id: str
    display_name: str
    investing_experience: str
    primary_goal: str | None = None
    risk_level: str | None = None
    bias_tags: list[str] = Field(default_factory=list)
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


class ToolDefinition(BaseModel):
    name: str
    category: str = "runtime"
    permission_level: Literal["read"] = "read"
    input_schema_version: str = "tool_input_v1"
    output_schema_version: str = "tool_output_v1"
    timeout_ms: int = 500
    purpose: str = ""
    allowed_intents: list[str] = Field(default_factory=list)
    search_terms: list[str] = Field(default_factory=list)
    budget_cost: int = Field(default=1, ge=1)
    evidence_required: bool = False


class ToolResult(BaseModel):
    tool_name: str
    permission_level: Literal["read"] = "read"
    output_payload: dict[str, Any]
    evidence_refs: list[EvidenceRef] = Field(default_factory=list)


class WorkerFinding(BaseModel):
    claim: str
    explanation: str | None = None
    evidence_refs: list[str] = Field(default_factory=list)
    support_level: Literal["direct", "contextual", "unsupported"] = "contextual"
    safety_boundary: str | None = None


class WorkerOutput(BaseModel):
    worker_name: str
    intent: Intent
    skill_names: list[str] = Field(default_factory=list)
    learning_outcome: str | None = None
    skill_output_guidance: list[str] = Field(default_factory=list)
    skill_forbidden_language: list[str] = Field(default_factory=list)
    findings: list[str]
    structured_findings: list[WorkerFinding] = Field(default_factory=list)
    evidence_refs: list[EvidenceRef] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    recommended_actions: list[str] = Field(default_factory=list)
    state_update_proposals: list[StateUpdateProposal] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1, default=0.7)
    limitations: list[str] = Field(default_factory=list)
    schema_version: str = "worker_output_v2"


class PlannedToolCall(BaseModel):
    tool_name: str
    purpose: str
    required: bool = True


class ToolSelectionSignal(BaseModel):
    tool_name: str
    score: float
    matched_terms: list[str] = Field(default_factory=list)
    source: Literal["required", "scored_search", "intent_fallback"]
    reason: str


class SkillSelection(BaseModel):
    skill_name: str
    skill_version: str
    score: float
    matched_terms: list[str] = Field(default_factory=list)
    required_tools: list[str] = Field(default_factory=list)
    output_guidance: list[str] = Field(default_factory=list)
    forbidden_language: list[str] = Field(default_factory=list)
    learning_outcome: str
    reason: str


class PlanConstraint(BaseModel):
    name: str
    status: Literal["pass", "warn", "fail"]
    detail: str


class AgentPlan(BaseModel):
    schema_version: str = "agent_plan_v1"
    primary_intent: Intent
    detected_intents: list[Intent]
    worker_names: list[WorkerName]
    selected_skills: list[SkillSelection] = Field(default_factory=list)
    planned_tools: list[PlannedToolCall]
    tool_selection_signals: list[ToolSelectionSignal] = Field(default_factory=list)
    constraints: list[PlanConstraint] = Field(default_factory=list)
    max_tool_calls: int = 8
    requires_human_review: bool = False
    rationale: str

    @property
    def tool_names(self) -> list[str]:
        return [tool.tool_name for tool in self.planned_tools]

    @property
    def skill_names(self) -> list[str]:
        return [skill.skill_name for skill in self.selected_skills]


class WorkerValidationResult(BaseModel):
    schema_version: str = "worker_validation_v1"
    status: Literal["pass", "warn", "fail"]
    findings_count: int
    evidence_backed_findings: int
    unsupported_findings: int
    worker_names: list[str]
    issues: list[str] = Field(default_factory=list)


class FinalResponseValidationResult(BaseModel):
    schema_version: str = "final_response_validation_v1"
    status: Literal["pass", "warn", "fail"]
    citation_count: int
    unsupported_citations: list[str] = Field(default_factory=list)
    removed_unsafe_actions: list[str] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)


class RuntimeTraceEvent(BaseModel):
    schema_version: str = "runtime_trace_event_v1"
    name: str
    kind: Literal["plan", "tool", "worker", "guardrail", "composer"]
    status: Literal["completed", "warn", "failed"]
    attributes: dict[str, Any] = Field(default_factory=dict)


class PolicyResult(BaseModel):
    status: PolicyStatus
    reason: str
    blocked_terms: list[str] = Field(default_factory=list)
    revised_answer: str | None = None


@dataclass(slots=True)
class OrchestratorRunResult:
    response: AdvisorResponse
    intent: str
    run_status: str
    policy_status: str
    context_snapshot: ContextSnapshot
    latency_ms: int
    tool_trace: dict | None = None
    fallback_reason: str | None = None
