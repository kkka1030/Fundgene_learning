from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

RiskLevel = Literal["conservative", "balanced", "growth"]


class BehaviorQuestionnaireSubmitRequest(BaseModel):
    questionnaire_version: str = Field(min_length=1, max_length=32)
    answers: dict[str, int] = Field(min_length=1)

    @field_validator("questionnaire_version")
    @classmethod
    def validate_questionnaire_version(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Questionnaire version cannot be empty.")
        return normalized

    @field_validator("answers")
    @classmethod
    def validate_answers(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for key, raw_score in value.items():
            question_id = key.strip()
            score = int(raw_score)
            if not question_id:
                raise ValueError("Question ids cannot be empty.")
            if score < 1 or score > 5:
                raise ValueError("Questionnaire scores must be between 1 and 5.")
            normalized[question_id] = score
        return normalized


class BehaviorQuestionnaireSubmitResponse(BaseModel):
    user_id: str
    risk_score: int
    risk_level: RiskLevel
    bias_tags: list[str]
    evidence: list[str]
    submitted_at: datetime


class BehaviorProfileResponse(BaseModel):
    user_id: str
    risk_level: RiskLevel | None
    bias_tags: list[str]
    evidence: list[str]
    updated_at: datetime | None


class BehaviorTrainingPlanResponse(BaseModel):
    user_id: str
    focus_bias_tag: str | None
    focus_title: str
    guidance: str
    recommended_scenario_slug: str | None = None
    recent_review_summary: str | None = None
    next_actions: list[str]
