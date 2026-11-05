from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


NewsItemType = Literal["news", "policy"]


class NewsItemSummary(BaseModel):
    id: str
    item_type: NewsItemType
    title: str
    summary: str | None = None
    url: str
    source_name: str
    source_url: str
    published_at: datetime | None = None
    fetched_at: datetime
    latest_analysis_id: str | None = None


class NewsListResponse(BaseModel):
    items: list[NewsItemSummary] = Field(default_factory=list)


class NewsItemDetailResponse(BaseModel):
    item: NewsItemSummary


class NewsAnalyzeRequest(BaseModel):
    item_id: str | None = Field(default=None, max_length=36)
    item_type: NewsItemType | None = None
    headline: str | None = Field(default=None, max_length=300)
    body: str | None = Field(default=None, max_length=2000)

    @field_validator("item_id", "headline", "body", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            return value
        normalized = value.strip()
        return normalized or None

    @model_validator(mode="after")
    def validate_source(self) -> "NewsAnalyzeRequest":
        if self.item_id is not None:
            return self
        if self.headline is None or len(self.headline) < 4:
            raise ValueError("Provide a news item id or a manual headline.")
        if self.body is None or len(self.body) < 12:
            raise ValueError("Manual news analysis requires a meaningful body.")
        return self


class AgentCitationResponse(BaseModel):
    id: str
    source_type: NewsItemType
    source_item_id: str
    source_name: str
    title: str
    url: str


class NewsAgentProcessStep(BaseModel):
    key: str
    label: str
    status: Literal["completed", "warning", "failed"]
    detail: str


class NewsAnalysisResponse(BaseModel):
    id: str
    item: NewsItemSummary
    facts: list[str]
    impact_paths: list[str]
    uncertainty_notes: list[str]
    beginner_translation: str
    related_learning_topics: list[str]
    recommended_next_actions: list[str]
    risk_notice: str
    citations: list[AgentCitationResponse]
    model_status: Literal["enhanced", "fallback", "skipped", "legacy"]
    model_provider: str | None = None
    model_name: str | None = None
    fallback_reason: str | None = None
    agent_process: list[NewsAgentProcessStep] = Field(default_factory=list)
    generated_at: datetime
