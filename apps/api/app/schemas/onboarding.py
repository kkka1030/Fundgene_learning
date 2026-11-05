from pydantic import BaseModel, Field, field_validator

from app.schemas.user import (
    InvestingExperience,
    MonthlyContributionBand,
    UserMeResponse,
)


class OnboardingProfileUpsertRequest(BaseModel):
    display_name: str = Field(min_length=2, max_length=120)
    investing_experience: InvestingExperience
    monthly_contribution_band: MonthlyContributionBand | None = None
    primary_goal: str | None = Field(default=None, max_length=500)

    @field_validator("display_name")
    @classmethod
    def validate_display_name(cls, value: str) -> str:
        normalized = value.strip()
        if len(normalized) < 2:
            raise ValueError("Display name must be at least 2 characters.")
        return normalized

    @field_validator("primary_goal")
    @classmethod
    def normalize_primary_goal(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized = value.strip()
        return normalized or None


class OnboardingProfileUpsertResponse(BaseModel):
    user: UserMeResponse
