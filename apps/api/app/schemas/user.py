from typing import Literal

from pydantic import BaseModel, ConfigDict

InvestingExperience = Literal["beginner", "starter", "intermediate"]
MonthlyContributionBand = Literal["under_3000", "3000_10000", "above_10000"]


class UserMeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    display_name: str
    investing_experience: InvestingExperience
    monthly_contribution_band: MonthlyContributionBand | None
    primary_goal: str | None
    onboarding_completed: bool
