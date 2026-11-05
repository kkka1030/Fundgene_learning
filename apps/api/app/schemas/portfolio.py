from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator, model_validator

SUPPORTED_FUND_TYPES = {
    "equity",
    "mixed",
    "bond",
    "money_market",
    "international",
    "commodity",
}


class PortfolioHoldingInput(BaseModel):
    fund_code: str = Field(min_length=1, max_length=32)
    fund_name: str = Field(min_length=1, max_length=160)
    fund_type: str = Field(min_length=1, max_length=64)
    market_value: float = Field(gt=0)

    @field_validator("fund_code", "fund_name")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        normalized = " ".join(value.strip().split())
        if not normalized:
            raise ValueError("Text field cannot be empty.")
        return normalized

    @field_validator("fund_type")
    @classmethod
    def normalize_fund_type(cls, value: str) -> str:
        normalized = " ".join(value.strip().lower().split())
        if normalized not in SUPPORTED_FUND_TYPES:
            raise ValueError(
                "fund_type must be one of: "
                + ", ".join(sorted(SUPPORTED_FUND_TYPES))
            )
        return normalized


class PortfolioSnapshotCreateRequest(BaseModel):
    snapshot_date: date
    cash_value: float = Field(ge=0)
    holdings: list[PortfolioHoldingInput] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def validate_unique_fund_codes(self) -> "PortfolioSnapshotCreateRequest":
        seen: set[str] = set()
        duplicates: set[str] = set()
        for holding in self.holdings:
            normalized_code = holding.fund_code.strip().upper()
            if normalized_code in seen:
                duplicates.add(normalized_code)
            seen.add(normalized_code)
        if duplicates:
            raise ValueError(
                "Duplicate fund_code values are not allowed in one portfolio snapshot."
            )
        return self


class PortfolioHoldingResponse(BaseModel):
    fund_code: str
    fund_name: str
    fund_type: str
    market_value: float
    weight: float


class PortfolioReportResponse(BaseModel):
    snapshot_id: str
    snapshot_date: date
    total_value: float
    cash_value: float
    summary: str
    risk_exposure: list[str]
    concentration_flags: list[str]
    allocation_balance: list[str]
    recommended_next_actions: list[str]
    holdings: list[PortfolioHoldingResponse]
    generated_at: datetime


class PortfolioLatestResponse(BaseModel):
    report: PortfolioReportResponse | None = None


class PortfolioHistoryItem(BaseModel):
    snapshot_id: str
    snapshot_date: date
    total_value: float
    summary: str
    generated_at: datetime


class PortfolioHistoryResponse(BaseModel):
    items: list[PortfolioHistoryItem] = Field(default_factory=list)
