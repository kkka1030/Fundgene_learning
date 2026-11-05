import re
from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from app.schemas.user import UserMeResponse

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PASSWORD_MIN_LENGTH = 8


class AuthCredentialsRequest(BaseModel):
    email: str = Field(min_length=5, max_length=320)
    password: str = Field(min_length=PASSWORD_MIN_LENGTH, max_length=128)

    @field_validator("email")
    @classmethod
    def normalize_email(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not EMAIL_PATTERN.fullmatch(normalized):
            raise ValueError("Please provide a valid email address.")
        return normalized

    @field_validator("password")
    @classmethod
    def validate_password(cls, value: str) -> str:
        if len(value) < PASSWORD_MIN_LENGTH or not value.strip():
            raise ValueError(
                f"Password must be at least {PASSWORD_MIN_LENGTH} characters."
            )
        return value


class AuthSessionUserResponse(BaseModel):
    id: str
    email: str
    onboarding_completed: bool
    profile_id: str | None = None
    profile: UserMeResponse | None


class AuthSessionResponse(BaseModel):
    user: AuthSessionUserResponse


class AuthLogoutResponse(BaseModel):
    success: bool = True
    signed_out: bool = True


class AuthSessionInfoResponse(BaseModel):
    session_expires_at: datetime
    user: AuthSessionUserResponse
