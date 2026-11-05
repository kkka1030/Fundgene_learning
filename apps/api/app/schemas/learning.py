from typing import Literal

from pydantic import BaseModel, Field, field_validator

LearningProgressStatus = Literal["not_started", "in_progress", "completed"]


class LearningCourseSummary(BaseModel):
    slug: str
    title: str
    focus: str
    description: str
    estimated_duration_minutes: int
    section_count: int
    completed_section_count: int
    progress_percentage: int
    status: LearningProgressStatus
    next_section_slug: str | None = None
    next_section_title: str | None = None


class LearningPathResponse(BaseModel):
    path_slug: str
    title: str
    description: str
    overall_progress_percentage: int
    completed_courses_count: int
    total_courses: int
    recommended_course_slug: str | None = None
    recommended_course_title: str | None = None
    courses: list[LearningCourseSummary] = Field(default_factory=list)


class LearningSectionDetail(BaseModel):
    slug: str
    title: str
    summary: str
    estimated_duration_minutes: int
    position: int
    completed: bool


class LearningCourseDetailResponse(BaseModel):
    path_slug: str
    path_title: str
    course_slug: str
    title: str
    focus: str
    description: str
    estimated_duration_minutes: int
    section_count: int
    completed_section_count: int
    progress_percentage: int
    status: LearningProgressStatus
    next_section_slug: str | None = None
    next_section_title: str | None = None
    sections: list[LearningSectionDetail] = Field(default_factory=list)


class LearningProgressUpdateRequest(BaseModel):
    course_slug: str = Field(min_length=1, max_length=64)
    section_slug: str = Field(min_length=1, max_length=64)
    status: Literal["completed"]

    @field_validator("course_slug", "section_slug")
    @classmethod
    def normalize_slug(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Slug cannot be empty.")
        return normalized


class LearningProgressUpdateResponse(BaseModel):
    course_slug: str
    progress_percentage: int
    completed_section_count: int
    section_count: int
    status: LearningProgressStatus
    next_section_slug: str | None = None
    next_section_title: str | None = None
