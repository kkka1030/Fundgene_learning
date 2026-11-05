from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.course import Course
from app.models.course_section import CourseSection
from app.models.learning_path import LearningPath
from app.models.user import UserProfile
from app.models.user_course_progress import UserCourseProgress
from app.schemas.dashboard import DashboardLearningStatus
from app.schemas.learning import (
    LearningCourseDetailResponse,
    LearningCourseSummary,
    LearningPathResponse,
    LearningProgressUpdateRequest,
    LearningProgressUpdateResponse,
)

LEARNING_PATH_SLUG = "beginner-core-path"
LEARNING_PATH_TITLE = "Beginner Core Path"
LEARNING_PATH_DESCRIPTION = (
    "先用三门基础课建立基金共同语言，再把风险理解、配置原则和平台内下一步动作连成可执行学习路径。"
)
LEARNING_FIXTURE = [
    {
        "slug": "fund-basics",
        "title": "基金基础",
        "focus": "建立基金、净值、波动和长期持有的共同语言。",
        "description": "先看懂基金买了什么、净值为什么会波动，以及长期持有和短期情绪反应的差别。",
        "estimated_duration_minutes": 24,
        "sections": [
            {
                "slug": "what-a-fund-owns",
                "title": "基金到底持有什么",
                "summary": "先区分基金和单一股票，理解基金是在帮你持有一篮子资产。",
                "estimated_duration_minutes": 8,
            },
            {
                "slug": "nav-and-volatility",
                "title": "净值和波动怎么看",
                "summary": "把净值波动理解成持仓资产变化的结果，而不是简单的好坏信号。",
                "estimated_duration_minutes": 8,
            },
            {
                "slug": "long-term-framing",
                "title": "长期持有为什么重要",
                "summary": "建立周期视角，避免把短期涨跌误解成策略本身失效。",
                "estimated_duration_minutes": 8,
            },
        ],
    },
    {
        "slug": "risk-and-drawdown",
        "title": "风险与回撤",
        "focus": "理解风险等级、波动容忍和下跌中的行为偏差。",
        "description": "把风险等级和回撤拆开解释，再连接到用户自己的承受能力和行为反应。",
        "estimated_duration_minutes": 30,
        "sections": [
            {
                "slug": "risk-level-basics",
                "title": "风险等级不是收益承诺",
                "summary": "把风险等级理解成可能承受的波动区间，而不是未来回报保证。",
                "estimated_duration_minutes": 10,
            },
            {
                "slug": "drawdown-explained",
                "title": "回撤该怎么感受",
                "summary": "用简单数字例子理解从高点回落的幅度和情绪压力。",
                "estimated_duration_minutes": 10,
            },
            {
                "slug": "behavior-under-pressure",
                "title": "下跌时最容易失守的动作",
                "summary": "识别恐慌卖出和追热点背后的行为偏差，把它们和计划拆开。",
                "estimated_duration_minutes": 10,
            },
        ],
    },
    {
        "slug": "allocation-principles",
        "title": "配置原则",
        "focus": "从单只基金视角转向组合结构和资产分布。",
        "description": "开始建立组合意识，理解分散配置、主题暴露和长期配置纪律。",
        "estimated_duration_minutes": 28,
        "sections": [
            {
                "slug": "why-diversify",
                "title": "为什么不能只盯一只热门基金",
                "summary": "建立分散配置的第一原则，避免把单一主题误当成组合。",
                "estimated_duration_minutes": 9,
            },
            {
                "slug": "repeat-risk",
                "title": "不同基金也可能承担重复风险",
                "summary": "看清基金名称不同不等于风险真正分散。",
                "estimated_duration_minutes": 9,
            },
            {
                "slug": "rebalancing-principles",
                "title": "再平衡只谈原则不谈指令",
                "summary": "把再平衡理解成风险管理动作，而不是高频调仓冲动。",
                "estimated_duration_minutes": 10,
            },
        ],
    },
]


def ensure_learning_catalog(db: Session) -> None:
    existing_path = db.scalar(
        select(LearningPath).where(LearningPath.slug == LEARNING_PATH_SLUG)
    )
    if existing_path is not None:
        return

    path = LearningPath(
        slug=LEARNING_PATH_SLUG,
        title=LEARNING_PATH_TITLE,
        description=LEARNING_PATH_DESCRIPTION,
    )
    db.add(path)
    db.flush()

    for course_index, course_payload in enumerate(LEARNING_FIXTURE, start=1):
        course = Course(
            path_id=path.id,
            slug=course_payload["slug"],
            title=course_payload["title"],
            focus=course_payload["focus"],
            description=course_payload["description"],
            estimated_duration_minutes=course_payload["estimated_duration_minutes"],
            position=course_index,
        )
        db.add(course)
        db.flush()

        for section_index, section_payload in enumerate(
            course_payload["sections"], start=1
        ):
            section = CourseSection(
                course_id=course.id,
                slug=section_payload["slug"],
                title=section_payload["title"],
                summary=section_payload["summary"],
                estimated_duration_minutes=section_payload["estimated_duration_minutes"],
                position=section_index,
            )
            db.add(section)

    db.commit()


def _list_courses(db: Session) -> list[Course]:
    return list(
        db.scalars(
            select(Course)
            .join(LearningPath, LearningPath.id == Course.path_id)
            .where(LearningPath.slug == LEARNING_PATH_SLUG)
            .order_by(Course.position.asc(), Course.created_at.asc())
        )
    )


def _list_sections_for_courses(
    db: Session, *, course_ids: list[str]
) -> dict[str, list[CourseSection]]:
    if not course_ids:
        return {}

    sections = list(
        db.scalars(
            select(CourseSection)
            .where(CourseSection.course_id.in_(course_ids))
            .order_by(CourseSection.position.asc(), CourseSection.created_at.asc())
        )
    )
    grouped: dict[str, list[CourseSection]] = {}
    for section in sections:
        grouped.setdefault(section.course_id, []).append(section)
    return grouped


def _list_completed_section_ids(db: Session, *, user_id: str) -> set[str]:
    rows = list(
        db.scalars(
            select(UserCourseProgress.section_id).where(
                UserCourseProgress.user_id == user_id,
                UserCourseProgress.status == "completed",
            )
        )
    )
    return set(rows)


def _build_course_summary(
    course: Course,
    *,
    sections: list[CourseSection],
    completed_section_ids: set[str],
) -> LearningCourseSummary:
    section_count = len(sections)
    completed_sections = [section for section in sections if section.id in completed_section_ids]
    completed_section_count = len(completed_sections)
    progress_percentage = (
        round((completed_section_count / section_count) * 100) if section_count else 0
    )

    next_section = next(
        (section for section in sections if section.id not in completed_section_ids),
        None,
    )
    if completed_section_count == 0:
        status = "not_started"
    elif completed_section_count == section_count:
        status = "completed"
    else:
        status = "in_progress"

    return LearningCourseSummary(
        slug=course.slug,
        title=course.title,
        focus=course.focus,
        description=course.description,
        estimated_duration_minutes=course.estimated_duration_minutes,
        section_count=section_count,
        completed_section_count=completed_section_count,
        progress_percentage=progress_percentage,
        status=status,
        next_section_slug=next_section.slug if next_section else None,
        next_section_title=next_section.title if next_section else None,
    )


def get_learning_path(db: Session, *, user: UserProfile) -> LearningPathResponse:
    ensure_learning_catalog(db)
    path = db.scalar(select(LearningPath).where(LearningPath.slug == LEARNING_PATH_SLUG))
    if path is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Learning catalog is unavailable.",
        )

    courses = _list_courses(db)
    sections_by_course = _list_sections_for_courses(
        db, course_ids=[course.id for course in courses]
    )
    completed_section_ids = _list_completed_section_ids(db, user_id=user.id)
    course_summaries = [
        _build_course_summary(
            course,
            sections=sections_by_course.get(course.id, []),
            completed_section_ids=completed_section_ids,
        )
        for course in courses
    ]

    recommended_course = next(
        (course for course in course_summaries if course.status != "completed"),
        None,
    )
    total_courses = len(course_summaries)
    completed_courses_count = sum(
        1 for course in course_summaries if course.status == "completed"
    )
    total_sections = sum(course.section_count for course in course_summaries)
    completed_sections = sum(
        course.completed_section_count for course in course_summaries
    )
    overall_progress_percentage = (
        round((completed_sections / total_sections) * 100) if total_sections else 0
    )

    return LearningPathResponse(
        path_slug=path.slug,
        title=path.title,
        description=path.description,
        overall_progress_percentage=overall_progress_percentage,
        completed_courses_count=completed_courses_count,
        total_courses=total_courses,
        recommended_course_slug=recommended_course.slug if recommended_course else None,
        recommended_course_title=recommended_course.title if recommended_course else None,
        courses=course_summaries,
    )


def get_learning_course_detail(
    db: Session,
    *,
    user: UserProfile,
    course_slug: str,
) -> LearningCourseDetailResponse:
    ensure_learning_catalog(db)
    course = db.scalar(select(Course).where(Course.slug == course_slug))
    if course is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested learning course does not exist.",
        )

    path = db.get(LearningPath, course.path_id)
    sections = list(
        db.scalars(
            select(CourseSection)
            .where(CourseSection.course_id == course.id)
            .order_by(CourseSection.position.asc(), CourseSection.created_at.asc())
        )
    )
    completed_section_ids = _list_completed_section_ids(db, user_id=user.id)
    summary = _build_course_summary(
        course,
        sections=sections,
        completed_section_ids=completed_section_ids,
    )

    return LearningCourseDetailResponse(
        path_slug=path.slug if path else LEARNING_PATH_SLUG,
        path_title=path.title if path else LEARNING_PATH_TITLE,
        course_slug=course.slug,
        title=course.title,
        focus=course.focus,
        description=course.description,
        estimated_duration_minutes=course.estimated_duration_minutes,
        section_count=summary.section_count,
        completed_section_count=summary.completed_section_count,
        progress_percentage=summary.progress_percentage,
        status=summary.status,
        next_section_slug=summary.next_section_slug,
        next_section_title=summary.next_section_title,
        sections=[
            {
                "slug": section.slug,
                "title": section.title,
                "summary": section.summary,
                "estimated_duration_minutes": section.estimated_duration_minutes,
                "position": section.position,
                "completed": section.id in completed_section_ids,
            }
            for section in sections
        ],
    )


def update_learning_progress(
    db: Session,
    *,
    user: UserProfile,
    payload: LearningProgressUpdateRequest,
) -> LearningProgressUpdateResponse:
    ensure_learning_catalog(db)
    course = db.scalar(select(Course).where(Course.slug == payload.course_slug))
    if course is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested learning course does not exist.",
        )

    section = db.scalar(
        select(CourseSection).where(
            CourseSection.course_id == course.id,
            CourseSection.slug == payload.section_slug,
        )
    )
    if section is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Requested learning section does not exist for the given course.",
        )

    progress = db.scalar(
        select(UserCourseProgress).where(
            UserCourseProgress.user_id == user.id,
            UserCourseProgress.section_id == section.id,
        )
    )
    now = datetime.now(timezone.utc)
    if progress is None:
        progress = UserCourseProgress(
            user_id=user.id,
            course_id=course.id,
            section_id=section.id,
            status=payload.status,
            completed_at=now,
            created_at=now,
        )
        db.add(progress)
    else:
        progress.status = payload.status
        progress.completed_at = now

    db.commit()

    detail = get_learning_course_detail(db, user=user, course_slug=course.slug)
    return LearningProgressUpdateResponse(
        course_slug=detail.course_slug,
        progress_percentage=detail.progress_percentage,
        completed_section_count=detail.completed_section_count,
        section_count=detail.section_count,
        status=detail.status,
        next_section_slug=detail.next_section_slug,
        next_section_title=detail.next_section_title,
    )


def get_learning_overview(db: Session, *, user_id: str) -> DashboardLearningStatus:
    ensure_learning_catalog(db)
    user = db.get(UserProfile, user_id)
    if user is None:
        return DashboardLearningStatus(
            overall_progress_percentage=0,
            completed_courses_count=0,
            total_courses=0,
        )

    learning_path = get_learning_path(db, user=user)
    return DashboardLearningStatus(
        overall_progress_percentage=learning_path.overall_progress_percentage,
        completed_courses_count=learning_path.completed_courses_count,
        total_courses=learning_path.total_courses,
        recommended_course_slug=learning_path.recommended_course_slug,
        recommended_course_title=learning_path.recommended_course_title,
    )
