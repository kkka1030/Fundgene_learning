"""add learning and portfolio persistence baseline

Revision ID: 20260426_0006
Revises: 20260402_0005
Create Date: 2026-04-26 17:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260426_0006"
down_revision = "20260402_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "learning_paths",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("slug", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )

    op.create_table(
        "courses",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("path_id", sa.String(length=36), nullable=False),
        sa.Column("slug", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("focus", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("estimated_duration_minutes", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["path_id"], ["learning_paths.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_index("ix_courses_path_id", "courses", ["path_id"], unique=False)

    op.create_table(
        "course_sections",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("course_id", sa.String(length=36), nullable=False),
        sa.Column("slug", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("estimated_duration_minutes", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["course_id"], ["courses.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("course_id", "slug", name="uq_course_section_slug"),
    )
    op.create_index(
        "ix_course_sections_course_id", "course_sections", ["course_id"], unique=False
    )

    op.create_table(
        "user_course_progress",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("course_id", sa.String(length=36), nullable=False),
        sa.Column("section_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["course_id"], ["courses.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["section_id"], ["course_sections.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id", "section_id", name="uq_user_course_progress_section"
        ),
    )
    op.create_index(
        "ix_user_course_progress_user_id", "user_course_progress", ["user_id"], unique=False
    )
    op.create_index(
        "ix_user_course_progress_course_id",
        "user_course_progress",
        ["course_id"],
        unique=False,
    )
    op.create_index(
        "ix_user_course_progress_section_id",
        "user_course_progress",
        ["section_id"],
        unique=False,
    )

    op.create_table(
        "portfolio_snapshots",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("cash_value", sa.Float(), nullable=False),
        sa.Column("total_value", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_portfolio_snapshots_user_id",
        "portfolio_snapshots",
        ["user_id"],
        unique=False,
    )

    op.create_table(
        "portfolio_holdings",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("snapshot_id", sa.String(length=36), nullable=False),
        sa.Column("fund_code", sa.String(length=32), nullable=False),
        sa.Column("fund_name", sa.String(length=160), nullable=False),
        sa.Column("fund_type", sa.String(length=64), nullable=False),
        sa.Column("market_value", sa.Float(), nullable=False),
        sa.Column("weight", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["snapshot_id"], ["portfolio_snapshots.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_portfolio_holdings_snapshot_id",
        "portfolio_holdings",
        ["snapshot_id"],
        unique=False,
    )

    op.create_table(
        "portfolio_analyses",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("snapshot_id", sa.String(length=36), nullable=False),
        sa.Column("analysis_version", sa.String(length=32), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("risk_exposure", sa.JSON(), nullable=False),
        sa.Column("concentration_flags", sa.JSON(), nullable=False),
        sa.Column("allocation_balance", sa.JSON(), nullable=False),
        sa.Column("recommended_next_actions", sa.JSON(), nullable=False),
        sa.Column("total_value", sa.Float(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["snapshot_id"], ["portfolio_snapshots.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("snapshot_id"),
    )
    op.create_index(
        "ix_portfolio_analyses_user_id",
        "portfolio_analyses",
        ["user_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_portfolio_analyses_user_id", table_name="portfolio_analyses")
    op.drop_table("portfolio_analyses")
    op.drop_index("ix_portfolio_holdings_snapshot_id", table_name="portfolio_holdings")
    op.drop_table("portfolio_holdings")
    op.drop_index("ix_portfolio_snapshots_user_id", table_name="portfolio_snapshots")
    op.drop_table("portfolio_snapshots")
    op.drop_index(
        "ix_user_course_progress_section_id", table_name="user_course_progress"
    )
    op.drop_index(
        "ix_user_course_progress_course_id", table_name="user_course_progress"
    )
    op.drop_index("ix_user_course_progress_user_id", table_name="user_course_progress")
    op.drop_table("user_course_progress")
    op.drop_index("ix_course_sections_course_id", table_name="course_sections")
    op.drop_table("course_sections")
    op.drop_index("ix_courses_path_id", table_name="courses")
    op.drop_table("courses")
    op.drop_table("learning_paths")
