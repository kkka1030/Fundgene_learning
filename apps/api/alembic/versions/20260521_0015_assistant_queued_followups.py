"""add assistant queued followups

Revision ID: 20260521_0015
Revises: 20260521_0014
Create Date: 2026-05-21 20:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260521_0015"
down_revision = "20260521_0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "assistant_queued_followups",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("session_id", sa.String(length=36), nullable=False),
        sa.Column("queued_after_run_id", sa.String(length=36), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("discarded_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["queued_after_run_id"],
            ["agent_runs.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["chat_sessions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_assistant_queued_followups_user_id",
        "assistant_queued_followups",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_assistant_queued_followups_session_id",
        "assistant_queued_followups",
        ["session_id"],
        unique=False,
    )
    op.create_index(
        "ix_assistant_queued_followups_queued_after_run_id",
        "assistant_queued_followups",
        ["queued_after_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_assistant_queued_followups_active_session",
        "assistant_queued_followups",
        ["user_id", "session_id", "status", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_assistant_queued_followups_active_run",
        "assistant_queued_followups",
        ["user_id", "queued_after_run_id", "status", "created_at"],
        unique=False,
    )
    op.create_index(
        "uq_assistant_queued_followups_session_queued",
        "assistant_queued_followups",
        ["user_id", "session_id"],
        unique=True,
        postgresql_where=sa.text("status = 'queued'"),
        sqlite_where=sa.text("status = 'queued'"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_assistant_queued_followups_session_queued",
        table_name="assistant_queued_followups",
    )
    op.drop_index(
        "ix_assistant_queued_followups_active_run",
        table_name="assistant_queued_followups",
    )
    op.drop_index(
        "ix_assistant_queued_followups_active_session",
        table_name="assistant_queued_followups",
    )
    op.drop_index(
        "ix_assistant_queued_followups_queued_after_run_id",
        table_name="assistant_queued_followups",
    )
    op.drop_index(
        "ix_assistant_queued_followups_session_id",
        table_name="assistant_queued_followups",
    )
    op.drop_index(
        "ix_assistant_queued_followups_user_id",
        table_name="assistant_queued_followups",
    )
    op.drop_table("assistant_queued_followups")
