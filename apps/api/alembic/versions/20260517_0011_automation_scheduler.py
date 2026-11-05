"""add automation scheduler notifications

Revision ID: 20260517_0011
Revises: 20260517_0010
Create Date: 2026-05-17 23:10:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260517_0011"
down_revision = "20260517_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "automation_runs",
        sa.Column(
            "trigger_type",
            sa.String(length=32),
            nullable=False,
            server_default="manual",
        ),
    )
    op.add_column(
        "automation_runs",
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "automation_runs",
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.add_column(
        "automation_runs",
        sa.Column("agent_run_id", sa.String(length=36), nullable=True),
    )
    op.create_index(
        "ix_automation_settings_due",
        "automation_settings",
        ["enabled", "next_run_at"],
        unique=False,
    )

    op.create_table(
        "automation_notifications",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("automation_key", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("action_label", sa.String(length=120), nullable=True),
        sa.Column("action_route", sa.String(length=240), nullable=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["automation_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_automation_notifications_user_id",
        "automation_notifications",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_automation_notifications_run_id",
        "automation_notifications",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_automation_notifications_automation_key",
        "automation_notifications",
        ["automation_key"],
        unique=False,
    )
    op.create_index(
        "ix_automation_notifications_user_created",
        "automation_notifications",
        ["user_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_automation_notifications_user_created",
        table_name="automation_notifications",
    )
    op.drop_index(
        "ix_automation_notifications_automation_key",
        table_name="automation_notifications",
    )
    op.drop_index(
        "ix_automation_notifications_run_id",
        table_name="automation_notifications",
    )
    op.drop_index(
        "ix_automation_notifications_user_id",
        table_name="automation_notifications",
    )
    op.drop_table("automation_notifications")
    op.drop_index("ix_automation_settings_due", table_name="automation_settings")
    op.drop_column("automation_runs", "agent_run_id")
    op.drop_column("automation_runs", "error_message")
    op.drop_column("automation_runs", "due_at")
    op.drop_column("automation_runs", "trigger_type")
