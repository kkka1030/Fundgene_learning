"""add command center automation contracts

Revision ID: 20260517_0010
Revises: 20260429_0009
Create Date: 2026-05-17 21:40:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260517_0010"
down_revision = "20260429_0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_state_update_proposals",
        sa.Column(
            "user_decision_status",
            sa.String(length=32),
            nullable=False,
            server_default="pending",
        ),
    )
    op.add_column(
        "agent_state_update_proposals",
        sa.Column("decision_note", sa.Text(), nullable=True),
    )
    op.add_column(
        "agent_state_update_proposals",
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "agent_state_update_proposals",
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "automation_settings",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("automation_key", sa.String(length=64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("frequency", sa.String(length=80), nullable=False),
        sa.Column("read_scope", sa.JSON(), nullable=False),
        sa.Column("produces", sa.JSON(), nullable=False),
        sa.Column("requires_confirmation_for", sa.JSON(), nullable=False),
        sa.Column("safety_boundary", sa.Text(), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "automation_key", name="uq_automation_user_key"),
    )
    op.create_index(
        "ix_automation_settings_user_id",
        "automation_settings",
        ["user_id"],
        unique=False,
    )

    op.create_table(
        "daily_brief_preferences",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("cadence_key", sa.String(length=64), nullable=False),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("include_sources", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", name="uq_daily_brief_preferences_user"),
    )

    op.create_table(
        "automation_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("setting_id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("automation_key", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("output_ref", sa.String(length=160), nullable=True),
        sa.Column("output_payload", sa.JSON(), nullable=False),
        sa.Column("safety_boundary", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["setting_id"],
            ["automation_settings.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_automation_runs_setting_id",
        "automation_runs",
        ["setting_id"],
        unique=False,
    )
    op.create_index(
        "ix_automation_runs_user_id",
        "automation_runs",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_automation_runs_automation_key",
        "automation_runs",
        ["automation_key"],
        unique=False,
    )
    op.create_index(
        "ix_automation_runs_user_key_started",
        "automation_runs",
        ["user_id", "automation_key", "started_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_automation_runs_user_key_started", table_name="automation_runs")
    op.drop_index("ix_automation_runs_automation_key", table_name="automation_runs")
    op.drop_index("ix_automation_runs_user_id", table_name="automation_runs")
    op.drop_index("ix_automation_runs_setting_id", table_name="automation_runs")
    op.drop_table("automation_runs")
    op.drop_table("daily_brief_preferences")
    op.drop_index("ix_automation_settings_user_id", table_name="automation_settings")
    op.drop_table("automation_settings")
    op.drop_column("agent_state_update_proposals", "applied_at")
    op.drop_column("agent_state_update_proposals", "decided_at")
    op.drop_column("agent_state_update_proposals", "decision_note")
    op.drop_column("agent_state_update_proposals", "user_decision_status")
