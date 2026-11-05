"""add simulation and behavior training baseline

Revision ID: 20260426_0007
Revises: 20260426_0006
Create Date: 2026-04-26 22:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260426_0007"
down_revision = "20260426_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "scenarios",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("slug", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("bias_focus", sa.String(length=64), nullable=False),
        sa.Column("difficulty", sa.Integer(), nullable=False),
        sa.Column("estimated_duration_minutes", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )
    op.create_table(
        "scenario_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("scenario_id", sa.String(length=36), nullable=False),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("date_label", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("narrative", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("choices", sa.JSON(), nullable=False),
        sa.Column("recommended_choice_key", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["scenario_id"], ["scenarios.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("scenario_id", "step_index", name="uq_scenario_event_step"),
    )
    op.create_index("ix_scenario_events_scenario_id", "scenario_events", ["scenario_id"], unique=False)
    op.create_table(
        "simulation_sessions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("scenario_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("current_step", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["scenario_id"], ["scenarios.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_simulation_sessions_user_id", "simulation_sessions", ["user_id"], unique=False)
    op.create_index("ix_simulation_sessions_scenario_id", "simulation_sessions", ["scenario_id"], unique=False)
    op.create_table(
        "simulation_actions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("session_id", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("step_index", sa.Integer(), nullable=False),
        sa.Column("choice_key", sa.String(length=64), nullable=False),
        sa.Column("choice_label", sa.String(length=160), nullable=False),
        sa.Column("reflection", sa.Text(), nullable=True),
        sa.Column("is_recommended", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["scenario_events.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["session_id"], ["simulation_sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id", "event_id", name="uq_simulation_action_event"),
    )
    op.create_index("ix_simulation_actions_session_id", "simulation_actions", ["session_id"], unique=False)
    op.create_index("ix_simulation_actions_event_id", "simulation_actions", ["event_id"], unique=False)
    op.create_table(
        "simulation_reviews",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("session_id", sa.String(length=36), nullable=False),
        sa.Column("decision_summary", sa.Text(), nullable=False),
        sa.Column("bias_observations", sa.JSON(), nullable=False),
        sa.Column("coach_feedback", sa.Text(), nullable=False),
        sa.Column("recommended_next_actions", sa.JSON(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["session_id"], ["simulation_sessions.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("session_id"),
    )


def downgrade() -> None:
    op.drop_table("simulation_reviews")
    op.drop_index("ix_simulation_actions_event_id", table_name="simulation_actions")
    op.drop_index("ix_simulation_actions_session_id", table_name="simulation_actions")
    op.drop_table("simulation_actions")
    op.drop_index("ix_simulation_sessions_scenario_id", table_name="simulation_sessions")
    op.drop_index("ix_simulation_sessions_user_id", table_name="simulation_sessions")
    op.drop_table("simulation_sessions")
    op.drop_index("ix_scenario_events_scenario_id", table_name="scenario_events")
    op.drop_table("scenario_events")
    op.drop_table("scenarios")
