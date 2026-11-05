"""persist agent run events

Revision ID: 20260521_0014
Revises: 20260520_0013
Create Date: 2026-05-21 00:25:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260521_0014"
down_revision = "20260520_0013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_run_events",
        sa.Column("id", sa.String(length=80), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("phase", sa.String(length=48), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_agent_run_events_run_id",
        "agent_run_events",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_agent_run_events_run_sequence",
        "agent_run_events",
        ["run_id", "sequence"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_agent_run_events_run_sequence", table_name="agent_run_events")
    op.drop_index("ix_agent_run_events_run_id", table_name="agent_run_events")
    op.drop_table("agent_run_events")
