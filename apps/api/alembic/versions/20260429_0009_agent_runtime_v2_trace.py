"""add agent runtime v2 trace baseline

Revision ID: 20260429_0009
Revises: 20260426_0008
Create Date: 2026-04-29 22:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260429_0009"
down_revision = "20260426_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_runs",
        sa.Column(
            "run_type",
            sa.String(length=64),
            nullable=False,
            server_default="advisor_message",
        ),
    )
    op.add_column("agent_runs", sa.Column("intent", sa.String(length=32), nullable=True))
    op.add_column(
        "agent_runs",
        sa.Column(
            "orchestrator_version",
            sa.String(length=64),
            nullable=False,
            server_default="legacy_toolchain",
        ),
    )
    op.add_column(
        "agent_runs",
        sa.Column("policy_status", sa.String(length=32), nullable=True),
    )
    op.add_column("agent_runs", sa.Column("latency_ms", sa.Integer(), nullable=True))
    op.add_column(
        "agent_runs",
        sa.Column("context_snapshot", sa.JSON(), nullable=True),
    )
    op.add_column(
        "agent_runs",
        sa.Column("context_snapshot_version", sa.String(length=32), nullable=True),
    )
    op.create_index("ix_agent_runs_user_id", "agent_runs", ["user_id"], unique=False)
    op.create_index(
        "ix_agent_runs_created_at",
        "agent_runs",
        ["created_at"],
        unique=False,
    )

    op.create_table(
        "agent_steps",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("step_name", sa.String(length=80), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_payload", sa.JSON(), nullable=True),
        sa.Column("output_payload", sa.JSON(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_agent_steps_run_id", "agent_steps", ["run_id"], unique=False)

    op.create_table(
        "agent_tool_calls",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("step_id", sa.String(length=36), nullable=True),
        sa.Column("tool_name", sa.String(length=120), nullable=False),
        sa.Column("permission_level", sa.String(length=32), nullable=False),
        sa.Column("input_payload", sa.JSON(), nullable=True),
        sa.Column("output_payload", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["step_id"], ["agent_steps.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_agent_tool_calls_run_id",
        "agent_tool_calls",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_agent_tool_calls_step_id",
        "agent_tool_calls",
        ["step_id"],
        unique=False,
    )

    op.create_table(
        "agent_evidence_refs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("step_id", sa.String(length=36), nullable=True),
        sa.Column("worker_name", sa.String(length=80), nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.String(length=128), nullable=True),
        sa.Column("source_version", sa.String(length=64), nullable=True),
        sa.Column("quote_or_summary", sa.Text(), nullable=False),
        sa.Column("claim", sa.Text(), nullable=False),
        sa.Column("support_summary", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["step_id"], ["agent_steps.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_agent_evidence_refs_run_id",
        "agent_evidence_refs",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        "ix_agent_evidence_refs_step_id",
        "agent_evidence_refs",
        ["step_id"],
        unique=False,
    )

    op.create_table(
        "agent_state_update_proposals",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("target_type", sa.String(length=80), nullable=False),
        sa.Column("target_id", sa.String(length=128), nullable=True),
        sa.Column("patch_payload", sa.JSON(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("validator_status", sa.String(length=32), nullable=False),
        sa.Column("validator_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_agent_state_update_proposals_run_id",
        "agent_state_update_proposals",
        ["run_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_agent_state_update_proposals_run_id",
        table_name="agent_state_update_proposals",
    )
    op.drop_table("agent_state_update_proposals")
    op.drop_index("ix_agent_evidence_refs_step_id", table_name="agent_evidence_refs")
    op.drop_index("ix_agent_evidence_refs_run_id", table_name="agent_evidence_refs")
    op.drop_table("agent_evidence_refs")
    op.drop_index("ix_agent_tool_calls_step_id", table_name="agent_tool_calls")
    op.drop_index("ix_agent_tool_calls_run_id", table_name="agent_tool_calls")
    op.drop_table("agent_tool_calls")
    op.drop_index("ix_agent_steps_run_id", table_name="agent_steps")
    op.drop_table("agent_steps")
    op.drop_index("ix_agent_runs_created_at", table_name="agent_runs")
    op.drop_index("ix_agent_runs_user_id", table_name="agent_runs")
    op.drop_column("agent_runs", "context_snapshot_version")
    op.drop_column("agent_runs", "context_snapshot")
    op.drop_column("agent_runs", "latency_ms")
    op.drop_column("agent_runs", "policy_status")
    op.drop_column("agent_runs", "orchestrator_version")
    op.drop_column("agent_runs", "intent")
    op.drop_column("agent_runs", "run_type")
