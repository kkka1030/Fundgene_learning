"""add news policy analysis and hardening baseline

Revision ID: 20260426_0008
Revises: 20260426_0007
Create Date: 2026-04-26 23:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260426_0008"
down_revision = "20260426_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "news_items",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=True),
        sa.Column("source_name", sa.String(length=160), nullable=False),
        sa.Column("source_url", sa.String(length=500), nullable=False),
        sa.Column("external_id", sa.String(length=512), nullable=False),
        sa.Column("title", sa.String(length=300), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("url", sa.String(length=1000), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "source_url",
            "external_id",
            name="uq_news_item_source_external",
        ),
    )
    op.create_index(
        "ix_news_items_user_id",
        "news_items",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_news_items_published_at",
        "news_items",
        ["published_at"],
        unique=False,
    )
    op.create_index(
        "ix_news_items_fetched_at",
        "news_items",
        ["fetched_at"],
        unique=False,
    )

    op.create_table(
        "policy_items",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("source_name", sa.String(length=160), nullable=False),
        sa.Column("source_url", sa.String(length=500), nullable=False),
        sa.Column("external_id", sa.String(length=512), nullable=False),
        sa.Column("title", sa.String(length=300), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("url", sa.String(length=1000), nullable=False),
        sa.Column("policy_area", sa.String(length=64), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_url",
            "external_id",
            name="uq_policy_item_source_external",
        ),
    )
    op.create_index(
        "ix_policy_items_published_at",
        "policy_items",
        ["published_at"],
        unique=False,
    )
    op.create_index(
        "ix_policy_items_fetched_at",
        "policy_items",
        ["fetched_at"],
        unique=False,
    )

    op.create_table(
        "news_analyses",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("news_item_id", sa.String(length=36), nullable=True),
        sa.Column("policy_item_id", sa.String(length=36), nullable=True),
        sa.Column("analysis_version", sa.String(length=32), nullable=False),
        sa.Column("facts", sa.JSON(), nullable=False),
        sa.Column("impact_paths", sa.JSON(), nullable=False),
        sa.Column("uncertainty_notes", sa.JSON(), nullable=False),
        sa.Column("beginner_translation", sa.Text(), nullable=False),
        sa.Column("related_learning_topics", sa.JSON(), nullable=False),
        sa.Column("recommended_next_actions", sa.JSON(), nullable=False),
        sa.Column("risk_notice", sa.Text(), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(news_item_id IS NOT NULL AND policy_item_id IS NULL) OR "
            "(news_item_id IS NULL AND policy_item_id IS NOT NULL)",
            name="ck_news_analysis_exactly_one_item",
        ),
        sa.ForeignKeyConstraint(["news_item_id"], ["news_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["policy_item_id"],
            ["policy_items.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["user_profiles.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_news_analyses_user_id", "news_analyses", ["user_id"], unique=False)
    op.create_index(
        "ix_news_analyses_news_item_id",
        "news_analyses",
        ["news_item_id"],
        unique=False,
    )
    op.create_index(
        "ix_news_analyses_policy_item_id",
        "news_analyses",
        ["policy_item_id"],
        unique=False,
    )
    op.create_index(
        "ix_news_analyses_generated_at",
        "news_analyses",
        ["generated_at"],
        unique=False,
    )

    op.create_table(
        "agent_citations",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("analysis_id", sa.String(length=36), nullable=False),
        sa.Column("source_type", sa.String(length=16), nullable=False),
        sa.Column("source_item_id", sa.String(length=36), nullable=False),
        sa.Column("source_name", sa.String(length=160), nullable=False),
        sa.Column("title", sa.String(length=300), nullable=False),
        sa.Column("url", sa.String(length=1000), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["analysis_id"],
            ["news_analyses.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_agent_citations_analysis_id",
        "agent_citations",
        ["analysis_id"],
        unique=False,
    )
    op.create_index(
        "ix_agent_citations_source_item_id",
        "agent_citations",
        ["source_item_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_agent_citations_source_item_id", table_name="agent_citations")
    op.drop_index("ix_agent_citations_analysis_id", table_name="agent_citations")
    op.drop_table("agent_citations")
    op.drop_index("ix_news_analyses_generated_at", table_name="news_analyses")
    op.drop_index("ix_news_analyses_policy_item_id", table_name="news_analyses")
    op.drop_index("ix_news_analyses_news_item_id", table_name="news_analyses")
    op.drop_index("ix_news_analyses_user_id", table_name="news_analyses")
    op.drop_table("news_analyses")
    op.drop_index("ix_policy_items_fetched_at", table_name="policy_items")
    op.drop_index("ix_policy_items_published_at", table_name="policy_items")
    op.drop_table("policy_items")
    op.drop_index("ix_news_items_fetched_at", table_name="news_items")
    op.drop_index("ix_news_items_published_at", table_name="news_items")
    op.drop_index("ix_news_items_user_id", table_name="news_items")
    op.drop_table("news_items")
