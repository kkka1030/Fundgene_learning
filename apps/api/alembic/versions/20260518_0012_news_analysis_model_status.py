"""add news analysis model status metadata

Revision ID: 20260518_0012
Revises: 20260517_0011
Create Date: 2026-05-18 13:20:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260518_0012"
down_revision = "20260517_0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "news_analyses",
        sa.Column(
            "model_status",
            sa.String(length=32),
            nullable=False,
            server_default="legacy",
        ),
    )
    op.add_column(
        "news_analyses",
        sa.Column("model_provider", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "news_analyses",
        sa.Column("model_name", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "news_analyses",
        sa.Column("fallback_reason", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("news_analyses", "fallback_reason")
    op.drop_column("news_analyses", "model_name")
    op.drop_column("news_analyses", "model_provider")
    op.drop_column("news_analyses", "model_status")
