"""align onboarding schema for MVP Spine A

Revision ID: 20260402_0003
Revises: 20260402_0002
Create Date: 2026-04-02 23:10:00
"""

from alembic import op
import sqlalchemy as sa


revision = "20260402_0003"
down_revision = "20260402_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_users_email", table_name="users")
    op.rename_table("users", "user_profiles")
    op.alter_column(
        "user_profiles",
        "id",
        existing_type=sa.String(length=36),
        type_=sa.String(length=128),
        existing_nullable=False,
    )
    op.alter_column(
        "user_profiles",
        "experience_level",
        existing_type=sa.String(length=32),
        new_column_name="investing_experience",
        existing_nullable=False,
    )
    op.add_column(
        "user_profiles",
        sa.Column("monthly_contribution_band", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "user_profiles",
        sa.Column("primary_goal", sa.Text(), nullable=True),
    )
    op.add_column(
        "user_profiles",
        sa.Column(
            "onboarding_completed",
            sa.Boolean(),
            nullable=True,
            server_default=sa.false(),
        ),
    )
    op.execute(
        """
        UPDATE user_profiles
        SET onboarding_completed = CASE
            WHEN onboarding_completed_at IS NOT NULL THEN TRUE
            ELSE FALSE
        END
        """
    )
    op.alter_column(
        "user_profiles",
        "onboarding_completed",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=None,
    )
    op.drop_column("user_profiles", "onboarding_completed_at")
    op.drop_column("user_profiles", "email")

    op.rename_table("risk_questionnaire_submissions", "risk_questionnaires")
    op.alter_column(
        "risk_questionnaires",
        "user_id",
        existing_type=sa.String(length=36),
        type_=sa.String(length=128),
        existing_nullable=False,
    )
    op.alter_column(
        "risk_questionnaires",
        "score",
        existing_type=sa.Integer(),
        new_column_name="risk_score",
        existing_nullable=False,
    )
    op.execute(
        """
        ALTER INDEX ix_risk_questionnaire_submissions_user_id
        RENAME TO ix_risk_questionnaires_user_id
        """
    )

    op.alter_column(
        "behavior_profiles",
        "user_id",
        existing_type=sa.String(length=36),
        type_=sa.String(length=128),
        existing_nullable=False,
    )
    op.add_column(
        "behavior_profiles",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        """
        UPDATE behavior_profiles
        SET updated_at = generated_at
        """
    )
    op.alter_column(
        "behavior_profiles",
        "updated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )
    op.drop_column("behavior_profiles", "source_submission_id")
    op.drop_column("behavior_profiles", "score")
    op.drop_column("behavior_profiles", "improvement_plan")
    op.drop_column("behavior_profiles", "summary")
    op.drop_column("behavior_profiles", "generated_at")


def downgrade() -> None:
    op.add_column(
        "behavior_profiles",
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "behavior_profiles",
        sa.Column("summary", sa.Text(), nullable=True),
    )
    op.add_column(
        "behavior_profiles",
        sa.Column("improvement_plan", sa.JSON(), nullable=True),
    )
    op.add_column(
        "behavior_profiles",
        sa.Column("score", sa.Integer(), nullable=True),
    )
    op.add_column(
        "behavior_profiles",
        sa.Column("source_submission_id", sa.String(length=36), nullable=True),
    )
    op.execute(
        """
        UPDATE behavior_profiles
        SET
            generated_at = updated_at,
            summary = 'Downgraded from simplified MVP behavior profile.',
            improvement_plan = '[]'::json,
            score = 0,
            source_submission_id = latest.id
        FROM (
            SELECT DISTINCT ON (user_id) id, user_id
            FROM risk_questionnaires
            ORDER BY user_id, submitted_at DESC
        ) AS latest
        WHERE behavior_profiles.user_id = latest.user_id
        """
    )
    op.alter_column(
        "behavior_profiles",
        "source_submission_id",
        existing_type=sa.String(length=36),
        nullable=False,
    )
    op.create_unique_constraint(
        "behavior_profiles_source_submission_id_key",
        "behavior_profiles",
        ["source_submission_id"],
    )
    op.alter_column(
        "behavior_profiles",
        "generated_at",
        existing_type=sa.DateTime(timezone=True),
        nullable=False,
    )
    op.alter_column(
        "behavior_profiles",
        "score",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.alter_column(
        "behavior_profiles",
        "improvement_plan",
        existing_type=sa.JSON(),
        nullable=False,
    )
    op.alter_column(
        "behavior_profiles",
        "summary",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.drop_column("behavior_profiles", "updated_at")
    op.alter_column(
        "behavior_profiles",
        "user_id",
        existing_type=sa.String(length=128),
        type_=sa.String(length=36),
        existing_nullable=False,
    )

    op.execute(
        """
        ALTER INDEX ix_risk_questionnaires_user_id
        RENAME TO ix_risk_questionnaire_submissions_user_id
        """
    )
    op.alter_column(
        "risk_questionnaires",
        "risk_score",
        existing_type=sa.Integer(),
        new_column_name="score",
        existing_nullable=False,
    )
    op.alter_column(
        "risk_questionnaires",
        "user_id",
        existing_type=sa.String(length=128),
        type_=sa.String(length=36),
        existing_nullable=False,
    )
    op.rename_table("risk_questionnaires", "risk_questionnaire_submissions")

    op.add_column(
        "user_profiles",
        sa.Column("email", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "user_profiles",
        sa.Column("onboarding_completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        """
        UPDATE user_profiles
        SET
            email = id || '@local.fundgene.dev',
            onboarding_completed_at = CASE
                WHEN onboarding_completed THEN updated_at
                ELSE NULL
            END
        """
    )
    op.alter_column(
        "user_profiles",
        "email",
        existing_type=sa.String(length=255),
        nullable=False,
    )
    op.drop_column("user_profiles", "onboarding_completed")
    op.drop_column("user_profiles", "primary_goal")
    op.drop_column("user_profiles", "monthly_contribution_band")
    op.alter_column(
        "user_profiles",
        "investing_experience",
        existing_type=sa.String(length=32),
        new_column_name="experience_level",
        existing_nullable=False,
    )
    op.alter_column(
        "user_profiles",
        "id",
        existing_type=sa.String(length=128),
        type_=sa.String(length=36),
        existing_nullable=False,
    )
    op.rename_table("user_profiles", "users")
    op.create_index("ix_users_email", "users", ["email"], unique=True)
