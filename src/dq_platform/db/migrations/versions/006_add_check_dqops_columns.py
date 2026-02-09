"""Add DQOps-style columns to checks table.

Revision ID: 006_add_check_dqops_columns
Revises: 005_add_more_dqops_checks
Create Date: 2026-02-03

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "006_add_check_dqops_columns"
down_revision: str | None = "005_add_more_dqops_checks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add DQOps-style columns to checks table."""
    # Create check_mode enum
    op.execute("CREATE TYPE check_mode AS ENUM ('profiling', 'monitoring', 'partitioned')")

    # Create check_time_scale enum
    op.execute("CREATE TYPE check_time_scale AS ENUM ('daily', 'monthly')")

    # Add check_mode column with default
    op.add_column(
        "checks",
        sa.Column(
            "check_mode",
            sa.Enum("profiling", "monitoring", "partitioned", name="check_mode"),
            nullable=False,
            server_default="monitoring",
        ),
    )

    # Add time_scale column
    op.add_column(
        "checks",
        sa.Column(
            "time_scale",
            sa.Enum("daily", "monthly", name="check_time_scale"),
            nullable=True,
        ),
    )

    # Add partition_by_column
    op.add_column(
        "checks",
        sa.Column(
            "partition_by_column",
            sa.String(255),
            nullable=True,
        ),
    )

    # Add rule_parameters
    op.add_column(
        "checks",
        sa.Column(
            "rule_parameters",
            sa.dialects.postgresql.JSONB,
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Remove DQOps-style columns from checks table."""
    op.drop_column("checks", "rule_parameters")
    op.drop_column("checks", "partition_by_column")
    op.drop_column("checks", "time_scale")
    op.drop_column("checks", "check_mode")
    op.execute("DROP TYPE check_time_scale")
    op.execute("DROP TYPE check_mode")
