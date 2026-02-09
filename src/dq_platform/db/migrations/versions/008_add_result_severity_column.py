"""Add severity and executed_sql columns to check_results.

Revision ID: 008_add_result_severity_column
Revises: 007_add_percent_check_types
Create Date: 2026-02-03

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "008_add_result_severity_column"
down_revision: str | None = "007_add_percent_check_types"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add missing columns to check_results table."""
    # Add severity column with default value
    op.add_column(
        "check_results",
        sa.Column("severity", sa.String(10), nullable=False, server_default="passed"),
    )
    # Add executed_sql column
    op.add_column(
        "check_results",
        sa.Column("executed_sql", sa.Text, nullable=True),
    )
    # Create index for severity
    op.create_index(
        "ix_check_results_severity",
        "check_results",
        ["severity"],
    )


def downgrade() -> None:
    """Remove columns."""
    op.drop_index("ix_check_results_severity", table_name="check_results")
    op.drop_column("check_results", "executed_sql")
    op.drop_column("check_results", "severity")
