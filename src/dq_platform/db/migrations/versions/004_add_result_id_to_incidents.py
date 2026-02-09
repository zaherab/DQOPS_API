"""Add result_id to incidents table.

Revision ID: 004_add_result_id_to_incidents
Revises: 003_add_dqops_style_checks
Create Date: 2026-02-03

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "004_add_result_id_to_incidents"
down_revision: Union[str, None] = "003_add_dqops_style_checks"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add result_id column to incidents table.

    Note: We don't add a FK constraint to check_results because it's a
    TimescaleDB hypertable with a composite primary key (id, executed_at).
    """
    op.add_column(
        "incidents",
        sa.Column(
            "result_id",
            sa.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_incidents_result_id",
        "incidents",
        ["result_id"],
    )


def downgrade() -> None:
    """Remove result_id column from incidents table."""
    op.drop_index("ix_incidents_result_id", table_name="incidents")
    op.drop_column("incidents", "result_id")
