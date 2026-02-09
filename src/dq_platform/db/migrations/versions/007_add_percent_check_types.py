"""Add distinct_percent and duplicate_percent check types.

Revision ID: 007_add_percent_check_types
Revises: 006_add_check_dqops_columns
Create Date: 2026-02-03

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "007_add_percent_check_types"
down_revision: Union[str, None] = "006_add_check_dqops_columns"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add new check types to the enum."""
    op.execute("ALTER TYPE check_type ADD VALUE IF NOT EXISTS 'distinct_percent'")
    op.execute("ALTER TYPE check_type ADD VALUE IF NOT EXISTS 'duplicate_percent'")


def downgrade() -> None:
    """Downgrade is not supported for enum value removal in PostgreSQL."""
    pass
