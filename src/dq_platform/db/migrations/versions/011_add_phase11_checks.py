"""Add Phase 11 DQOps check types.

Revision ID: 011_add_phase11_checks
Revises: 010_add_missing_dqops_checks
Create Date: 2026-02-05

Adds 8 new check types:
- Phase 11a: Import external results (1)
- Phase 11b: Generic change detection (7)

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "011_add_phase11_checks"
down_revision: Union[str, None] = "010_add_missing_dqops_checks"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# All 8 new check types to add
NEW_CHECK_TYPES = [
    # Phase 11a: Import external results
    "import_custom_result_on_table",

    # Phase 11b: Generic change detection
    "row_count_change",
    "nulls_percent_change",
    "distinct_count_change",
    "distinct_percent_change",
    "mean_change",
    "median_change",
    "sum_change",
]


def upgrade() -> None:
    """Add new check types to the enum."""
    for check_type in NEW_CHECK_TYPES:
        op.execute(f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'")


def downgrade() -> None:
    """Downgrade is not supported for enum value removal in PostgreSQL."""
    pass
