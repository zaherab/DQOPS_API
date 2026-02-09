"""Add missing DQOps check types for phase 10.

Revision ID: 010_add_missing_dqops_checks
Revises: 009_add_new_check_types
Create Date: 2026-02-05

Adds 11 new check types:
- Phase 10a: Text length percent checks (2)
- Phase 10b: Column-level custom SQL checks (5)
- Phase 10c: Table-level custom SQL checks (1)
- Phase 10d: Schema detection checks (3)

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "010_add_missing_dqops_checks"
down_revision: Union[str, None] = "009_add_new_check_types"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# All 11 new check types to add
NEW_CHECK_TYPES = [
    # Phase 10a: Text length percent checks
    "text_length_below_min_length_percent",
    "text_length_above_max_length_percent",

    # Phase 10b: Column-level custom SQL checks
    "sql_condition_failed_on_column",
    "sql_condition_passed_percent_on_column",
    "sql_aggregate_expression_on_column",
    "sql_invalid_value_count_on_column",
    "import_custom_result_on_column",

    # Phase 10c: Table-level custom SQL checks
    "sql_invalid_record_count_on_table",

    # Phase 10d: Schema detection checks
    "column_list_changed",
    "column_list_or_order_changed",
    "column_types_changed",
]


def upgrade() -> None:
    """Add new check types to the enum."""
    for check_type in NEW_CHECK_TYPES:
        op.execute(f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'")


def downgrade() -> None:
    """Downgrade is not supported for enum value removal in PostgreSQL."""
    pass
