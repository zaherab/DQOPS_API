"""Add DQOps-style check types to check_type enum.

Revision ID: 002_add_dqops_check_types
Revises: 001_initial
Create Date: 2026-02-01

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "002_add_dqops_check_types"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# New check types to add (29 total)
NEW_CHECK_TYPES = [
    # Volume (table-level)
    "row_count_exact",
    "row_count_compare",
    # Schema (table-level)
    "schema_column_list",
    "schema_column_order",
    # Completeness (column-level)
    "completeness_percent",
    # Numeric/Statistical (column-level)
    "column_min",
    "column_max",
    "column_mean",
    "column_median",
    "column_stddev",
    "column_sum",
    "column_quantile",
    # Text (column-level)
    "text_length_range",
    "text_length_exact",
    # Patterns (column-level)
    "regex_not_match",
    "like_pattern",
    "forbidden_values",
    # Datatype (column-level)
    "column_type",
    "date_parseable",
    "json_parseable",
    "datetime_format",
    # Uniqueness (column-level)
    "uniqueness_percent",
    "distinct_values_in_set",
    "most_common_value",
    # Ordering (column-level)
    "values_increasing",
    "values_decreasing",
    # Multi-column (table-level)
    "column_pair_equal",
    "composite_key_unique",
    "multicolumn_unique",
]

# Check types that may have been added manually earlier
POSSIBLY_EXISTING = [
    "value_range",
    "regex_pattern",
    "allowed_values",
    "column_pair_comparison",
]


def upgrade() -> None:
    # Add check types that may already exist (from manual migrations)
    for check_type in POSSIBLY_EXISTING:
        op.execute(
            f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'"
        )

    # Add all new DQOps-style check types
    for check_type in NEW_CHECK_TYPES:
        op.execute(
            f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'"
        )


def downgrade() -> None:
    # PostgreSQL does not support removing enum values directly.
    # To downgrade, you would need to:
    # 1. Create a new enum type without the new values
    # 2. Update all tables using the enum
    # 3. Drop the old enum
    # 4. Rename the new enum
    #
    # This is destructive and not recommended for production.
    # For safety, this downgrade does nothing.
    pass
