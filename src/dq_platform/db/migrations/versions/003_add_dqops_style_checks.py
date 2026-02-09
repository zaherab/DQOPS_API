"""Add DQOps-style check types to check_type enum.

Revision ID: 003_add_dqops_style_checks
Revises: 002_add_dqops_check_types
Create Date: 2026-02-03

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "003_add_dqops_style_checks"
down_revision: str | None = "002_add_dqops_check_types"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# DQOps-style check types to add
DQOPS_CHECK_TYPES = [
    # Volume checks with change detection
    "row_count_change_1_day",
    "row_count_change_7_days",
    "row_count_change_30_days",
    # Schema checks
    "column_count_changed",
    # Timeliness checks
    "data_staleness",
    # Nulls checks
    "not_nulls_count",
    "not_nulls_percent",
    "empty_column_found",
    # Numeric/Statistical checks
    "number_below_min_value",
    "number_above_max_value",
    "number_in_range_percent",
    "min_in_range",
    "max_in_range",
    "sum_in_range",
    "mean_in_range",
    "median_in_range",
    # Text checks
    "text_min_length",
    "text_max_length",
    "text_mean_length",
    "text_length_below_min_length",
    "text_length_above_max_length",
    "text_length_in_range_percent",
    "empty_text_found",
    "whitespace_text_found",
    "text_not_matching_regex_found",
    # Geographic checks
    "invalid_latitude",
    "invalid_longitude",
    # Boolean checks
    "true_percent",
    "false_percent",
    # DateTime checks
    "date_values_in_future_percent",
    # Custom SQL checks
    "sql_condition_failed_on_table",
    "sql_aggregate_expression_on_table",
]


def upgrade() -> None:
    """Add DQOps-style check types to the enum."""
    for check_type in DQOPS_CHECK_TYPES:
        op.execute(f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'")


def downgrade() -> None:
    """Downgrade is not supported for enum value removal in PostgreSQL."""
    pass
