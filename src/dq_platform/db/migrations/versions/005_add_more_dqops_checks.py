"""Add more DQOps-style check types to check_type enum.

Revision ID: 005_add_more_dqops_checks
Revises: 004_add_result_id_to_incidents
Create Date: 2026-02-03

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "005_add_more_dqops_checks"
down_revision: Union[str, None] = "004_add_result_id_to_incidents"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Additional DQOps-style check types to add
NEW_CHECK_TYPES = [
    # DateTime
    "date_in_range_percent",
    # Pattern/Format
    "invalid_email_format_found",
    "invalid_email_format_percent",
    "invalid_uuid_format_found",
    "invalid_uuid_format_percent",
    "invalid_ip4_format_found",
    "invalid_ip4_format_percent",
    "invalid_ip6_format_found",
    "invalid_ip6_format_percent",
    "invalid_usa_phone_format_found",
    "invalid_usa_phone_format_percent",
    "invalid_usa_zipcode_format_found",
    "invalid_usa_zipcode_format_percent",
    # Referential Integrity
    "foreign_key_not_found",
    "foreign_key_found_percent",
    # Table-level Uniqueness
    "duplicate_record_count",
    "duplicate_record_percent",
    # Custom SQL
    "sql_condition_failed_on_table",
    "sql_aggregate_expression_on_table",
]


def upgrade() -> None:
    """Add new DQOps-style check types to the enum."""
    for check_type in NEW_CHECK_TYPES:
        op.execute(
            f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'"
        )


def downgrade() -> None:
    """Downgrade is not supported for enum value removal in PostgreSQL."""
    pass
