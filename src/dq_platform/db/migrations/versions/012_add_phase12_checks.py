"""Add Phase 12 DQOps check types.

Revision ID: 012_add_phase12_checks
Revises: 011_add_phase11_checks
Create Date: 2026-02-07

Adds 19 new check types:
- Phase 12a: Anomaly detection (10)
- Phase 12b: Cross-source comparison (9)

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "012_add_phase12_checks"
down_revision: str | None = "011_add_phase11_checks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# All 19 new check types to add
NEW_CHECK_TYPES = [
    # Phase 12a: Anomaly detection
    "row_count_anomaly",
    "data_freshness_anomaly",
    "nulls_percent_anomaly",
    "distinct_count_anomaly",
    "distinct_percent_anomaly",
    "sum_anomaly",
    "mean_anomaly",
    "median_anomaly",
    "min_anomaly",
    "max_anomaly",
    # Phase 12b: Cross-source comparison
    "row_count_match",
    "column_count_match",
    "sum_match",
    "min_match",
    "max_match",
    "mean_match",
    "not_null_count_match",
    "null_count_match",
    "distinct_count_match",
]


def upgrade() -> None:
    """Add new check types to the enum."""
    for check_type in NEW_CHECK_TYPES:
        op.execute(f"ALTER TYPE check_type ADD VALUE IF NOT EXISTS '{check_type}'")


def downgrade() -> None:
    """Downgrade is not supported for enum value removal in PostgreSQL."""
    pass
