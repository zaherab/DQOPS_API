"""Purge unrunnable change-detection and sql-aggregate checks.

Revision ID: 017_purge_change_checks
Revises: 016_purge_data_staleness
Create Date: 2026-04-20

Third and broadest companion to 015/016. The `*_change_{1_day,7_days,30_days}`
sensors query `check_results` on the MONITORED database (where that table
doesn't exist) and rely on a `check_id` parameter that the framework never
injects. `sql_aggregate_value` needs a user-authored `sql_expression`.

Both classes now declare `required_params`, so `CheckService.create_check`'s
dry-run validation blocks them going forward. This migration removes any
that were persisted before the sensor declarations caught up.

Same narrow scope: only checks that have never produced a result (safe —
no historical data is being discarded).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "017_purge_change_checks"
down_revision: str = "016_purge_data_staleness"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


# These check-type values are registered in the CheckType enum, but their
# sensor templates cannot produce valid SQL without parameters that no
# current caller supplies.
_UNRUNNABLE = (
    # Change-detection family (18 types).
    "nulls_percent_change_1_day",
    "nulls_percent_change_7_days",
    "nulls_percent_change_30_days",
    "distinct_count_change_1_day",
    "distinct_count_change_7_days",
    "distinct_count_change_30_days",
    "distinct_percent_change_1_day",
    "distinct_percent_change_7_days",
    "distinct_percent_change_30_days",
    "mean_change_1_day",
    "mean_change_7_days",
    "mean_change_30_days",
    "median_change_1_day",
    "median_change_7_days",
    "median_change_30_days",
    "sum_change_1_day",
    "sum_change_7_days",
    "sum_change_30_days",
    "row_count_change_1_day",
    "row_count_change_7_days",
    "row_count_change_30_days",
    # Needs user-authored SQL.
    "sql_aggregate_value",
)


def upgrade() -> None:
    op.execute(
        f"""
        DELETE FROM checks
        WHERE check_type::text IN {_UNRUNNABLE!r}
          AND id NOT IN (SELECT check_id FROM check_results)
        """
    )


def downgrade() -> None:
    # Broken-by-definition rows — nothing meaningful to restore.
    pass
