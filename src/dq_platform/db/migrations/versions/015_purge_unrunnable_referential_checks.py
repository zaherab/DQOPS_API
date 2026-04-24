"""Purge unrunnable referential / cross-table checks.

Revision ID: 015_purge_unrunnable_checks
Revises: 014_perf_indexes
Create Date: 2026-04-20

Deletes referential and cross-table checks that were persisted without the
`reference_table` parameter they need to render valid SQL. Every execution
attempt of these checks fails (SQL syntax error on the empty Jinja variable)
or falls through to the GX executor which reports a misleading "not
implemented" error — but the checks still show up in dimension scores as
"check_count > 0, passed_count = 0", creating phantom failures in the UI.

Scope is intentionally narrow:
  - Only the known referential / cross-table types.
  - Only checks that have never produced a result (NOT IN check_results).
  - Only checks with empty or reference_table-less parameters.

This makes the migration safe to re-run and impossible to lose real data with.
Child rows in jobs / incidents / schedules cascade automatically via the
existing ON DELETE CASCADE foreign keys.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "015_purge_unrunnable_checks"
down_revision: str = "014_perf_indexes"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


_UNRUNNABLE_WITHOUT_REFERENCE_TABLE = (
    "foreign_key_found_percent",
    "foreign_key_not_found",
    "total_sum_match_percent",
    "total_min_match_percent",
    "total_max_match_percent",
    "total_average_match_percent",
    "total_not_null_count_match_percent",
    "row_count_match_percent",
)


def upgrade() -> None:
    op.execute(
        f"""
        DELETE FROM checks
        WHERE check_type::text IN {_UNRUNNABLE_WITHOUT_REFERENCE_TABLE!r}
          AND id NOT IN (SELECT check_id FROM check_results)
          AND (
            parameters IS NULL
            OR parameters = '{{}}'::jsonb
            OR NOT (parameters ? 'reference_table')
          )
        """
    )


def downgrade() -> None:
    # Deleted checks were broken by definition (never produced a result, no
    # reference_table). There is no meaningful state to restore.
    pass
