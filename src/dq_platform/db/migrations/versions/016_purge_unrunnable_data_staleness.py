"""Purge unrunnable data_staleness checks.

Revision ID: 016_purge_data_staleness
Revises: 015_purge_unrunnable_checks
Create Date: 2026-04-20

Companion to 015. The `data_staleness` sensor is table-level but its Jinja
template renders `MAX({{ timestamp_column }})` — without `timestamp_column`
in parameters it produces `MAX()` which is a SQL syntax error. Checks got
persisted without that parameter because the sensor did not declare it as
required; the sensor now does, so create-time validation blocks future
occurrences.

Same narrow scope as 015: only delete checks that (a) have never produced
a result and (b) are missing the required parameter.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "016_purge_data_staleness"
down_revision: str = "015_purge_unrunnable_checks"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DELETE FROM checks
        WHERE check_type::text = 'data_staleness'
          AND id NOT IN (SELECT check_id FROM check_results)
          AND (
            parameters IS NULL
            OR parameters = '{}'::jsonb
            OR NOT (parameters ? 'timestamp_column')
          )
        """
    )


def downgrade() -> None:
    # The deleted rows were broken by definition — nothing meaningful to
    # restore.
    pass
