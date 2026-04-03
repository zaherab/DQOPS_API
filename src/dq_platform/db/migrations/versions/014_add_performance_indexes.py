"""Add performance indexes for common query patterns.

Revision ID: 014_perf_indexes
Revises: 013_notif_and_connectors
Create Date: 2026-03-15

Adds indexes on:
- checks.is_active (filtered list queries)
- checks.(is_active, created_at) (sorted active checks)
- checks.connection_id (FK lookup)
- connections.is_active (filtered list queries)
- schedules.(is_active, next_run_at) (due schedule polling)
- schedules.check_id (FK lookup)
- jobs.check_id (FK lookup)
- jobs.(status, created_at) (job status queries)
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "014_perf_indexes"
down_revision: str = "013_notif_and_connectors"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    # Checks table
    op.create_index("ix_checks_is_active", "checks", ["is_active"], if_not_exists=True)
    op.create_index("ix_checks_is_active_created_at", "checks", ["is_active", "created_at"], if_not_exists=True)
    op.create_index("ix_checks_connection_id", "checks", ["connection_id"], if_not_exists=True)

    # Connections table
    op.create_index("ix_connections_is_active", "connections", ["is_active"], if_not_exists=True)

    # Schedules table
    op.create_index("ix_schedules_is_active_next_run_at", "schedules", ["is_active", "next_run_at"], if_not_exists=True)
    op.create_index("ix_schedules_check_id", "schedules", ["check_id"], if_not_exists=True)

    # Jobs table
    op.create_index("ix_jobs_check_id", "jobs", ["check_id"], if_not_exists=True)
    op.create_index("ix_jobs_status_created_at", "jobs", ["status", "created_at"], if_not_exists=True)


def downgrade() -> None:
    op.drop_index("ix_jobs_status_created_at", table_name="jobs")
    op.drop_index("ix_jobs_check_id", table_name="jobs")
    op.drop_index("ix_schedules_check_id", table_name="schedules")
    op.drop_index("ix_schedules_is_active_next_run_at", table_name="schedules")
    op.drop_index("ix_connections_is_active", table_name="connections")
    op.drop_index("ix_checks_connection_id", table_name="checks")
    op.drop_index("ix_checks_is_active_created_at", table_name="checks")
    op.drop_index("ix_checks_is_active", table_name="checks")
