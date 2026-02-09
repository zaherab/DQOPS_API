"""Initial migration - create all tables.

Revision ID: 001_initial
Revises:
Create Date: 2026-02-01

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Create enum types
    op.execute("CREATE TYPE connection_type AS ENUM ('postgresql', 'mysql', 'sqlserver', 'bigquery', 'snowflake')")
    op.execute(
        "CREATE TYPE check_type AS ENUM ('row_count', 'row_count_min', 'row_count_max', 'schema_column_count', 'schema_column_exists', 'table_availability', 'data_freshness', 'null_count', 'null_percent', 'not_null', 'distinct_count', 'duplicate_count', 'unique', 'custom_sql')"  # noqa: E501
    )
    op.execute("CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled')")
    op.execute("CREATE TYPE incident_status AS ENUM ('open', 'acknowledged', 'resolved')")
    op.execute("CREATE TYPE incident_severity AS ENUM ('low', 'medium', 'high', 'critical')")

    # Create connections table
    op.create_table(
        "connections",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column(
            "connection_type",
            postgresql.ENUM(
                "postgresql", "mysql", "sqlserver", "bigquery", "snowflake", name="connection_type", create_type=False
            ),
            nullable=False,
        ),
        sa.Column("config_encrypted", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # Create checks table
    op.create_table(
        "checks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column(
            "connection_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("connections.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "check_type",
            postgresql.ENUM(
                "row_count",
                "row_count_min",
                "row_count_max",
                "schema_column_count",
                "schema_column_exists",
                "table_availability",
                "data_freshness",
                "null_count",
                "null_percent",
                "not_null",
                "distinct_count",
                "duplicate_count",
                "unique",
                "custom_sql",
                name="check_type",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column("target_schema", sa.String(255), nullable=True),
        sa.Column("target_table", sa.String(255), nullable=False),
        sa.Column("target_column", sa.String(255), nullable=True),
        sa.Column("parameters", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_checks_connection_id", "checks", ["connection_id"])

    # Create jobs table
    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "check_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("checks.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "status",
            postgresql.ENUM(
                "pending", "running", "completed", "failed", "cancelled", name="job_status", create_type=False
            ),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_jobs_check_id", "jobs", ["check_id"])
    op.create_index("ix_jobs_status", "jobs", ["status"])

    # Create check_results table (will be converted to hypertable)
    op.create_table(
        "check_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column(
            "check_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("checks.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("connection_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_table", sa.String(255), nullable=False),
        sa.Column("target_column", sa.String(255), nullable=True),
        sa.Column("check_type", sa.String(50), nullable=False),
        sa.Column("actual_value", sa.Float, nullable=True),
        sa.Column("expected_value", sa.Float, nullable=True),
        sa.Column("passed", sa.Boolean, nullable=False),
        sa.Column("execution_time_ms", sa.Integer, nullable=True),
        sa.Column("rows_scanned", sa.Integer, nullable=True),
        sa.Column("result_details", postgresql.JSONB, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.PrimaryKeyConstraint("id", "executed_at"),
    )
    op.create_index("ix_check_results_check_id_executed_at", "check_results", ["check_id", "executed_at"])
    op.create_index("ix_check_results_connection_id", "check_results", ["connection_id"])
    op.create_index("ix_check_results_passed", "check_results", ["passed"])

    # Convert check_results to TimescaleDB hypertable
    op.execute("SELECT create_hypertable('check_results', 'executed_at', if_not_exists => TRUE)")

    # Create incidents table
    op.create_table(
        "incidents",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "check_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("checks.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "status",
            postgresql.ENUM("open", "acknowledged", "resolved", name="incident_status", create_type=False),
            nullable=False,
            server_default="open",
        ),
        sa.Column(
            "severity",
            postgresql.ENUM("low", "medium", "high", "critical", name="incident_severity", create_type=False),
            nullable=False,
            server_default="medium",
        ),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("first_failure_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("failure_count", sa.Integer, nullable=False, server_default="1"),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", sa.String(255), nullable=True),
        sa.Column("resolution_notes", sa.Text, nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_by", sa.String(255), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_incidents_status", "incidents", ["status"])
    op.create_index("ix_incidents_check_id_status", "incidents", ["check_id", "status"])

    # Create schedules table
    op.create_table(
        "schedules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column(
            "check_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("checks.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("cron_expression", sa.String(100), nullable=False),
        sa.Column("timezone", sa.String(50), nullable=False, server_default="UTC"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_schedules_check_id", "schedules", ["check_id"])
    op.create_index("ix_schedules_is_active", "schedules", ["is_active"])


def downgrade() -> None:
    op.drop_table("schedules")
    op.drop_table("incidents")
    op.drop_table("check_results")
    op.drop_table("jobs")
    op.drop_table("checks")
    op.drop_table("connections")

    op.execute("DROP TYPE IF EXISTS incident_severity")
    op.execute("DROP TYPE IF EXISTS incident_status")
    op.execute("DROP TYPE IF EXISTS job_status")
    op.execute("DROP TYPE IF EXISTS check_type")
    op.execute("DROP TYPE IF EXISTS connection_type")
