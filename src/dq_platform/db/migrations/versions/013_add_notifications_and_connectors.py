"""Add notification channels table and new connector types.

Revision ID: 013_notif_and_connectors
Revises: 012_add_phase12_checks
Create Date: 2026-02-09

Adds:
- notification_channels table for webhook alert configuration
- 4 new connection_type enum values: redshift, duckdb, oracle, databricks

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "013_notif_and_connectors"
down_revision: str | None = "012_add_phase12_checks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# New connection types
NEW_CONNECTION_TYPES = ["redshift", "duckdb", "oracle", "databricks"]


def upgrade() -> None:
    """Create notification_channels table and add connector enum values."""

    # Create notification_channel_type enum
    notification_channel_type = postgresql.ENUM("webhook", name="notification_channel_type", create_type=False)
    notification_channel_type.create(op.get_bind(), checkfirst=True)

    # Create notification_channels table
    op.create_table(
        "notification_channels",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column(
            "channel_type",
            notification_channel_type,
            nullable=False,
            server_default="webhook",
        ),
        sa.Column("config", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column(
            "events",
            postgresql.JSONB,
            nullable=False,
            server_default='["incident.opened", "incident.resolved"]',
        ),
        sa.Column("min_severity", sa.String(20), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    # Add new connection_type enum values
    for conn_type in NEW_CONNECTION_TYPES:
        op.execute(f"ALTER TYPE connection_type ADD VALUE IF NOT EXISTS '{conn_type}'")


def downgrade() -> None:
    """Drop notification_channels table. Enum values cannot be removed in PostgreSQL."""
    op.drop_table("notification_channels")
    op.execute("DROP TYPE IF EXISTS notification_channel_type")
