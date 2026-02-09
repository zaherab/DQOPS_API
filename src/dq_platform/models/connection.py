"""Connection model for data source connections."""

import enum
from typing import TYPE_CHECKING, Any

from sqlalchemy import Enum, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dq_platform.core.encryption import decrypt_config
from dq_platform.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin

if TYPE_CHECKING:
    from dq_platform.models.check import Check


class ConnectionType(str, enum.Enum):
    """Supported database connection types."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLSERVER = "sqlserver"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    DUCKDB = "duckdb"
    ORACLE = "oracle"
    DATABRICKS = "databricks"


class Connection(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Data source connection configuration."""

    __tablename__ = "connections"

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    connection_type: Mapped[ConnectionType] = mapped_column(
        Enum(
            ConnectionType,
            name="connection_type",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
    )
    # Encrypted connection configuration (host, port, database, user, password, etc.)
    config_encrypted: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    # Optional metadata (tags, labels, etc.)
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, nullable=True, default=dict)
    # Soft delete
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    # Relationships
    checks: Mapped[list["Check"]] = relationship("Check", back_populates="connection", lazy="selectin")

    @property
    def decrypted_config(self) -> dict[str, Any]:
        """Get decrypted connection configuration with connection_type included."""
        config = decrypt_config(self.config_encrypted)
        # Include connection_type so the ConnectorFactory can create the right connector
        config["type"] = self.connection_type.value
        return config

    def __repr__(self) -> str:
        return f"<Connection(id={self.id}, name={self.name}, type={self.connection_type})>"
