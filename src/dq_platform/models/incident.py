"""Incident model for tracking check failures."""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dq_platform.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class IncidentStatus(str, enum.Enum):
    """Incident lifecycle status."""

    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class IncidentSeverity(str, enum.Enum):
    """Incident severity level."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Incident(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Check failure incident."""

    __tablename__ = "incidents"

    # Check reference
    check_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("checks.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Result reference (optional - links to the specific check result)
    result_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("check_results.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Incident status
    status: Mapped[IncidentStatus] = mapped_column(
        Enum(
            IncidentStatus,
            name="incident_status",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=IncidentStatus.OPEN,
    )

    # Severity
    severity: Mapped[IncidentSeverity] = mapped_column(
        Enum(
            IncidentSeverity,
            name="incident_severity",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=IncidentSeverity.MEDIUM,
    )

    # Incident details
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # First and last failure info
    first_failure_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_failure_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    failure_count: Mapped[int] = mapped_column(default=1, nullable=False)

    # Resolution info
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    resolution_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Acknowledgment info
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Additional metadata
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, nullable=True, default=dict)

    # Relationships
    check: Mapped["Check"] = relationship("Check", back_populates="incidents", lazy="joined")

    __table_args__ = (
        Index("ix_incidents_status", "status"),
        Index("ix_incidents_check_id_status", "check_id", "status"),
    )

    def __repr__(self) -> str:
        return f"<Incident(id={self.id}, check_id={self.check_id}, status={self.status})>"


# Import at end to avoid circular imports
from dq_platform.models.check import Check  # noqa: E402
