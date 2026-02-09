"""CheckResult model for storing check execution results."""

import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Index, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from dq_platform.models.base import Base


class ResultSeverity(str, enum.Enum):
    """Severity levels for check results."""

    PASSED = "passed"
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


class CheckResult(Base):
    """Check execution result - stored as TimescaleDB hypertable."""

    __tablename__ = "check_results"

    # Composite primary key for time-series partitioning
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    executed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        server_default=func.now(),
        nullable=False,
    )

    # Check reference (denormalized for query performance)
    check_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("checks.id", ondelete="CASCADE"),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Denormalized fields for efficient querying
    connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )
    target_table: Mapped[str] = mapped_column(String(255), nullable=False)
    target_column: Mapped[str | None] = mapped_column(String(255), nullable=True)
    check_type: Mapped[str] = mapped_column(String(50), nullable=False)

    # Result values
    actual_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    expected_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    passed: Mapped[bool] = mapped_column(nullable=False)
    severity: Mapped[ResultSeverity] = mapped_column(
        String(10),
        nullable=False,
        default=ResultSeverity.PASSED,
    )

    # Execution metrics
    execution_time_ms: Mapped[int | None] = mapped_column(nullable=True)
    rows_scanned: Mapped[int | None] = mapped_column(nullable=True)

    # Additional result details
    result_details: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Error information (if check failed to execute)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Executed SQL (for DQOps-style checks)
    executed_sql: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Indexes for common query patterns
    __table_args__ = (
        Index("ix_check_results_check_id_executed_at", "check_id", "executed_at"),
        Index("ix_check_results_connection_id", "connection_id"),
        Index("ix_check_results_passed", "passed"),
        Index("ix_check_results_severity", "severity"),
    )

    def __repr__(self) -> str:
        return f"<CheckResult(id={self.id}, check_id={self.check_id}, passed={self.passed})>"
