"""Schedule model for cron-based check scheduling."""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dq_platform.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Schedule(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Cron schedule for check execution."""

    __tablename__ = "schedules"

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Check reference
    check_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("checks.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Cron expression (e.g., "0 */6 * * *" for every 6 hours)
    cron_expression: Mapped[str] = mapped_column(String(100), nullable=False)

    # Timezone for cron evaluation
    timezone: Mapped[str] = mapped_column(String(50), nullable=False, default="UTC")

    # Schedule status
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    # Execution tracking
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationships
    check: Mapped["Check"] = relationship("Check", back_populates="schedules", lazy="joined")

    def __repr__(self) -> str:
        return f"<Schedule(id={self.id}, check_id={self.check_id}, cron={self.cron_expression})>"


# Import at end to avoid circular imports
from dq_platform.models.check import Check  # noqa: E402
