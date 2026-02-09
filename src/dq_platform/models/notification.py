"""Notification channel model for webhook alerts."""

from __future__ import annotations

import enum
from typing import Any

from sqlalchemy import Enum, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from dq_platform.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class NotificationChannelType(str, enum.Enum):
    """Supported notification channel types."""

    WEBHOOK = "webhook"


class NotificationChannel(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Webhook notification channel configuration."""

    __tablename__ = "notification_channels"

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    channel_type: Mapped[NotificationChannelType] = mapped_column(
        Enum(
            NotificationChannelType,
            name="notification_channel_type",
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=NotificationChannelType.WEBHOOK,
    )

    # Webhook config: {"url": "https://...", "headers": {"Authorization": "..."}}
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    # Which events trigger this channel: ["incident.opened", "incident.resolved"]
    events: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, default=lambda: ["incident.opened", "incident.resolved"]
    )

    # Minimum severity to trigger: "warning" | "error" | "fatal" | None (all)
    min_severity: Mapped[str | None] = mapped_column(String(20), nullable=True)

    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    def __repr__(self) -> str:
        return f"<NotificationChannel(id={self.id}, name={self.name}, type={self.channel_type})>"
