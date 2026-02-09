"""Notification channel schemas."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class NotificationChannelCreate(BaseModel):
    """Schema for creating a notification channel."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = None
    channel_type: str = Field(default="webhook")
    config: dict[str, Any] = Field(..., description='Webhook config: {"url": "https://...", "headers": {...}}')
    events: list[str] = Field(
        default=["incident.opened", "incident.resolved"],
        description="Events to subscribe to",
    )
    min_severity: str | None = Field(default=None, description="Minimum severity: warning, error, or fatal")


class NotificationChannelUpdate(BaseModel):
    """Schema for updating a notification channel."""

    name: str | None = Field(None, min_length=1, max_length=255)
    description: str | None = None
    config: dict[str, Any] | None = None
    events: list[str] | None = None
    min_severity: str | None = None
    is_active: bool | None = None


class NotificationChannelResponse(BaseModel):
    """Schema for notification channel response."""

    id: UUID
    name: str
    description: str | None
    channel_type: str
    config: dict[str, Any]
    events: list[str]
    min_severity: str | None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
