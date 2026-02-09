"""Notification channel endpoints."""

from uuid import UUID

from fastapi import APIRouter, Query, status

from dq_platform.api.deps import APIKey, NotificationServiceDep
from dq_platform.schemas.common import PaginatedResponse
from dq_platform.schemas.notification import (
    NotificationChannelCreate,
    NotificationChannelResponse,
    NotificationChannelUpdate,
)

router = APIRouter()


@router.post("", response_model=NotificationChannelResponse, status_code=status.HTTP_201_CREATED)
async def create_channel(
    data: NotificationChannelCreate,
    service: NotificationServiceDep,
    api_key: APIKey,
) -> NotificationChannelResponse:
    """Create a notification channel."""
    channel = await service.create(
        name=data.name,
        description=data.description,
        channel_type=data.channel_type,
        config=data.config,
        events=data.events,
        min_severity=data.min_severity,
    )
    return NotificationChannelResponse.model_validate(channel)


@router.get("", response_model=PaginatedResponse[NotificationChannelResponse])
async def list_channels(
    service: NotificationServiceDep,
    api_key: APIKey,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    is_active: bool | None = None,
) -> PaginatedResponse[NotificationChannelResponse]:
    """List notification channels."""
    channels, total = await service.list(offset=offset, limit=limit, is_active=is_active)
    return PaginatedResponse(
        items=[NotificationChannelResponse.model_validate(c) for c in channels],
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get("/{channel_id}", response_model=NotificationChannelResponse)
async def get_channel(
    channel_id: UUID,
    service: NotificationServiceDep,
    api_key: APIKey,
) -> NotificationChannelResponse:
    """Get a notification channel by ID."""
    channel = await service.get(channel_id)
    if not channel:
        from dq_platform.api.errors import NotFoundError
        raise NotFoundError("NotificationChannel", str(channel_id))
    return NotificationChannelResponse.model_validate(channel)


@router.patch("/{channel_id}", response_model=NotificationChannelResponse)
async def update_channel(
    channel_id: UUID,
    data: NotificationChannelUpdate,
    service: NotificationServiceDep,
    api_key: APIKey,
) -> NotificationChannelResponse:
    """Update a notification channel."""
    channel = await service.update(
        channel_id=channel_id,
        name=data.name,
        description=data.description,
        config=data.config,
        events=data.events,
        min_severity=data.min_severity,
        is_active=data.is_active,
    )
    if not channel:
        from dq_platform.api.errors import NotFoundError
        raise NotFoundError("NotificationChannel", str(channel_id))
    return NotificationChannelResponse.model_validate(channel)


@router.delete("/{channel_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_channel(
    channel_id: UUID,
    service: NotificationServiceDep,
    api_key: APIKey,
) -> None:
    """Delete a notification channel."""
    deleted = await service.delete(channel_id)
    if not deleted:
        from dq_platform.api.errors import NotFoundError
        raise NotFoundError("NotificationChannel", str(channel_id))


@router.post("/{channel_id}/test")
async def test_channel(
    channel_id: UUID,
    service: NotificationServiceDep,
    api_key: APIKey,
) -> dict:
    """Send a test webhook to verify configuration."""
    return await service.send_test(channel_id)
