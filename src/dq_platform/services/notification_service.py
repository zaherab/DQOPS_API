"""Notification service - dispatch webhook notifications on incident events."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.models.incident import Incident
from dq_platform.models.notification import NotificationChannel

logger = logging.getLogger(__name__)

# Severity ordering for min_severity filtering
_SEVERITY_ORDER = {"warning": 1, "error": 2, "fatal": 3}


class NotificationService:
    """Service for managing notification channels and dispatching webhooks."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ── CRUD ──────────────────────────────────────────────────────────

    async def create(
        self,
        name: str,
        config: dict[str, Any],
        channel_type: str = "webhook",
        events: list[str] | None = None,
        min_severity: str | None = None,
        description: str | None = None,
    ) -> NotificationChannel:
        """Create a notification channel."""
        channel = NotificationChannel(
            name=name,
            description=description,
            channel_type=channel_type,
            config=config,
            events=events or ["incident.opened", "incident.resolved"],
            min_severity=min_severity,
        )
        self.db.add(channel)
        await self.db.flush()
        return channel

    async def get(self, channel_id: uuid.UUID) -> NotificationChannel | None:
        result = await self.db.execute(select(NotificationChannel).where(NotificationChannel.id == channel_id))
        return result.scalar_one_or_none()

    async def list_channels(
        self,
        offset: int = 0,
        limit: int = 100,
        is_active: bool | None = None,
    ) -> tuple[list[NotificationChannel], int]:
        query = select(NotificationChannel)
        count_query = select(NotificationChannel.id)

        if is_active is not None:
            query = query.where(NotificationChannel.is_active == is_active)
            count_query = count_query.where(NotificationChannel.is_active == is_active)

        count_result = await self.db.execute(count_query)
        total = len(count_result.all())

        query = query.offset(offset).limit(limit).order_by(NotificationChannel.created_at.desc())
        result = await self.db.execute(query)
        channels = list(result.scalars().all())
        return channels, total

    async def update(
        self,
        channel_id: uuid.UUID,
        **kwargs: Any,
    ) -> NotificationChannel | None:
        channel = await self.get(channel_id)
        if not channel:
            return None
        for key, value in kwargs.items():
            if value is not None and hasattr(channel, key):
                setattr(channel, key, value)
        await self.db.flush()
        await self.db.refresh(channel)
        return channel

    async def delete(self, channel_id: uuid.UUID) -> bool:
        channel = await self.get(channel_id)
        if not channel:
            return False
        await self.db.delete(channel)
        await self.db.flush()
        return True

    # ── Dispatch ──────────────────────────────────────────────────────

    async def dispatch_event(
        self,
        event_type: str,
        incident: Incident,
    ) -> int:
        """Send webhook notifications for an incident event.

        Args:
            event_type: e.g. "incident.opened", "incident.resolved"
            incident: The incident that triggered the event.

        Returns:
            Number of webhooks successfully sent.
        """
        channels = await self._get_matching_channels(event_type, incident)
        if not channels:
            return 0

        payload = self._build_payload(event_type, incident)
        sent = 0

        async with httpx.AsyncClient(timeout=10.0) as client:
            for channel in channels:
                try:
                    url = channel.config.get("url")
                    if not url:
                        continue
                    headers = channel.config.get("headers", {})
                    headers.setdefault("Content-Type", "application/json")

                    resp = await client.post(url, json=payload, headers=headers)
                    resp.raise_for_status()
                    sent += 1
                except Exception:
                    logger.warning(
                        "Webhook delivery failed for channel %s (%s)",
                        channel.id,
                        channel.name,
                        exc_info=True,
                    )

        return sent

    async def send_test(self, channel_id: uuid.UUID) -> dict[str, Any]:
        """Send a test webhook to verify channel configuration."""
        channel = await self.get(channel_id)
        if not channel:
            return {"success": False, "error": "Channel not found"}

        url = channel.config.get("url")
        if not url:
            return {"success": False, "error": "No URL configured"}

        payload = {
            "event": "test",
            "timestamp": datetime.now(UTC).isoformat(),
            "message": "Test notification from DQ Platform",
        }

        try:
            headers = channel.config.get("headers", {})
            headers.setdefault("Content-Type", "application/json")
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
            return {"success": True, "status_code": resp.status_code}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Internal ──────────────────────────────────────────────────────

    async def _get_matching_channels(
        self,
        event_type: str,
        incident: Incident,
    ) -> list[NotificationChannel]:
        """Get active channels that match this event and severity."""
        result = await self.db.execute(
            select(NotificationChannel).where(
                NotificationChannel.is_active == True,  # noqa: E712
            )
        )
        channels = list(result.scalars().all())

        matched = []
        for ch in channels:
            # Filter by event type
            if event_type not in (ch.events or []):
                continue
            # Filter by min_severity
            if ch.min_severity:
                incident_sev = _map_incident_severity(incident)
                min_order = _SEVERITY_ORDER.get(ch.min_severity, 0)
                actual_order = _SEVERITY_ORDER.get(incident_sev, 0)
                if actual_order < min_order:
                    continue
            matched.append(ch)

        return matched

    @staticmethod
    def _build_payload(event_type: str, incident: Incident) -> dict[str, Any]:
        """Build the webhook JSON payload."""
        return {
            "event": event_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "incident": {
                "id": str(incident.id),
                "title": incident.title,
                "severity": incident.severity.value if incident.severity else "medium",
                "status": incident.status.value if incident.status else "open",
                "failure_count": incident.failure_count,
                "check_id": str(incident.check_id),
                "description": incident.description,
            },
        }


def _map_incident_severity(incident: Incident) -> str:
    """Map IncidentSeverity enum to the check severity string."""
    sev_map = {"low": "warning", "medium": "error", "high": "fatal", "critical": "fatal"}
    return sev_map.get(incident.severity.value, "error")
