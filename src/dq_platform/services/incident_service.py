"""Incident service - managing check failure incidents."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.models.incident import Incident, IncidentSeverity, IncidentStatus

logger = logging.getLogger(__name__)


class IncidentService:
    """Service for managing incidents."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_incident(
        self,
        check_id: uuid.UUID,
        result_id: uuid.UUID,
        title: str,
        description: str,
        severity: str = "error",
    ) -> Incident:
        """Create a new incident for a check failure.

        Args:
            check_id: Check UUID.
            result_id: Check result UUID.
            title: Incident title.
            description: Incident description.
            severity: Severity level string.

        Returns:
            Created incident.
        """
        now = datetime.now(timezone.utc)

        # Map string severity to enum
        severity_map = {
            "warning": IncidentSeverity.LOW,
            "error": IncidentSeverity.MEDIUM,
            "fatal": IncidentSeverity.HIGH,
        }
        incident_severity = severity_map.get(severity, IncidentSeverity.MEDIUM)

        incident = Incident(
            check_id=check_id,
            result_id=result_id,
            status=IncidentStatus.OPEN,
            severity=incident_severity,
            title=title,
            description=description,
            first_failure_at=now,
            last_failure_at=now,
            failure_count=1,
        )

        self.db.add(incident)
        await self.db.flush()
        await self._notify("incident.opened", incident)
        return incident

    async def create_or_update_incident(
        self,
        check_id: uuid.UUID,
        check_name: str,
        failure_message: str,
        severity: IncidentSeverity = IncidentSeverity.MEDIUM,
    ) -> Incident:
        """Create a new incident or update existing open incident.

        If an open incident already exists for this check, increment the
        failure count and update last_failure_at instead of creating new.

        Args:
            check_id: Check UUID.
            check_name: Check name for incident title.
            failure_message: Failure message for description.
            severity: Incident severity level.

        Returns:
            Created or updated incident.
        """
        # Check for existing open incident
        result = await self.db.execute(
            select(Incident).where(
                Incident.check_id == check_id,
                Incident.status.in_([IncidentStatus.OPEN, IncidentStatus.ACKNOWLEDGED]),
            )
        )
        existing = result.scalar_one_or_none()

        now = datetime.now(timezone.utc)

        if existing:
            # Update existing incident
            existing.failure_count += 1
            existing.last_failure_at = now
            existing.description = failure_message
            await self.db.flush()
            return existing

        # Create new incident
        incident = Incident(
            check_id=check_id,
            status=IncidentStatus.OPEN,
            severity=severity,
            title=f"Check failed: {check_name}",
            description=failure_message,
            first_failure_at=now,
            last_failure_at=now,
        )

        self.db.add(incident)
        await self.db.flush()
        await self._notify("incident.opened", incident)
        return incident

    async def resolve_incident(
        self,
        check_id: uuid.UUID,
        resolved_by: str = "system",
        resolution_notes: str | None = None,
    ) -> Incident | None:
        """Resolve open incident for a check (called on check success).

        Args:
            check_id: Check UUID.
            resolved_by: Who resolved the incident.
            resolution_notes: Optional resolution notes.

        Returns:
            Resolved incident or None if no open incident.
        """
        result = await self.db.execute(
            select(Incident).where(
                Incident.check_id == check_id,
                Incident.status.in_([IncidentStatus.OPEN, IncidentStatus.ACKNOWLEDGED]),
            )
        )
        incident = result.scalar_one_or_none()

        if incident:
            incident.status = IncidentStatus.RESOLVED
            incident.resolved_at = datetime.now(timezone.utc)
            incident.resolved_by = resolved_by
            incident.resolution_notes = resolution_notes or "Check passed"
            await self.db.flush()
            await self._notify("incident.resolved", incident)

        return incident

    async def get(self, incident_id: uuid.UUID) -> Incident:
        """Get an incident by ID.

        Args:
            incident_id: Incident UUID.

        Returns:
            Incident instance.

        Raises:
            NotFoundError: If incident not found.
        """
        result = await self.db.execute(
            select(Incident).where(Incident.id == incident_id)
        )
        incident = result.scalar_one_or_none()

        if not incident:
            raise NotFoundError("Incident", str(incident_id))

        return incident

    async def list(
        self,
        offset: int = 0,
        limit: int = 100,
        check_id: uuid.UUID | None = None,
        status: IncidentStatus | None = None,
        severity: IncidentSeverity | None = None,
    ) -> tuple[list[Incident], int]:
        """List incidents with pagination and filters.

        Args:
            offset: Number of records to skip.
            limit: Maximum number of records to return.
            check_id: Optional filter by check.
            status: Optional filter by status.
            severity: Optional filter by severity.

        Returns:
            Tuple of (incidents, total_count).
        """
        query = select(Incident)

        if check_id:
            query = query.where(Incident.check_id == check_id)
        if status:
            query = query.where(Incident.status == status)
        if severity:
            query = query.where(Incident.severity == severity)

        # Get total count
        count_query = select(Incident.id)
        if check_id:
            count_query = count_query.where(Incident.check_id == check_id)
        if status:
            count_query = count_query.where(Incident.status == status)
        if severity:
            count_query = count_query.where(Incident.severity == severity)

        count_result = await self.db.execute(count_query)
        total = len(count_result.all())

        # Get paginated results
        query = query.offset(offset).limit(limit).order_by(Incident.created_at.desc())
        result = await self.db.execute(query)
        incidents = list(result.scalars().all())

        return incidents, total

    async def _notify(self, event_type: str, incident: Incident) -> None:
        """Dispatch webhook notifications (fire-and-forget, never raises)."""
        try:
            from dq_platform.services.notification_service import NotificationService
            notif_service = NotificationService(self.db)
            await notif_service.dispatch_event(event_type, incident)
        except Exception:
            logger.warning("Notification dispatch failed for %s", event_type, exc_info=True)

    async def update_status(
        self,
        incident_id: uuid.UUID,
        status: IncidentStatus,
        by: str,
        notes: str | None = None,
    ) -> Incident:
        """Update incident status.

        Args:
            incident_id: Incident UUID.
            status: New status.
            by: Who is making the update.
            notes: Optional notes.

        Returns:
            Updated incident.

        Raises:
            ValidationError: If status transition is invalid.
        """
        incident = await self.get(incident_id)
        now = datetime.now(timezone.utc)

        # Validate status transitions
        valid_transitions = {
            IncidentStatus.OPEN: [IncidentStatus.ACKNOWLEDGED, IncidentStatus.RESOLVED],
            IncidentStatus.ACKNOWLEDGED: [IncidentStatus.RESOLVED, IncidentStatus.OPEN],
            IncidentStatus.RESOLVED: [IncidentStatus.OPEN],  # Reopen
        }

        if status not in valid_transitions.get(incident.status, []):
            raise ValidationError(
                f"Cannot transition from '{incident.status.value}' to '{status.value}'"
            )

        incident.status = status

        if status == IncidentStatus.ACKNOWLEDGED:
            incident.acknowledged_at = now
            incident.acknowledged_by = by
        elif status == IncidentStatus.RESOLVED:
            incident.resolved_at = now
            incident.resolved_by = by
            incident.resolution_notes = notes

        await self.db.flush()
        return incident
