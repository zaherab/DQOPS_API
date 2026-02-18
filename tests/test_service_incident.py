"""Unit tests for IncidentService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.models.incident import Incident, IncidentSeverity, IncidentStatus
from dq_platform.services.incident_service import IncidentService


class TestIncidentService:
    """Test suite for IncidentService."""

    @pytest_asyncio.fixture
    async def mock_db(self):
        """Create a mock database session."""
        db = AsyncMock()
        db.add = MagicMock()
        db.flush = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Create an IncidentService instance."""
        return IncidentService(mock_db)

    async def test_create_incident(self, service, mock_db):
        """Test create_incident() creates an incident successfully."""
        check_id = uuid4()
        result_id = uuid4()

        result = await service.create_incident(
            check_id=check_id,
            result_id=result_id,
            title="Test Incident",
            description="Something went wrong",
            severity="error",
        )

        assert isinstance(result, Incident)
        assert result.check_id == check_id
        assert result.result_id == result_id
        assert result.title == "Test Incident"
        assert result.description == "Something went wrong"
        assert result.severity == IncidentSeverity.MEDIUM  # error maps to medium
        assert result.status == IncidentStatus.OPEN
        assert result.failure_count == 1
        assert result.first_failure_at is not None
        assert result.last_failure_at is not None
        mock_db.add.assert_called_once()
        mock_db.flush.assert_called_once()

    async def test_create_incident_maps_severity_warning(self, service, mock_db):
        """Test create_incident() maps warning severity to LOW."""
        check_id = uuid4()
        result_id = uuid4()

        result = await service.create_incident(
            check_id=check_id,
            result_id=result_id,
            title="Warning Incident",
            description="Test description",
            severity="warning",
        )
        assert result.severity == IncidentSeverity.LOW

    async def test_create_incident_maps_severity_fatal(self, service, mock_db):
        """Test create_incident() maps fatal severity to HIGH."""
        check_id = uuid4()
        result_id = uuid4()

        result = await service.create_incident(
            check_id=check_id,
            result_id=result_id,
            title="Fatal Incident",
            description="Test description",
            severity="fatal",
        )
        assert result.severity == IncidentSeverity.HIGH

    async def test_create_or_update_incident_new(self, service, mock_db):
        """Test create_or_update_incident() creates new incident."""
        check_id = uuid4()

        # Mock no existing incident - need to handle multiple execute calls
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None

        async def mock_execute(*args, **kwargs):
            return mock_result

        mock_db.execute = mock_execute

        result = await service.create_or_update_incident(
            check_id=check_id,
            check_name="test-check",
            failure_message="Check failed",
            severity=IncidentSeverity.MEDIUM,
        )

        assert result.check_id == check_id
        assert result.title == "Check failed: test-check"
        assert result.description == "Check failed"
        mock_db.add.assert_called_once()

    async def test_create_or_update_incident_existing(self, service, mock_db):
        """Test create_or_update_incident() updates existing open incident."""
        check_id = uuid4()
        existing_incident = MagicMock(spec=Incident)
        existing_incident.check_id = check_id
        existing_incident.status = IncidentStatus.OPEN
        existing_incident.failure_count = 2
        existing_incident.first_failure_at = datetime.now(UTC)
        existing_incident.last_failure_at = datetime.now(UTC)

        # Mock existing incident found
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = existing_incident
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.create_or_update_incident(
            check_id=check_id,
            check_name="test-check",
            failure_message="Check failed again",
            severity=IncidentSeverity.MEDIUM,
        )

        assert result == existing_incident
        assert result.failure_count == 3  # Incremented
        assert result.description == "Check failed again"  # Updated
        mock_db.add.assert_not_called()  # Should not create new

    async def test_resolve_incident_success(self, service, mock_db):
        """Test resolve_incident() resolves an open incident."""
        check_id = uuid4()
        mock_incident = MagicMock(spec=Incident)
        mock_incident.check_id = check_id
        mock_incident.status = IncidentStatus.OPEN
        mock_incident.resolved_at = None

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_incident
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.resolve_incident(
            check_id=check_id,
            resolved_by="test-user",
            resolution_notes="Fixed the issue",
        )

        assert result == mock_incident
        assert result.status == IncidentStatus.RESOLVED
        assert result.resolved_by == "test-user"
        assert result.resolution_notes == "Fixed the issue"
        assert result.resolved_at is not None

    async def test_resolve_incident_no_open_incident(self, service, mock_db):
        """Test resolve_incident() returns None when no open incident."""
        check_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.resolve_incident(check_id=check_id)

        assert result is None

    async def test_get_success(self, service, mock_db):
        """Test get() returns incident when found."""
        incident_id = uuid4()
        mock_incident = MagicMock(spec=Incident)
        mock_incident.id = incident_id

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_incident
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get(incident_id)

        assert result == mock_incident

    async def test_get_not_found(self, service, mock_db):
        """Test get() raises NotFoundError when incident doesn't exist."""
        incident_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        with pytest.raises(NotFoundError) as exc_info:
            await service.get(incident_id)

        assert "Incident" in str(exc_info.value)

    async def test_list_incidents_with_filters(self, service, mock_db):
        """Test list_incidents() with filters."""
        check_id = uuid4()
        mock_incidents = [MagicMock(spec=Incident) for _ in range(5)]

        mock_count_result = MagicMock()
        mock_count_result.all.return_value = [(i,) for i in range(5)]

        mock_data_result = MagicMock()
        mock_data_result.scalars.return_value.all.return_value = mock_incidents

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_data_result])

        incidents, total = await service.list_incidents(
            check_id=check_id,
            status=IncidentStatus.OPEN,
            severity=IncidentSeverity.HIGH,
            offset=0,
            limit=10,
        )

        assert incidents == mock_incidents
        assert total == 5

    async def test_update_status_acknowledge(self, service, mock_db):
        """Test update_status() from OPEN to ACKNOWLEDGED."""
        incident_id = uuid4()
        mock_incident = MagicMock(spec=Incident)
        mock_incident.id = incident_id
        mock_incident.status = IncidentStatus.OPEN
        mock_incident.acknowledged_at = None
        mock_incident.acknowledged_by = None

        with patch.object(service, "get", AsyncMock(return_value=mock_incident)):
            result = await service.update_status(
                incident_id=incident_id,
                status=IncidentStatus.ACKNOWLEDGED,
                by="test-user",
                notes="Investigating",
            )

        assert result.status == IncidentStatus.ACKNOWLEDGED
        assert result.acknowledged_by == "test-user"
        assert result.acknowledged_at is not None

    async def test_update_status_resolve(self, service, mock_db):
        """Test update_status() from ACKNOWLEDGED to RESOLVED."""
        incident_id = uuid4()
        mock_incident = MagicMock(spec=Incident)
        mock_incident.id = incident_id
        mock_incident.status = IncidentStatus.ACKNOWLEDGED
        mock_incident.resolved_at = None

        with patch.object(service, "get", AsyncMock(return_value=mock_incident)):
            result = await service.update_status(
                incident_id=incident_id,
                status=IncidentStatus.RESOLVED,
                by="test-user",
                notes="Fixed",
            )

        assert result.status == IncidentStatus.RESOLVED
        assert result.resolved_by == "test-user"
        assert result.resolution_notes == "Fixed"

    async def test_update_status_reopen(self, service, mock_db):
        """Test update_status() from RESOLVED to OPEN."""
        incident_id = uuid4()
        mock_incident = MagicMock(spec=Incident)
        mock_incident.id = incident_id
        mock_incident.status = IncidentStatus.RESOLVED

        with patch.object(service, "get", AsyncMock(return_value=mock_incident)):
            result = await service.update_status(
                incident_id=incident_id,
                status=IncidentStatus.OPEN,
                by="test-user",
            )

        assert result.status == IncidentStatus.OPEN

    async def test_update_status_invalid_transition(self, service, mock_db):
        """Test update_status() rejects invalid transitions."""
        incident_id = uuid4()
        mock_incident = MagicMock(spec=Incident)
        mock_incident.id = incident_id
        mock_incident.status = IncidentStatus.RESOLVED

        with patch.object(service, "get", AsyncMock(return_value=mock_incident)):
            with pytest.raises(ValidationError) as exc_info:
                await service.update_status(
                    incident_id=incident_id,
                    status=IncidentStatus.ACKNOWLEDGED,  # Can't go from RESOLVED to ACKNOWLEDGED
                    by="test-user",
                )

        assert "Cannot transition" in str(exc_info.value)
