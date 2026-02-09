"""Unit tests for scheduler task and notification service."""

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dq_platform.models.incident import Incident, IncidentSeverity, IncidentStatus
from dq_platform.models.notification import NotificationChannel, NotificationChannelType
from dq_platform.services.notification_service import NotificationService, _map_incident_severity

# ── Fixtures ──────────────────────────────────────────────────────────


def _make_incident(**kwargs) -> Incident:
    """Create a mock Incident for testing."""
    defaults = {
        "id": uuid.uuid4(),
        "check_id": uuid.uuid4(),
        "status": IncidentStatus.OPEN,
        "severity": IncidentSeverity.MEDIUM,
        "title": "Check failed: nulls_percent",
        "description": "15% nulls found, threshold is 10%",
        "failure_count": 1,
        "first_failure_at": datetime.now(UTC),
        "last_failure_at": datetime.now(UTC),
    }
    defaults.update(kwargs)
    incident = MagicMock(spec=Incident)
    for k, v in defaults.items():
        setattr(incident, k, v)
    return incident


def _make_channel(**kwargs) -> NotificationChannel:
    """Create a mock NotificationChannel for testing."""
    defaults = {
        "id": uuid.uuid4(),
        "name": "Slack Webhook",
        "channel_type": NotificationChannelType.WEBHOOK,
        "config": {"url": "https://hooks.example.com/test", "headers": {}},
        "events": ["incident.opened", "incident.resolved"],
        "min_severity": None,
        "is_active": True,
    }
    defaults.update(kwargs)
    channel = MagicMock(spec=NotificationChannel)
    for k, v in defaults.items():
        setattr(channel, k, v)
    return channel


# ── Notification Payload Tests ────────────────────────────────────────


class TestBuildPayload:
    """Test NotificationService._build_payload."""

    def test_builds_correct_structure(self):
        incident = _make_incident()
        payload = NotificationService._build_payload("incident.opened", incident)

        assert payload["event"] == "incident.opened"
        assert "timestamp" in payload
        assert payload["incident"]["id"] == str(incident.id)
        assert payload["incident"]["title"] == incident.title
        assert payload["incident"]["severity"] == incident.severity.value
        assert payload["incident"]["status"] == incident.status.value
        assert payload["incident"]["failure_count"] == 1
        assert payload["incident"]["check_id"] == str(incident.check_id)

    def test_resolved_event(self):
        incident = _make_incident(status=IncidentStatus.RESOLVED)
        payload = NotificationService._build_payload("incident.resolved", incident)

        assert payload["event"] == "incident.resolved"
        assert payload["incident"]["status"] == "resolved"


# ── Severity Mapping Tests ────────────────────────────────────────────


class TestSeverityMapping:
    """Test _map_incident_severity helper."""

    def test_low_maps_to_warning(self):
        incident = _make_incident(severity=IncidentSeverity.LOW)
        assert _map_incident_severity(incident) == "warning"

    def test_medium_maps_to_error(self):
        incident = _make_incident(severity=IncidentSeverity.MEDIUM)
        assert _map_incident_severity(incident) == "error"

    def test_high_maps_to_fatal(self):
        incident = _make_incident(severity=IncidentSeverity.HIGH)
        assert _map_incident_severity(incident) == "fatal"

    def test_critical_maps_to_fatal(self):
        incident = _make_incident(severity=IncidentSeverity.CRITICAL)
        assert _map_incident_severity(incident) == "fatal"


# ── Dispatch Event Tests ──────────────────────────────────────────────


class TestDispatchEvent:
    """Test NotificationService.dispatch_event."""

    @pytest.mark.asyncio
    async def test_dispatch_no_channels_returns_zero(self):
        db = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = []
        db.execute = AsyncMock(return_value=result_mock)

        service = NotificationService(db)
        incident = _make_incident()
        sent = await service.dispatch_event("incident.opened", incident)

        assert sent == 0

    @pytest.mark.asyncio
    async def test_dispatch_sends_to_matching_channel(self):
        channel = _make_channel()

        db = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [channel]
        db.execute = AsyncMock(return_value=result_mock)

        service = NotificationService(db)
        incident = _make_incident()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch("dq_platform.services.notification_service.httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = mock_instance

            sent = await service.dispatch_event("incident.opened", incident)

        assert sent == 1
        mock_instance.post.assert_called_once()
        call_kwargs = mock_instance.post.call_args
        assert call_kwargs[1]["json"]["event"] == "incident.opened"

    @pytest.mark.asyncio
    async def test_dispatch_filters_by_event_type(self):
        # Channel only listens for incident.resolved, not opened
        channel = _make_channel(events=["incident.resolved"])

        db = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [channel]
        db.execute = AsyncMock(return_value=result_mock)

        service = NotificationService(db)
        incident = _make_incident()

        sent = await service.dispatch_event("incident.opened", incident)
        assert sent == 0

    @pytest.mark.asyncio
    async def test_dispatch_filters_by_min_severity(self):
        # Channel requires fatal severity, incident is medium (maps to "error")
        channel = _make_channel(min_severity="fatal")

        db = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [channel]
        db.execute = AsyncMock(return_value=result_mock)

        service = NotificationService(db)
        incident = _make_incident(severity=IncidentSeverity.MEDIUM)

        sent = await service.dispatch_event("incident.opened", incident)
        assert sent == 0

    @pytest.mark.asyncio
    async def test_dispatch_passes_severity_filter_when_high_enough(self):
        # Channel requires error, incident is high (maps to "fatal" >= "error")
        channel = _make_channel(min_severity="error")

        db = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [channel]
        db.execute = AsyncMock(return_value=result_mock)

        service = NotificationService(db)
        incident = _make_incident(severity=IncidentSeverity.HIGH)

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch("dq_platform.services.notification_service.httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = mock_instance

            sent = await service.dispatch_event("incident.opened", incident)

        assert sent == 1

    @pytest.mark.asyncio
    async def test_dispatch_failure_doesnt_raise(self):
        channel = _make_channel()

        db = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [channel]
        db.execute = AsyncMock(return_value=result_mock)

        service = NotificationService(db)
        incident = _make_incident()

        with patch("dq_platform.services.notification_service.httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(side_effect=Exception("Connection refused"))
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = mock_instance

            # Should not raise
            sent = await service.dispatch_event("incident.opened", incident)

        assert sent == 0

    @pytest.mark.asyncio
    async def test_dispatch_skips_channel_without_url(self):
        channel = _make_channel(config={"headers": {}})  # No url

        db = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = [channel]
        db.execute = AsyncMock(return_value=result_mock)

        service = NotificationService(db)
        incident = _make_incident()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch("dq_platform.services.notification_service.httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post = AsyncMock(return_value=mock_response)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = mock_instance

            sent = await service.dispatch_event("incident.opened", incident)

        assert sent == 0
        mock_instance.post.assert_not_called()


# ── Scheduler Task Tests ──────────────────────────────────────────────


class TestProcessScheduledChecks:
    """Test process_scheduled_checks task."""

    @pytest.mark.asyncio
    async def test_no_due_schedules_dispatches_zero(self):
        """When no schedules are due, nothing is dispatched."""
        from dq_platform.workers.tasks import _process_scheduled_checks_async

        mock_schedule_service = AsyncMock()
        mock_schedule_service.get_due_schedules = AsyncMock(return_value=[])

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.commit = AsyncMock()

        mock_factory = MagicMock(return_value=mock_session)

        with (
            patch("dq_platform.workers.tasks._create_task_session_factory", return_value=mock_factory),
            patch("dq_platform.workers.tasks.ScheduleService", return_value=mock_schedule_service),
        ):
            result = await _process_scheduled_checks_async()

        assert result["dispatched"] == 0
        assert result["schedule_ids"] == []

    @pytest.mark.asyncio
    async def test_due_schedules_creates_jobs_and_dispatches(self):
        """Due schedules result in job creation and execute_check.delay calls."""
        from dq_platform.workers.tasks import _process_scheduled_checks_async

        schedule1 = MagicMock()
        schedule1.id = uuid.uuid4()
        schedule1.check_id = uuid.uuid4()

        schedule2 = MagicMock()
        schedule2.id = uuid.uuid4()
        schedule2.check_id = uuid.uuid4()

        mock_schedule_service = AsyncMock()
        mock_schedule_service.get_due_schedules = AsyncMock(return_value=[schedule1, schedule2])
        mock_schedule_service.mark_executed = AsyncMock()

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()

        mock_factory = MagicMock(return_value=mock_session)

        with (
            patch("dq_platform.workers.tasks._create_task_session_factory", return_value=mock_factory),
            patch("dq_platform.workers.tasks.ScheduleService", return_value=mock_schedule_service),
            patch("dq_platform.workers.tasks.execute_check") as mock_execute,
        ):
            result = await _process_scheduled_checks_async()

        assert result["dispatched"] == 2
        assert len(result["schedule_ids"]) == 2
        assert mock_execute.delay.call_count == 2
        assert mock_schedule_service.mark_executed.call_count == 2
        assert mock_session.add.call_count == 2

    @pytest.mark.asyncio
    async def test_mark_executed_called_for_each_schedule(self):
        """mark_executed is called with the correct schedule ID."""
        from dq_platform.workers.tasks import _process_scheduled_checks_async

        schedule = MagicMock()
        schedule.id = uuid.uuid4()
        schedule.check_id = uuid.uuid4()

        mock_schedule_service = AsyncMock()
        mock_schedule_service.get_due_schedules = AsyncMock(return_value=[schedule])
        mock_schedule_service.mark_executed = AsyncMock()

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()

        mock_factory = MagicMock(return_value=mock_session)

        with (
            patch("dq_platform.workers.tasks._create_task_session_factory", return_value=mock_factory),
            patch("dq_platform.workers.tasks.ScheduleService", return_value=mock_schedule_service),
            patch("dq_platform.workers.tasks.execute_check"),
        ):
            await _process_scheduled_checks_async()

        mock_schedule_service.mark_executed.assert_called_once_with(schedule.id)


# ── Connector Registration Tests ──────────────────────────────────────


class TestConnectorRegistration:
    """Test that all connectors are properly registered."""

    def test_all_nine_connectors_registered(self):
        from dq_platform.connectors.factory import CONNECTOR_MAP
        from dq_platform.models.connection import ConnectionType

        expected = {
            ConnectionType.POSTGRESQL,
            ConnectionType.MYSQL,
            ConnectionType.SQLSERVER,
            ConnectionType.BIGQUERY,
            ConnectionType.SNOWFLAKE,
            ConnectionType.REDSHIFT,
            ConnectionType.DUCKDB,
            ConnectionType.ORACLE,
            ConnectionType.DATABRICKS,
        }
        assert set(CONNECTOR_MAP.keys()) == expected

    def test_connector_factory_lists_all_types(self):
        from dq_platform.connectors.factory import ConnectorFactory

        types = ConnectorFactory.list_supported_types()
        assert len(types) == 9
        assert "redshift" in types
        assert "duckdb" in types
        assert "oracle" in types
        assert "databricks" in types

    def test_connection_type_enum_has_nine_values(self):
        from dq_platform.models.connection import ConnectionType

        assert len(ConnectionType) == 9
        assert ConnectionType.REDSHIFT.value == "redshift"
        assert ConnectionType.DUCKDB.value == "duckdb"
        assert ConnectionType.ORACLE.value == "oracle"
        assert ConnectionType.DATABRICKS.value == "databricks"
