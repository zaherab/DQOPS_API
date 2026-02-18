"""Unit tests for CheckService."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio

from dq_platform.api.errors import NotFoundError, ValidationError
from dq_platform.models.check import Check, CheckMode, CheckTimeScale, CheckType
from dq_platform.services.check_service import CheckService


class TestCheckService:
    """Test suite for CheckService."""

    @pytest_asyncio.fixture
    async def mock_db(self):
        """Create a mock database session."""
        db = AsyncMock()
        db.add = MagicMock()
        db.flush = AsyncMock()
        db.commit = AsyncMock()
        return db

    @pytest.fixture
    def service(self, mock_db):
        """Create a CheckService instance."""
        return CheckService(mock_db)

    async def test_create_check_success(self, service, mock_db):
        """Test create_check() creates a check successfully."""
        connection_id = uuid4()

        result = await service.create_check(
            name="test-check",
            connection_id=connection_id,
            check_type=CheckType.NULLS_PERCENT,
            target_table="users",
            target_column="email",
        )

        assert isinstance(result, Check)
        assert result.name == "test-check"
        assert result.check_type == CheckType.NULLS_PERCENT
        assert result.target_table == "users"
        assert result.target_column == "email"
        mock_db.add.assert_called_once()
        mock_db.commit.assert_called_once()

    async def test_create_check_column_level_validation(self, service, mock_db):
        """Test create_check() validates column-level checks require target_column."""
        connection_id = uuid4()

        # nulls_percent is a column-level check
        with pytest.raises(ValidationError) as exc_info:
            await service.create_check(
                name="test-check",
                connection_id=connection_id,
                check_type=CheckType.NULLS_PERCENT,
                target_table="users",
                target_column=None,  # Missing required column
            )

        assert "column" in str(exc_info.value).lower()
        assert "requires" in str(exc_info.value).lower()

    async def test_create_check_partitioned_requires_partition_column(self, service, mock_db):
        """Test create_check() validates partitioned mode requires partition_by_column."""
        connection_id = uuid4()

        with pytest.raises(ValidationError) as exc_info:
            await service.create_check(
                name="test-check",
                connection_id=connection_id,
                check_type=CheckType.ROW_COUNT,
                target_table="users",
                check_mode=CheckMode.PARTITIONED,
                partition_by_column=None,  # Missing required for partitioned
            )

        assert "partition" in str(exc_info.value).lower()

    async def test_create_check_with_all_fields(self, service, mock_db):
        """Test create_check() with all optional fields."""
        connection_id = uuid4()

        result = await service.create_check(
            name="full-check",
            description="A comprehensive check",
            connection_id=connection_id,
            check_type=CheckType.NULLS_PERCENT,
            target_schema="analytics",
            target_table="users",
            target_column="email",
            parameters={"max_percent": 5.0},
            metadata={"team": "data", "priority": "high"},
            check_mode=CheckMode.MONITORING,
            time_scale=CheckTimeScale.DAILY,
            rule_parameters={
                "warning": {"max_percent": 2.0},
                "error": {"max_percent": 5.0},
            },
        )

        assert result.description == "A comprehensive check"
        assert result.target_schema == "analytics"
        assert result.parameters == {"max_percent": 5.0}
        assert result.metadata_ == {"team": "data", "priority": "high"}
        assert result.check_mode == CheckMode.MONITORING
        assert result.time_scale == CheckTimeScale.DAILY
        assert result.rule_parameters == {
            "warning": {"max_percent": 2.0},
            "error": {"max_percent": 5.0},
        }

    async def test_get_check_success(self, service, mock_db):
        """Test get_check() returns check when found."""
        check_id = uuid4()
        mock_check = MagicMock(spec=Check)
        mock_check.id = check_id
        mock_check.is_active = True

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_check
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get_check(check_id)

        assert result == mock_check

    async def test_get_check_not_found(self, service, mock_db):
        """Test get_check() returns None when check doesn't exist."""
        check_id = uuid4()

        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service.get_check(check_id)

        assert result is None

    async def test_list_checks_with_filters(self, service, mock_db):
        """Test list_checks() with various filters."""
        connection_id = uuid4()
        mock_checks = [MagicMock(spec=Check) for _ in range(3)]

        mock_count_result = MagicMock()
        mock_count_result.all.return_value = [(i,) for i in range(3)]

        mock_data_result = MagicMock()
        mock_data_result.scalars.return_value.all.return_value = mock_checks

        mock_db.execute = AsyncMock(side_effect=[mock_count_result, mock_data_result])

        checks, total = await service.list_checks(
            connection_id=connection_id,
            check_type=CheckType.NULLS_PERCENT,
            check_mode=CheckMode.MONITORING,
            target_table="users",
            is_active=True,
            offset=0,
            limit=10,
        )

        assert checks == mock_checks
        assert total == 3

    async def test_update_check_success(self, service, mock_db):
        """Test update_check() updates fields correctly."""
        check_id = uuid4()
        mock_check = MagicMock(spec=Check)
        mock_check.name = "old-name"
        mock_check.description = "old-description"
        mock_check.parameters = {"old": "params"}

        with patch.object(service, "get_check", AsyncMock(return_value=mock_check)):
            result = await service.update_check(
                check_id=check_id,
                name="new-name",
                description="new-description",
                parameters={"new": "params"},
            )

        assert result.name == "new-name"
        assert result.description == "new-description"
        assert result.parameters == {"new": "params"}
        mock_db.flush.assert_called_once()

    async def test_update_check_partial(self, service, mock_db):
        """Test update_check() with partial fields only updates provided fields."""
        check_id = uuid4()
        mock_check = MagicMock(spec=Check)
        mock_check.name = "old-name"
        mock_check.description = "old-description"
        mock_check.target_table = "old-table"

        with patch.object(service, "get_check", AsyncMock(return_value=mock_check)):
            result = await service.update_check(
                check_id=check_id,
                name="new-name",
                # description and target_table not provided
            )

        assert result.name == "new-name"
        # These should remain unchanged since we didn't pass them
        assert result.description == "old-description"
        assert result.target_table == "old-table"

    async def test_update_check_not_found(self, service, mock_db):
        """Test update_check() returns None when check not found."""
        check_id = uuid4()

        with patch.object(service, "get_check", AsyncMock(return_value=None)):
            result = await service.update_check(check_id=check_id, name="new-name")

        assert result is None

    async def test_delete_check_success(self, service, mock_db):
        """Test delete_check() performs soft delete."""
        check_id = uuid4()
        mock_check = MagicMock(spec=Check)
        mock_check.is_active = True

        with patch.object(service, "get_check", AsyncMock(return_value=mock_check)):
            result = await service.delete_check(check_id)

        assert result is True
        assert mock_check.is_active is False
        mock_db.flush.assert_called_once()

    async def test_delete_check_not_found(self, service, mock_db):
        """Test delete_check() returns False when check not found."""
        check_id = uuid4()

        with patch.object(service, "get_check", AsyncMock(return_value=None)):
            result = await service.delete_check(check_id)

        assert result is False

    async def test_preview_check_not_found(self, service, mock_db):
        """Test preview_check() raises NotFoundError when check doesn't exist."""
        check_id = uuid4()

        with patch.object(service, "get_check", AsyncMock(return_value=None)):
            with pytest.raises(NotFoundError):
                await service.preview_check(check_id)

    async def test_preview_check_config_connection_not_found(self, service, mock_db):
        """Test preview_check_config() raises NotFoundError when connection doesn't exist."""
        connection_id = uuid4()

        with patch(
            "dq_platform.services.check_service.ConnectionService"
        ) as mock_conn_service_class:
            mock_conn_service = MagicMock()
            mock_conn_service.get_connection = AsyncMock(return_value=None)
            mock_conn_service_class.return_value = mock_conn_service

            with pytest.raises(NotFoundError):
                await service.preview_check_config(
                    connection_id=connection_id,
                    check_type=CheckType.NULLS_PERCENT,
                    target_table="users",
                )

    async def test_get_historical_values(self, service, mock_db):
        """Test _get_historical_values() returns sensor values."""
        check_id = uuid4()
        mock_check = MagicMock(spec=Check)
        mock_check.id = check_id

        # Mock historical results
        mock_result = MagicMock()
        mock_result.all.return_value = [(10.5,), (20.3,), (15.0,)]
        mock_db.execute = AsyncMock(return_value=mock_result)

        result = await service._get_historical_values(mock_check, days=30)

        assert result == [10.5, 20.3, 15.0]
