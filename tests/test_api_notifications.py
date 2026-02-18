"""Tests for notification API endpoints."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from dq_platform.models.notification import NotificationChannel


class TestNotificationAPI:
    """Test suite for notification endpoints."""

    @pytest.fixture
    async def channel(self, db_session):
        """Create a test notification channel."""
        channel = NotificationChannel(
            name="test-webhook",
            description="Test webhook channel",
            channel_type="webhook",
            config={
                "url": "https://hooks.example.com/webhook",
                "headers": {"Authorization": "Bearer test"},
            },
            events=["incident.opened", "incident.resolved"],
            min_severity="warning",
        )
        db_session.add(channel)
        await db_session.flush()
        return channel

    def test_create_channel_success(self, sync_client: TestClient):
        """POST /notifications/channels - Create channel returns 201."""
        response = sync_client.post(
            "/api/v1/notifications/channels",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "new-webhook",
                "description": "A new webhook channel",
                "channel_type": "webhook",
                "config": {
                    "url": "https://hooks.example.com/webhook",
                    "headers": {"Authorization": "Bearer token"},
                },
                "events": ["incident.opened"],
                "min_severity": "error",
            },
        )

        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "new-webhook"
        assert data["channel_type"] == "webhook"
        assert data["config"]["url"] == "https://hooks.example.com/webhook"
        assert data["is_active"] is True

    def test_list_channels(self, sync_client: TestClient, channel):
        """GET /notifications/channels - List channels returns 200."""
        response = sync_client.get(
            "/api/v1/notifications/channels",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert data["total"] >= 1

    def test_list_channels_with_active_filter(self, sync_client: TestClient, channel):
        """GET /notifications/channels - Filter by is_active works."""
        response = sync_client.get(
            "/api/v1/notifications/channels?is_active=true",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert all(item["is_active"] is True for item in data["items"])

    def test_get_channel_success(self, sync_client: TestClient, channel):
        """GET /notifications/channels/{id} - Get channel returns 200."""
        channel_id = str(channel.id)
        response = sync_client.get(
            f"/api/v1/notifications/channels/{channel_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == channel_id
        assert data["name"] == "test-webhook"
        assert data["channel_type"] == "webhook"

    def test_get_channel_not_found(self, sync_client: TestClient):
        """GET /notifications/channels/{id} - Get non-existent channel returns 404."""
        fake_id = str(uuid4())
        response = sync_client.get(
            f"/api/v1/notifications/channels/{fake_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404

    def test_update_channel_success(self, sync_client: TestClient, channel):
        """PATCH /notifications/channels/{id} - Update channel returns 200."""
        channel_id = str(channel.id)
        response = sync_client.patch(
            f"/api/v1/notifications/channels/{channel_id}",
            headers={"X-API-Key": "test-key"},
            json={
                "name": "updated-webhook",
                "description": "Updated description",
                "events": ["incident.opened", "incident.resolved", "incident.acknowledged"],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "updated-webhook"
        assert data["description"] == "Updated description"

    def test_update_channel_not_found(self, sync_client: TestClient):
        """PATCH /notifications/channels/{id} - Update non-existent channel returns 404."""
        fake_id = str(uuid4())
        response = sync_client.patch(
            f"/api/v1/notifications/channels/{fake_id}",
            headers={"X-API-Key": "test-key"},
            json={"name": "updated-name"},
        )

        assert response.status_code == 404

    def test_delete_channel_success(self, sync_client: TestClient, channel):
        """DELETE /notifications/channels/{id} - Delete channel returns 204."""
        channel_id = str(channel.id)
        response = sync_client.delete(
            f"/api/v1/notifications/channels/{channel_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 204

        # Verify it's deleted
        get_response = sync_client.get(
            f"/api/v1/notifications/channels/{channel_id}",
            headers={"X-API-Key": "test-key"},
        )
        assert get_response.status_code == 404

    def test_delete_channel_not_found(self, sync_client: TestClient):
        """DELETE /notifications/channels/{id} - Delete non-existent channel returns 404."""
        fake_id = str(uuid4())
        response = sync_client.delete(
            f"/api/v1/notifications/channels/{fake_id}",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 404

    def test_test_channel_success(self, sync_client: TestClient, channel):
        """POST /notifications/channels/{id}/test - Test webhook returns 200."""
        channel_id = str(channel.id)

        with patch(
            "dq_platform.services.notification_service.httpx.AsyncClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status.return_value = None
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__ = MagicMock(return_value=mock_client)
            mock_client_class.return_value.__aexit__ = MagicMock(return_value=False)

            response = sync_client.post(
                f"/api/v1/notifications/channels/{channel_id}/test",
                headers={"X-API-Key": "test-key"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "status_code" in data

    def test_test_channel_not_found(self, sync_client: TestClient):
        """POST /notifications/channels/{id}/test - Test non-existent channel returns error."""
        fake_id = str(uuid4())
        response = sync_client.post(
            f"/api/v1/notifications/channels/{fake_id}/test",
            headers={"X-API-Key": "test-key"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "not found" in data["error"].lower()

    def test_test_channel_failure(self, sync_client: TestClient, channel):
        """POST /notifications/channels/{id}/test - Failed webhook returns error info."""
        channel_id = str(channel.id)

        with patch(
            "dq_platform.services.notification_service.httpx.AsyncClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.post.side_effect = Exception("Connection timeout")
            mock_client_class.return_value.__aenter__ = MagicMock(return_value=mock_client)
            mock_client_class.return_value.__aexit__ = MagicMock(return_value=False)

            response = sync_client.post(
                f"/api/v1/notifications/channels/{channel_id}/test",
                headers={"X-API-Key": "test-key"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "timeout" in data["error"].lower()
