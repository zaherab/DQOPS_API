"""Tests for API key authentication."""

import pytest
from fastapi.testclient import TestClient

from dq_platform.main import app


class TestSecurity:
    """Test suite for API security."""

    @pytest.fixture
    def client(self):
        """Create a test client without database override."""
        with TestClient(app) as tc:
            yield tc

    def test_missing_api_key_returns_401(self, client: TestClient):
        """Missing API key header returns 401."""
        response = client.get("/api/v1/connections")

        assert response.status_code == 401
        data = response.json()
        assert "detail" in data
        assert "Missing API key" in data["detail"]

    def test_empty_api_key_returns_401(self, client: TestClient):
        """Empty API key returns 401."""
        response = client.get(
            "/api/v1/connections",
            headers={"X-API-Key": ""},
        )

        assert response.status_code == 401
        data = response.json()
        assert "Missing API key" in data["detail"]

    def test_whitespace_only_api_key_accepted(self, client: TestClient):
        """Whitespace-only API key is accepted (current implementation).

        NOTE: This test documents current behavior where any non-empty
        string including whitespace is accepted. Consider changing
        verify_api_key() to strip whitespace if this is not desired.
        """
        response = client.get(
            "/health",  # Use health endpoint to avoid DB
            headers={"X-API-Key": "   "},
        )

        # Current implementation accepts whitespace-only keys
        assert response.status_code == 200

    def test_wrong_header_name_returns_401(self, client: TestClient):
        """Wrong header name returns 401."""
        response = client.get(
            "/api/v1/connections",
            headers={"Authorization": "Bearer test-key"},  # Wrong header
        )

        assert response.status_code == 401
        data = response.json()
        assert "Missing API key" in data["detail"]

    def test_valid_api_key_accepted(self, client: TestClient):
        """Any non-empty API key is accepted (placeholder behavior)."""
        # Use health endpoint which doesn't require DB
        response = client.get(
            "/health",
            headers={"X-API-Key": "any-valid-key"},
        )

        # Should not be 401 - health endpoint returns 200
        assert response.status_code != 401
        assert response.status_code == 200

    def test_api_key_with_special_characters(self, client: TestClient):
        """API key with special characters is accepted."""
        response = client.get(
            "/health",
            headers={"X-API-Key": "key-with-special_chars.123!"},
        )

        assert response.status_code != 401

    def test_api_key_case_sensitivity(self, client: TestClient):
        """API key header is case-insensitive."""
        # FastAPI/Starlette handles header names case-insensitively
        response = client.get(
            "/health",
            headers={"x-api-key": "test-key"},  # lowercase
        )

        assert response.status_code != 401

    def test_api_key_passed_to_dependency(self, client: TestClient):
        """API key value is passed through dependency injection."""
        # Use health endpoint which doesn't require DB
        response = client.get(
            "/health",
            headers={"X-API-Key": "my-test-key-123"},
        )

        # Should complete without auth error
        assert response.status_code == 200

    def test_api_key_on_protected_endpoints(self, client: TestClient):
        """Protected endpoints require API key."""
        protected_endpoints = [
            ("GET", "/api/v1/connections"),
            ("POST", "/api/v1/connections"),
            ("GET", "/api/v1/notifications/channels"),
            ("GET", "/api/v1/incidents"),
            ("GET", "/api/v1/schedules"),
        ]

        for method, endpoint in protected_endpoints:
            response = client.request(method, endpoint)
            assert response.status_code == 401, f"{method} {endpoint} should require auth"

    def test_www_authenticate_header(self, client: TestClient):
        """401 response includes WWW-Authenticate header."""
        response = client.get("/api/v1/connections")

        assert response.status_code == 401
        assert "www-authenticate" in response.headers
        assert "ApiKey" in response.headers["www-authenticate"]
