"""Tests for exception handling."""

import pytest
from fastapi.testclient import TestClient

from dq_platform.api.errors import (
    ConflictError,
    DQPlatformError,
    NotFoundError,
    ValidationError,
)


class TestErrorHandlers:
    """Test suite for error handlers."""

    def test_not_found_error_structure(self, sync_client: TestClient):
        """NotFoundError returns 404 with proper JSON structure."""
        from fastapi import APIRouter
        from dq_platform.main import app

        # Create a test route that raises NotFoundError
        test_router = APIRouter()

        @test_router.get("/test-not-found")
        async def test_not_found():
            raise NotFoundError("Resource", "123")

        app.include_router(test_router, prefix="/test-error")

        try:
            response = sync_client.get("/test-error/test-not-found")

            assert response.status_code == 404
            data = response.json()
            assert "error" in data
            assert "message" in data["error"]
            assert "type" in data["error"]
            assert "NotFoundError" in data["error"]["type"]
            assert "123" in data["error"]["message"]
            assert "Resource" in data["error"]["message"]
        finally:
            # Clean up - remove test router
            app.routes = [r for r in app.routes if not hasattr(r, "path") or "/test-error" not in r.path]

    def test_validation_error_structure(self, sync_client: TestClient):
        """ValidationError returns 422 with proper JSON structure."""
        from fastapi import APIRouter
        from dq_platform.main import app

        test_router = APIRouter()

        @test_router.get("/test-validation")
        async def test_validation():
            raise ValidationError("Invalid input data")

        app.include_router(test_router, prefix="/test-error")

        try:
            response = sync_client.get("/test-error/test-validation")

            assert response.status_code == 422
            data = response.json()
            assert "error" in data
            assert "message" in data["error"]
            assert "type" in data["error"]
            assert "ValidationError" in data["error"]["type"]
            assert "Invalid input" in data["error"]["message"]
        finally:
            app.routes = [r for r in app.routes if not hasattr(r, "path") or "/test-error" not in r.path]

    def test_conflict_error_structure(self, sync_client: TestClient):
        """ConflictError returns 409 with proper JSON structure."""
        from fastapi import APIRouter
        from dq_platform.main import app

        test_router = APIRouter()

        @test_router.get("/test-conflict")
        async def test_conflict():
            raise ConflictError("Resource already exists")

        app.include_router(test_router, prefix="/test-error")

        try:
            response = sync_client.get("/test-error/test-conflict")

            assert response.status_code == 409
            data = response.json()
            assert "error" in data
            assert "message" in data["error"]
            assert "type" in data["error"]
            assert "ConflictError" in data["error"]["type"]
            assert "already exists" in data["error"]["message"]
        finally:
            app.routes = [r for r in app.routes if not hasattr(r, "path") or "/test-error" not in r.path]

    def test_generic_exception_returns_500(self, sync_client: TestClient):
        """Generic exception returns 500 with sanitized message."""
        from fastapi import APIRouter
        from dq_platform.main import app

        test_router = APIRouter()

        @test_router.get("/test-generic-error")
        async def test_generic():
            raise ValueError("Internal secret details that should not be exposed")

        app.include_router(test_router, prefix="/test-error")

        try:
            response = sync_client.get("/test-error/test-generic-error")

            assert response.status_code == 500
            data = response.json()
            assert "error" in data
            assert "message" in data["error"]
            # Message should be sanitized, not expose internal details
            assert "Internal server error" in data["error"]["message"]
            assert "secret details" not in data["error"]["message"]
            assert data["error"]["type"] == "InternalError"
        finally:
            app.routes = [r for r in app.routes if not hasattr(r, "path") or "/test-error" not in r.path]

    def test_dqplatform_error_base_class(self):
        """DQPlatformError base class stores message and status code."""
        error = DQPlatformError("Test message", status_code=418)

        assert error.message == "Test message"
        assert error.status_code == 418
        assert str(error) == "Test message"

    def test_not_found_error_default_message(self):
        """NotFoundError formats default message correctly."""
        error = NotFoundError("Connection", "abc-123")

        assert "Connection" in error.message
        assert "abc-123" in error.message
        assert error.status_code == 404

    def test_validation_error_default_status(self):
        """ValidationError has correct default status code."""
        error = ValidationError("Field X is required")

        assert error.status_code == 422
        assert error.message == "Field X is required"

    def test_conflict_error_default_status(self):
        """ConflictError has correct default status code."""
        error = ConflictError("Duplicate entry")

        assert error.status_code == 409
        assert "Duplicate" in error.message

    def test_connection_error_structure(self, sync_client: TestClient):
        """ConnectionError returns 502 with proper structure."""
        from fastapi import APIRouter
        from dq_platform.api.errors import ConnectionError
        from dq_platform.main import app

        test_router = APIRouter()

        @test_router.get("/test-connection-error")
        async def test_connection():
            raise ConnectionError("Failed to connect to database")

        app.include_router(test_router, prefix="/test-error")

        try:
            response = sync_client.get("/test-error/test-connection-error")

            assert response.status_code == 502
            data = response.json()
            assert "error" in data
            assert "ConnectionError" in data["error"]["type"]
            assert "database" in data["error"]["message"]
        finally:
            app.routes = [r for r in app.routes if not hasattr(r, "path") or "/test-error" not in r.path]

    def test_execution_error_structure(self, sync_client: TestClient):
        """ExecutionError returns 500 with proper structure."""
        from fastapi import APIRouter
        from dq_platform.api.errors import ExecutionError
        from dq_platform.main import app

        test_router = APIRouter()

        @test_router.get("/test-execution-error")
        async def test_execution():
            raise ExecutionError("Check execution failed")

        app.include_router(test_router, prefix="/test-error")

        try:
            response = sync_client.get("/test-error/test-execution-error")

            assert response.status_code == 500
            data = response.json()
            assert "error" in data
            assert "ExecutionError" in data["error"]["type"]
            assert "execution failed" in data["error"]["message"]
        finally:
            app.routes = [r for r in app.routes if not hasattr(r, "path") or "/test-error" not in r.path]
