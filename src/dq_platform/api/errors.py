"""API error handlers."""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse


class DQPlatformError(Exception):
    """Base exception for DQ Platform."""

    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class NotFoundError(DQPlatformError):
    """Resource not found."""

    def __init__(self, resource: str, resource_id: str):
        super().__init__(
            f"{resource} with id '{resource_id}' not found",
            status_code=status.HTTP_404_NOT_FOUND,
        )


class ValidationError(DQPlatformError):
    """Validation error."""

    def __init__(self, message: str):
        super().__init__(message, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


class ConflictError(DQPlatformError):
    """Conflict error."""

    def __init__(self, message: str):
        super().__init__(message, status_code=status.HTTP_409_CONFLICT)


class ConnectionError(DQPlatformError):
    """Connection error."""

    def __init__(self, message: str):
        super().__init__(message, status_code=status.HTTP_502_BAD_GATEWAY)


class ExecutionError(DQPlatformError):
    """Check execution error."""

    def __init__(self, message: str):
        super().__init__(message, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


def register_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers for the application."""

    @app.exception_handler(DQPlatformError)
    async def dq_platform_error_handler(
        request: Request, exc: DQPlatformError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "message": exc.message,
                    "type": type(exc).__name__,
                }
            },
        )

    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": {
                    "message": "Internal server error",
                    "type": "InternalError",
                }
            },
        )
