"""Structured JSON logging configuration."""

import logging
import sys
from contextvars import ContextVar
from typing import Any

from pythonjsonlogger import jsonlogger

# Context variable to store request ID
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)


class RequestIdFilter(logging.Filter):
    """Add request_id to log records from context variable."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add request_id to log record."""
        request_id = request_id_var.get()
        record.request_id = request_id if request_id else "-"  # type: ignore[attr-defined]
        return True


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with request ID support."""

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s %(request_id)s",
            rename_fields={
                "asctime": "timestamp",
                "levelname": "level",
            },
        )

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        """Add custom fields to log record."""
        super().add_fields(log_record, record, message_dict)
        # Ensure request_id is included
        log_record.setdefault("request_id", getattr(record, "request_id", "-"))


def setup_logging(log_level: int = logging.INFO) -> None:
    """Configure structured JSON logging for the application.

    Args:
        log_level: The logging level to use (default: INFO).
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create stdout handler with JSON formatter
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(CustomJsonFormatter())
    stdout_handler.addFilter(RequestIdFilter())

    root_logger.addHandler(stdout_handler)

    # Set specific loggers to appropriate levels
    logging.getLogger("uvicorn").setLevel(log_level)
    logging.getLogger("uvicorn.access").setLevel(log_level)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
