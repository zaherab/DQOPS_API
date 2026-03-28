"""Application configuration using pydantic-settings."""

import logging
from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Application
    app_name: str = "DQ Platform"
    debug: bool = False
    api_prefix: str = "/api/v1"

    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/dq_platform"
    database_pool_size: int = 5
    database_max_overflow: int = 10

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Celery
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/0"

    # Security
    encryption_key: str = ""  # Fernet key for credential encryption
    api_key_header: str = "X-API-Key"
    valid_api_keys: list[str] = []  # empty = dev mode (accept any non-empty key)

    # Rate limiting
    rate_limit_default: str = "100/minute"

    # CORS
    cors_allowed_origins: list[str] = ["*"]

    # Network security
    allow_private_network_connections: bool = False

    # Scheduling
    schedule_batch_size: int = 100

    # Execution
    check_execution_timeout: int = 300  # seconds
    max_concurrent_checks: int = 10

    @model_validator(mode="after")
    def _warn_insecure_defaults(self) -> "Settings":
        """Log warnings for insecure default configurations."""
        if not self.encryption_key:
            logger.warning(
                "SECURITY: encryption_key is empty — credentials are stored unencrypted. "
                "Set ENCRYPTION_KEY in production."
            )
        if not self.debug and not self.valid_api_keys:
            logger.warning(
                "SECURITY: valid_api_keys is empty outside debug mode — "
                "any non-empty API key will be accepted. Set VALID_API_KEYS in production."
            )
        if not self.debug and self.cors_allowed_origins == ["*"]:
            logger.warning(
                "SECURITY: CORS allows all origins ('*') outside debug mode. "
                "Set CORS_ALLOWED_ORIGINS to specific origins in production."
            )
        return self


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
