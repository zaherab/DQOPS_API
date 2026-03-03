"""API security and authentication."""

import logging

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from dq_platform.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

api_key_header = APIKeyHeader(name=settings.api_key_header, auto_error=False)


async def get_api_key(
    api_key: str | None = Security(api_key_header),
) -> str | None:
    """Extract API key from request header.

    Args:
        api_key: API key from header.

    Returns:
        API key if present, None otherwise.
    """
    return api_key


async def verify_api_key(
    api_key: str | None = Depends(get_api_key),
) -> str:
    """Verify API key is present and valid.

    When ``settings.valid_api_keys`` is empty (dev mode), any non-empty key
    is accepted. When populated, the key must match one of the configured
    values.

    Args:
        api_key: API key from header.

    Returns:
        Validated API key.

    Raises:
        HTTPException: If API key is missing or invalid.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    if settings.valid_api_keys and api_key not in settings.valid_api_keys:
        logger.warning("Rejected invalid API key: %s...", api_key[:8])
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key",
        )

    return api_key


# Optional: Dependency for routes that don't require auth
async def optional_api_key(
    api_key: str | None = Depends(get_api_key),
) -> str | None:
    """Optional API key - returns None if not provided."""
    return api_key
