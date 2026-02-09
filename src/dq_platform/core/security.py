"""API security and authentication."""

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from dq_platform.config import get_settings

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

    This is a placeholder implementation. In production, you would:
    1. Look up the API key in a database
    2. Verify it's not expired or revoked
    3. Return the associated user/tenant information

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

    # TODO: In production, validate against stored API keys
    # For now, accept any non-empty key
    return api_key


# Optional: Dependency for routes that don't require auth
async def optional_api_key(
    api_key: str | None = Depends(get_api_key),
) -> str | None:
    """Optional API key - returns None if not provided."""
    return api_key
