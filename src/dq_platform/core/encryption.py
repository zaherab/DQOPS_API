"""Credential encryption using Fernet symmetric encryption."""

import json
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from dq_platform.config import get_settings


def get_fernet() -> Fernet:
    """Get Fernet instance with encryption key from settings.

    Returns:
        Fernet instance.

    Raises:
        ValueError: If encryption key is not configured.
    """
    settings = get_settings()
    if not settings.encryption_key:
        raise ValueError("ENCRYPTION_KEY environment variable is not set")

    return Fernet(settings.encryption_key.encode())


def encrypt_config(config: dict[str, Any]) -> dict[str, str]:
    """Encrypt sensitive fields in connection configuration.

    Encrypts the entire config dict as a JSON string and stores it
    under the 'encrypted_data' key.

    Args:
        config: Connection configuration with sensitive fields.

    Returns:
        Dict with encrypted data.
    """
    if not config:
        return {}

    fernet = get_fernet()
    config_json = json.dumps(config)
    encrypted = fernet.encrypt(config_json.encode())

    return {"encrypted_data": encrypted.decode()}


def decrypt_config(encrypted_config: dict[str, Any]) -> dict[str, Any]:
    """Decrypt connection configuration.

    Args:
        encrypted_config: Dict with 'encrypted_data' key containing encrypted config.

    Returns:
        Original configuration dict.

    Raises:
        InvalidToken: If decryption fails (wrong key or corrupted data).
    """
    if not encrypted_config:
        return {}

    encrypted_data = encrypted_config.get("encrypted_data")
    if not encrypted_data:
        # Return as-is if not encrypted (for backwards compatibility)
        return encrypted_config

    fernet = get_fernet()
    try:
        decrypted = fernet.decrypt(encrypted_data.encode())
        result: dict[str, Any] = json.loads(decrypted.decode())
        return result
    except InvalidToken:
        raise ValueError("Failed to decrypt configuration - invalid encryption key")


def generate_encryption_key() -> str:
    """Generate a new Fernet encryption key.

    Returns:
        Base64-encoded encryption key string.
    """
    return Fernet.generate_key().decode()
