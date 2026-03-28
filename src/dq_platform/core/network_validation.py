"""Network validation utilities to prevent SSRF attacks."""

import ipaddress
import socket
from urllib.parse import urlparse


def validate_host(host: str, allow_private: bool = False) -> None:
    """Validate that a hostname does not resolve to a private/reserved IP.

    Args:
        host: Hostname or IP address to validate.
        allow_private: If True, skip private network checks (for dev/testing).

    Raises:
        ValueError: If the host resolves to a disallowed address.
    """
    if allow_private:
        return

    try:
        # Try parsing as IP address directly first
        try:
            addr = ipaddress.ip_address(host)
            _check_ip(addr)
            return
        except ValueError:
            pass

        # Resolve hostname to IP addresses
        results = socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
        if not results:
            raise ValueError(f"Could not resolve hostname: {host}")

        for family, _, _, _, sockaddr in results:
            ip_str = sockaddr[0]
            addr = ipaddress.ip_address(ip_str)
            _check_ip(addr)

    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Failed to validate host {host!r}: {e}") from e


def validate_url(url: str, allow_private: bool = False) -> None:
    """Validate that a URL does not point to a private/reserved network.

    Args:
        url: URL to validate.
        allow_private: If True, skip private network checks.

    Raises:
        ValueError: If the URL scheme is invalid or host is disallowed.
    """
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"URL scheme must be http or https, got: {parsed.scheme!r}")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError(f"URL has no hostname: {url!r}")

    validate_host(hostname, allow_private=allow_private)


def _check_ip(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> None:
    """Check if an IP address is private, reserved, or otherwise disallowed.

    Raises:
        ValueError: If the IP is in a disallowed range.
    """
    if addr.is_loopback:
        raise ValueError(f"Loopback addresses are not allowed: {addr}")
    if addr.is_private:
        raise ValueError(f"Private network addresses are not allowed: {addr}")
    if addr.is_reserved:
        raise ValueError(f"Reserved addresses are not allowed: {addr}")
    if addr.is_link_local:
        raise ValueError(f"Link-local addresses are not allowed: {addr}")
    if addr.is_multicast:
        raise ValueError(f"Multicast addresses are not allowed: {addr}")

    # Block cloud metadata endpoints (169.254.169.254)
    if isinstance(addr, ipaddress.IPv4Address) and str(addr) == "169.254.169.254":
        raise ValueError(f"Cloud metadata endpoint is not allowed: {addr}")
