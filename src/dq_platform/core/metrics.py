"""Lightweight in-process metrics registry.

Deliberately tiny — no Prometheus dependency. A thread-safe dict of
counters plus a snapshot for the `/metrics` endpoint. Enough to drive
log-based or poll-based alerting; swap for a real metrics backend later
without touching call sites.

Usage:
    from dq_platform.core.metrics import metrics
    metrics.incr("sensor_transpile_failures")
    snapshot = metrics.snapshot()
"""

from __future__ import annotations

import threading


class _Metrics:
    """Process-local counter registry. One instance per worker/uvicorn proc.

    Counters are NOT shared across processes — each uvicorn worker and each
    Celery worker keeps its own. The `/metrics` endpoint reports the
    serving process's view; an aggregator (or per-pod scrape) sums them.
    For a single-binary deployment that is sufficient signal.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}

    def incr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + amount

    def get(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counters)

    def reset(self) -> None:
        """Test-only — clear all counters."""
        with self._lock:
            self._counters.clear()


metrics = _Metrics()


# Known metric names — referenced from one place so call sites can't drift.
SENSOR_TRANSPILE_FAILURES = "sensor_transpile_failures"
SENSOR_TRANSPILE_OK = "sensor_transpile_ok"
SENSOR_UNSUPPORTED_SKIPS = "sensor_unsupported_skips"
