"""Celery workers module."""

from dq_platform.workers.celery_app import celery_app

__all__ = ["celery_app"]
