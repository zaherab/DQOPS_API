"""Celery application configuration."""

from celery import Celery

from dq_platform.config import get_settings

settings = get_settings()

celery_app = Celery(
    "dq_platform",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["dq_platform.workers.tasks"],
)

# Celery configuration
celery_app.conf.update(
    # Task serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    # Timezone
    timezone="UTC",
    enable_utc=True,
    # Task tracking
    task_track_started=True,
    task_time_limit=settings.check_execution_timeout,
    task_soft_time_limit=settings.check_execution_timeout - 30,
    # Reliability
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Concurrency
    worker_concurrency=settings.max_concurrent_checks,
    worker_prefetch_multiplier=1,
    # Result backend
    result_expires=3600,  # 1 hour
    # Beat schedule for processing scheduled checks
    beat_schedule={
        "process-scheduled-checks": {
            "task": "dq_platform.workers.tasks.process_scheduled_checks",
            "schedule": 60.0,  # Every 60 seconds
        },
    },
)
