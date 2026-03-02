# Multi-stage Dockerfile for DQ Platform
# Stage 1: Builder - Install dependencies
FROM python:3.14-slim AS builder

# Install build-time system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    unixodbc-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
# Copy pyproject.toml and README.md first for better layer caching
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir -e .

# Stage 2: Runtime - Minimal image with application code
FROM python:3.14-slim AS runtime

# Install runtime system dependencies only (no compilers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    unixodbc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -u 1000 appuser

# Copy application code
COPY --chown=appuser:appuser src/ /app/src/
COPY --chown=appuser:appuser alembic.ini /app/

# Set working directory
WORKDIR /app

# Switch to non-root user
USER appuser

# Default command: run the API server
CMD ["uvicorn", "dq_platform.main:app", "--host", "0.0.0.0", "--port", "8000"]
