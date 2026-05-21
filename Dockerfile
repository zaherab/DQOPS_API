# Multi-stage Dockerfile for DQ Platform
# Stage 1: Builder - Install dependencies
FROM python:3.12.7-slim-bookworm AS builder

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
FROM python:3.12.7-slim-bookworm AS runtime

# Install runtime system dependencies only (no compilers).
# msodbcsql18 is the actual SQL Server ODBC driver — unixodbc alone is
# only the driver *manager* and pyodbc fails with "Can't open lib" without
# the vendor driver. Pulled from Microsoft's apt repo.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    unixodbc \
    curl \
    gnupg \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
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

# Shared-data mountpoint, owned by appuser so file-based connectors
# (DuckDB) can read/write when this path is backed by a named volume.
RUN mkdir -p /shared && chown appuser:appuser /shared

# Copy application code
COPY --chown=appuser:appuser src/ /app/src/
COPY --chown=appuser:appuser alembic.ini /app/

# Set working directory
WORKDIR /app

# Switch to non-root user
USER appuser

EXPOSE 8000

# Default command: run the API server
CMD ["uvicorn", "dq_platform.main:app", "--host", "0.0.0.0", "--port", "8000"]
