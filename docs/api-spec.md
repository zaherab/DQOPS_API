# API Specification

REST API endpoints for DQ Platform. All endpoints return JSON and require authentication via API key.

## Authentication

Include API key in the `X-API-Key` header:

```
X-API-Key: dqp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

## Base URL

```
http://localhost:8000/api/v1
```

## Health Check

```http
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

---

## Connections

Manage data source connections.

### List Connections

```http
GET /api/v1/connections
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `connection_type` | string | Filter by type (postgresql, mysql, etc.) |
| `limit` | integer | Max results (default: 100, max: 1000) |
| `offset` | integer | Pagination offset |

**Response:**
```json
{
  "items": [
    {
      "id": "uuid",
      "name": "production-db",
      "connection_type": "postgresql",
      "description": "Production analytics database",
      "is_active": true,
      "created_at": "2024-01-15T10:00:00Z",
      "updated_at": "2024-01-15T10:00:00Z"
    }
  ],
  "total": 10,
  "limit": 100,
  "offset": 0
}
```

### Create Connection

```http
POST /api/v1/connections
```

**Request Body:**
```json
{
  "name": "production-db",
  "connection_type": "postgresql",
  "config": {
    "host": "db.example.com",
    "port": 5432,
    "database": "analytics",
    "user": "readonly",
    "password": "secret",
    "ssl_mode": "require"
  },
  "description": "Production analytics database"
}
```

**Supported Connection Types:**
- `postgresql` - PostgreSQL, Aurora, RDS, Cloud SQL
- `mysql` - MySQL, MariaDB
- `sqlserver` - SQL Server, Azure SQL
- `bigquery` - Google BigQuery
- `snowflake` - Snowflake
- `redshift` - Amazon Redshift
- `databricks` - Databricks
- `duckdb` - DuckDB
- `oracle` - Oracle Database

**Response:** `201 Created`
```json
{
  "id": "uuid",
  "name": "production-db",
  "connection_type": "postgresql",
  "description": "Production analytics database",
  "is_active": true,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

### Get Connection

```http
GET /api/v1/connections/{connection_id}
```

### Update Connection

```http
PUT /api/v1/connections/{connection_id}
```

**Request Body:**
```json
{
  "name": "production-db-v2",
  "config": {
    "host": "new-db.example.com",
    "port": 5432,
    "database": "analytics",
    "user": "readonly",
    "password": "new-secret"
  }
}
```

### Delete Connection

```http
DELETE /api/v1/connections/{connection_id}
```

**Response:** `204 No Content`

### Test Connection

```http
POST /api/v1/connections/{connection_id}/test
```

**Response:**
```json
{
  "success": true,
  "message": "Connection successful",
  "latency_ms": 45
}
```

### Get Schemas

```http
GET /api/v1/connections/{connection_id}/schemas
```

**Response:**
```json
{
  "items": [
    {"name": "public", "table_count": 42},
    {"name": "analytics", "table_count": 15}
  ],
  "total": 2,
  "limit": 100,
  "offset": 0
}
```

### Get Tables

```http
GET /api/v1/connections/{connection_id}/schemas/{schema_name}/tables
```

**Response:**
```json
{
  "items": [
    {
      "name": "orders",
      "schema": "public",
      "columns": [
        {"name": "id", "type": "bigint", "nullable": false},
        {"name": "customer_id", "type": "bigint", "nullable": false},
        {"name": "total", "type": "numeric(10,2)", "nullable": false},
        {"name": "created_at", "type": "timestamp", "nullable": false}
      ]
    }
  ],
  "total": 1,
  "limit": 100,
  "offset": 0
}
```

---

## Checks

Define and manage data quality checks.

### List Checks

```http
GET /api/v1/checks
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `connection_id` | uuid | Filter by connection |
| `target_table` | string | Filter by table name |
| `check_type` | string | Filter by check type |
| `check_mode` | string | Filter by mode (profiling, monitoring, partitioned) |
| `is_active` | boolean | Filter by active status |
| `limit` | integer | Max results (default: 100, max: 1000) |
| `offset` | integer | Pagination offset |

### Create Check

```http
POST /api/v1/checks
```

**Request Body (Table-level Check):**
```json
{
  "name": "orders_row_count",
  "connection_id": "uuid",
  "check_type": "row_count",
  "check_mode": "monitoring",
  "time_scale": "daily",
  "target_schema": "public",
  "target_table": "orders",
  "rule_parameters": {
    "error": {"min_count": 1000}
  },
  "is_active": true
}
```

**Request Body (Column-level Check):**
```json
{
  "name": "email_null_check",
  "connection_id": "uuid",
  "check_type": "nulls_percent",
  "check_mode": "monitoring",
  "time_scale": "daily",
  "target_schema": "public",
  "target_table": "users",
  "target_column": "email",
  "rule_parameters": {
    "warning": {"max_percent": 5.0},
    "error": {"max_percent": 10.0}
  }
}
```

**Request Body (Partitioned Check):**
```json
{
  "name": "daily_partition_nulls",
  "connection_id": "uuid",
  "check_type": "nulls_percent",
  "check_mode": "partitioned",
  "time_scale": "daily",
  "target_schema": "public",
  "target_table": "events",
  "target_column": "user_id",
  "partition_by_column": "created_at",
  "rule_parameters": {
    "error": {"max_percent": 1.0}
  }
}
```

### Get Check

```http
GET /api/v1/checks/{check_id}
```

### Update Check

```http
PATCH /api/v1/checks/{check_id}
```

**Request Body:** (any subset of check fields)
```json
{
  "name": "updated-check-name",
  "is_active": false,
  "rule_parameters": {
    "warning": {"max_percent": 3.0},
    "error": {"max_percent": 8.0}
  }
}
```

### Delete Check

```http
DELETE /api/v1/checks/{check_id}
```

**Response:** `204 No Content`

### Run Check

Execute a check asynchronously.

```http
POST /api/v1/checks/{check_id}/run
```

**Request Body:** (optional)
```json
{
  "triggered_by": "api"
}
```

**Response:**
```json
{
  "job_id": "uuid",
  "task_id": "celery-task-id",
  "status": "pending",
  "message": "Check execution started"
}
```

### Preview Check

Run a check synchronously without saving results (dry run).

```http
POST /api/v1/checks/{check_id}/preview
```

**Response:**
```json
{
  "check_id": "uuid",
  "check_name": "email_null_check",
  "check_type": "nulls_percent",
  "severity": "error",
  "passed": false,
  "sensor_value": 15.5,
  "expected": "<= 10.0%",
  "actual": "15.5%",
  "message": "Null percentage exceeds threshold",
  "executed_sql": "SELECT (COUNT(*) FILTER (WHERE email IS NULL) * 100.0 / COUNT(*)) FROM public.users",
  "executed_at": "2024-01-15T12:00:00Z"
}
```

### Batch Run Checks

Run multiple checks asynchronously.

```http
POST /api/v1/checks/batch/run
```

**Request Body:**
```json
{
  "check_ids": ["uuid1", "uuid2", "uuid3"],
  "triggered_by": "api"
}
```

**Response:**
```json
[
  {
    "check_id": "uuid1",
    "job_id": "uuid",
    "task_id": "celery-task-id",
    "status": "started"
  },
  {
    "check_id": "uuid2",
    "status": "error",
    "message": "Check not found"
  }
]
```

### Validate/Preview Check Config

Preview a check configuration without saving it.

```http
POST /api/v1/checks/validate/preview
```

**Request Body:**
```json
{
  "connection_id": "uuid",
  "check_type": "nulls_percent",
  "target_schema": "public",
  "target_table": "users",
  "target_column": "email",
  "rule_parameters": {
    "error": {"max_percent": 5.0}
  }
}
```

### List Check Types

```http
GET /api/v1/checks/types
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `category` | string | Filter by category (volume, nulls, uniqueness, etc.) |
| `column_level_only` | boolean | Only column-level checks |
| `table_level_only` | boolean | Only table-level checks |

**Response:**
```json
[
  {
    "type": "nulls_percent",
    "description": "Percentage of null values in column",
    "is_column_level": true,
    "category": "nulls"
  },
  {
    "type": "row_count",
    "description": "Count of rows in table",
    "is_column_level": false,
    "category": "volume"
  }
]
```

### List Check Categories

```http
GET /api/v1/checks/categories
```

**Response:** `["volume", "nulls", "uniqueness", "numeric", "text", "patterns", ...]`

### List Check Modes

```http
GET /api/v1/checks/modes
```

**Response:** `["profiling", "monitoring", "partitioned"]`

### List Time Scales

```http
GET /api/v1/checks/time-scales
```

**Response:** `["daily", "monthly"]`

---

## Jobs

Track check execution jobs.

### List Jobs

```http
GET /api/v1/jobs
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `check_id` | uuid | Filter by check |
| `status` | string | Filter: pending, running, completed, failed, cancelled |
| `limit` | integer | Max results (default: 100) |
| `offset` | integer | Pagination offset |

**Response:**
```json
{
  "items": [
    {
      "id": "uuid",
      "check_id": "uuid",
      "status": "completed",
      "triggered_by": "api",
      "created_at": "2024-01-15T12:00:00Z",
      "started_at": "2024-01-15T12:00:01Z",
      "completed_at": "2024-01-15T12:00:03Z",
      "result": {
        "passed": true,
        "sensor_value": 1523456
      }
    }
  ],
  "total": 50,
  "limit": 100,
  "offset": 0
}
```

### Get Job

```http
GET /api/v1/jobs/{job_id}
```

### Cancel Job

```http
POST /api/v1/jobs/{job_id}/cancel
```

---

## Results

Query check execution results.

### Query Results

```http
GET /api/v1/results
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `check_id` | uuid | Filter by check |
| `connection_id` | uuid | Filter by connection |
| `passed` | boolean | Filter by pass/fail |
| `from_date` | datetime | Results after this time (ISO 8601) |
| `to_date` | datetime | Results before this time (ISO 8601) |
| `limit` | integer | Max results (default: 100, max: 1000) |
| `offset` | integer | Pagination offset |

**Response:**
```json
{
  "items": [
    {
      "id": "uuid",
      "check_id": "uuid",
      "check_name": "orders_row_count",
      "passed": true,
      "sensor_value": 1523456,
      "severity": null,
      "executed_at": "2024-01-15T12:00:00Z",
      "execution_time_ms": 1845
    }
  ],
  "total": 500,
  "limit": 100,
  "offset": 0
}
```

### Get Results Summary

Aggregated statistics for check results.

```http
GET /api/v1/results/summary
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `check_id` | uuid | Filter by check |
| `connection_id` | uuid | Filter by connection |
| `from_date` | datetime | Results after this time |
| `to_date` | datetime | Results before this time |

**Response:**
```json
{
  "total_checks": 10,
  "total_executions": 168,
  "passed": 165,
  "failed": 3,
  "pass_rate": 0.982,
  "by_severity": {
    "warning": 2,
    "error": 1,
    "fatal": 0
  }
}
```

---

## Incidents

Manage data quality incidents.

### List Incidents

```http
GET /api/v1/incidents
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `status` | string | Filter: open, acknowledged, resolved |
| `severity` | string | Filter: warning, error, fatal |
| `check_id` | uuid | Filter by check |
| `limit` | integer | Max results (default: 100) |
| `offset` | integer | Pagination offset |

**Response:**
```json
{
  "items": [
    {
      "id": "uuid",
      "check_id": "uuid",
      "check_name": "orders_row_count",
      "status": "open",
      "severity": "error",
      "failure_count": 3,
      "first_seen": "2024-01-15T10:00:00Z",
      "last_seen": "2024-01-15T12:00:00Z"
    }
  ],
  "total": 5,
  "limit": 100,
  "offset": 0
}
```

### Get Incident

```http
GET /api/v1/incidents/{incident_id}
```

### Update Incident Status

```http
PATCH /api/v1/incidents/{incident_id}
```

**Request Body:**
```json
{
  "status": "acknowledged",
  "by": "john.doe@example.com",
  "notes": "Investigating root cause"
}
```

---

## Schedules

Manage check schedules.

### List Schedules

```http
GET /api/v1/schedules
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `check_id` | uuid | Filter by check |
| `is_active` | boolean | Filter by active status |
| `limit` | integer | Max results (default: 100) |
| `offset` | integer | Pagination offset |

### Create Schedule

```http
POST /api/v1/schedules
```

**Request Body:**
```json
{
  "name": "hourly-orders-check",
  "check_id": "uuid",
  "cron_expression": "0 * * * *",
  "timezone": "UTC",
  "description": "Run every hour"
}
```

### Get Schedule

```http
GET /api/v1/schedules/{schedule_id}
```

### Update Schedule

```http
PUT /api/v1/schedules/{schedule_id}
```

**Request Body:**
```json
{
  "name": "updated-schedule",
  "cron_expression": "0 */6 * * *",
  "is_active": false
}
```

### Delete Schedule

```http
DELETE /api/v1/schedules/{schedule_id}
```

---

## Notifications

Manage notification channels.

### List Channels

```http
GET /api/v1/notifications/channels
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `is_active` | boolean | Filter by active status |
| `limit` | integer | Max results (default: 100) |
| `offset` | integer | Pagination offset |

### Create Channel

```http
POST /api/v1/notifications/channels
```

**Request Body (Webhook):**
```json
{
  "name": "slack-alerts",
  "description": "Slack webhook for critical alerts",
  "channel_type": "webhook",
  "config": {
    "url": "https://hooks.slack.com/services/xxx",
    "method": "POST",
    "headers": {
      "Content-Type": "application/json"
    }
  },
  "events": ["check.failed", "incident.created"],
  "min_severity": "error"
}
```

### Get Channel

```http
GET /api/v1/notifications/channels/{channel_id}
```

### Update Channel

```http
PATCH /api/v1/notifications/channels/{channel_id}
```

### Delete Channel

```http
DELETE /api/v1/notifications/channels/{channel_id}
```

---

## Error Responses

All errors follow this format:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid check configuration",
    "details": [
      {"field": "rule_parameters.error.min_count", "message": "must be a positive integer"}
    ]
  }
}
```

**Error Codes:**
| Code | HTTP Status | Description |
|------|-------------|-------------|
| `UNAUTHORIZED` | 401 | Missing or invalid API key |
| `FORBIDDEN` | 403 | API key lacks permission |
| `NOT_FOUND` | 404 | Resource not found |
| `VALIDATION_ERROR` | 422 | Invalid request body |
| `CONFLICT` | 409 | Resource already exists |
| `INTERNAL_ERROR` | 500 | Server error |
