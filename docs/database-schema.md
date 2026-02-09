# Database Schema

PostgreSQL schema for DQ Platform.

## Entity Relationship Diagram

```
┌─────────────────┐       ┌─────────────────┐
│  organizations  │       │   workspaces    │
├─────────────────┤       ├─────────────────┤
│ id              │──┐    │ id              │
│ name            │  └───▶│ organization_id │
│ created_at      │       │ name            │
└─────────────────┘       │ created_at      │
                          └────────┬────────┘
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
        ▼                          ▼                          ▼
┌───────────────┐         ┌───────────────┐         ┌───────────────┐
│  connections  │         │   api_keys    │         │   webhooks    │
├───────────────┤         ├───────────────┤         ├───────────────┤
│ id            │         │ id            │         │ id            │
│ workspace_id  │         │ workspace_id  │         │ workspace_id  │
│ name          │         │ key_hash      │         │ url           │
│ type          │         │ name          │         │ events        │
│ config (enc)  │         │ last_used_at  │         │ enabled       │
└───────┬───────┘         └───────────────┘         └───────────────┘
        │
        │
        ▼
┌─────────────────────┐
│  check_definitions  │
├─────────────────────┤
│ id                  │
│ connection_id       │──────────────────────┐
│ name                │                      │
│ type                │                      │
│ target_table        │                      │
│ target_column       │                      │
│ config              │                      │
│ severity            │                      │
│ enabled             │                      │
└──────────┬──────────┘                      │
           │                                 │
     ┌─────┴─────┐                          │
     │           │                          │
     ▼           ▼                          │
┌─────────┐  ┌───────────┐                  │
│schedules│  │ incidents │                  │
├─────────┤  ├───────────┤                  │
│ id      │  │ id        │                  │
│check_id │  │ check_id  │                  │
│ cron    │  │ status    │                  │
│ enabled │  │ severity  │                  │
│ next_run│  │first_seen │                  │
└─────────┘  │ last_seen │                  │
             └───────────┘                  │
                                            │
                                            ▼
                                   ┌─────────────────┐
                                   │  check_results  │
                                   ├─────────────────┤
                                   │ id              │
                                   │ check_id        │
                                   │ executed_at     │ (partition key)
                                   │ sensor_value    │
                                   │ rule_passed     │
                                   │ severity        │
                                   │ execution_time  │
                                   └─────────────────┘
```

---

## Tables

### organizations

Multi-tenant organization (top-level tenant).

```sql
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_organizations_name ON organizations(name);
```

### workspaces

Isolated workspace within an organization.

```sql
CREATE TABLE workspaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(organization_id, name)
);

CREATE INDEX idx_workspaces_org ON workspaces(organization_id);
```

### api_keys

API keys for authentication, scoped to workspace.

```sql
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    key_prefix VARCHAR(12) NOT NULL,  -- "dqp_abc123..." for identification
    key_hash VARCHAR(255) NOT NULL,   -- bcrypt hash of full key
    scopes TEXT[] NOT NULL DEFAULT '{}',
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_api_keys_workspace ON api_keys(workspace_id);
CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);
```

### connections

Data source connections with encrypted credentials.

```sql
CREATE TYPE connection_type AS ENUM (
    'postgresql', 'mysql', 'sqlserver',
    'bigquery', 'snowflake', 'redshift', 'databricks'
);

CREATE TYPE connection_status AS ENUM (
    'unknown', 'healthy', 'unhealthy', 'error'
);

CREATE TABLE connections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    type connection_type NOT NULL,
    config_encrypted BYTEA NOT NULL,  -- Fernet-encrypted JSON
    status connection_status NOT NULL DEFAULT 'unknown',
    last_tested_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(workspace_id, name)
);

CREATE INDEX idx_connections_workspace ON connections(workspace_id);
CREATE INDEX idx_connections_type ON connections(type);
```

### check_definitions

Data quality check configurations.

```sql
CREATE TYPE check_target_type AS ENUM ('table', 'column');
CREATE TYPE check_severity AS ENUM ('warning', 'error', 'fatal');

CREATE TABLE check_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,

    -- Check type and target
    check_type VARCHAR(100) NOT NULL,  -- 'row_count_min', 'null_percent', etc.
    target_type check_target_type NOT NULL,
    target_schema VARCHAR(255),
    target_table VARCHAR(255) NOT NULL,
    target_column VARCHAR(255),  -- NULL for table-level checks

    -- Configuration
    config JSONB NOT NULL DEFAULT '{}',  -- check-type specific params
    severity check_severity NOT NULL DEFAULT 'error',
    enabled BOOLEAN NOT NULL DEFAULT true,

    -- Metadata
    tags TEXT[] NOT NULL DEFAULT '{}',
    data_product_id VARCHAR(255),  -- Optional MLG integration
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(connection_id, name)
);

CREATE INDEX idx_checks_connection ON check_definitions(connection_id);
CREATE INDEX idx_checks_target ON check_definitions(target_schema, target_table);
CREATE INDEX idx_checks_type ON check_definitions(check_type);
CREATE INDEX idx_checks_enabled ON check_definitions(enabled) WHERE enabled = true;
CREATE INDEX idx_checks_data_product ON check_definitions(data_product_id)
    WHERE data_product_id IS NOT NULL;
```

### check_results

Time-series storage for check execution results. Partitioned by month.

```sql
CREATE TABLE check_results (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    check_id UUID NOT NULL REFERENCES check_definitions(id) ON DELETE CASCADE,

    -- Execution info
    executed_at TIMESTAMPTZ NOT NULL,
    execution_time_ms INTEGER,

    -- Result
    sensor_value NUMERIC,  -- Measured value from sensor
    rule_passed BOOLEAN NOT NULL,
    severity check_severity,  -- NULL if passed

    -- Error tracking
    error_message TEXT,

    PRIMARY KEY (executed_at, id)
) PARTITION BY RANGE (executed_at);

-- Create partitions for current and next 3 months
CREATE TABLE check_results_2024_01 PARTITION OF check_results
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE check_results_2024_02 PARTITION OF check_results
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
-- ... continue for each month

CREATE INDEX idx_results_check ON check_results(check_id, executed_at DESC);
CREATE INDEX idx_results_failed ON check_results(check_id, executed_at DESC)
    WHERE rule_passed = false;
```

### incidents

Grouped check failures.

```sql
CREATE TYPE incident_status AS ENUM ('open', 'acknowledged', 'resolved');

CREATE TABLE incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    check_id UUID NOT NULL REFERENCES check_definitions(id) ON DELETE CASCADE,

    -- Status
    status incident_status NOT NULL DEFAULT 'open',
    severity check_severity NOT NULL,

    -- Tracking
    failure_count INTEGER NOT NULL DEFAULT 1,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,

    -- Metadata
    acknowledged_by VARCHAR(255),
    resolution_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_incidents_check ON incidents(check_id);
CREATE INDEX idx_incidents_status ON incidents(status) WHERE status != 'resolved';
CREATE UNIQUE INDEX idx_incidents_open ON incidents(check_id)
    WHERE status IN ('open', 'acknowledged');  -- Only one open incident per check
```

### schedules

Cron-based check schedules.

```sql
CREATE TABLE schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    check_id UUID NOT NULL REFERENCES check_definitions(id) ON DELETE CASCADE,

    -- Schedule config
    cron_expression VARCHAR(100) NOT NULL,
    timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
    enabled BOOLEAN NOT NULL DEFAULT true,

    -- Tracking
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    last_run_job_id UUID,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(check_id)  -- One schedule per check
);

CREATE INDEX idx_schedules_next_run ON schedules(next_run_at)
    WHERE enabled = true;
```

### jobs

Check execution job tracking.

```sql
CREATE TYPE job_status AS ENUM ('queued', 'running', 'completed', 'failed', 'cancelled');

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    check_id UUID NOT NULL REFERENCES check_definitions(id) ON DELETE CASCADE,

    -- Status
    status job_status NOT NULL DEFAULT 'queued',

    -- Timing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Result (populated on completion)
    result_id BIGINT,  -- References check_results
    error_message TEXT,

    -- Trigger info
    triggered_by VARCHAR(50) NOT NULL DEFAULT 'api',  -- 'api', 'schedule', 'webhook'
    schedule_id UUID REFERENCES schedules(id)
);

CREATE INDEX idx_jobs_check ON jobs(check_id, created_at DESC);
CREATE INDEX idx_jobs_status ON jobs(status) WHERE status IN ('queued', 'running');
```

### webhooks

Notification webhook configurations.

```sql
CREATE TABLE webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,

    -- Config
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    secret VARCHAR(255),  -- For HMAC signature
    events TEXT[] NOT NULL,  -- ['check.failed', 'incident.created', ...]
    enabled BOOLEAN NOT NULL DEFAULT true,

    -- Tracking
    last_triggered_at TIMESTAMPTZ,
    last_response_code INTEGER,
    failure_count INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhooks_workspace ON webhooks(workspace_id);
CREATE INDEX idx_webhooks_enabled ON webhooks(workspace_id, enabled) WHERE enabled = true;
```

### audit_log

Track changes for compliance.

```sql
CREATE TABLE audit_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    workspace_id UUID NOT NULL,

    -- What changed
    entity_type VARCHAR(50) NOT NULL,  -- 'connection', 'check', 'schedule', etc.
    entity_id UUID NOT NULL,
    action VARCHAR(20) NOT NULL,  -- 'create', 'update', 'delete'

    -- Who changed it
    api_key_id UUID,

    -- Change details
    changes JSONB,  -- {"field": {"old": x, "new": y}}

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_audit_workspace ON audit_log(workspace_id, created_at DESC);
CREATE INDEX idx_audit_entity ON audit_log(entity_type, entity_id, created_at DESC);
```

---

## Useful Queries

### Active checks with latest results

```sql
SELECT
    c.id,
    c.name,
    c.target_table,
    c.check_type,
    r.executed_at AS last_run,
    r.rule_passed AS last_passed,
    r.sensor_value AS last_value
FROM check_definitions c
LEFT JOIN LATERAL (
    SELECT * FROM check_results
    WHERE check_id = c.id
    ORDER BY executed_at DESC
    LIMIT 1
) r ON true
WHERE c.enabled = true;
```

### Pass rate by table (last 7 days)

```sql
SELECT
    c.target_table,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN r.rule_passed THEN 1 ELSE 0 END) AS passed,
    ROUND(AVG(CASE WHEN r.rule_passed THEN 1.0 ELSE 0.0 END) * 100, 1) AS pass_rate
FROM check_results r
JOIN check_definitions c ON r.check_id = c.id
WHERE r.executed_at > NOW() - INTERVAL '7 days'
GROUP BY c.target_table
ORDER BY pass_rate ASC;
```

### Open incidents with check details

```sql
SELECT
    i.id AS incident_id,
    i.severity,
    i.failure_count,
    i.first_seen,
    i.last_seen,
    c.name AS check_name,
    c.target_table,
    conn.name AS connection_name
FROM incidents i
JOIN check_definitions c ON i.check_id = c.id
JOIN connections conn ON c.connection_id = conn.id
WHERE i.status IN ('open', 'acknowledged')
ORDER BY
    CASE i.severity WHEN 'fatal' THEN 1 WHEN 'error' THEN 2 ELSE 3 END,
    i.first_seen DESC;
```

### Checks due for execution

```sql
SELECT
    s.id AS schedule_id,
    c.id AS check_id,
    c.name,
    s.cron_expression,
    s.next_run_at
FROM schedules s
JOIN check_definitions c ON s.check_id = c.id
WHERE s.enabled = true
  AND c.enabled = true
  AND s.next_run_at <= NOW()
ORDER BY s.next_run_at;
```

---

## Migrations

Use a migration tool like Alembic (Python) to manage schema changes.

```bash
# Create migration
alembic revision --autogenerate -m "add_check_tags"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```
