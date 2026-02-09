# Key Differentiators

How DQ Platform differs from DQOps and similar tools.

---

## 1. API-First Architecture

**DQOps Pain Point:** DQOps is designed as a CLI/UI tool first, with API as an afterthought. Running checks requires local installation or complex Docker setups.

**Our Approach:**
- Pure REST API - no CLI or UI required
- Stateless API servers (scale horizontally)
- Every operation available via API
- OpenAPI 3.1 spec auto-generated
- SDK generation for Python, TypeScript, Go

**Why it matters:** Teams can integrate DQ checks into any workflow (CI/CD, Airflow, custom apps) without installing anything locally.

```bash
# Run a check from anywhere
curl -X POST https://dq.example.com/api/v1/checks/abc123/run \
  -H "X-API-Key: $API_KEY"
```

---

## 2. Simplified Deployment

**DQOps Pain Point:** Requires Java 17 + Python 3.8 + local filesystem storage + optional cloud sync. Configuration stored in YAML files that must be version-controlled separately.

**Our Approach:**

```bash
# Single container, all config in database
docker run -e DATABASE_URL=postgres://... dq-platform
```

| Aspect | DQOps | DQ Platform |
|--------|-------|-------------|
| Runtime | Java 17 + Python 3.8 | Python 3.11 only |
| Config Storage | YAML files + Git | PostgreSQL |
| Results Storage | Local Hive data lake | PostgreSQL/TimescaleDB |
| Containers | 1 (but needs mounted volume) | 1 (stateless) |

**Why it matters:**
- Easier to deploy in Kubernetes (no persistent volume needed for config)
- Easier to backup/restore (standard database tools)
- No filesystem coordination issues in replicated deployments

---

## 3. Native Multi-Tenancy

**DQOps Pain Point:** Single-tenant by design. Each team needs their own DQOps instance with separate storage.

**Our Approach:**

```
Organization A (tenant)
  └── Workspace: Production
  └── Workspace: Staging

Organization B (tenant)
  └── Workspace: Analytics
  └── Workspace: ML-Pipeline
```

- Built-in organization/workspace isolation
- API keys scoped to workspaces
- Shared infrastructure, isolated data
- Resource quotas per tenant

**Why it matters:** One deployment serves multiple teams or customers. Essential for:
- Platform teams serving multiple data teams
- SaaS offering to external customers
- Cost efficiency through shared infrastructure

---

## 4. Database-Native Configuration

**DQOps Pain Point:** All configuration in YAML files. Requires Git for versioning. Hard to query "which checks are failing across all tables?"

**Our Approach:**

- All configuration in PostgreSQL
- Query checks, results, incidents with SQL
- API for bulk operations
- Audit trail built-in (who changed what, when)

```sql
-- Find all failing checks in last 24h
SELECT c.name, c.target_table, COUNT(*) as failures
FROM check_results r
JOIN check_definitions c ON r.check_id = c.id
WHERE r.executed_at > NOW() - INTERVAL '24 hours'
  AND r.rule_passed = false
GROUP BY c.id;

-- Checks by table with pass rate
SELECT
  target_table,
  COUNT(*) as total_checks,
  AVG(CASE WHEN rule_passed THEN 1 ELSE 0 END) as pass_rate
FROM check_results r
JOIN check_definitions c ON r.check_id = c.id
GROUP BY target_table;
```

**Optional GitOps:**
- Export configuration to YAML for version control
- Import from YAML for infrastructure-as-code workflows
- But database remains source of truth at runtime

**Why it matters:** Easier reporting, dashboards, and integrations without parsing YAML files.

---

## 5. Lightweight Check Definitions

**DQOps Pain Point:** Complex sensor + rule separation. Custom checks require understanding Jinja2 templates AND Python rules in specific file structures.

**Our Approach:**

Pre-built check types with simple JSON configuration:

```json
{
  "name": "orders_not_empty",
  "type": "row_count_min",
  "connection_id": "uuid",
  "target_table": "public.orders",
  "config": {
    "min_count": 1
  },
  "severity": "error"
}
```

Custom SQL without templates:

```json
{
  "name": "no_negative_totals",
  "type": "custom_sql",
  "connection_id": "uuid",
  "target_table": "public.orders",
  "config": {
    "sql": "SELECT COUNT(*) FROM public.orders WHERE total < 0",
    "operator": "equals",
    "expected": 0
  },
  "severity": "error"
}
```

| Aspect | DQOps | DQ Platform |
|--------|-------|-------------|
| Simple check | YAML + understand sensor types | JSON with documented params |
| Custom SQL | Jinja2 template file + Python rule | Inline SQL string |
| Learning curve | High (Jinja2 + Python + file structure) | Low (JSON + SQL) |

**Why it matters:** Lower learning curve. Most users need standard checks, not custom Jinja2 templates.

---

## 6. MLG Platform Integration

**DQOps Pain Point:** Standalone tool. Integrating DQ results into governance platforms requires custom ETL.

**Our Approach:**

Native integration with MLG (Minimum Lovable Governance):

```json
{
  "check_id": "uuid",
  "data_product_id": "mlg-product-123",
  "on_failure": {
    "update_product_status": "degraded",
    "create_incident": true,
    "notify_owner": true
  }
}
```

**Integration Points:**
- Link checks to Data Products by ID
- DQ scores feed into Data Product health metrics
- Shared data source connections (configure once)
- Webhook events to MLG on check completion
- Unified incident management

**Why it matters:** Unified governance story - DQ is part of the data product lifecycle, not a separate silo.

---

## 7. Simpler Scheduling

**DQOps Pain Point:** Scheduling requires running DQOps as a daemon or integrating with external schedulers (Airflow, cron).

**Our Approach:**

Built-in scheduler with cron expressions:

```json
POST /api/v1/schedules
{
  "name": "hourly-check",
  "check_id": "uuid",
  "cron_expression": "0 */6 * * *"
}
```

- No external dependencies
- Schedule per-check
- View next scheduled runs
- Pause/resume without deleting

```json
GET /api/v1/schedules/uuid

{
  "id": "uuid",
  "check_id": "uuid",
  "cron": "0 */6 * * *",
  "enabled": true,
  "last_run": "2024-01-15T12:00:00Z",
  "next_run": "2024-01-15T18:00:00Z"
}
```

**Why it matters:** Works out of the box. No need to set up Airflow just to run hourly checks.

---

## Summary Comparison

| Aspect | DQOps | DQ Platform |
|--------|-------|-------------|
| **Primary Interface** | CLI/UI | REST API |
| **Deployment** | Java + Python + filesystem | Single container + PostgreSQL |
| **Configuration** | YAML files + Git | Database + API |
| **Multi-tenancy** | No (one instance per team) | Yes (built-in) |
| **Custom checks** | Jinja2 + Python files | JSON config or raw SQL |
| **Scheduling** | External or daemon mode | Built-in scheduler |
| **Governance integration** | Manual/ETL | Native MLG hooks |
| **Target users** | Data engineers with local setup | Platform teams, API consumers |

---

## What We Don't Do (And Why)

To stay focused, we explicitly avoid:

| Feature | Reason |
|---------|--------|
| Built-in UI | API-first philosophy; use Grafana or build custom |
| Data transformation | DQ only; use dbt/Airflow for transforms |
| Data lineage | Defer to MLG or dedicated lineage tools |
| ML anomaly detection | Statistical methods sufficient for v1 |
| Real-time streaming | Batch is simpler and covers most use cases |
