# Getting Started with DQ Platform

A step-by-step guide to get DQ Platform running and execute your first data quality checks.

## Prerequisites

- Docker and Docker Compose
- curl (for API examples)
- (Optional) Python 3.11+ for local development

## Step 1: Start the Platform

```bash
# Clone and enter the repository
git clone <repository-url>
cd dq-platform

# Set up environment variables
cp .env.example .env

# Generate an encryption key and add it to .env
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Copy the output and set ENCRYPTION_KEY in .env

# Start all services
docker compose up -d

# Wait for services to be healthy (about 30 seconds)
docker compose ps
```

You should see all services showing `(healthy)` status:
- `dq-platform-postgres` 
- `dq-platform-redis`
- `dq-platform-api`

## Step 2: Verify the API

```bash
# Health check
curl http://localhost:8000/health
# {"status":"healthy","version":"0.1.0"}

# Open Swagger UI in browser
open http://localhost:8000/docs
```

## Step 3: Create a Database Connection

Let's connect to the platform's own database as an example:

```bash
# Create the connection
curl -X POST http://localhost:8000/api/v1/connections \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d '{
    "name": "local-postgres",
    "connection_type": "postgresql",
    "config": {
      "host": "postgres",
      "port": 5432,
      "database": "dq_platform",
      "user": "postgres",
      "password": "postgres"
    }
  }'
```

Save the returned `id` - you'll need it for the next steps.

## Step 4: Explore Your Data

```bash
# Replace with your connection ID
CONN_ID="your-connection-uuid"

# List schemas
curl "http://localhost:8000/api/v1/connections/$CONN_ID/schemas" \
  -H "X-API-Key: test-key"

# List tables in public schema
curl "http://localhost:8000/api/v1/connections/$CONN_ID/schemas/public/tables" \
  -H "X-API-Key: test-key"

# List columns for a table
curl "http://localhost:8000/api/v1/connections/$CONN_ID/schemas/public/tables/checks/columns" \
  -H "X-API-Key: test-key"
```

## Step 5: Create Your First Check

### Check 1: Row Count Validation

```bash
curl -X POST http://localhost:8000/api/v1/checks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d "{
    \"name\": \"checks-row-count\",
    \"description\": \"Verify checks table has reasonable row count\",
    \"connection_id\": \"$CONN_ID\",
    \"check_type\": \"row_count\",
    \"check_mode\": \"monitoring\",
    \"target_schema\": \"public\",
    \"target_table\": \"checks\",
    \"parameters\": {
      \"min_rows\": 0,
      \"max_rows\": 10000
    },
    \"rule_parameters\": {
      \"warning\": {\"max_value\": 5000},
      \"error\": {\"max_value\": 10000}
    }
  }"
```

Save the check `id` from the response.

### Check 2: Null Validation

```bash
curl -X POST http://localhost:8000/api/v1/checks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d "{
    \"name\": \"checks-name-not-null\",
    \"description\": \"Verify name column has no nulls\",
    \"connection_id\": \"$CONN_ID\",
    \"check_type\": \"not_null\",
    \"check_mode\": \"monitoring\",
    \"target_schema\": \"public\",
    \"target_table\": \"checks\",
    \"target_column\": \"name\",
    \"rule_parameters\": {
      \"error\": {\"max_count\": 0}
    }
  }"
```

## Step 6: Run the Checks

```bash
# Replace with your check IDs
CHECK_ID_1="your-first-check-uuid"
CHECK_ID_2="your-second-check-uuid"

# Run first check
curl -X POST "http://localhost:8000/api/v1/checks/$CHECK_ID_1/run" \
  -H "X-API-Key: test-key"

# Run second check
curl -X POST "http://localhost:8000/api/v1/checks/$CHECK_ID_2/run" \
  -H "X-API-Key: test-key"
```

## Step 7: View Results

```bash
# Get results for a specific check
curl "http://localhost:8000/api/v1/results?check_id=$CHECK_ID_1&limit=5" \
  -H "X-API-Key: test-key" | python3 -m json.tool

# Get overall summary
curl "http://localhost:8000/api/v1/results/summary" \
  -H "X-API-Key: test-key" | python3 -m json.tool
```

Example output:
```json
{
  "total_executions": 1031,
  "passed": 832,
  "failed": 199,
  "pass_rate": 80.7,
  "avg_execution_time_ms": 325.11
}
```

## Step 8: Preview Mode (Dry Run)

Test a check without saving it:

```bash
curl -X POST http://localhost:8000/api/v1/checks/validate/preview \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d "{
    \"connection_id\": \"$CONN_ID\",
    \"check_type\": \"row_count\",
    \"target_schema\": \"public\",
    \"target_table\": \"checks\",
    \"rule_parameters\": {
      \"error\": {\"min_count\": 1}
    }
  }"
```

## Step 9: View Available Check Types

```bash
# List all check types
curl "http://localhost:8000/api/v1/checks/types" \
  -H "X-API-Key: test-key" | python3 -m json.tool

# List check categories
curl "http://localhost:8000/api/v1/checks/categories" \
  -H "X-API-Key: test-key"
```

## Step 10: Monitor Jobs

When you run a check, it creates a job that runs asynchronously:

```bash
# List all jobs
curl "http://localhost:8000/api/v1/jobs" \
  -H "X-API-Key: test-key" | python3 -m json.tool

# Get specific job status
JOB_ID="your-job-uuid"
curl "http://localhost:8000/api/v1/jobs/$JOB_ID" \
  -H "X-API-Key: test-key" | python3 -m json.tool
```

## Common Operations

### List All Connections
```bash
curl "http://localhost:8000/api/v1/connections" \
  -H "X-API-Key: test-key" | python3 -m json.tool
```

### List All Checks
```bash
curl "http://localhost:8000/api/v1/checks" \
  -H "X-API-Key: test-key" | python3 -m json.tool
```

### Update a Check
```bash
curl -X PATCH "http://localhost:8000/api/v1/checks/$CHECK_ID" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key" \
  -d '{
    "is_active": false
  }'
```

### Delete a Check
```bash
curl -X DELETE "http://localhost:8000/api/v1/checks/$CHECK_ID" \
  -H "X-API-Key: test-key"
```

## Troubleshooting

### Services not starting
```bash
# Check logs
docker compose logs migrate
docker compose logs api

# Restart everything
docker compose down
docker compose up -d
```

### Connection test fails
- Verify the database host is accessible from the Docker network
- Use `postgres` as host when connecting to the containerized Postgres
- Check credentials in the connection config

### Check execution fails
```bash
# View worker logs
docker compose logs worker --tail 50

# Check job status for error messages
curl "http://localhost:8000/api/v1/jobs/$JOB_ID" \
  -H "X-API-Key: test-key"
```

## Next Steps

- Explore [171 available check types](docs/DQOPS_IMPLEMENTATION.md)
- Learn about [scheduling checks](docs/api-spec.md)
- Set up [webhook notifications](docs/api-spec.md)
- Read the [architecture overview](docs/architecture.md)

## Clean Up

```bash
# Stop all services
docker compose down

# Remove all data (WARNING: permanent)
docker compose down -v
```
