---
description: Run live cloud platform integration tests
---

# Live Platform Test

Execute tests against real cloud database platforms (Databricks, Snowflake, BigQuery, etc.).

## Instructions

When the user asks to test against cloud platforms, run live integration tests, or verify platform compatibility:

1. **Check credential requirements**:

   ```bash
   # Check for .env file
   ls -la .env

   # Check for credential environment variables
   env | grep -E '(DATABRICKS|SNOWFLAKE|BIGQUERY|AWS|AZURE)'
   ```

   **Required credentials per platform:**

   **Databricks:** `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`

   **Snowflake:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`

   **BigQuery:** `BIGQUERY_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS` (path to service account JSON)

   **Redshift:** `REDSHIFT_HOST`, `REDSHIFT_PORT`, `REDSHIFT_DATABASE`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`

2. **Warn user if credentials missing**:

   ```markdown
   ⚠️  **Credentials Required**

   Live platform tests require cloud credentials. Please ensure you have:
   - Created `.env` file from `.env.example`
   - Added credentials for target platform(s)
   - Verified credentials are valid

   See `.env.example` for required variables.
   ```

3. **Run platform-specific tests**:

   ```bash
   # All platforms
   make test-live-all
   # or: BENCHBOX_ENABLE_EXPERIMENTAL=0 uv run -- python -m pytest -m "live_integration" -v

   # Single platform: make test-live-{databricks|snowflake|bigquery}
   # or: BENCHBOX_ENABLE_EXPERIMENTAL=0 uv run -- python -m pytest -m "live_{platform}" -v
   ```

4. **Monitor test execution**:

   Live tests are slower due to: network latency, data upload, remote execution, resource provisioning.

   Typical execution time: 5-15 minutes per platform

5. **Analyze test results** - check for:

   **Connection:** Can connect, valid credentials, required permissions

   **Data generation:** Generates correctly, uploads succeed, tables created, loading completes

   **Query execution:** No errors, SQL dialect correct, results returned, performance acceptable

   **Cleanup:** Tables dropped, resources released, no dangling objects

6. **Common issues**:

   **Authentication:** Invalid credentials, expired tokens, wrong identifiers, missing permissions

   **Network:** Firewall blocking, VPN required, DNS failures, timeouts

   **Permission:** Insufficient DB permissions, cannot create tables/schemas, cannot upload/execute

   **Quota/billing:** Account suspended, quota exceeded, billing not configured, resource limits

   **SQL dialect:** Translation failed, platform-specific syntax, unsupported functions, type incompatibilities

7. **Report results**:

   ```markdown
   ## Live Platform Test Results

   ### Databricks
   ✅ Connection: Successful
   ✅ Data Generation: 1GB in 45s
   ✅ Query Execution: 22/22 queries passed
   ⏱️  Average Query Time: 2.3s

   ### Snowflake
   ✅ Connection: Successful
   ⚠️  Data Generation: Slow (120s for 1GB)
   ❌ Query Execution: 21/22 queries passed
   - Query 15: Syntax error in LIMIT clause

   ### BigQuery
   ❌ Connection: Failed
   Error: Invalid credentials - service account JSON not found

   ### Summary
   - Databricks: Fully functional ✅
   - Snowflake: Needs query fix ⚠️
   - BigQuery: Credentials needed ❌

   ### Next Steps
   1. Fix Snowflake query 15 LIMIT syntax
   2. Configure BigQuery credentials
   3. Re-run failed tests
   ```

8. **Cost awareness**:

   Warn user that live tests incur cloud costs (data transfer, compute, storage, network egress).

   Suggest: Use small scale factors (SF=1), clean up resources, run off-peak, monitor billing.

## Test Files

Live integration tests are located in:
- `tests/integration/platforms/test_databricks_live.py`
- `tests/integration/platforms/test_snowflake_live.py`
- `tests/integration/platforms/test_bigquery_live.py`
- Additional platform-specific test files

## Platform Adapters

Platform adapters handle platform-specific logic:
- `benchbox/platforms/databricks/adapter.py`
- `benchbox/platforms/snowflake.py`
- `benchbox/platforms/bigquery.py`
- `benchbox/platforms/redshift.py`
- `benchbox/platforms/clickhouse.py`

## Credential Storage

**Security best practices:**
- Never commit `.env` file to git
- Use environment variables for CI/CD
- Rotate credentials regularly
- Use service accounts with minimal permissions
- Enable audit logging

**For CI/CD:**
- Store credentials in GitHub Secrets
- Use platform-specific credential management (AWS Secrets Manager, etc.)
- Enable credential scanning
- Implement automatic credential rotation

## Example .env File

```bash
# Databricks
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcd1234
DATABRICKS_TOKEN=dapi...

# Snowflake
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=benchbox_test
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=BENCHBOX_TEST
SNOWFLAKE_SCHEMA=PUBLIC

# BigQuery
BIGQUERY_PROJECT=my-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# AWS (for Redshift)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
```

## Quick Platform Test Script

Create `_project/test_platform.py` for manual testing:

```python
from benchbox.platforms.databricks import DatabricksAdapter

# Quick connection test
adapter = DatabricksAdapter()
conn = adapter.get_connection()
result = conn.sql("SELECT 1 as test").collect()
print(f"Connection successful: {result}")
```

## Notes

- Live tests are pre-approved in settings.local.json
- Tests should clean up after themselves
- Use test-specific schemas/databases
- Document platform-specific quirks
- Keep test data small to minimize costs
- Monitor for flaky tests (network issues)
