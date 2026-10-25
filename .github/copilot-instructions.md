# Repository guidance

## Project architecture

This repository implements a batch lakehouse for VN Beauty retail data. Run Docker Compose commands from the repository root.

The main data flow is:

1. `scripts/landing_upload.py` uploads source CSVs from `data/landing/` to MinIO, partitioned as `master/snapshot_date=...` or `daily/dt=...`. Daily uploads publish `_manifest.json` and `_SUCCESS` after the CSVs.
2. `jobs/bronze/bronze_load.py` validates landing files against manifests and writes raw, string-valued records plus ingestion metadata to Iceberg Bronze tables. Daily loads overwrite affected date partitions, so reruns are idempotent; file outcomes are recorded in `ops.ingest_log`.
3. `jobs/silver/silver_load.py` applies the per-dataset rules in `config/silver/*.yml`: type conversion, lookups, data-quality rules, deduplication, and merge or snapshot replacement. Rejected rows go to `ops.quarantine`; rule results go to `ops.dq_results`. Silver master/reference datasets must be loaded before sales datasets that look them up.
4. `dbt/models/staging/` defines ephemeral staging models over Silver; `dbt/models/gold/` builds Gold dimensions and facts on Trino/Iceberg. Fact model grain and integrity checks are documented in the SQL/YAML model files. `dbt/macros/gold_helpers.sql` centralizes timezone conversion, date keys, and load timestamps.

`docker-compose.yml` runs the supporting services (Postgres metadata, MinIO object storage, Iceberg REST catalog, Trino) and defines on-demand Spark and dbt jobs. Spark Iceberg/S3 settings are centralized in `jobs/common/spark_session.py`.

## Configuration and conventions

- Shared, non-secret settings belong in `config/settings.env`; credentials belong in local `.env` and must not be committed. `.env.example` is the setup template. Runtime code uses required environment variables; `jobs/common/spark_session.py:env()` raises an error when one is missing.
- Keep Silver dataset behavior in `config/silver/<dataset>.yml` rather than adding dataset-specific branches to `silver_load.py`. Follow `order` and `group` fields for dependency sequencing; master lookups are loaded before dependent sales datasets. Rules run in YAML declaration order.
- Bronze preserves source values and provenance; put data cleaning, typing, quality rules, and canonicalization in Silver. Preserve lineage columns when changing Silver output.
- Daily Silver datasets merge on their configured key, with `version_col` controlling whether a source row can replace an existing version. Master datasets replace the snapshot. Keep these semantics and the declared grain stable.
- In dbt, use `source('silver', ...)` for Silver inputs and `ref(...)` for other dbt models. Staging is ephemeral; Gold is materialized as tables. Preserve documented fact grain and sentinel dimension rows (such as `-1`) when changing joins so facts do not lose rows.
- Use the existing Compose service environments for execution; do not assume local Python, Spark, dbt, Trino, or MinIO installations are configured.

## Local database MCP

For trusted local exploration, Trino is published at `http://localhost:8090` with the `lakehouse` catalog; from another Compose service its address is `http://trino:8080`. In Copilot CLI, manage MCP servers with `/mcp`; keep client configuration and credentials local, not in the repository. The current Trino setup uses HTTP without authentication, so do not expose it to an MCP client outside a trusted local environment. Before enabling a database MCP, provide a read-only connection (the current Trino endpoint allows writes); before shared or remote access, also configure TLS, authentication, and network restrictions. Do not grant an MCP server writes to the lakehouse.

## Production rollout considerations

Before treating the Compose stack as a real deployment, confirm the target environment and expected data volume, then configure:

- Secret management and access control: replace any credential-like values in example files with placeholders, rotate values that may have been shared, use unique least-privilege service identities, and keep secrets in the deployment platform’s secret store.
- Network security: enable authenticated and encrypted access to Trino, MinIO, Iceberg REST, and PostgreSQL as appropriate; restrict published ports and service-to-service access. The current Trino endpoint is unauthenticated HTTP, and Spark/Trino use MinIO root credentials.
- Durable storage and recovery: choose production object storage, encryption/versioning and retention policies; back up PostgreSQL’s Iceberg catalog metadata and test restoration alongside the object data. Compose named volumes are local persistence, not a backup plan.
- Operational execution: add a scheduler/orchestrator for ordered master, Bronze, Silver, and Gold jobs; define retries, alerting, logs/metrics, service readiness checks, and recovery/backfill procedures. `AIRFLOW_DB` is configured, but no Airflow service or DAGs are present.
- Data operations: validate source delivery/manifests and retention, set ownership and PII handling rules, monitor `ops.dq_results` and quarantine, and decide which quality failures should stop downstream publishing. Confirm the configured cutoff, namespaces, bucket names, date ranges, and partition strategy match the intended data.
- Capacity: size Spark, Trino, and storage for actual volume and concurrency; the current Compose memory limits and single dbt thread are demo-oriented constraints.

## Build, run, and test

There is no repository-wide lint command or configured lint target. The project’s executable checks are Compose-based.

From the repository root:

```sh
# Configure local credentials, then build containers and start infrastructure
cp .env.example .env
# Edit .env before use; keep it untracked.
docker compose build
docker compose up -d

# Verify Spark connectivity to Iceberg and MinIO
docker compose run --rm spark /opt/jobs/smoke_test.py

# Upload source data (dry-run first if desired)
docker compose run --rm landing-upload --dry-run
docker compose run --rm landing-upload

# Load a master snapshot before dependent daily sales data
docker compose run --rm spark bronze/bronze_load.py --kind master --snapshot 2026-09-30
docker compose run --rm spark silver/silver_load.py --group master --snapshot 2026-09-30
docker compose run --rm spark bronze/bronze_load.py --kind daily --start 2023-03-01 --end 2023-03-31
docker compose run --rm spark silver/silver_load.py --group sales --start 2023-03-01 --end 2023-03-31

# Build Gold models and run their schema/data tests
docker compose run --rm dbt debug
docker compose run --rm dbt build

# Run one dbt singular test at a time
docker compose run --rm dbt test --select assert_gold_no_row_loss
docker compose run --rm dbt test --select assert_payments_grain_unique

# Backfill by month; each script accepts inclusive YYYY-MM start and end
bash scripts/backfill_bronze.sh 2021-01 2026-06
bash scripts/backfill_silver.sh 2021-01 2026-06
```

`data/landing/` is local input data and is gitignored. The project uses no standalone Python test runner; `jobs/smoke_test.py`, Silver dry-runs (`--dry-run`), and dbt tests are the existing validation paths. Run `docker compose run --rm dbt test --select <test_name>` to select a single dbt test.
