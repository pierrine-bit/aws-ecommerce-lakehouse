# E-Commerce Lakehouse on AWS

[![CI](https://github.com/pierrine-bit/aws-ecommerce-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/pierrine-bit/aws-ecommerce-lakehouse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Terraform](https://img.shields.io/badge/Terraform-%E2%89%A5%201.10-7B42BC)](versions.tf)

A lakehouse for e-commerce transaction data, provisioned end to end with
Terraform. It validates raw product, order, and order-item files, materialises
ACID Delta Lake tables on S3, registers them for SQL access through Athena, and
archives consumed source files — orchestrated by one Step Functions state machine
with retries, quarantined rejects, and email alerting on failure.

The design goal was a pipeline that is **trustworthy**, not merely functional:
every stage is gated on the previous one succeeding, invalid rows are quarantined
rather than dropped, and no raw file is archived until the curated tables have
been independently proven to hold data.

**Contents** ·
[Architecture](#architecture) ·
[Design decisions](#design-decisions) ·
[Data model](#data-model) ·
[Pipeline](#pipeline) ·
[Operations](#operations) ·
[Configuration](#configuration) ·
[Getting started](#getting-started) ·
[Querying](#querying) ·
[Testing and CI](#testing-and-ci) ·
[Troubleshooting](#troubleshooting) ·
[Limitations](#limitations)

---

## Architecture

```text
  Raw files (CSV / XLSX)
          │  uploaded by Terraform
          ▼
      S3  raw/
          │
          ▼
  ┌────────────────────────────────────────────────────┐
  │  Step Functions                                    │
  │                                                    │
  │   1  Glue Spark ETL       →  lakehouse-dwh/        │
  │   2  Quality gate                                  │
  │   3  Delta crawler        →  Glue Data Catalog     │
  │   4  Athena row check                              │
  │   5  Lambda archival      →  archived/<timestamp>/ │
  └────────────────────────┬───────────────────────────┘
                           │  failure, timeout, or abort
                           ▼
                  EventBridge  →  SNS  →  email
```

Stages run sequentially, each conditional on the one before it. Rows failing
validation branch out of the ETL into `rejected/`.

| Stage | Service | Responsibility |
| ----- | ------- | -------------- |
| ETL | Glue 4.0 Spark, 2 × `G.1X` | Validate, deduplicate, and merge into Delta tables |
| Quality gate | Glue Python shell, 1 DPU | Assert the curated tables exist, are catalogued, and are fresh |
| Crawler | Glue Delta crawler | Keep Catalog schemas in step with the Delta tables |
| Row check | Athena | Confirm each table is non-empty before anything is archived |
| Archival | Lambda (Python 3.11) | Move consumed raw files to a timestamped archive prefix |
| Orchestration | Step Functions (STANDARD) | Sequencing, retries, failure isolation |
| Alerting | EventBridge + SNS | Notify on Glue job and pipeline failures |

---

## Design decisions

| Decision | Rationale |
| -------- | --------- |
| **Delta Lake, not plain Parquet** | Orders arrive in monthly batches and the same order can reappear. Delta `MERGE` makes re-runs idempotent; Parquet would need full rewrites or a bespoke dedup layer. |
| **Quarantine rejects, don't drop them** | Invalid rows are written to `rejected/` with a reason and timestamp. Data defects are usually upstream problems worth investigating — silently filtering destroys the evidence. |
| **A separate quality gate** | Validating the *outcome* in its own job separates "did the transformation work" from "did we land trustworthy data", and lets the gate be tested and re-run alone. |
| **Python shell for the gate, not Spark** | The gate only performs S3 listings and Catalog lookups. 1 DPU avoids both cluster start-up latency and idle worker cost. |
| **Row check via a failing query** | `SELECT 1 / COUNT(*)` divides by zero on an empty table, so emptiness surfaces through the retry/catch machinery already in place — no extra stage to fetch and branch on results. |
| **Archive last** | Raw files move only after the tables are proven present, catalogued, fresh, and non-empty. A failed run leaves the source data untouched and retryable. |
| **Flags omit stages, not skip them** | Disabling the crawler or row check rewires the preceding stage's `Next`, so the deployed machine honestly reflects what runs. |
| **Native S3 state locking** | Terraform 1.10's `use_lockfile` locks state in S3 directly, removing the DynamoDB table older setups needed. |

---

## Data model

### Sources

| File | Format | Grain |
| ---- | ------ | ----- |
| `products.csv` | CSV | One row per product |
| `orders_apr_2025.xlsx` | XLSX | One row per order |
| `order_items_apr_2025.xlsx` | XLSX | One row per order line |

```text
products          orders             order_items
--------          ------             -----------
product_id        order_num          id
department_id     order_id           order_id
department        user_id            user_id
product_name      order_timestamp    days_since_prior_order
                  total_amount       product_id
                  date               add_to_cart_order
                                     reordered
                                     order_timestamp
                                     date
```

Schemas are declared explicitly, never inferred, so an upstream rename or type
drift fails at read time instead of propagating into the curated layer.

### Curated tables

| Table | Merge key | Partitioned by | Referential integrity |
| ----- | --------- | -------------- | --------------------- |
| `products` | `product_id` | `department` | — |
| `orders` | `order_id` | `order_date` | — |
| `order_items` | `id` | `order_date` | `product_id` → `products`, `order_id` → `orders` |

Partitions match the dominant query patterns — product analysis by department,
revenue by date — so Athena prunes rather than scans. Each table carries an
`ingestion_timestamp`; `orders` and `order_items` also carry `order_ts` and
`order_date`.

### Storage layout

```text
raw/                 landing zone, one prefix per dataset
lakehouse-dwh/       curated Delta tables
scripts/             ETL and quality-gate job code
rejected/            quarantined rows as JSON, with reject reason
archived/            consumed raw files, timestamped
athena-results/      query output
tmp/                 Glue scratch space
```

The bucket enforces versioning, SSE-S3 encryption, and a full public-access
block. Four lifecycle rules bound growth: `archived/` and `rejected/` expire on
their configured schedules, noncurrent versions after 30 days, and incomplete
multipart uploads after 7 days.

---

## Pipeline

### ETL

Datasets are processed in dependency order — products, then orders, then order
items, whose referential integrity is checked against the two tables written
before it. Each passes through:

1. **Read with an explicit schema.** Spark reads CSV directly; XLSX is parsed via
   pandas and `openpyxl`, as Spark has no native Excel reader.
2. **Normalise types.** Unparseable numerics become genuine SQL `NULL`, not
   `NaN` — a distinction that matters, since `NaN` passes an `isNull()` check and
   crashes an `int()` cast.
3. **Assert required columns**, naming any that are missing.
4. **Split valid from invalid** on null business keys, unparseable timestamps,
   and business rules: `total_amount` must be non-negative;
   `days_since_prior_order` may be null (a first order) but never negative.
5. **Check referential integrity** for order items, via semi- and anti-joins
   against the curated tables.
6. **Deduplicate** on the merge key.
7. **Write to Delta** — first run creates a partitioned table, later runs `MERGE`
   on the business key.
8. **Register** the table in the Glue Data Catalog.

Read, rejected, and written row counts are logged per dataset, so a run's lineage
is reconstructable from CloudWatch alone.

### Quality gate

For each of the three tables, the gate asserts that:

- a Delta transaction log exists under the table's `_delta_log/` prefix;
- the table is registered in the Glue Data Catalog; and
- the newest Delta commit is no older than `max_data_age_hours`.

The freshness check is the important one. Presence alone is weak: a Delta log
left by a previous run would satisfy it even if today's run wrote nothing.
Comparing commit age against a threshold turns a silent no-op into a hard
failure. AWS clients use standard retry mode, so a transient API blip does not
fail the gate spuriously.

### Orchestration

| # | State | Type | Notes |
| - | ----- | ---- | ----- |
| 1 | `SimulateFileArrival` | Pass | Documents the expected raw-zone contract |
| 2 | `RunDeltaLakeETL` | Task | Glue job, synchronous; 30 min job timeout |
| 3 | `RunQualityChecks` | Task | Glue job, synchronous; 10 min job timeout |
| 4 | `RunCatalogCrawler` | Task | Optional; tolerates an already-running crawler |
| 5 | `PrepareAthenaValidation` / `RunAthenaValidation` | Pass + Map | Optional; 3 tables checked concurrently |
| 6 | `ArchiveRawFiles` | Task | Lambda |
| 7 | `PipelineSucceeded` / `PipelineFailed` | Terminal | Failure cause names where to look |

Resilience is layered deliberately: Glue tasks retry twice with exponential
backoff; the Lambda retries three times on the AWS-recommended transient error
set; the crawler stage catches `Glue.CrawlerRunningException` and continues
rather than failing because a previous crawl is still finishing. Everything else
catches `States.ALL` and routes to `PipelineFailed`, so no error path is
unhandled. State-level timeouts sit above the job timeouts as a backstop.

### Archival

The Lambda moves every raw object to `archived/<YYYY>/<MM>/<DD>/<HHMMSS>/`,
preserving the original key. Per-object failures are collected rather than
thrown immediately, so one unreadable object cannot block the rest of the zone —
then the function raises, failing the run with the full list.

---

## Operations

### Observability

Dedicated CloudWatch log groups for Glue and Step Functions, both with 14-day
retention. The ETL streams continuous logs and publishes job metrics; the state
machine logs at `ALL` with execution data included.

### Alerting

Two EventBridge rules publish to an SNS topic:

- **Glue job failure** — either job entering `FAILED` or `TIMEOUT`, whether run by
  the pipeline or triggered by hand, so a manual failure is never invisible.
- **Pipeline failure** — the state machine entering `FAILED`, `TIMED_OUT`, or
  `ABORTED`, whichever state raised the error.

Set `alert_email` to subscribe an address. The topic ARN is also an output, so
further subscribers can be attached without touching this configuration.

### Security

- **Three narrowly scoped IAM roles** — Glue, Lambda, and Step Functions — rather
  than one shared role, each limited to the prefixes, Catalog resources, and job
  ARNs it needs.
- **The archival Lambda cannot read curated data.** It holds
  `GetObject`/`DeleteObject` only under `raw/`, `PutObject` only under
  `archived/`, and prefix-conditioned `ListBucket`.
- **Athena executes as the Step Functions role**, so that role alone carries the
  Catalog read and results-write permissions.
- **Encryption at rest** via SSE-S3, all public access blocked, and remote state
  encrypted, versioned, and locked.

One concession is documented in code: Spark writes zero-byte `<prefix>_$folder$`
markers at the bucket root, which no `<prefix>/*` pattern matches. Those exact
keys are granted explicitly rather than widening the policy to the whole bucket.

### Cost

Sized for the workload rather than for headroom. ETL compute dominates — two
`G.1X` workers, bounded by a 30-minute timeout. The gate runs on 1 DPU instead of
a Spark cluster. Athena costs fall out of partition pruning. S3 growth is capped
by lifecycle rules, and state locking uses S3 natively rather than DynamoDB.

`force_destroy` is enabled so teardown leaves nothing billable — appropriate for
a project environment, worth reconsidering for production.

---

## Configuration

Every variable has a working default, so `terraform apply` succeeds against an
unmodified `terraform.tfvars`.

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `aws_region` | `eu-west-1` | Deployment region |
| `project_name` | `ecommerce-lakehouse` | Prefix for all resource names |
| `bucket_name` | *generated* | Lakehouse bucket; defaults to `<project>-<account>-<region>` |
| `local_data_dir` | `./data` | Local folder holding the raw source files |
| `crawler_enabled` | `true` | Include the crawler stage |
| `athena_validation_enabled` | `true` | Include the row-check stage |
| `max_data_age_hours` | `24` | Age at which the gate treats a table as stale |
| `archive_retention_days` | `90` | Retention for `archived/` |
| `rejected_retention_days` | `30` | Retention for `rejected/` |
| `alert_email` | `""` | Alert recipient; **no alerts are sent while unset** |

Setting `alert_email` creates an SNS subscription that must be confirmed via the
link AWS sends before alerts begin arriving.

---

## Getting started

### Prerequisites

| Requirement | Version | Notes |
| ----------- | ------- | ----- |
| Terraform | ≥ 1.10.0 | Required for `use_lockfile` state locking |
| AWS CLI | v2 | Credentials able to create the resources |
| Python | 3.11 | Only to run the tests locally |

```bash
aws configure
aws sts get-caller-identity        # confirm the target account
```

### 1. Bootstrap the state backend

A one-time configuration creating the versioned, encrypted bucket that holds
Terraform state. Its name must match the `backend "s3"` block in `versions.tf`.

```bash
cd bootstrap

cat > terraform.tfvars <<'EOF'
aws_region        = "eu-west-1"
state_bucket_name = "ecommerce-lakehouse-tfstate-<your-account-id>"
EOF

terraform init
terraform apply
cd ..
```

### 2. Deploy

```bash
cp terraform.tfvars.example terraform.tfvars    # then edit — see Configuration

terraform init
terraform validate
terraform apply
```

### 3. Run the pipeline

```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw state_machine_arn) \
  --input file://examples/start-execution.json
```

Outputs: `s3_bucket`, `glue_database`, `glue_jobs`, `crawler_name`,
`athena_workgroup`, `state_machine_arn`, `pipeline_alerts_topic_arn`.

---

## Querying

Query through the provisioned workgroup, which enforces its own results location.

```sql
-- Product distribution by department
SELECT department, COUNT(*) AS product_count
FROM ecommerce_lakehouse.products
GROUP BY department
ORDER BY product_count DESC;
```

```sql
-- Daily revenue, partition-pruned on order_date
SELECT order_date,
       SUM(total_amount) AS revenue,
       COUNT(DISTINCT order_id) AS orders
FROM ecommerce_lakehouse.orders
WHERE order_date BETWEEN DATE '2025-04-01' AND DATE '2025-04-30'
GROUP BY order_date
ORDER BY order_date;
```

```sql
-- Top departments by line-item volume
SELECT p.department,
       COUNT(*) AS line_items,
       SUM(CAST(oi.reordered AS BIGINT)) AS reorders
FROM ecommerce_lakehouse.order_items oi
JOIN ecommerce_lakehouse.products p ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY line_items DESC
LIMIT 10;
```

---

## Testing and CI

Both job modules separate their transformation and validation logic from
`bootstrap()`, which resolves Glue arguments and builds AWS clients. Since
`awsglue` exists only inside a live Glue job, that split is what makes the logic
testable at all — tests exercise the real functions with a local Spark session
and mocked AWS clients.

```bash
pip install -r requirements-dev.txt
pytest -q
```

Spark tests skip automatically when PySpark or a JVM is missing, so the suite
stays runnable without a local Spark install while executing in full in CI.
Dependencies live in `requirements-dev.txt` rather than inline in the workflow,
so local and CI environments cannot drift — an undeclared dependency that happens
to be installed on a workstation is exactly what turns a green local run red.

CI runs on every push and pull request to `main`: unit tests first (so a logic
regression fails fast), then `terraform fmt -check`, `terraform init
-backend=false` — no AWS credentials required — and `terraform validate`.

---

## Troubleshooting

| Symptom | Cause and resolution |
| ------- | -------------------- |
| Failure straight after ETL | Check the Glue run in `/aws-glue/ecommerce-lakehouse`; per-stage logs name the dataset. |
| Gate reports missing Delta logs | The ETL wrote nothing. Confirm the raw zone is populated — a prior run may have archived it. |
| Gate reports stale tables | Newest commit exceeds `max_data_age_hours`. Expected when re-running the gate alone. |
| Gate reports missing Catalog entries | Registration failed. Verify `--enable-glue-datacatalog` and the role's `glue:CreateTable`. |
| Row check fails for one table | That table is empty — the ETL likely rejected every row. Inspect `rejected/<table>/`. |
| High rejection counts | Read `reject_reason` in `rejected/<table>/`; these are upstream defects, preserved deliberately. |
| No alert emails | `alert_email` unset, or the SNS confirmation link was never clicked. |
| Raw zone empty on a second run | By design — archival moved the files. Restore from `archived/` or re-upload. |

---

## Limitations

- **Ingestion is batch and manually triggered.** `SimulateFileArrival` is a
  placeholder; an S3 notification or schedule would make it event-driven.
- **The Excel path does not scale horizontally.** XLSX is parsed on the driver, so
  a file larger than driver memory fails. CSV or Parquet upstream removes this.
- **The state bucket name is hard-coded**, as Terraform forbids variables in
  backend blocks. `-backend-config` would make it portable across accounts.
- **The gate checks freshness and presence, not distributions.** Column profiling
  or a framework such as Deequ would deepen it.
- **Rejected rows are written but not surfaced.** A rejection-rate alarm would
  make them an active signal.
- **No table maintenance.** Delta `OPTIMIZE` and `VACUUM` are unscheduled, so
  small files accumulate across many incremental runs.

---

## Teardown

```bash
terraform destroy
```

The state backend survives by design — its bucket carries `prevent_destroy`, so
`terraform destroy` inside `bootstrap/` will refuse to run. Removing it is an
explicit manual decision: drop the lifecycle block, or empty and delete the
bucket outside Terraform.

---

## Repository layout

```text
.github/workflows/ci.yml      test, format, and validate on every push
bootstrap/                    one-time S3 state backend
data/                         source datasets
examples/                     sample execution input
glue_scripts/
  lakehouse_delta_etl.py      Spark ETL and Delta merge logic
  quality_checks.py           post-ETL quality gate
lambda/archive_files.py       raw-zone archival
tests/                        unit tests for both job modules
alerts.tf                     SNS topic and EventBridge failure rules
glue.tf                       Glue jobs, database, crawler, Athena workgroup
iam.tf                        least-privilege roles and policies
lambda.tf                     archival function
main.tf                       S3 bucket, hardening, lifecycle, uploads
outputs.tf
providers.tf
requirements-dev.txt          test-only Python dependencies
step_functions.tf             state machine definition
variables.tf
versions.tf                   provider constraints and S3 backend
```

---

**Stack** — Terraform · S3 · Glue (Spark and Python shell) · Delta Lake ·
Step Functions · Lambda · Glue Data Catalog · Athena · EventBridge · SNS ·
CloudWatch · IAM · Python · PySpark · GitHub Actions
