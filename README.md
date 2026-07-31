# E-Commerce Lakehouse on AWS

[![CI](https://github.com/pierrine-bit/aws-ecommerce-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/pierrine-bit/aws-ecommerce-lakehouse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Terraform](https://img.shields.io/badge/Terraform-%E2%89%A5%201.10-7B42BC)](versions.tf)

A lakehouse for e-commerce transaction data, provisioned end to end with
Terraform. It validates raw product, order, and order-item files, materialises
ACID Delta Lake tables on S3, registers them for SQL access through Athena, and
archives consumed source files — orchestrated by one Step Functions state machine
with retries, quarantined rejects, and email alerting on failure.

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

Stages run sequentially, each conditional on the one before it. Raw files are
archived only after the curated tables are confirmed non-empty, so a failed run
leaves the source data untouched and retryable.

**Design notes**

- **Delta Lake** rather than plain Parquet, so `MERGE` makes re-runs idempotent.
- **Rejects are quarantined, not dropped** — data defects are usually upstream
  problems worth investigating.
- **The Athena row check is `SELECT 1 / COUNT(*)`**, so an empty table surfaces
  through the retry/catch machinery already in place.

---

## Data model

Sources: `products.csv`, `orders_apr_2025.xlsx`, `order_items_apr_2025.xlsx`.
Schemas are declared explicitly, never inferred, so upstream drift fails at read
time rather than propagating into the curated layer.

| Table | Merge key | Partitioned by | Referential integrity |
| ----- | --------- | -------------- | --------------------- |
| `products` | `product_id` | `department` | — |
| `orders` | `order_id` | `order_date` | — |
| `order_items` | `id` | `order_date` | `product_id` → `products`, `order_id` → `orders` |

```text
raw/                 landing zone
lakehouse-dwh/       curated Delta tables
rejected/            quarantined rows, with reject reason
archived/            consumed raw files, timestamped
scripts/             job code
athena-results/      query output
```

The bucket enforces versioning, SSE-S3 encryption, and a public-access block.
Lifecycle rules expire `archived/`, `rejected/`, and noncurrent versions on
configured schedules.

---

## Pipeline

**ETL** (Glue Spark) processes products, then orders, then order items — the last
validated against the two tables written before it. Per dataset: read with an
explicit schema, normalise types, assert required columns, split valid from
invalid on null keys and business rules, check referential integrity,
deduplicate, then `MERGE` into Delta and register in the Catalog.

**Quality gate** runs on 1 DPU as a Glue Python shell job, since it only makes S3
and Catalog metadata calls. It asserts that each table has a Delta transaction
log, is registered in the Catalog, and has a commit no older than
`max_data_age_hours`. The freshness check matters most: presence alone would pass
on a log left by a previous run even if today's wrote nothing.

**Orchestration** (Step Functions) retries Glue tasks with exponential backoff,
retries the Lambda on transient errors, tolerates an already-running crawler, and
routes every other error to a terminal failure state that raises an alert.

---

## Configuration

Every variable has a working default, so `terraform apply` succeeds against an
unmodified `terraform.tfvars`.

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `aws_region` | `eu-west-1` | Deployment region |
| `project_name` | `ecommerce-lakehouse` | Prefix for all resource names |
| `bucket_name` | *generated* | Defaults to `<project>-<account>-<region>` |
| `local_data_dir` | `./data` | Local folder holding the raw source files |
| `crawler_enabled` | `true` | Include the crawler stage |
| `athena_validation_enabled` | `true` | Include the row-check stage |
| `max_data_age_hours` | `24` | Age at which the gate treats a table as stale |
| `archive_retention_days` | `90` | Retention for `archived/` |
| `rejected_retention_days` | `30` | Retention for `rejected/` |
| `alert_email` | `""` | Alert recipient; **no alerts are sent while unset** |

Setting `alert_email` creates an SNS subscription that must be confirmed via the
link AWS sends before alerts arrive.

---

## Getting started

Requires Terraform ≥ 1.10 (for `use_lockfile` state locking), AWS CLI v2, and
Python 3.11 if you want to run the tests.

### 1. Bootstrap the state backend

One-time setup of the bucket holding Terraform state. Its name must match the
`backend "s3"` block in `versions.tf`.

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

---

## Querying

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

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest -q
```

Both job modules keep their transformation logic separate from the Glue argument
resolution, so the logic is testable without a live Glue context. Spark tests skip
automatically when PySpark or a JVM is missing. CI runs the tests, then
`terraform fmt -check`, `init -backend=false`, and `validate` on every push.

---

## Teardown

```bash
terraform destroy
```

The state bucket carries `prevent_destroy` and survives by design; removing it is
a deliberate manual step.

---

## Repository layout

```text
bootstrap/           one-time S3 state backend
data/                source datasets
glue_scripts/        Spark ETL and quality-gate jobs
lambda/              archival handler code
tests/               unit tests for both job modules
alerts.tf            SNS topic and EventBridge failure rules
glue.tf              Glue jobs, database, crawler, Athena workgroup
iam.tf               least-privilege roles and policies
lambda.tf            archival function resource
main.tf              S3 bucket, hardening, lifecycle, uploads
step_functions.tf    state machine definition
versions.tf          provider constraints and S3 backend
```

## Known limitations

Ingestion is batch and manually triggered. XLSX parsing happens on the Spark
driver, so it does not scale to files beyond driver memory. The state bucket name
is hard-coded, as Terraform forbids variables in backend blocks. Delta `OPTIMIZE`
and `VACUUM` are not scheduled.
