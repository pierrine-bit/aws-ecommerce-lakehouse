# E-Commerce Lakehouse on AWS

[![CI](https://github.com/pierrine-bit/aws-ecommerce-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/pierrine-bit/aws-ecommerce-lakehouse/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Terraform](https://img.shields.io/badge/Terraform-%E2%89%A5%201.10-7B42BC)](versions.tf)

## Introduction

This project builds a batch lakehouse for e-commerce transaction data, deployed
end to end with Terraform. Raw product, order, and order-item files land in S3,
are schema-validated and deduplicated by a Glue Spark job, and are merged into
ACID Delta Lake tables registered in the Glue Data Catalog for querying through
Athena. Once the curated layer is verified, the consumed source files are
archived.

The engineering emphasis is on **trust rather than throughput**. A pipeline that
moves data is straightforward; one whose output can be relied on without manual
inspection is not. Every stage is gated on its predecessor, invalid rows are
quarantined with their rejection reason rather than filtered away, and the raw
zone is only cleared after the curated tables have been independently proven to
hold queryable data.

Concretely, the design guarantees:

- **Idempotent re-runs.** `MERGE` on business keys means replaying a batch
  converges on the same tables rather than duplicating rows.
- **Fail-closed archival.** Raw files move only once the curated tables are
  proven present, catalogued, fresh, and non-empty. A failed run leaves the
  source data exactly where it was.
- **No silent data loss.** Every rejected row is persisted with a reason and
  timestamp, so a quality problem remains diagnosable after the fact.
- **Contained blast radius.** Each component assumes its own role; the archival
  Lambda holds no read access to the curated zone.

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

Stages execute sequentially, each conditional on the one before it. Any failure
short-circuits to a terminal state, which is itself an observable event.

---

## Design decisions

| Decision | Rationale | Accepted cost |
| -------- | --------- | ------------- |
| Delta Lake over plain Parquet | Batches can legitimately resend an order; `MERGE` upserts on the business key make re-runs idempotent | A Delta runtime dependency, and small-file growth without scheduled `OPTIMIZE` |
| Quarantine rejects rather than drop them | Data defects are usually upstream problems; filtering destroys the evidence needed to fix them | Storage cost, and the rejects need monitoring to become an active signal |
| Quality gate as a 1-DPU Python shell job | It performs only S3 and Catalog metadata calls, so a Spark cluster would sit idle | Cannot inspect row-level distributions — presence and freshness only |
| Row check via `SELECT 1 / COUNT(*)` | Division by zero makes emptiness surface through the retry/catch machinery already in place | The failure reads as an arithmetic error until you know the idiom |
| Native S3 state locking | Terraform 1.10's `use_lockfile` removes the DynamoDB lock table entirely | Hard floor of Terraform ≥ 1.10 for every contributor |

---

## Data model

Sources are `products.csv`, `orders_apr_2025.xlsx`, and
`order_items_apr_2025.xlsx`. Schemas are declared explicitly rather than
inferred, so an upstream rename or type drift fails loudly at read time instead
of propagating downstream.

| Table | Merge key | Partitioned by | Referential integrity |
| ----- | --------- | -------------- | --------------------- |
| `products` | `product_id` | `department` | — |
| `orders` | `order_id` | `order_date` | — |
| `order_items` | `id` | `order_date` | `product_id` → `products`, `order_id` → `orders` |

Partition columns follow the dominant access patterns — product analysis by
department, revenue by date — so Athena prunes partitions instead of scanning
whole tables.

```text
raw/                 landing zone
lakehouse-dwh/       curated Delta tables
rejected/            quarantined rows, with reject reason
archived/            consumed raw files, timestamped
scripts/             job code
athena-results/      query output
```

Versioning, SSE-S3 encryption, and a full public-access block are enforced.
Lifecycle rules bound growth on `archived/`, `rejected/`, noncurrent versions,
and abandoned multipart uploads.

---

## How it works

### ETL

Glue 4.0 Spark on two `G.1X` workers. Datasets are processed in dependency
order — products, then orders, then order items, the last validated against the
two tables written before it. Per dataset:

1. Read against an explicit schema.
2. Normalise types, so unparseable numerics become SQL `NULL` rather than `NaN`.
3. Assert required columns are present.
4. Split valid from invalid on null business keys and business rules.
5. Resolve referential integrity by semi- and anti-join.
6. Deduplicate on the merge key.
7. `MERGE` into Delta and register the table in the Catalog.

Read, rejected, and written counts are logged per dataset, making a run's lineage
reconstructable from CloudWatch alone.

### Quality gate

A 1-DPU Glue Python shell job asserting that each table holds a Delta
transaction log, is registered in the Catalog, and carries a commit no older than
`max_data_age_hours`. Freshness is the load-bearing check: presence alone would
pass on a transaction log left by a previous successful run, even if today's run
wrote nothing at all.

### Orchestration

A STANDARD state machine with layered resilience. Glue tasks retry twice with
exponential backoff, the Lambda three times on the AWS-recommended transient
error set, and the crawler stage catches `Glue.CrawlerRunningException` and
proceeds rather than failing a run because a previous crawl is still finishing.
Every other error routes to a terminal failure state, so no path is unhandled.

---

## Deployment

Requires Terraform ≥ 1.10 (for `use_lockfile` state locking), AWS CLI v2, and
Python 3.11 to run the tests.

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

### 2. Deploy the lakehouse

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

Both job modules separate their transformation logic from the Glue argument
resolution. Since `awsglue` exists only inside a live Glue job, that split is
what makes the business logic testable at all — the suite exercises the real
functions against a local Spark session and mocked AWS clients. Spark tests skip
when PySpark or a JVM is absent, so the suite stays runnable locally while
executing in full in CI.

CI runs the tests first, so a logic regression fails fast, then
`terraform fmt -check`, `init -backend=false`, and `terraform validate`.

---

## Teardown

```bash
terraform destroy
```

Two constraints are worth knowing, both encoded in the configuration:

- **The Athena workgroup needs recursive deletion.** Athena treats a workgroup
  holding query-execution history as non-empty and refuses to delete it, so
  `force_destroy = true` is set on the resource.
- **The state bucket is protected by design.** It carries `prevent_destroy`
  because it must outlive the environments whose state it holds. That guard binds
  Terraform only — the S3 API is not constrained by it — and the bucket has no
  `force_destroy`, so every object version must be removed before deletion.

---

## Known limitations

Ingestion is batch and manually triggered; an S3 notification or schedule would
make it event-driven. XLSX parsing happens on the Spark driver, so it does not
scale beyond driver memory. The state bucket name is hard-coded, as Terraform
forbids variables in backend blocks. The gate validates presence and freshness
but not distributions, and Delta `OPTIMIZE`/`VACUUM` are unscheduled, so small
files accumulate over many incremental runs.

---

Licensed under the [MIT License](LICENSE).
