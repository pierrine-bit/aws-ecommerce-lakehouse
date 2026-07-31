# E-Commerce Lakehouse on AWS

A production-grade lakehouse for e-commerce transaction data, provisioned end to
end as Infrastructure as Code. The platform ingests raw product, order, and order
line-item files, enforces schema and business-rule validation, materialises
ACID-compliant Delta Lake tables on Amazon S3, registers them in the Glue Data
Catalog for SQL access through Athena, and archives consumed source files — all
orchestrated by a single Step Functions state machine with retry, failure
isolation, and email alerting.

The design goal was not simply to move data, but to make the pipeline
**trustworthy**: every stage is gated on the previous one succeeding, invalid
records are quarantined rather than dropped, and no raw file is archived until
the curated tables have been independently confirmed to hold rows.

---

## Contents

- [Architecture](#architecture)
- [Design Decisions](#design-decisions)
- [Source Data](#source-data)
- [Storage Layout](#storage-layout)
- [Transformation and Data Quality](#transformation-and-data-quality)
- [The Quality Gate](#the-quality-gate)
- [Orchestration](#orchestration)
- [Observability and Alerting](#observability-and-alerting)
- [Security Posture](#security-posture)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Running the Pipeline](#running-the-pipeline)
- [Analytics with Athena](#analytics-with-athena)
- [Testing and CI](#testing-and-ci)
- [Operational Runbook](#operational-runbook)
- [Cost Profile](#cost-profile)
- [Limitations and Future Work](#limitations-and-future-work)
- [Teardown](#teardown)
- [Repository Layout](#repository-layout)

---

## Architecture

Stages execute **sequentially**, each conditional on its predecessor succeeding.
Any failure short-circuits to a terminal failure state, which is itself an
observable event that triggers an alert.

```text
   ┌────────────────────────┐
   │  Raw e-commerce files  │
   │  (CSV / XLSX)          │
   └───────────┬────────────┘
               │  uploaded by Terraform
               ▼
       S3 Raw Zone  (raw/)
               │
               ▼
   ┌────────────────────────────────────────────────────────────┐
   │  Step Functions state machine  (STANDARD)                  │
   │                                                            │
   │   Glue Spark ETL ──────────▶ Delta Lake tables             │
   │        │                     (lakehouse-dwh/)              │
   │        ▼                                                   │
   │   Quality gate (Python shell)                              │
   │        │                                                   │
   │        ▼                                                   │
   │   Glue Delta crawler ──────▶ Glue Data Catalog ──▶ Athena   │
   │        │                                                   │
   │        ▼                                                   │
   │   Athena row validation  (per table, in parallel)          │
   │        │                                                   │
   │        ▼                                                   │
   │   Lambda archival ────────▶ archived/<timestamp>/…         │
   └───────────────────────┬────────────────────────────────────┘
                           │ on failure, timeout, or abort
                           ▼
                    EventBridge ──▶ SNS ──▶ email
```

Rejected records branch out of the ETL into `rejected/`, keeping the curated
tables clean without discarding evidence.

---

## Design Decisions

The reasoning behind the choices that shaped this implementation.

**Delta Lake over plain Parquet.** Order data arrives in monthly batches and the
same order can legitimately appear again in a later file. Delta's `MERGE` gives
idempotent upserts keyed on the natural business key, so re-running the pipeline
converges on the same state instead of duplicating rows. Plain Parquet would have
required either full-table rewrites or a bespoke deduplication layer.

**Quarantine, don't drop.** Rows failing validation are written to `rejected/`
with a `reject_reason` and `rejected_at` stamp rather than silently filtered out.
Data quality problems in e-commerce sources are usually upstream defects worth
investigating; a pipeline that hides them destroys the evidence needed to fix
them.

**A separate quality gate, not inline assertions.** Validation of the *outcome*
runs as its own job after the ETL. This separates "did the transformation logic
work" from "did the platform actually land trustworthy data", and it means the
gate can be reasoned about, tested, and re-run independently.

**Python shell, not Spark, for the gate.** The gate performs S3 listings and Glue
Catalog lookups — metadata operations with no distributed compute requirement.
Running it as a 1-DPU Python shell job instead of a Spark job removes both the
cluster start-up latency and the cost of workers that would sit idle.

**Row validation via a deliberately failing query.** The Athena stage runs
`SELECT 1 / COUNT(*)` against each table. When a table is empty, the division by
zero fails the query, which surfaces through the state machine's existing
`Retry`/`Catch` machinery. This avoids a separate stage to fetch, parse, and
branch on query results — the failure semantics we already handle become the
assertion mechanism.

**Archive last.** Raw files are only moved out of the landing zone after the
curated tables have been proven to exist, be catalogued, be fresh, and be
non-empty. If any of those checks fail, the source data remains exactly where it
was and the run can be retried without recovery work.

**Feature flags on optional stages.** The crawler and Athena validation stages
are toggleable. When disabled they are omitted from the state machine definition
entirely — the preceding stage's `Next` is rewired to the following enabled stage
— rather than being present-but-skipped. This keeps execution histories clean and
makes the deployed machine an honest reflection of what actually runs.

**Remote state with native S3 locking.** State lives in a versioned, encrypted S3
bucket created by a one-time bootstrap configuration. Terraform 1.10's
`use_lockfile` provides state locking directly in S3, removing the DynamoDB lock
table that older setups required — one less resource to provision, pay for, and
keep in sync.

---

## Source Data

| Dataset                    | Format | Grain                  |
| -------------------------- | ------ | ---------------------- |
| `products.csv`             | CSV    | One row per product    |
| `orders_apr_2025.xlsx`     | XLSX   | One row per order      |
| `order_items_apr_2025.xlsx`| XLSX   | One row per order line |

Schemas are declared explicitly in the ETL rather than inferred, so an upstream
column rename or type drift fails loudly at read time instead of propagating
silently into the curated layer.

```text
products                orders                  order_items
--------                ------                  -----------
product_id              order_num               id
department_id           order_id                order_id
department              user_id                 user_id
product_name            order_timestamp         days_since_prior_order
                        total_amount            product_id
                        date                    add_to_cart_order
                                                reordered
                                                order_timestamp
                                                date
```

---

## Storage Layout

A single bucket, partitioned by purpose:

```text
raw/                    landing zone, one prefix per dataset
├── products/
├── orders/
└── order_items/

lakehouse-dwh/          curated Delta Lake tables
├── products/
├── orders/
└── order_items/

scripts/                ETL and quality-check job code
rejected/               quarantined records, JSON, with reject reason
archived/               consumed raw files, timestamped
athena-results/         Athena query output
tmp/                    Glue scratch space
```

The bucket enforces versioning, SSE-S3 (AES256) encryption at rest, and a full
public-access block. Four lifecycle rules bound storage growth: `archived/`
expires after `archive_retention_days`, `rejected/` after
`rejected_retention_days`, noncurrent object versions after 30 days, and
incomplete multipart uploads are aborted after 7 days.

### Curated tables

| Table         | Merge key    | Partitioned by | Referential integrity          |
| ------------- | ------------ | -------------- | ------------------------------ |
| `products`    | `product_id` | `department`   | —                              |
| `orders`      | `order_id`   | `order_date`   | —                              |
| `order_items` | `id`         | `order_date`   | `product_id` → `products`, `order_id` → `orders` |

Partition columns were chosen to match the dominant query patterns —
department-level product analysis and date-ranged revenue reporting — so Athena
prunes partitions rather than scanning whole tables. Each table also carries an
`ingestion_timestamp`; `orders` and `order_items` additionally carry the parsed
`order_ts` and derived `order_date`.

---

## Transformation and Data Quality

The Glue Spark job processes the three datasets in dependency order — products,
then orders, then order items — because order-item referential integrity is
validated against the two tables written before it.

Each dataset passes through the same sequence:

1. **Read with an explicit schema.** CSV is read directly by Spark; XLSX is
   downloaded and parsed via pandas and `openpyxl`, since Spark has no native
   Excel reader.
2. **Type normalisation.** Numeric columns are coerced, and unparseable values
   become genuine SQL `NULL`s rather than floating-point `NaN`. This distinction
   matters: `NaN` silently passes an `isNull()` check and crashes an `int()`
   cast, so nullable columns are rebuilt as object-dtype Series to guarantee
   `None` survives into Spark.
3. **Required-column assertion.** Missing columns raise immediately, naming the
   table and the absent columns.
4. **Validation split.** Rows are partitioned into valid and invalid sets by null
   business keys, unparseable timestamps, and explicit business rules —
   `total_amount` must be non-negative, and `days_since_prior_order` may be null
   (a customer's first order) but never negative.
5. **Referential integrity.** For order items, semi- and anti-joins against the
   curated `products` and `orders` tables separate resolvable rows from orphans.
6. **Deduplication** on the merge key.
7. **Delta write.** First run creates a partitioned Delta table; subsequent runs
   `MERGE` on the business key, updating matched rows and inserting new ones.
8. **Catalog registration** of the table against the Glue Data Catalog.

Every stage logs read, rejected, and written row counts, so a run's data
lineage is reconstructable from CloudWatch alone.

---

## The Quality Gate

A Glue Python shell job asserts, for each of the three tables, that:

- a Delta transaction log exists under the table's `_delta_log/` prefix;
- the table is registered in the Glue Data Catalog; and
- the most recent Delta commit is no older than `max_data_age_hours`.

The freshness check is the important one. Presence alone is a weak assertion: a
Delta log left behind by a previous successful run would satisfy it even if
today's run wrote nothing at all. Comparing the newest commit timestamp against a
configurable threshold turns a silent no-op into a hard failure.

Any violation raises, failing the job and therefore the pipeline, so raw files
are never archived on the strength of stale or unregistered tables. AWS clients
use standard retry mode with exponential backoff so a transient API blip does not
fail the gate spuriously.

---

## Orchestration

A STANDARD Step Functions state machine, with full execution-data logging to
CloudWatch:

| # | State                                          | Type       | Notes                                                    |
| - | ---------------------------------------------- | ---------- | -------------------------------------------------------- |
| 1 | `SimulateFileArrival`                          | Pass       | Documents the expected raw-zone contract                 |
| 2 | `RunDeltaLakeETL`                              | Task       | Glue job, synchronous; 30 min job timeout                |
| 3 | `RunQualityChecks`                             | Task       | Glue job, synchronous; 10 min job timeout                |
| 4 | `RunCatalogCrawler`                            | Task       | Optional; tolerates an already-running crawler           |
| 5 | `PrepareAthenaValidation` / `RunAthenaValidation` | Pass + Map | Optional; validates 3 tables concurrently              |
| 6 | `ArchiveRawFiles`                              | Task       | Lambda, 3 min timeout                                    |
| 7 | `PipelineSucceeded` / `PipelineFailed`         | Terminal   | Failure cause names where to look                        |

State-level timeouts sit above the job timeouts as a backstop, so a task that
somehow stops reporting cannot hold an execution open indefinitely.

Resilience is layered deliberately. Glue tasks retry twice with exponential
backoff on service and task failures; the Lambda invocation retries three times
on the AWS-recommended set of transient Lambda errors; the crawler stage catches
`Glue.CrawlerRunningException` and proceeds rather than failing a run because a
previous crawl is still finishing. Everything else catches `States.ALL` and
routes to `PipelineFailed`, so no error path is unhandled.

---

## Observability and Alerting

**Logging.** Dedicated CloudWatch log groups for Glue and Step Functions, both
with 14-day retention. The ETL job streams continuous logs and publishes job
metrics; the state machine logs at `ALL` with execution data included.

**Alerting.** Two EventBridge rules publish to an SNS topic:

- **Glue job failure** — either job entering `FAILED` or `TIMEOUT`, whether
  invoked by the pipeline or manually. Catching this independently means a
  hand-triggered job failure is not invisible.
- **Pipeline failure** — the state machine entering `FAILED`, `TIMED_OUT`, or
  `ABORTED`, regardless of which state raised the error.

Setting `alert_email` subscribes an address; the topic ARN is also exported as an
output so further subscribers (Slack relay, Lambda, SQS) can be attached without
modifying this configuration.

---

## Security Posture

- **Three narrowly scoped IAM roles** — one each for Glue, Lambda, and Step
  Functions — rather than a single shared role. Each is restricted to the specific
  S3 prefixes, Glue Catalog resources, and job or function ARNs it needs.
- **The archival Lambda cannot read the curated zone.** Its policy grants
  `GetObject`/`DeleteObject` only under `raw/`, `PutObject` only under
  `archived/`, and prefix-conditioned `ListBucket`.
- **Athena queries execute as the Step Functions role**, so that role — not a
  broader principal — carries the Catalog read and results-write permissions.
- **Encryption at rest** via SSE-S3, with all public access blocked.
- **Remote state is encrypted and versioned**, with locking enforced.

One notable concession is documented in code: Spark's S3 filesystem writes
zero-byte `<prefix>_$folder$` directory markers at the bucket root, which no
`<prefix>/*` pattern matches. Those exact keys are granted explicitly rather than
widening the policy to the whole bucket.

---

## Configuration

Every variable has a working default, so `terraform apply` succeeds against an
unmodified `terraform.tfvars`. Override in `terraform.tfvars`.

| Variable                    | Default               | Purpose                                                           |
| --------------------------- | --------------------- | ----------------------------------------------------------------- |
| `aws_region`                | `eu-west-1`           | Deployment region                                                 |
| `project_name`              | `ecommerce-lakehouse` | Prefix for all resource names                                     |
| `bucket_name`               | *generated*           | Lakehouse bucket; defaults to `<project>-<account>-<region>`       |
| `local_data_dir`            | `./data`              | Local folder holding the raw source files                         |
| `crawler_enabled`           | `true`                | Include the Glue crawler stage                                    |
| `athena_validation_enabled` | `true`                | Include the Athena row-validation stage                           |
| `max_data_age_hours`        | `24`                  | Age at which the gate treats a Delta table as stale               |
| `archive_retention_days`    | `90`                  | Retention for `archived/` before S3 expiry                        |
| `rejected_retention_days`   | `30`                  | Retention for `rejected/` before S3 expiry                        |
| `alert_email`               | `""`                  | Alert recipient; **no alerts are delivered while unset**          |

Setting `alert_email` creates an SNS email subscription that must be confirmed
via the link AWS sends before alerts begin arriving.

---

## Deployment

### Prerequisites

| Requirement | Version | Notes                                                    |
| ----------- | ------- | -------------------------------------------------------- |
| Terraform   | ≥ 1.10.0 | Required for native S3 state locking (`use_lockfile`)   |
| AWS CLI     | v2      | Configured with credentials able to create the resources |
| Python      | 3.11    | Only needed to run the unit tests locally                |
| Git         | any     | —                                                        |

```bash
aws configure
aws sts get-caller-identity    # confirm the target account
```

### 1. Bootstrap the state backend

A one-time configuration that creates the versioned, encrypted S3 bucket holding
Terraform state. The bucket name must match the `backend "s3"` block in
`versions.tf`.

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
cp terraform.tfvars.example terraform.tfvars   # then edit — see Configuration

terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

Terraform provisions the bucket and its hardening, both Glue jobs, the Catalog
database, the Delta crawler, the Athena workgroup, the archival Lambda, the state
machine, the SNS topic and EventBridge rules, all IAM roles and policies, and the
CloudWatch log groups — then uploads the three source datasets and both job
scripts.

---

## Running the Pipeline

```bash
aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw state_machine_arn) \
  --input file://examples/start-execution.json
```

Useful outputs: `s3_bucket`, `glue_database`, `glue_jobs`, `crawler_name`,
`athena_workgroup`, `state_machine_arn`, `pipeline_alerts_topic_arn`.

---

## Analytics with Athena

Query through the provisioned workgroup, which enforces its own results location.

Product distribution by department:

```sql
SELECT department,
       COUNT(*) AS product_count
FROM ecommerce_lakehouse.products
GROUP BY department
ORDER BY product_count DESC;
```

Daily revenue — partition-pruned on `order_date`:

```sql
SELECT order_date,
       SUM(total_amount) AS revenue,
       COUNT(DISTINCT order_id) AS orders
FROM ecommerce_lakehouse.orders
WHERE order_date BETWEEN DATE '2025-04-01' AND DATE '2025-04-30'
GROUP BY order_date
ORDER BY order_date;
```

Top departments by line-item volume, joining across the curated tables:

```sql
SELECT p.department,
       COUNT(*) AS line_items,
       SUM(CAST(oi.reordered AS BIGINT)) AS reorders
FROM ecommerce_lakehouse.order_items oi
JOIN ecommerce_lakehouse.products p
  ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY line_items DESC
LIMIT 10;
```

---

## Testing and CI

The ETL and quality-check modules deliberately separate their pure
transformation and validation logic from `bootstrap()`, which resolves Glue job
arguments and builds AWS clients. Because `awsglue` only exists inside a live
Glue job, this split is what makes the business logic unit-testable at all — the
tests exercise the real functions with a local Spark session and mocked AWS
clients rather than asserting against a deployed environment.

```bash
pip install -r requirements-dev.txt
pytest -q
```

Test dependencies are declared in `requirements-dev.txt` rather than inline in
the workflow, so the local and CI environments cannot drift — an undeclared
dependency that happens to be present on a workstation is exactly the kind of
gap that turns a green local run into a red pipeline.

Spark-dependent tests skip automatically when PySpark or a JVM is unavailable, so
the suite stays runnable on a workstation without a local Spark install while
still executing in full in CI.

GitHub Actions runs on every push and pull request to `main`:

1. Unit tests, with Java, PySpark, and Delta Lake installed so no test is skipped
2. `terraform fmt -check -recursive`
3. `terraform init -backend=false` — validation needs no AWS credentials
4. `terraform validate`

Running the tests before the Terraform checks means a logic regression fails fast
rather than waiting on provider initialisation.

---

## Operational Runbook

| Symptom                                            | Likely cause and resolution                                                                                                  |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `PipelineFailed` immediately after ETL             | Inspect the Glue job run in the `/aws-glue/ecommerce-lakehouse` log group; per-stage logs name the dataset that failed.       |
| Quality gate reports missing Delta logs            | The ETL wrote nothing. Confirm the raw zone is populated — a prior run may have already archived the files.                   |
| Quality gate reports stale tables                  | The newest commit is older than `max_data_age_hours`. Expected when re-running the gate alone; raise the threshold or re-run the ETL. |
| Quality gate reports missing Catalog entries       | Catalog registration failed. Verify the Glue job has `--enable-glue-datacatalog` and the role holds `glue:CreateTable`.       |
| Athena validation fails for one table              | That table is empty. The ETL likely rejected every row — inspect `rejected/<table>/` for the reject reasons.                  |
| Unexpectedly high rejection counts                 | Read `reject_reason` in `rejected/<table>/`; these are upstream data defects, preserved for exactly this purpose.             |
| No alert emails                                    | `alert_email` is unset, or the SNS subscription confirmation link was never clicked.                                          |
| Second run finds an empty raw zone                 | By design — archival moves consumed files to `archived/<timestamp>/`. Restore from there or re-upload to re-run.              |

---

## Cost Profile

The platform is deliberately sized for the workload rather than for headroom.
The main levers:

- **ETL compute** dominates: two `G.1X` workers on Glue 4.0, capped by a 30-minute
  job timeout.
- **The quality gate runs on 1 DPU as a Python shell job**, avoiding a Spark
  cluster for what are purely metadata calls.
- **Athena is charged by bytes scanned**, which partitioning on `order_date` and
  `department` directly reduces.
- **S3 growth is bounded** by the four lifecycle rules rather than accumulating
  indefinitely.
- **State locking uses S3 natively**, eliminating a DynamoDB table.

`force_destroy` is enabled on the bucket so that teardown is complete and leaves
nothing billable behind — appropriate for a project environment, and something to
reconsider before production use.

---

## Limitations and Future Work

Stated plainly, since knowing the boundaries of a design is part of it:

- **Ingestion is batch and manually triggered.** `SimulateFileArrival` is a
  placeholder; an S3 event notification or EventBridge schedule would make this
  event-driven.
- **The Excel path is not horizontally scalable.** XLSX files are parsed on the
  driver via pandas, so a file larger than driver memory would fail. Converting
  sources to CSV or Parquet upstream would remove the constraint.
- **The state backend bucket name is hard-coded** in `versions.tf`, as Terraform
  does not permit variables in backend blocks. Partial backend configuration via
  `-backend-config` would make it portable across accounts.
- **Quality checks assert freshness and presence, not distributions.** Column-level
  profiling, null-rate thresholds, or a framework such as Deequ or Great
  Expectations would deepen the gate.
- **Rejected records are written but not surfaced.** A rejection-rate alarm would
  turn quarantined data from a passive artifact into an active signal.
- **No table maintenance.** Delta `OPTIMIZE` and `VACUUM` are not scheduled; small
  files would accumulate across many incremental runs.

---

## Teardown

```bash
terraform destroy
```

The state backend is a separate configuration and survives by design. Its bucket
carries `prevent_destroy`, so `terraform destroy` inside `bootstrap/` will refuse
to run — deliberately, since the bucket must outlive the environments whose state
it holds. Removing it is an explicit, manual decision: drop the lifecycle block,
or empty and delete the bucket outside Terraform.

---

## Repository Layout

```text
aws-ecommerce-lakehouse/
│
├── .github/workflows/ci.yml      test, format, and validate on every push
├── bootstrap/                    one-time S3 state backend
├── data/                         source datasets
├── examples/                     sample state machine execution input
├── glue_scripts/
│   ├── lakehouse_delta_etl.py    Spark ETL and Delta merge logic
│   └── quality_checks.py         post-ETL quality gate
├── lambda/archive_files.py       raw-zone archival
├── tests/                        unit tests for both job modules
├── alerts.tf                     SNS topic and EventBridge failure rules
├── glue.tf                       Glue jobs, database, crawler, Athena workgroup
├── iam.tf                        least-privilege roles and policies
├── lambda.tf                     archival function
├── main.tf                       S3 bucket, hardening, lifecycle, uploads
├── outputs.tf
├── providers.tf
├── requirements-dev.txt          test-only Python dependencies
├── step_functions.tf             state machine definition
├── terraform.tfvars.example
├── variables.tf
├── versions.tf                   provider constraints and S3 backend
└── README.md
```

---

## Technology Stack

Terraform · Amazon S3 · AWS Glue (Spark and Python shell) · Delta Lake ·
AWS Step Functions · AWS Lambda · Glue Data Catalog · Amazon Athena ·
Amazon EventBridge · Amazon SNS · CloudWatch · IAM · Python · PySpark ·
GitHub Actions
