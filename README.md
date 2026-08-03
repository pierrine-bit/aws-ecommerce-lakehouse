# E-Commerce Lakehouse on AWS

An AWS lakehouse for processing e-commerce transaction data. Raw product, order,
and order-item datasets are stored in Amazon S3, validated and deduplicated by AWS
Glue Spark jobs, and written as Delta Lake tables for analytics through the AWS
Glue Data Catalog and Amazon Athena. AWS Step Functions orchestrates the ETL
workflow, while Terraform provisions the infrastructure and GitHub Actions runs automated tests and Terraform validation on every push.

## Architecture

```text
          S3 Raw
            │
            ▼
┌───────────────────────┐
│ AWS Step Functions    │
│                       │
│ 1. Glue Spark ETL     │
│ 2. Quality Gate       │
│ 3. Glue Crawler       │
│ 4. Athena Validation  │
│ 5. Archive Files      │
└───────────────────────┘
      │           │
      │           ├──────► S3 Rejected  
      │           └──────► S3 Archived 
      ▼
  Delta Lake
      │
      ▼
 Glue Catalog
      │
      ▼
    Athena

Failure at any stage ──────► EventBridge → SNS
```

Each stage depends on the one before it. If something fails the workflow stops and
raises an SNS alert. The merge is keyed on business IDs, so re-running a batch
doesn't add duplicates.

## Project structure

```text
aws-ecommerce-lakehouse/
├── main.tf
├── glue.tf
├── step_functions.tf
├── lambda.tf
├── alerts.tf
├── iam.tf
├── variables.tf
├── outputs.tf
├── providers.tf
├── versions.tf
├── terraform.tfvars.example
├── requirements-dev.txt
├── bootstrap/
├── data/
├── examples/
├── tests/
├── glue_scripts/
│   ├── lakehouse_delta_etl.py
│   └── quality_checks.py
├── lambda/
│   └── archive_files.py
└── .github/workflows/
    └── ci.yml
```

## Data model

```text
┌──────────────────────────────────────────┐
│                 PRODUCTS                 │
├──────────────────────────────────────────┤
│ bigint  product_id     PK (Merge Key)    │
│ string  department     Partition Key     │
└──────────────────────────────────────────┘
                     │
                     │ 1 product supplies N order items
                     ▼
┌──────────────────────────────────────────┐
│               ORDER_ITEMS                │
├──────────────────────────────────────────┤
│ bigint  id             PK (Merge Key)    │
│ bigint  order_id       FK → ORDERS       │
│ bigint  product_id     FK → PRODUCTS     │
│ bigint  user_id                          │
│ date    order_date     Partition Key     │
└──────────────────────────────────────────┘
                     ▲
                     │ 1 order contains N order items
                     │
┌──────────────────────────────────────────┐
│                  ORDERS                  │
├──────────────────────────────────────────┤
│ bigint  order_id       PK (Merge Key)    │
│ date    order_date     Partition Key     │
└──────────────────────────────────────────┘
```

The sources are `products.csv`, `orders_apr_2025.xlsx` and
`order_items_apr_2025.xlsx`. Each order item points at both an order and a product,
so products and orders load first and referential integrity is checked during
validation. An item whose parents can't be resolved is rejected rather than written.

Schema inference is off. Each dataset uses an explicit Spark schema, so an
unexpected column or a changed type fails the read rather than reaching the curated
layer. Partitions follow the queries people actually run, which keeps Athena scans
down.

Delta rather than plain Parquet, because monthly batches can legitimately resend an
order. Merging on the business key means a replayed batch converges on the same
tables instead of duplicating rows. With Parquet you'd need full rewrites or a
separate dedup step to get the same result.

## Pipeline

### ETL

Glue Spark reads each dataset against its schema, validates types and mandatory
fields, separates valid rows from rejected ones, deduplicates on the merge key,
then merges into Delta. CSV goes straight into Spark. The Excel workbooks have to
go through pandas first, since Spark can't read them natively.

| Dataset | Rejected when |
| ------- | ------------- |
| `products` | `product_id` is null |
| `orders` | `order_id` or `user_id` null · `order_timestamp` unparseable · `total_amount` null or negative |
| `order_items` | any key null · `order_timestamp` unparseable · `days_since_prior_order` negative · `product_id` or `order_id` unresolved |

### Quality gate

This one runs as a 1-DPU Python shell job rather than Spark, because it only reads
S3 and Catalog metadata and a cluster would sit idle. It checks that Delta
transaction logs exist, that Catalog tables are available, and that the latest
commit is inside the freshness threshold.

Freshness is the check that does the real work. Without it, a transaction log left
behind by an earlier run would satisfy a presence check even if today's run wrote
nothing at all. When the gate fails the workflow stops and the raw files stay where
they are.

### Catalog and archival

A Glue Crawler refreshes the Catalog, then an Athena query confirms each table has
rows. Only after that does a Lambda move the processed files into a timestamped
folder under `archived/`.

The row check is `SELECT 1 / COUNT(*)`. An empty table divides by zero, so
emptiness surfaces through the same retry handling as any other query error and
doesn't need a stage of its own to fetch and inspect results.

Archiving last is deliberate. Since the raw zone is only cleared once the curated
tables are proven usable, a failed run leaves the source data untouched and can
just be retried.

## Security

Glue, Lambda and Step Functions each get their own IAM role, holding only what they
need. The archival Lambda can move files from `raw/` to `archived/` but has no
access to the curated data in `lakehouse-dwh/`.

On the bucket: SSE-S3 encryption, versioning, and public access blocked on every
setting. Terraform state sits in a remote backend with locking, so two applies
can't collide.

## Testing

```bash
pip install -r requirements-dev.txt
pytest -q
```

The ETL logic is kept separate from Glue job initialization, which is what makes it
testable without a live Glue context. Spark-dependent tests skip themselves when
there's no local Spark.

## Configuration

Every Terraform variable has a working default, so the stack deploys as-is.
`terraform.tfvars.example` lists them all. Worth knowing: `alert_email` starts
empty, so the SNS topic and rules get created but nothing actually reaches you
until you set an address and confirm the subscription.

## Deployment

Requires AWS CLI v2, Terraform, and Python 3.x for the tests.

### 1. Create the remote backend

Terraform won't take a variable inside a `backend` block, so the state bucket name
is written directly into `versions.tf`. Set that and `state_bucket_name` below to
the same globally unique name, or step 2 can't initialise. Run once per account.

```bash
cd bootstrap
cat > terraform.tfvars <<'EOF'
aws_region        = "eu-west-1"
state_bucket_name = "ecommerce-lakehouse-tfstate-<your-account-id>"
EOF
terraform init && terraform apply
cd ..
```

### 2. Deploy the infrastructure

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
```

Terraform uploads the datasets and Glue scripts as part of the apply, so there's
nothing to copy into S3 by hand.

### 3. Run the pipeline

```bash
ARN=$(aws stepfunctions start-execution \
  --state-machine-arn $(terraform output -raw state_machine_arn) \
  --input file://examples/start-execution.json \
  --query executionArn --output text)

aws stepfunctions describe-execution --execution-arn $ARN --query status
```

Status hits `SUCCEEDED` after a few minutes, most of that Glue cluster start-up.
Re-running is safe, but a successful run empties the raw zone, so restore from
`archived/<timestamp>/` if you want to reprocess the same batch.

## Querying

Query through the `ecommerce-lakehouse-athena` workgroup. It pins its own results
location, so anything you run from the default `primary` workgroup ends up
somewhere else.

```sql
SELECT order_date, SUM(total_amount) AS revenue
FROM ecommerce_lakehouse.orders
GROUP BY order_date
ORDER BY order_date;
```

## Teardown

```bash
terraform destroy
```

The Athena workgroup needs `force_destroy`. Query execution history is enough to
count as non-empty, which isn't obvious from the error you get back. The state
bucket keeps `prevent_destroy`, so removing it is a manual step.