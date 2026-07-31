resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/${local.name}"
  retention_in_days = 14
  tags              = local.tags
}

resource "aws_glue_catalog_database" "lakehouse" {
  name        = replace(local.name, "-", "_")
  description = "Glue Data Catalog database for e-commerce Delta Lake tables."

  # Spark resolves the database location when registering a table against the
  # Glue Data Catalog. Left unset, it hands Hadoop an empty string and the ETL
  # dies with "Can not create a Path from an empty string".
  location_uri = "s3://${aws_s3_bucket.lakehouse.bucket}/${local.processed_prefix}/"
}

resource "aws_glue_job" "delta_etl" {
  name              = "${local.name}-delta-etl"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 1
  tags              = local.tags

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${local.scripts_prefix}/lakehouse_delta_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"          = "s3://${aws_s3_bucket.lakehouse.bucket}/tmp/"
    "--bucket"           = aws_s3_bucket.lakehouse.bucket
    "--database"         = aws_glue_catalog_database.lakehouse.name
    "--raw_prefix"       = local.raw_prefix
    "--processed_prefix" = local.processed_prefix
    "--rejected_prefix"  = "rejected"
    "--datalake-formats" = "delta"
    "--conf"             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    # Without this, Spark SQL uses an in-memory metastore and the script's
    # CREATE TABLE registers into a catalog that dies with the job - leaving
    # the Data Catalog empty for the downstream quality gate.
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue.name
    "--continuous-log-logStreamPrefix"   = "delta-etl"
    "--enable-metrics"                   = "true"
    "--additional-python-modules"        = "openpyxl==3.1.5,pandas==1.5.3"
  }
}

resource "aws_glue_job" "quality_checks" {
  name         = "${local.name}-quality-checks"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"
  max_capacity = 1
  timeout      = 10
  max_retries  = 1
  tags         = local.tags

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${local.scripts_prefix}/quality_checks.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--bucket"             = aws_s3_bucket.lakehouse.bucket
    "--processed_prefix"   = local.processed_prefix
    "--database"           = aws_glue_catalog_database.lakehouse.name
    "--max_data_age_hours" = tostring(var.max_data_age_hours)
  }
}

resource "aws_glue_crawler" "delta_crawler" {
  name          = "${local.name}-delta-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.lakehouse.name
  tags          = local.tags

  delta_target {
    delta_tables   = ["s3://${aws_s3_bucket.lakehouse.bucket}/${local.processed_prefix}/"]
    write_manifest = true
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_athena_workgroup" "lakehouse" {
  name = "${local.name}-athena"
  # Athena treats a workgroup holding query-execution history as non-empty and
  # refuses to delete it, so teardown needs the recursive delete option.
  force_destroy = true
  tags          = local.tags

  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.bucket}/athena-results/"
    }
  }
}
