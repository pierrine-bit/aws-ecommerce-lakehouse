data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_inline" {
  statement {
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/${local.raw_prefix}/*",
      "${aws_s3_bucket.lakehouse.arn}/${local.processed_prefix}/*",
      "${aws_s3_bucket.lakehouse.arn}/rejected/*",
      "${aws_s3_bucket.lakehouse.arn}/${local.scripts_prefix}/*",
      "${aws_s3_bucket.lakehouse.arn}/tmp/*",

      # Spark's S3 filesystem writes a zero-byte "<prefix>_$folder$" directory
      # marker for the parent of each write path. Those keys sit at the bucket
      # root with no slash, so the "<prefix>/*" patterns above never match them
      # and the Delta write fails with AccessDenied.
      "${aws_s3_bucket.lakehouse.arn}/${local.raw_prefix}_$folder$",
      "${aws_s3_bucket.lakehouse.arn}/${local.processed_prefix}_$folder$",
      "${aws_s3_bucket.lakehouse.arn}/rejected_$folder$",
      "${aws_s3_bucket.lakehouse.arn}/tmp_$folder$",
    ]
  }

  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
      # DeleteTable backs the script's "DROP TABLE IF EXISTS" before it
      # re-registers each Delta table; without it, re-runs fail once the
      # catalog entries already exist.
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition"
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lakehouse.name}",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*",
    ]
  }

  statement {
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_inline" {
  name   = "${local.name}-glue-inline"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_inline.json
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.name}-archive-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = local.tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_s3" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.raw_prefix}/*", "${local.archive_prefix}/*"]
    }
  }

  statement {
    actions   = ["s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/${local.raw_prefix}/*"]
  }

  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/${local.archive_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "lambda_s3" {
  name   = "${local.name}-archive-lambda-s3"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_s3.json
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "${local.name}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  tags               = local.tags
}

data "aws_iam_policy_document" "sfn_inline" {
  statement {
    actions = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.delta_etl.name}",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.quality_checks.name}",
    ]
  }

  statement {
    actions = ["glue:StartCrawler", "glue:GetCrawler"]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:crawler/${aws_glue_crawler.delta_crawler.name}",
    ]
  }

  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.archive.arn]
  }

  # Athena executes queries as the calling principal (this role), so it
  # needs the query-execution permission itself plus read access to the
  # Glue Catalog metadata and the underlying Delta table data in S3, and
  # write access to the workgroup's query results location.
  statement {
    actions = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:StopQueryExecution"]
    resources = [
      "arn:aws:athena:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:workgroup/${aws_athena_workgroup.lakehouse.name}",
    ]
  }

  statement {
    actions = [
      "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
    ]
    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lakehouse.name}",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse.name}/*",
    ]
  }

  statement {
    actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    actions = ["s3:GetObject", "s3:PutObject", "s3:ListMultipartUploadParts", "s3:AbortMultipartUpload"]
    resources = [
      "${aws_s3_bucket.lakehouse.arn}/${local.processed_prefix}/*",
      "${aws_s3_bucket.lakehouse.arn}/athena-results/*",
    ]
  }

  # These logging APIs manage account-level log delivery subscriptions and do
  # not support resource-level permissions, so AWS requires resources = ["*"].
  statement {
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn_inline" {
  name   = "${local.name}-sfn-inline"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.sfn_inline.json
}
