data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  name        = var.project_name
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_name}-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}"

  raw_prefix       = "raw"
  processed_prefix = "lakehouse-dwh"
  archive_prefix   = "archived"
  scripts_prefix   = "scripts"

  tags = {
    Project = var.project_name
    Managed = "terraform"
  }
}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = local.bucket_name
  force_destroy = true
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "expire-archived-raw-files"
    status = "Enabled"

    filter {
      prefix = "${local.archive_prefix}/"
    }

    expiration {
      days = var.archive_retention_days
    }
  }

  rule {
    id     = "expire-rejected-records"
    status = "Enabled"

    filter {
      prefix = "rejected/"
    }

    expiration {
      days = var.rejected_retention_days
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_object" "products" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "${local.raw_prefix}/products/products.csv"
  source = "${var.local_data_dir}/products.csv"
  etag   = filemd5("${var.local_data_dir}/products.csv")
}

resource "aws_s3_object" "orders" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "${local.raw_prefix}/orders/orders_apr_2025.xlsx"
  source = "${var.local_data_dir}/orders_apr_2025.xlsx"
  etag   = filemd5("${var.local_data_dir}/orders_apr_2025.xlsx")
}

resource "aws_s3_object" "order_items" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "${local.raw_prefix}/order_items/order_items_apr_2025.xlsx"
  source = "${var.local_data_dir}/order_items_apr_2025.xlsx"
  etag   = filemd5("${var.local_data_dir}/order_items_apr_2025.xlsx")
}

resource "aws_s3_object" "glue_lakehouse_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "${local.scripts_prefix}/lakehouse_delta_etl.py"
  source = "./glue_scripts/lakehouse_delta_etl.py"
  etag   = filemd5("./glue_scripts/lakehouse_delta_etl.py")
}

resource "aws_s3_object" "glue_quality_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "${local.scripts_prefix}/quality_checks.py"
  source = "./glue_scripts/quality_checks.py"
  etag   = filemd5("./glue_scripts/quality_checks.py")
}
