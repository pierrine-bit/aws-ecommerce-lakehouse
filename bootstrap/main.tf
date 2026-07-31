terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# One-time bootstrap: creates the S3 bucket that holds the main project's
# Terraform state. Applied once, on its own (local) state, before the main
# project is pointed at an S3 backend. Terraform 1.10+ supports native S3
# locking (use_lockfile), so no DynamoDB lock table is required.
resource "aws_s3_bucket" "tf_state" {
  bucket = var.state_bucket_name
  # Protect the state bucket from accidental `terraform destroy` in this
  # bootstrap config - it must outlive the main project's lifecycle.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket                  = aws_s3_bucket.tf_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
