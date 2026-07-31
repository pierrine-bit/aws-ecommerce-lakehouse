terraform {
  required_version = ">= 1.10.0"

  # State bucket is created by ./bootstrap. Terraform 1.10+ supports native
  # S3 locking (use_lockfile), so no DynamoDB lock table is required.
  backend "s3" {
    bucket       = "ecommerce-lakehouse-tfstate-682033471539"
    key          = "ecommerce-lakehouse/terraform.tfstate"
    region       = "eu-west-1"
    encrypt      = true
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}
