variable "aws_region" {
  description = "AWS region where the lakehouse will be deployed."
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Project prefix used for naming AWS resources."
  type        = string
  default     = "ecommerce-lakehouse"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name. Leave empty to generate one from account and region."
  type        = string
  default     = ""
}

variable "local_data_dir" {
  description = "Local folder containing sample raw data files."
  type        = string
  default     = "./data"
}

variable "crawler_enabled" {
  description = "Whether Step Functions should run the Glue crawler after ETL."
  type        = bool
  default     = true
}

variable "athena_validation_enabled" {
  description = "Whether Step Functions should run an Athena query per table to confirm it has rows before archiving raw files."
  type        = bool
  default     = true
}

variable "max_data_age_hours" {
  description = "Maximum age, in hours, a Delta table's most recent commit may be before quality checks flag it as stale."
  type        = number
  default     = 24
}

variable "archive_retention_days" {
  description = "Number of days to retain archived raw files before S3 expires them."
  type        = number
  default     = 90
}

variable "rejected_retention_days" {
  description = "Number of days to retain rejected records before S3 expires them."
  type        = number
  default     = 30
}

variable "alert_email" {
  description = "Optional email address to subscribe to pipeline failure alerts. Leave empty to skip creating a subscription."
  type        = string
  default     = ""
}
