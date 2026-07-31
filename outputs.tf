output "s3_bucket" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.lakehouse.name
}

output "glue_jobs" {
  value = {
    etl     = aws_glue_job.delta_etl.name
    quality = aws_glue_job.quality_checks.name
  }
}

output "crawler_name" {
  value = aws_glue_crawler.delta_crawler.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lakehouse.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.lakehouse_pipeline.arn
}

output "pipeline_alerts_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}
