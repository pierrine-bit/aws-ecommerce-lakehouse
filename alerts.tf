resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.name}-pipeline-alerts"
  tags = local.tags
}

resource "aws_sns_topic_subscription" "pipeline_alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

data "aws_iam_policy_document" "pipeline_alerts_topic_policy" {
  statement {
    sid     = "AllowEventBridgePublish"
    effect  = "Allow"
    actions = ["sns:Publish"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    resources = [aws_sns_topic.pipeline_alerts.arn]
  }
}

resource "aws_sns_topic_policy" "pipeline_alerts" {
  arn    = aws_sns_topic.pipeline_alerts.arn
  policy = data.aws_iam_policy_document.pipeline_alerts_topic_policy.json
}

# Alert on individual Glue job failures/timeouts, independent of whether the
# job was run inside the Step Functions pipeline or triggered manually.
resource "aws_cloudwatch_event_rule" "glue_job_failed" {
  name        = "${local.name}-glue-job-failed"
  description = "Fires when a lakehouse Glue job fails or times out."
  tags        = local.tags

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.delta_etl.name, aws_glue_job.quality_checks.name]
      state   = ["FAILED", "TIMEOUT"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_job_failed_sns" {
  rule      = aws_cloudwatch_event_rule.glue_job_failed.name
  target_id = "pipeline-alerts-sns"
  arn       = aws_sns_topic.pipeline_alerts.arn
}

# Alert on the pipeline as a whole failing, timing out, or being aborted,
# regardless of which state raised the error.
resource "aws_cloudwatch_event_rule" "pipeline_failed" {
  name        = "${local.name}-pipeline-failed"
  description = "Fires when the lakehouse Step Functions pipeline fails, times out, or is aborted."
  tags        = local.tags

  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn = [aws_sfn_state_machine.lakehouse_pipeline.arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "pipeline_failed_sns" {
  rule      = aws_cloudwatch_event_rule.pipeline_failed.name
  target_id = "pipeline-alerts-sns"
  arn       = aws_sns_topic.pipeline_alerts.arn
}
