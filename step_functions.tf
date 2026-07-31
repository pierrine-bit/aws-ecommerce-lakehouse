resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/vendedlogs/states/${local.name}"
  retention_in_days = 14
  tags              = local.tags
}

locals {
  validation_tables = ["products", "orders", "order_items"]

  # When the crawler/Athena validation are disabled, each hop skips straight
  # to the next enabled stage (or archival) and that state is omitted from
  # the machine entirely.
  post_crawler_state        = var.athena_validation_enabled ? "PrepareAthenaValidation" : "ArchiveRawFiles"
  post_quality_checks_state = var.crawler_enabled ? "RunCatalogCrawler" : local.post_crawler_state

  crawler_state = var.crawler_enabled ? {
    RunCatalogCrawler = {
      Type     = "Task"
      Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
      Parameters = {
        Name = aws_glue_crawler.delta_crawler.name
      }
      Catch = [{
        ErrorEquals = ["Glue.CrawlerRunningException"]
        Next        = local.post_crawler_state
        }, {
        ErrorEquals = ["States.ALL"]
        Next        = "PipelineFailed"
      }]
      Next = local.post_crawler_state
    }
  } : {}

  # Validates each Delta table actually has rows by running a query that
  # itself fails (division by zero) when COUNT(*) is 0. That makes an empty
  # table surface as a normal Athena query failure, which the existing
  # Retry/Catch machinery below already knows how to handle - no need to
  # fetch and branch on query results separately.
  athena_validation_state = jsondecode(var.athena_validation_enabled ? jsonencode({
    PrepareAthenaValidation = {
      Type = "Pass"
      Result = {
        tables = local.validation_tables
      }
      ResultPath = "$.validation"
      Next       = "RunAthenaValidation"
    }
    RunAthenaValidation = {
      Type           = "Map"
      ItemsPath      = "$.validation.tables"
      MaxConcurrency = 3
      ItemProcessor = {
        ProcessorConfig = {
          Mode = "INLINE"
        }
        StartAt = "ValidateTableHasRows"
        States = {
          ValidateTableHasRows = {
            Type     = "Task"
            Resource = "arn:aws:states:::athena:startQueryExecution.sync"
            Parameters = {
              # Each iteration's input is the bare table name from ItemsPath,
              # so "$" is the item itself - the Map context object is not
              # populated for an ItemProcessor in INLINE mode.
              "QueryString.$" = "States.Format('SELECT 1 / COUNT(*) AS row_count_check FROM \"${aws_glue_catalog_database.lakehouse.name}\".\"{}\"', $)"
              WorkGroup       = aws_athena_workgroup.lakehouse.name
            }
            Retry = [{
              ErrorEquals     = ["Athena.AmazonAthenaException", "States.TaskFailed"]
              IntervalSeconds = 15
              MaxAttempts     = 2
              BackoffRate     = 2
            }]
            End = true
          }
        }
      }
      Catch = [{
        ErrorEquals = ["States.ALL"]
        Next        = "PipelineFailed"
      }]
      Next = "ArchiveRawFiles"
    }
  }) : jsonencode({}))
}

resource "aws_sfn_state_machine" "lakehouse_pipeline" {
  name     = "${local.name}-state-machine"
  role_arn = aws_iam_role.step_functions.arn
  type     = "STANDARD"
  tags     = local.tags

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "E-commerce lakehouse pipeline using Glue Delta Lake, Glue Catalog, Lambda archive, and Step Functions"
    StartAt = "SimulateFileArrival"
    States = merge({
      SimulateFileArrival = {
        Type = "Pass"
        Result = {
          message = "Raw files are expected under S3 raw/products, raw/orders, and raw/order_items prefixes."
        }
        Next = "RunDeltaLakeETL"
      }
      RunDeltaLakeETL = {
        Type           = "Task"
        Resource       = "arn:aws:states:::glue:startJobRun.sync"
        TimeoutSeconds = 3600
        Parameters = {
          JobName = aws_glue_job.delta_etl.name
        }
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
        }]
        Next = "RunQualityChecks"
      }
      RunQualityChecks = {
        Type           = "Task"
        Resource       = "arn:aws:states:::glue:startJobRun.sync"
        TimeoutSeconds = 1800
        Parameters = {
          JobName = aws_glue_job.quality_checks.name
        }
        Retry = [{
          ErrorEquals     = ["Glue.AWSGlueException", "States.TaskFailed"]
          IntervalSeconds = 30
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
        }]
        Next = local.post_quality_checks_state
      }
      ArchiveRawFiles = {
        Type           = "Task"
        Resource       = aws_lambda_function.archive.arn
        TimeoutSeconds = 180
        Retry = [{
          # AWS-recommended default retry set for Lambda invocations via Step
          # Functions: covers throttling, transient SDK errors, and service errors.
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
          IntervalSeconds = 10
          MaxAttempts     = 3
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "PipelineFailed"
        }]
        Next = "PipelineSucceeded"
      }
      PipelineSucceeded = {
        Type = "Succeed"
      }
      PipelineFailed = {
        Type  = "Fail"
        Cause = "Lakehouse pipeline failed. Check CloudWatch, Step Functions execution history, and Glue job logs."
      }
    }, local.crawler_state, local.athena_validation_state)
  })
}
