data "archive_file" "archive_lambda" {
  type        = "zip"
  source_file = "./lambda/archive_files.py"
  output_path = "./archive_files.zip"
}

resource "aws_lambda_function" "archive" {
  function_name    = "${local.name}-archive-raw-files"
  role             = aws_iam_role.lambda.arn
  handler          = "archive_files.lambda_handler"
  runtime          = "python3.11"
  filename         = data.archive_file.archive_lambda.output_path
  source_code_hash = data.archive_file.archive_lambda.output_base64sha256
  timeout          = 120
  memory_size      = 128
  tags             = local.tags

  environment {
    variables = {
      BUCKET         = aws_s3_bucket.lakehouse.bucket
      SOURCE_PREFIX  = local.raw_prefix
      ARCHIVE_PREFIX = local.archive_prefix
    }
  }
}
