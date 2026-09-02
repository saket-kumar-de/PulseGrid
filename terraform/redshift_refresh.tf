data "archive_file" "missing_dates_lambda" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/missing_dates/handler.py"
  output_path = "${path.module}/../lambdas/missing_dates/handler.zip"
}

resource "aws_iam_role" "lambda_missing_dates" {
  name = "${var.project_name}-${var.environment}-missing-dates-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_missing_dates_dynamodb" {
  name = "${var.project_name}-${var.environment}-missing-dates-dynamodb-access"
  role = aws_iam_role.lambda_missing_dates.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["dynamodb:GetItem"]
      Resource = aws_dynamodb_table.watermarks.arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_missing_dates_basic" {
  role       = aws_iam_role.lambda_missing_dates.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "missing_dates" {
  function_name    = "${var.project_name}-${var.environment}-missing-dates"
  role             = aws_iam_role.lambda_missing_dates.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.missing_dates_lambda.output_path
  source_code_hash = data.archive_file.missing_dates_lambda.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      WATERMARK_TABLE      = aws_dynamodb_table.watermarks.name
      PIPELINE_ID           = "redshift_refresh"
      UPSTREAM_PIPELINE_ID  = "sensor_etl"
    }
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}