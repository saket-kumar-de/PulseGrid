resource "aws_dynamodb_table" "watermarks" {
  name         = "${var.project_name}-${var.environment}-watermarks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_id"

  attribute {
    name = "pipeline_id"
    type = "S"
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

data "archive_file" "missing_hours_lambda" {
  type        = "zip"
  source_file = "${path.module}/../lambdas/missing_hours/handler.py"
  output_path = "${path.module}/../lambdas/missing_hours/handler.zip"
}

resource "aws_iam_role" "lambda_missing_hours" {
  name = "${var.project_name}-${var.environment}-missing-hours-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_missing_hours_dynamodb" {
  name = "${var.project_name}-${var.environment}-missing-hours-dynamodb-access"
  role = aws_iam_role.lambda_missing_hours.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["dynamodb:GetItem"]
      Resource = aws_dynamodb_table.watermarks.arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_missing_hours_basic" {
  role       = aws_iam_role.lambda_missing_hours.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "missing_hours" {
  function_name    = "${var.project_name}-${var.environment}-missing-hours"
  role             = aws_iam_role.lambda_missing_hours.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.missing_hours_lambda.output_path
  source_code_hash = data.archive_file.missing_hours_lambda.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      WATERMARK_TABLE = aws_dynamodb_table.watermarks.name
      PIPELINE_ID     = "sensor_etl"
    }
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}