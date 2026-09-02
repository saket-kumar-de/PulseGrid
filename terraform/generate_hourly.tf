data "archive_file" "generate_hourly_lambda" {
  type        = "zip"
  output_path = "${path.module}/../lambdas/generate_hourly/handler.zip"

  source {
    content  = file("${path.module}/../lambdas/generate_hourly/handler.py")
    filename = "handler.py"
  }
  source {
    content  = file("${path.module}/../src/sensor_etl/__init__.py")
    filename = "sensor_etl/__init__.py"
  }
  source {
    content  = file("${path.module}/../src/sensor_etl/config.py")
    filename = "sensor_etl/config.py"
  }
  source {
    content  = file("${path.module}/../src/sensor_etl/generate.py")
    filename = "sensor_etl/generate.py"
  }
}

resource "aws_iam_role" "lambda_generate_hourly" {
  name = "${var.project_name}-${var.environment}-generate-hourly-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_generate_hourly_s3" {
  name = "${var.project_name}-${var.environment}-generate-hourly-s3-access"
  role = aws_iam_role.lambda_generate_hourly.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject"]
      Resource = "${aws_s3_bucket.raw.arn}/*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_generate_hourly_basic" {
  role       = aws_iam_role.lambda_generate_hourly.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "generate_hourly" {
  function_name    = "${var.project_name}-${var.environment}-generate-hourly"
  role             = aws_iam_role.lambda_generate_hourly.arn
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.generate_hourly_lambda.output_path
  source_code_hash = data.archive_file.generate_hourly_lambda.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      RAW_BUCKET = aws_s3_bucket.raw.bucket
    }
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_iam_role" "scheduler_generate_hourly" {
  name = "${var.project_name}-${var.environment}-scheduler-generate-hourly-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_generate_hourly_access" {
  name = "${var.project_name}-${var.environment}-scheduler-generate-hourly-access"
  role = aws_iam_role.scheduler_generate_hourly.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = aws_lambda_function.generate_hourly.arn
    }]
  })
}

resource "aws_scheduler_schedule" "generate_hourly" {
  name        = "${var.project_name}-${var.environment}-generate-hourly"
  description = "Simulates hourly device telemetry arriving in raw -- fully independent of sensor_etl's own orchestration"
  state       = "ENABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(0 * * * ? *)"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.generate_hourly.arn
    role_arn = aws_iam_role.scheduler_generate_hourly.arn
  }
}