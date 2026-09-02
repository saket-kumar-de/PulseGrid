resource "aws_iam_role" "step_functions_sensor_etl" {
  name = "${var.project_name}-${var.environment}-sfn-sensor-etl-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "step_functions_sensor_etl_access" {
  name = "${var.project_name}-${var.environment}-sfn-sensor-etl-access"
  role = aws_iam_role.step_functions_sensor_etl.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["dynamodb:UpdateItem"]
        Resource = aws_dynamodb_table.watermarks.arn
      },
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [aws_lambda_function.missing_hours.arn, aws_lambda_function.missing_dates.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartCrawler", "glue:GetCrawler"]
        Resource = "arn:aws:glue:*:*:crawler/pulsegrid-*"
      },
      {
        Effect   = "Allow"
        Action   = ["redshift-data:BatchExecuteStatement", "redshift-data:DescribeStatement", "redshift-data:GetStatementResult"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.redshift_refresh_svc.arn
      }
    ]
  })
}

resource "aws_sfn_state_machine" "sensor_etl" {
  name     = "${var.project_name}-${var.environment}-sensor-etl-orchestration"
  role_arn = aws_iam_role.step_functions_sensor_etl.arn
  definition = templatefile("${path.module}/../state_machines/sensor_etl.asl.json", {
    redshift_secret_arn = aws_secretsmanager_secret.redshift_refresh_svc.arn
  })

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}