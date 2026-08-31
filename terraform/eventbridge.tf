resource "aws_iam_role" "scheduler_sensor_etl" {
  name = "${var.project_name}-${var.environment}-scheduler-sensor-etl-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "scheduler_sensor_etl_access" {
  name = "${var.project_name}-${var.environment}-scheduler-sensor-etl-access"
  role = aws_iam_role.scheduler_sensor_etl.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.sensor_etl.arn
    }]
  })
}

resource "aws_scheduler_schedule" "sensor_etl_daily" {
  name        = "${var.project_name}-${var.environment}-sensor-etl-daily"
  description = "Triggers sensor_etl daily at 00:30 UTC"
  state       = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(30 0 * * ? *)"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.sensor_etl.arn
    role_arn = aws_iam_role.scheduler_sensor_etl.arn
  }
}