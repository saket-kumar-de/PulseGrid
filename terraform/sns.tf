variable "alert_email" {
  description = "Email address to receive pipeline failure notifications"
  type        = string
}

resource "aws_sns_topic" "pipeline_failures" {
  name = "${var.project_name}-${var.environment}-pipeline-failures"

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_sns_topic_subscription" "pipeline_failures_email" {
  topic_arn = aws_sns_topic.pipeline_failures.arn
  protocol  = "email"
  endpoint  = var.alert_email
}