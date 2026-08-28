resource "aws_glue_catalog_database" "raw" {
  name = "${var.project_name}_${var.environment}_raw_db"
}

resource "aws_glue_crawler" "raw" {
  name          = "${var.project_name}-${var.environment}-raw-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.raw.name

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}