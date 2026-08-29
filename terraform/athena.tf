resource "aws_glue_catalog_database" "curated" {
  name = "${var.project_name}_${var.environment}_curated_db"
}

resource "aws_glue_crawler" "curated" {
  name          = "${var.project_name}-${var.environment}-curated-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.curated.name

  s3_target {
    path = "s3://${aws_s3_bucket.curated.bucket}/sensor_readings/"
  }
  s3_target {
    path = "s3://${aws_s3_bucket.curated.bucket}/audit/pipeline_runs/"
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

resource "aws_athena_workgroup" "pulsegrid" {
  name = "${var.project_name}-${var.environment}"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.glue_assets.bucket}/athena-results/"
    }
    # Safety net: fails any query scanning more than 1GB. Won't interfere with
    # this project's actual data volume, but catches a query that accidentally
    # skipped partition filtering and scanned far more than intended.
    bytes_scanned_cutoff_per_query = 1073741824
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_athena_named_query" "pruned_example" {
  name      = "sensor_readings_by_date_PRUNED_EXAMPLE"
  database  = aws_glue_catalog_database.curated.name
  workgroup = aws_athena_workgroup.pulsegrid.name
  query     = "SELECT device_type, AVG(temperature_c) AS avg_temp FROM sensor_readings WHERE dt = '2026-08-26' GROUP BY device_type;"
}
