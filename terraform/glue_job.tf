resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.glue_assets.id
  key    = "scripts/etl_job.py"
  source = "${path.module}/../glue_jobs/etl_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/etl_job.py")
}

resource "aws_glue_job" "sensor_etl" {
  name              = "${var.project_name}-${var.environment}-sensor-etl"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 15

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_assets.bucket}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--raw_database"                     = aws_glue_catalog_database.raw.name
    "--raw_table"                        = "pulsegrid_dev_raw"
    "--curated_bucket"                   = aws_s3_bucket.curated.bucket
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_assets.bucket}/glue-temp/"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}