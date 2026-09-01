# Role assumed by Glue jobs and crawlers to read/write S3 and update the Data Catalog
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-${var.environment}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# AWS-managed policy covering standard Glue permissions (Data Catalog, CloudWatch logs)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy scoping S3 access to just this project's two buckets
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-${var.environment}-glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
      Resource = [
        aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*",
        aws_s3_bucket.curated.arn, "${aws_s3_bucket.curated.arn}/*",
        aws_s3_bucket.glue_assets.arn, "${aws_s3_bucket.glue_assets.arn}/*",
      ]
    }]
  })
}

# Lets the Glue job self-write its own watermark advance directly (see
# etl_job.py's advance_watermark()) -- UpdateItem only, no GetItem needed,
# since the forward-only comparison happens entirely inside DynamoDB's own
# atomic ConditionExpression, never read back into the script.
resource "aws_iam_role_policy" "glue_watermark_access" {
  name = "${var.project_name}-${var.environment}-glue-watermark-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["dynamodb:UpdateItem"]
      Resource = aws_dynamodb_table.watermarks.arn
    }]
  })
}