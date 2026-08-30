# --- Networking: VPC/subnets ---
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-${var.environment}-redshift-sg"
  description = "Allow inbound access to Redshift Serverless"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# --- IAM role Redshift assumes, for Spectrum to read S3 + Glue Catalog ---
resource "aws_iam_role" "redshift_role" {
  name = "${var.project_name}-${var.environment}-redshift-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"] }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_access" {
  name = "${var.project_name}-${var.environment}-redshift-access"
  role = aws_iam_role.redshift_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.curated.arn, "${aws_s3_bucket.curated.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/pulsegrid_*",
          "arn:aws:glue:*:*:table/pulsegrid_*/*"
        ]
      }
    ]
  })
}

# --- Redshift Serverless ---
resource "aws_redshiftserverless_namespace" "pulsegrid" {
  namespace_name      = "${var.project_name}-${var.environment}"
  db_name             = "pulsegrid"
  admin_username      = "pulsegrid_admin"
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_role.arn]

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_redshiftserverless_workgroup" "pulsegrid" {
  namespace_name      = aws_redshiftserverless_namespace.pulsegrid.namespace_name
  workgroup_name      = "${var.project_name}-${var.environment}-wg"
  base_capacity       = 8
  subnet_ids          = data.aws_subnets.default.ids
  security_group_ids  = [aws_security_group.redshift.id]
  publicly_accessible = true

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# --- Prerequisite for daily_device_health_summary: quarantine data isn't
# crawled yet. Separate crawler (not added to the existing curated crawler)
# specifically to avoid a table_prefix collision -- table_prefix applies to
# ALL of a crawler's targets, so adding this to the existing crawler would
# rename sensor_readings/pipeline_runs too.

resource "aws_glue_crawler" "curated_quarantine" {
  name          = "${var.project_name}-${var.environment}-curated-quarantine-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.curated.name
  table_prefix  = "quarantine_"

  s3_target {
    path = "s3://${aws_s3_bucket.curated.bucket}/quarantine/sensor_readings/"
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