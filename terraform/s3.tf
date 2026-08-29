# Raw zone: landing area for simulator batch uploads, partitioned by dt/hour
resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project_name}-${var.environment}-raw"
  force_destroy = true # lets `terraform destroy` remove non-empty buckets during dev

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "raw"
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Curated zone: cleaned, validated Parquet output from the Glue ETL job
resource "aws_s3_bucket" "curated" {
  bucket        = "${var.project_name}-${var.environment}-curated"
  force_destroy = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "curated"
  }
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Glue assets: For Glue scripts and spark's temp / shuffle files
resource "aws_s3_bucket" "glue_assets" {
  bucket        = "${var.project_name}-${var.environment}-glue-assets"
  force_destroy = true

  tags = {
    Environment = var.environment
    Layer       = "assets"
    Project     = var.project_name
  }
}
