output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "curated_bucket_name" {
  value = aws_s3_bucket.curated.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}

output "raw_glue_database" {
  value = aws_glue_catalog_database.raw.name
}

output "raw_crawler_name" {
  value = aws_glue_crawler.raw.name
}