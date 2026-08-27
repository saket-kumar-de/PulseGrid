output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "curated_bucket_name" {
  value = aws_s3_bucket.curated.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}
