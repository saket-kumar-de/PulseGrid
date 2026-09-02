resource "aws_secretsmanager_secret" "redshift_refresh_svc" {
  name        = "${var.project_name}-${var.environment}-redshift-refresh-svc-credentials"
  description = "Credentials for redshift_refresh_svc, used by the Redshift Data API"

  tags = {
    Environment            = var.environment
    Project                = var.project_name
    RedshiftDataFullAccess = "serverless"
  }
}

resource "aws_secretsmanager_secret_version" "redshift_refresh_svc" {
  secret_id = aws_secretsmanager_secret.redshift_refresh_svc.id
  secret_string = jsonencode({
    username = "redshift_refresh_svc"
    password = var.redshift_refresh_svc_password
  })
}