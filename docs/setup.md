# Setup

One-time deployment instructions — getting from a fresh clone to a genuinely working pipeline. For day-to-day operation once deployed (triggering runs, backfills, checking state), see [`run-project-end-to-end.md`](run-project-end-to-end.md).

## Prerequisites

- An AWS account
- [Terraform](https://developer.hashicorp.com/terraform/downloads) — this project pins the AWS provider to `~> 5.0`
- Python 3.10+
- An AWS region — this project defaults to `ap-south-1` (`terraform/variables.tf`); change it there if you'd rather deploy elsewhere
- An email address you can access, for pipeline failure notifications

## 1. Create a deploy IAM user

This project doesn't use one broad admin policy — the actual deploy permissions are split across **4 separate IAM policies**, attached to one IAM user, because a single combined policy hit AWS's 6,144-character limit during development:

- `pulsegrid-deploy-core` — S3, IAM role management, `PassRole` scoped to this project's own roles
- `pulsegrid-deploy-analytics` — Glue, Athena
- `pulsegrid-deploy-infra` — Redshift Serverless, its networking
- `pulsegrid-deploy-orchestration` — DynamoDB, Lambda, Step Functions, Scheduler, Secrets Manager, Redshift Data API, SNS

Create the user, attach all 4 policies, generate an access key, then:

```bash
aws configure
```

## 2. Clone and configure

```bash
git clone <your-fork-url>
cd pulsegrid-sensor-etl
pip install -e .
```

Create `terraform/terraform.tfvars` — **never commit this file**, it's already gitignored:

```hcl
redshift_admin_password       = "<choose a real password>"
redshift_refresh_svc_password = "<choose a different real password>"
alert_email                   = "<your real email address>"
```

**If you're deploying this to your own AWS account, not just reading it:** S3 bucket names are globally unique across *all* of AWS, not just your account. This project's buckets are named from `project_name`+`environment` (e.g. `pulsegrid-dev-raw`) — since the original names are already taken, you'll need to change `project_name` in `terraform/variables.tf` to something unique to you before `apply` will succeed.

## 3. Before you apply — a real sequencing warning

**Both EventBridge schedules deploy already `ENABLED`.** The moment `apply` completes, `generate_hourly` starts firing every hour, and `sensor_etl_daily`/`redshift_refresh` will attempt to run on their normal schedule — including against a Redshift setup you likely haven't finished yet (step 5 below).

This won't corrupt anything — every lock correctly releases even on failure, proven extensively during development — but a run failing repeatedly before you've finished setup is a confusing thing to stumble into unexplained. If you'd rather have a clean window to finish setup first, either:
- Set `state = "DISABLED"` on both `aws_scheduler_schedule` resources (`terraform/eventbridge.tf` and `terraform/generate_hourly.tf`) before your first `apply`, re-enabling once step 6 confirms everything works, **or**
- Just move quickly through steps 4–5 right after `apply`, accepting a possible failed run or two in the meantime.

## 4. Deploy the infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This creates everything Terraform manages: S3 buckets, Glue crawlers and the ETL job, the Redshift Serverless namespace/workgroup, DynamoDB, all 3 Lambdas, the Step Functions state machine, Secrets Manager, an SNS topic for failure alerts, and both EventBridge schedules.

## 5. Confirm the SNS email subscription — a manual step Terraform can't finish for you

Terraform can *request* the email subscription, but AWS requires a human to click the confirmation link that gets emailed — this can't be automated. Check the inbox for `alert_email` (including spam — this is the single most common place it ends up) for an email from AWS Notifications, and click **Confirm subscription**.

Verify it actually took, rather than trust the click alone:

```bash
aws sns list-subscriptions-by-topic --topic-arn <topic-arn-from-terraform-output>
```

`"SubscriptionArn": "PendingConfirmation"` means it's still unconfirmed. A real ARN in that field means it's genuinely active.

## 6. Manual Redshift setup — not Terraform-managed

Three things Terraform deliberately doesn't own, since they're database-internal objects, not AWS resources. Run these in order, in Redshift Query Editor v2, connected as the admin user:

**a. The external (Spectrum) schema** — `sql/external_schema.sql`. Needs the Glue databases Terraform just created to already exist, so this must run *after* `apply`, never before.

**b. The 5 native KPI tables** — `sql/create_tables.sql`.

**c. The scoped service user**, used by `redshift_refresh` for automated runs:

```sql
CREATE USER redshift_refresh_svc PASSWORD '<the same password you put in terraform.tfvars>';

GRANT USAGE ON SCHEMA curated_spectrum TO redshift_refresh_svc;
GRANT TEMP ON DATABASE pulsegrid TO redshift_refresh_svc;
GRANT SELECT, INSERT, DELETE ON
  daily_device_health_summary, daily_motor_vibration_trend, daily_cold_storage_risk,
  daily_hvac_stability, daily_energy_voltage_summary
  TO redshift_refresh_svc;
```

This step is easy to miss entirely, since `terraform apply` succeeding gives no indication it's still required — but without it, `redshift_refresh` will fail on its very first real run (or every scheduled run, if you skipped the warning in step 3).

## 7. Confirm the deployment

- `terraform apply` completed with no errors
- The 5 KPI tables exist and are queryable (even empty) in Redshift
- `aws lambda invoke --function-name <project>-<env>-generate-hourly output.json` runs successfully and writes a real file to the `raw` bucket
- `pip install -e ".[dev]" && pytest tests/ -v` passes locally (21 tests)
- The SNS subscription shows a real ARN, not `PendingConfirmation` (step 5)
- If you disabled the schedules in step 3, re-enable both now (`state = "ENABLED"`, `terraform apply` again)