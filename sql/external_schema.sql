CREATE EXTERNAL SCHEMA IF NOT EXISTS curated_spectrum
FROM DATA CATALOG
DATABASE 'pulsegrid_dev_curated_db'
IAM_ROLE 'arn:aws:iam::932212589642:role/pulsegrid-dev-redshift-role'
REGION 'ap-south-1';