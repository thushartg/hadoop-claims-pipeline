CREATE DATABASE IF NOT EXISTS claims_db;
CREATE SCHEMA IF NOT EXISTS claims_db.warehouse;
USE DATABASE claims_db;
USE SCHEMA warehouse;
CREATE OR REPLACE STAGE claims_stage FILE_FORMAT = (TYPE = PARQUET);

CREATE OR REPLACE TABLE claims (
  claim_id STRING, member_id STRING, provider_id STRING, provider_type STRING,
  plan_type STRING, member_age_band STRING, member_state STRING, claim_type STRING,
  diagnosis_category STRING, procedure_category STRING, service_date DATE,
  submission_date DATE, billed_amount FLOAT, allowed_amount FLOAT, paid_amount FLOAT,
  claim_status STRING, denial_reason STRING, is_readmission_within_30d BOOLEAN,
  service_month STRING
);
