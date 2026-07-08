-- create_tables.hql
--
-- Registers the curated Parquet output of spark/etl_job.py as external
-- Hive tables so analysts can query claims data with plain SQL instead
-- of writing Spark jobs. Run this against the Hive stack brought up by
-- docker-compose.yml, after the ETL job has written to HDFS at
-- /data/curated (see scripts/02_run_etl.sh and scripts/03_create_hive_tables.sh).
--
-- Usage:
--   docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 \
--     -f /hive/create_tables.hql

CREATE DATABASE IF NOT EXISTS claims_warehouse;
USE claims_warehouse;

-- Cleaned, partitioned claim-level fact table
CREATE EXTERNAL TABLE IF NOT EXISTS claims (
    claim_id                 STRING,
    member_id                STRING,
    provider_id               STRING,
    provider_type             STRING,
    plan_type                 STRING,
    member_age_band           STRING,
    member_state              STRING,
    claim_type                STRING,
    diagnosis_category         STRING,
    procedure_category         STRING,
    service_date               DATE,
    submission_date             DATE,
    billed_amount               DOUBLE,
    allowed_amount               DOUBLE,
    paid_amount                 DOUBLE,
    claim_status                 STRING,
    denial_reason                 STRING,
    is_readmission_within_30d       BOOLEAN
)
PARTITIONED BY (service_month STRING)
STORED AS PARQUET
LOCATION '/data/curated/claims';

MSCK REPAIR TABLE claims;

-- Pre-aggregated tables written directly by the Spark job. These are
-- non-partitioned, small summary tables -- registering them as external
-- tables just gives analysts a stable SQL name to hit without
-- recomputing the aggregation in Hive.

CREATE EXTERNAL TABLE IF NOT EXISTS agg_monthly_spend (
    service_month     STRING,
    plan_type         STRING,
    total_paid        DOUBLE,
    total_billed      DOUBLE,
    claim_count       BIGINT,
    distinct_members  BIGINT
)
STORED AS PARQUET
LOCATION '/data/curated/agg_monthly_spend';

CREATE EXTERNAL TABLE IF NOT EXISTS agg_diagnosis_cost (
    diagnosis_category  STRING,
    total_paid          DOUBLE,
    avg_paid_per_claim  DOUBLE,
    claim_count         BIGINT
)
STORED AS PARQUET
LOCATION '/data/curated/agg_diagnosis_cost';

CREATE EXTERNAL TABLE IF NOT EXISTS agg_denial_by_provider_type (
    provider_type   STRING,
    total_claims    BIGINT,
    denied_claims   BIGINT,
    denial_rate     DOUBLE
)
STORED AS PARQUET
LOCATION '/data/curated/agg_denial_by_provider_type';

CREATE EXTERNAL TABLE IF NOT EXISTS agg_denial_by_reason (
    denial_reason         STRING,
    denied_claims         BIGINT,
    billed_amount_denied  DOUBLE
)
STORED AS PARQUET
LOCATION '/data/curated/agg_denial_by_reason';

CREATE EXTERNAL TABLE IF NOT EXISTS agg_provider_outliers (
    provider_id             STRING,
    provider_type           STRING,
    avg_billed              DOUBLE,
    claim_count             BIGINT,
    total_billed            DOUBLE,
    cohort_mean_avg_billed  DOUBLE,
    outlier_threshold       DOUBLE,
    is_cost_outlier         BOOLEAN
)
STORED AS PARQUET
LOCATION '/data/curated/agg_provider_outliers';

CREATE EXTERNAL TABLE IF NOT EXISTS agg_readmissions (
    diagnosis_category  STRING,
    inpatient_claims    BIGINT,
    readmissions_30d    BIGINT,
    readmission_rate    DOUBLE
)
STORED AS PARQUET
LOCATION '/data/curated/agg_readmissions';

SHOW TABLES;
