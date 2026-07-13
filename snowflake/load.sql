-- Reset before reload (also clears any stale files from prior failed loads)
TRUNCATE TABLE claims;
REMOVE @claims_stage;

-- Stage each service_month partition into its own stage subfolder to avoid
-- filename collisions (Spark reuses the same job UUID -> same part filenames
-- across every service_month=YYYY-MM directory).
PUT file://../curated/claims/service_month=2026-01/*.parquet @claims_stage/2026-01/ AUTO_COMPRESS=FALSE;
PUT file://../curated/claims/service_month=2026-02/*.parquet @claims_stage/2026-02/ AUTO_COMPRESS=FALSE;
PUT file://../curated/claims/service_month=2026-03/*.parquet @claims_stage/2026-03/ AUTO_COMPRESS=FALSE;
PUT file://../curated/claims/service_month=2026-04/*.parquet @claims_stage/2026-04/ AUTO_COMPRESS=FALSE;
PUT file://../curated/claims/service_month=2026-05/*.parquet @claims_stage/2026-05/ AUTO_COMPRESS=FALSE;
PUT file://../curated/claims/service_month=2026-06/*.parquet @claims_stage/2026-06/ AUTO_COMPRESS=FALSE;
PUT file://../curated/claims/service_month=2026-07/*.parquet @claims_stage/2026-07/ AUTO_COMPRESS=FALSE;

-- service_month is a Hive partition column, so it lives only in the file
-- path, not in the parquet payload itself. Recover it from METADATA$FILENAME.
COPY INTO claims (
  claim_id, member_id, provider_id, provider_type, plan_type, member_age_band,
  member_state, claim_type, diagnosis_category, procedure_category, service_date,
  submission_date, billed_amount, allowed_amount, paid_amount, claim_status,
  denial_reason, is_readmission_within_30d, service_month
)
FROM (
  SELECT
    $1:claim_id::STRING,
    $1:member_id::STRING,
    $1:provider_id::STRING,
    $1:provider_type::STRING,
    $1:plan_type::STRING,
    $1:member_age_band::STRING,
    $1:member_state::STRING,
    $1:claim_type::STRING,
    $1:diagnosis_category::STRING,
    $1:procedure_category::STRING,
    $1:service_date::DATE,
    $1:submission_date::DATE,
    $1:billed_amount::FLOAT,
    $1:allowed_amount::FLOAT,
    $1:paid_amount::FLOAT,
    $1:claim_status::STRING,
    $1:denial_reason::STRING,
    $1:is_readmission_within_30d::BOOLEAN,
    REGEXP_SUBSTR(METADATA$FILENAME, '^([0-9]{4}-[0-9]{2})/', 1, 1, 'e', 1)
  FROM @claims_stage
)
FILE_FORMAT = (TYPE = PARQUET);
