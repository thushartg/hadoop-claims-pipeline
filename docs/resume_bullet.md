# Resume bullet — verified

Spark ETL pipeline over 150,000 synthetic health-insurance claims, validated
end-to-end via a live Snowflake warehouse — PMPM, denial rate (7.7%–8.1%),
and 30-day readmission cohort analytics (8.4%–12.3% by diagnosis category)
all cross-verified against independent pandas recomputation and Spark's own
curated output.

## Verification basis

Every number above was independently reproduced three ways before being
treated as fact:

- **Spark's own curated Parquet output** (`curated/agg_readmissions`,
  `curated/agg_denial_by_provider_type`, `curated/agg_monthly_spend`), read
  directly, not just reasoned about from code.
- **A plain pandas recomputation** against `raw/claims.csv`, bypassing both
  Spark and Snowflake entirely.
- **A live Snowflake warehouse** (`CLAIMS_DB.WAREHOUSE.CLAIMS`, 150,000 rows,
  loaded via key-pair-authenticated `snowsql`).

All three agreed to the decimal on PMPM, denial rate, and readmission rate.

Two real bugs were caught and corrected in the process, not glossed over:
1. A naive readmission-rate query that divided by *all* claims per diagnosis
   category instead of inpatient claims only (the flag can only be `True` on
   inpatient claims) — understated the true rate by ~12x.
2. A denial-rate query filtering on `claim_status = 'Denied'` against data
   stored as lowercase `denied` — a case-sensitivity bug that silently
   returned 0% for every provider type.

The bullet intentionally does **not** claim HDFS or Hive validation. Only
`make local-smoke-test` (explicitly bypasses HDFS) and the Snowflake load
were exercised in this verification pass — `make up` / `make ingest` /
`make hive-tables` were not run, so the full Hadoop stack (HDFS, Hive) in
the README's architecture diagram is unverified by this pass and should not
be claimed as validated until it's actually exercised.
