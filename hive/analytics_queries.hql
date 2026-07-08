-- analytics_queries.hql
--
-- Ad-hoc analytical queries against the claims_warehouse tables created by
-- create_tables.hql. These go beyond the pre-computed Spark aggregates to
-- show direct SQL-on-Hadoop analysis against the partitioned fact table --
-- the kind of query an analyst would run without waiting on a new Spark job.
--
-- Usage:
--   docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 \
--     -f /hive/analytics_queries.hql

USE claims_warehouse;

-- 1. PMPM (paid-per-member-per-month) by plan type -- the standard
--    healthcare cost-trend metric payers report to actuarial teams.
SELECT
    service_month,
    plan_type,
    ROUND(SUM(paid_amount) / COUNT(DISTINCT member_id), 2) AS pmpm
FROM claims
GROUP BY service_month, plan_type
ORDER BY service_month, plan_type;

-- 2. Month-over-month change in total paid claims cost, overall.
SELECT
    service_month,
    total_paid,
    total_paid - LAG(total_paid) OVER (ORDER BY service_month) AS mom_change,
    ROUND(
        100.0 * (total_paid - LAG(total_paid) OVER (ORDER BY service_month))
        / LAG(total_paid) OVER (ORDER BY service_month), 2
    ) AS mom_pct_change
FROM (
    SELECT service_month, SUM(paid_amount) AS total_paid
    FROM claims
    GROUP BY service_month
) monthly;

-- 3. Claims processing lag (days from service to submission) by provider
--    type -- a proxy for billing-process efficiency/backlog.
SELECT
    provider_type,
    ROUND(AVG(DATEDIFF(submission_date, service_date)), 1) AS avg_days_to_submit,
    COUNT(*) AS claim_count
FROM claims
GROUP BY provider_type
ORDER BY avg_days_to_submit DESC;

-- 4. High-utilization members: 3+ inpatient claims in the window --
--    candidates for case management outreach.
SELECT
    member_id,
    COUNT(*) AS inpatient_claim_count,
    SUM(paid_amount) AS total_paid
FROM claims
WHERE claim_type = 'inpatient'
GROUP BY member_id
HAVING COUNT(*) >= 3
ORDER BY total_paid DESC
LIMIT 25;

-- 5. Denial rate trend by month -- is claim quality/documentation
--    getting better or worse over time.
SELECT
    service_month,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN claim_status = 'denied' THEN 1 ELSE 0 END) AS denied_claims,
    ROUND(SUM(CASE WHEN claim_status = 'denied' THEN 1 ELSE 0 END) / COUNT(*), 4) AS denial_rate
FROM claims
GROUP BY service_month
ORDER BY service_month;

-- 6. Top 10 providers by total paid amount, with their outlier flag
--    joined in from the pre-computed table (shows joining a Spark
--    aggregate with a Hive-side rollup on the same fact table).
SELECT
    c.provider_id,
    c.provider_type,
    SUM(c.paid_amount) AS total_paid,
    COUNT(*) AS claim_count,
    o.is_cost_outlier
FROM claims c
LEFT JOIN agg_provider_outliers o ON c.provider_id = o.provider_id
GROUP BY c.provider_id, c.provider_type, o.is_cost_outlier
ORDER BY total_paid DESC
LIMIT 10;

-- 7. Cost burden by member age band and diagnosis category -- e.g. is
--    cardiovascular spend concentrated in the 65+ band as expected.
SELECT
    member_age_band,
    diagnosis_category,
    SUM(paid_amount) AS total_paid,
    COUNT(*) AS claim_count
FROM claims
GROUP BY member_age_band, diagnosis_category
ORDER BY member_age_band, total_paid DESC;
