"""
etl_job.py

Core PySpark ETL for the health insurance claims pipeline.

Reads raw claim-level CSV (as would be sitting in HDFS at /data/raw/claims,
landed via a nightly extract from a claims adjudication system), cleans it,
then writes:

  1. A cleaned, partitioned Parquet claims table (curated/claims,
     partitioned by service_month)
  2. Monthly spend by plan type (curated/agg_monthly_spend)
  3. Cost burden by diagnosis category (curated/agg_diagnosis_cost)
  4. Denial-rate analysis by reason and provider type (curated/agg_denial_analysis)
  5. Provider cost-outlier flags (curated/agg_provider_outliers)
  6. 30-day inpatient readmission rate by diagnosis (curated/agg_readmissions)

All data is synthetic -- see data/generate_data.py. No real member or
provider identities are used anywhere in this pipeline.

Run modes:
  Local smoke test:  spark-submit etl_job.py --input file:///.../raw/claims.csv --output-base file:///.../curated
  On the cluster:    spark-submit --master yarn etl_job.py --input hdfs:///data/raw/claims --output-base hdfs:///data/curated
"""
import argparse

from pyspark.sql import SparkSession, functions as F


def build_spark(app_name="claims-etl"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_raw(spark, input_path):
    expected_cols = [
        "claim_id", "member_id", "provider_id", "provider_type", "plan_type",
        "member_age_band", "member_state", "claim_type", "diagnosis_category",
        "procedure_category", "service_date", "submission_date", "billed_amount",
        "allowed_amount", "paid_amount", "claim_status", "denial_reason",
        "is_readmission_within_30d",
    ]
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )
    missing = set(expected_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Raw input missing expected columns: {missing}")
    return df


def clean(df):
    df = df.dropDuplicates(["claim_id"])
    df = df.filter(
        F.col("claim_id").isNotNull() &
        F.col("member_id").isNotNull() &
        F.col("service_date").isNotNull() &
        F.col("claim_status").isin("paid", "partially_paid", "denied")
    )
    df = df.withColumn("service_date", F.to_date("service_date"))
    df = df.withColumn("submission_date", F.to_date("submission_date"))
    df = df.withColumn("service_month", F.date_format("service_date", "yyyy-MM"))
    for c in ["billed_amount", "allowed_amount", "paid_amount"]:
        df = df.withColumn(c, F.col(c).cast("double"))
    df = df.filter(F.col("billed_amount") >= 0)
    df = df.withColumn(
        "is_readmission_within_30d",
        F.col("is_readmission_within_30d").cast("boolean"),
    )
    return df


def monthly_spend(df):
    return (
        df.groupBy("service_month", "plan_type")
        .agg(
            F.sum("paid_amount").alias("total_paid"),
            F.sum("billed_amount").alias("total_billed"),
            F.count("*").alias("claim_count"),
            F.countDistinct("member_id").alias("distinct_members"),
        )
        .orderBy("service_month", "plan_type")
    )


def diagnosis_cost(df):
    return (
        df.groupBy("diagnosis_category")
        .agg(
            F.sum("paid_amount").alias("total_paid"),
            F.round(F.avg("paid_amount"), 2).alias("avg_paid_per_claim"),
            F.count("*").alias("claim_count"),
        )
        .orderBy(F.desc("total_paid"))
    )


def denial_analysis(df):
    total_by_provider_type = (
        df.groupBy("provider_type")
        .agg(F.count("*").alias("total_claims"))
    )
    denied_by_provider_type = (
        df.filter(F.col("claim_status") == "denied")
        .groupBy("provider_type")
        .agg(F.count("*").alias("denied_claims"))
    )
    by_provider_type = (
        total_by_provider_type.join(denied_by_provider_type, "provider_type", "left")
        .na.fill(0)
        .withColumn(
            "denial_rate",
            F.round(F.col("denied_claims") / F.col("total_claims"), 4),
        )
        .orderBy(F.desc("denial_rate"))
    )

    by_reason = (
        df.filter(F.col("claim_status") == "denied")
        .groupBy("denial_reason")
        .agg(F.count("*").alias("denied_claims"), F.sum("billed_amount").alias("billed_amount_denied"))
        .orderBy(F.desc("denied_claims"))
    )

    # Two related but distinct cuts -- keep them separate rather than
    # forcing an awkward join, and write both out.
    return by_provider_type, by_reason


def provider_outliers(df, z_threshold=2.0):
    provider_stats = (
        df.groupBy("provider_id", "provider_type")
        .agg(
            F.round(F.avg("billed_amount"), 2).alias("avg_billed"),
            F.count("*").alias("claim_count"),
            F.sum("billed_amount").alias("total_billed"),
        )
    )
    overall = provider_stats.agg(
        F.avg("avg_billed").alias("mean_avg_billed"),
        F.stddev("avg_billed").alias("stddev_avg_billed"),
    ).collect()[0]

    mean_val = overall["mean_avg_billed"] or 0.0
    stddev_val = overall["stddev_avg_billed"] or 0.0
    threshold = mean_val + z_threshold * stddev_val

    flagged = (
        provider_stats
        .withColumn("cohort_mean_avg_billed", F.lit(round(mean_val, 2)))
        .withColumn("outlier_threshold", F.lit(round(threshold, 2)))
        .withColumn("is_cost_outlier", F.col("avg_billed") > F.lit(threshold))
        .orderBy(F.desc("avg_billed"))
    )
    return flagged


def readmissions(df):
    inpatient = df.filter(F.col("claim_type") == "inpatient")
    return (
        inpatient.groupBy("diagnosis_category")
        .agg(
            F.count("*").alias("inpatient_claims"),
            F.sum(F.col("is_readmission_within_30d").cast("int")).alias("readmissions_30d"),
        )
        .withColumn(
            "readmission_rate",
            F.round(F.col("readmissions_30d") / F.col("inpatient_claims"), 4),
        )
        .orderBy(F.desc("readmission_rate"))
    )


def write_parquet_and_csv(df, output_base, name, partition_cols=None):
    parquet_path = f"{output_base}/{name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(parquet_path)

    csv_path = f"{output_base}/_csv/{name}"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to raw claims CSV (local file:// or hdfs://)")
    parser.add_argument("--output-base", required=True, help="Base output path for curated tables")
    args = parser.parse_args()

    spark = build_spark()

    raw = load_raw(spark, args.input)
    cleaned = clean(raw)

    write_parquet_and_csv(cleaned, args.output_base, "claims", partition_cols=["service_month"])
    write_parquet_and_csv(monthly_spend(cleaned), args.output_base, "agg_monthly_spend")
    write_parquet_and_csv(diagnosis_cost(cleaned), args.output_base, "agg_diagnosis_cost")

    denial_by_provider_type, denial_by_reason = denial_analysis(cleaned)
    write_parquet_and_csv(denial_by_provider_type, args.output_base, "agg_denial_by_provider_type")
    write_parquet_and_csv(denial_by_reason, args.output_base, "agg_denial_by_reason")

    write_parquet_and_csv(provider_outliers(cleaned), args.output_base, "agg_provider_outliers")
    write_parquet_and_csv(readmissions(cleaned), args.output_base, "agg_readmissions")

    print("ETL complete.")
    print(f"Raw rows: {raw.count()}, cleaned rows: {cleaned.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
