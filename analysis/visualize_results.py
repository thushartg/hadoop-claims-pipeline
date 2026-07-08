"""
visualize_results.py

Reads the ETL aggregate outputs (the _csv/ flattened copies written by
spark/etl_job.py alongside the Parquet/Hive tables) and produces summary
charts. This mimics the kind of BI-layer step that would normally sit on
top of Hive tables (Superset / Tableau / a notebook hitting Hive via JDBC)
-- here it's simplified to read directly from the CSV aggregates so it has
no dependency on a running Hive server.

Usage:
    python3 visualize_results.py --curated-base ../curated --out-dir ./charts
"""
import argparse
import glob
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd


def read_agg_csv(curated_base, name):
    pattern = os.path.join(curated_base, "_csv", name, "*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No CSV found for {name} at {pattern}")
    return pd.read_csv(files[0])


def chart_monthly_spend(curated_base, out_dir):
    df = read_agg_csv(curated_base, "agg_monthly_spend")
    pivot = df.pivot_table(index="service_month", columns="plan_type", values="total_paid", aggfunc="sum")
    ax = pivot.plot(kind="line", marker="o", figsize=(9, 5))
    ax.set_title("Monthly Paid Claims Amount by Plan Type")
    ax.set_ylabel("Total Paid ($)")
    ax.set_xlabel("Service Month")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "monthly_spend_by_plan.png"), dpi=130)
    plt.close()


def chart_diagnosis_cost(curated_base, out_dir):
    df = read_agg_csv(curated_base, "agg_diagnosis_cost").sort_values("total_paid", ascending=True)
    ax = df.plot(kind="barh", x="diagnosis_category", y="total_paid", legend=False, figsize=(9, 5), color="#2a6f97")
    ax.set_title("Total Paid Claims Cost by Diagnosis Category")
    ax.set_xlabel("Total Paid ($)")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "diagnosis_cost_burden.png"), dpi=130)
    plt.close()


def chart_denial_rate(curated_base, out_dir):
    df = read_agg_csv(curated_base, "agg_denial_by_provider_type").sort_values("denial_rate", ascending=True)
    ax = df.plot(kind="barh", x="provider_type", y="denial_rate", legend=False, figsize=(9, 5), color="#c1121f")
    ax.set_title("Claim Denial Rate by Provider Type")
    ax.set_xlabel("Denial Rate")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "denial_rate_by_provider_type.png"), dpi=130)
    plt.close()


def chart_provider_outliers(curated_base, out_dir):
    df = read_agg_csv(curated_base, "agg_provider_outliers")
    colors = df["is_cost_outlier"].map({True: "#c1121f", False: "#8d99ae"})
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.scatter(df["claim_count"], df["avg_billed"], c=colors, alpha=0.6, s=25)
    threshold = df["outlier_threshold"].iloc[0]
    ax.axhline(threshold, color="black", linestyle="--", linewidth=1, label=f"Outlier threshold (${threshold:,.0f})")
    ax.set_title("Provider Cost Profile — Avg Billed per Claim vs Claim Volume")
    ax.set_xlabel("Claim Count")
    ax.set_ylabel("Avg Billed per Claim ($)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "provider_cost_outliers.png"), dpi=130)
    plt.close()


def chart_readmissions(curated_base, out_dir):
    df = read_agg_csv(curated_base, "agg_readmissions").sort_values("readmission_rate", ascending=True)
    ax = df.plot(kind="barh", x="diagnosis_category", y="readmission_rate", legend=False, figsize=(9, 5), color="#6a4c93")
    ax.set_title("30-Day Inpatient Readmission Rate by Diagnosis Category")
    ax.set_xlabel("Readmission Rate")
    ax.set_ylabel("")
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "readmission_rate_by_diagnosis.png"), dpi=130)
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--curated-base", default="../curated")
    parser.add_argument("--out-dir", default="./charts")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    chart_monthly_spend(args.curated_base, args.out_dir)
    chart_diagnosis_cost(args.curated_base, args.out_dir)
    chart_denial_rate(args.curated_base, args.out_dir)
    chart_provider_outliers(args.curated_base, args.out_dir)
    chart_readmissions(args.curated_base, args.out_dir)

    print(f"Charts written to {args.out_dir}")


if __name__ == "__main__":
    main()
