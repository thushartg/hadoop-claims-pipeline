#!/bin/bash
# Runs the PySpark ETL job on the cluster (spark-master, submitting to
# the Spark standalone master, reading/writing HDFS paths).
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker cp "$PROJECT_ROOT/spark/etl_job.py" spark-master:/spark/etl_job.py

docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  /spark/etl_job.py \
  --input hdfs://namenode:8020/data/raw/claims.csv \
  --output-base hdfs://namenode:8020/data/curated

echo "ETL complete. Curated tables written to hdfs:///data/curated"
docker exec namenode hdfs dfs -ls /data/curated
