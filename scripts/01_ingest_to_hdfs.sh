#!/bin/bash
# Copies the synthetic claims CSV into the namenode container and lands it
# in HDFS at /data/raw/claims.csv. Stands in for a nightly Sqoop/Kafka
# Connect extract from a claims adjudication system.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker exec namenode hdfs dfs -mkdir -p /data/raw
docker cp "$PROJECT_ROOT/raw/claims.csv" namenode:/tmp/claims.csv
docker exec namenode hdfs dfs -put -f /tmp/claims.csv /data/raw/claims.csv

echo "Ingested claims.csv into HDFS at /data/raw/claims.csv"
docker exec namenode hdfs dfs -ls /data/raw
