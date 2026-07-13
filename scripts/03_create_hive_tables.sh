#!/bin/bash
# Registers the curated Parquet tables as external Hive tables.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker cp "$PROJECT_ROOT/hive/create_tables.hql" hive-server:/hive/create_tables.hql
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /hive/create_tables.hql

echo "Hive tables created in claims_warehouse."
