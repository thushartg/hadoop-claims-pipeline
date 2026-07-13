#!/bin/bash
# Runs the analytical HiveQL queries against claims_warehouse and prints
# results to stdout.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker cp "$PROJECT_ROOT/hive/analytics_queries.hql" hive-server:/hive/analytics_queries.hql
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /hive/analytics_queries.hql
