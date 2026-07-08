# Makefile
#
# Orchestrates the full Hadoop/Spark/Hive claims pipeline against the
# docker-compose stack. Run `make up` first and give the cluster ~60s to
# finish forming before running `make pipeline`.

.PHONY: up down ingest etl hive-tables analytics pipeline local-smoke-test charts

up:
	docker compose up -d
	@echo "Cluster starting. Waiting ~60s for HDFS/YARN/Hive to be ready..."
	sleep 60
	@echo "NameNode UI:        http://localhost:9870"
	@echo "ResourceManager UI: http://localhost:8088"
	@echo "Spark Master UI:    http://localhost:8080"

down:
	docker compose down -v

ingest:
	bash scripts/01_ingest_to_hdfs.sh

etl:
	bash scripts/02_run_etl.sh

hive-tables:
	bash scripts/03_create_hive_tables.sh

analytics:
	bash scripts/04_run_analytics.sh

pipeline: ingest etl hive-tables analytics

# Runs the ETL job locally (no Docker/HDFS needed) against the raw CSV --
# useful for iterating on transformation logic before pushing to the
# real cluster. Requires: pip install pyspark
local-smoke-test:
	cd spark && python3 etl_job.py \
		--input file://$(CURDIR)/raw/claims.csv \
		--output-base file://$(CURDIR)/curated

charts:
	cd analysis && python3 visualize_results.py --curated-base ../curated --out-dir ./charts
