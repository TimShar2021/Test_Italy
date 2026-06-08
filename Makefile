.PHONY: build up down logs dashboard-logs clean local-bootstrap local-extract local-plan local-dbt-run local-dbt-test local-query local-dashboard

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f airflow

dashboard-logs:
	docker compose logs -f dashboard

clean:
	rm -rf data/runtime/*.duckdb logs

local-bootstrap:
	python scripts/bootstrap_sources.py

local-extract:
	python scripts/extract_pos_incremental.py

local-plan:
	python scripts/load_plan.py

local-dbt-run:
	cd dbt/italy_dwh && dbt run --profiles-dir ../../.dbt

local-dbt-test:
	cd dbt/italy_dwh && dbt test --profiles-dir ../../.dbt

local-query:
	python scripts/query_sample.py

local-dashboard:
	python dashboard/app.py
