.PHONY: dbt-deps dbt-run dbt-test dbt-build dbt-docs lint check

dbt-deps:
	docker compose run --rm dbt deps

dbt-run:
	docker compose run --rm dbt run

dbt-test:
	docker compose run --rm dbt test

dbt-build:
	docker compose run --rm dbt build

dbt-docs:
	docker compose run --rm dbt docs generate

lint:
	docker compose run --rm --entrypoint sqlfluff dbt lint models

check: lint dbt-build
