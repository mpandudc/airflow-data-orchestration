.PHONY: env config up down clean backfill import-errors check verify

START ?= 2026-01-01
END ?= 2026-01-07
SCHED = docker compose exec -T airflow-scheduler

# Generates local secrets once; never committed (.env is gitignored).
env:
	@test -f .env || python3 scripts/make_env.py > .env
	@echo ".env ready"

config: env
	docker compose config --quiet

up: env
	docker compose up -d --build --wait

down:
	docker compose down

clean:
	docker compose down --volumes --remove-orphans

backfill:
	$(SCHED) airflow dags backfill trade_marts_daily -s $(START) -e $(END)

import-errors:
	$(SCHED) airflow dags list-import-errors

check:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v

verify: env
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify_runtime.py
