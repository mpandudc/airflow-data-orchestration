# Verification evidence

Verified on 2026-10-02 (Asia/Jakarta) with `make check` and `make verify`.

- Host: Proxmox LXC 206 `data` (4 vCPU, 8 GB RAM), Docker 29.1.3, run as a
  separate compose project beside the live data stack.
- Airflow 2.10.5 (LocalExecutor), dbt-core 1.12.5 in its own venv,
  dbt-clickhouse-marts pinned at `49d9f49`, ClickHouse 26.8.15.10.
- Offline: 7 unit tests passed, 4 DAG tests skipped (no Airflow on the bare host).
- Inside the image: all 11 tests passed, including the 4 DAG integrity tests;
  `airflow dags list-import-errors` empty.
- `airflow dags backfill` 2026-01-01..07: 7/7 DAG runs and 42/42 task instances
  `success` in 142 s.
- Marts: 244 rows in `fact_trades` = 244 distinct raw trades; PnL through
  2026-01-07; 7 audit rows in `ops.pipeline_runs`.
- Rerun of 2026-01-03 with `--reset-dagruns`: marts and audit unchanged.
- `airflow tasks test ... data_quality 2026-03-01` (date never loaded) failed
  with `data quality gate failed`, as intended.

Raw numbers: [results/verification.json](results/verification.json).
