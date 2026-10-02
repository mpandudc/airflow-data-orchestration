# airflow-data-orchestration

Apache Airflow orchestrating the [dbt-clickhouse-marts](https://github.com/mpandudc/dbt-clickhouse-marts)
project on ClickHouse: one idempotent run per logical date, a data quality
gate after dbt, an audit table, and failure alerts. Backfills and reruns
converge to the same marts.

Part of the homelab data platform (Proxmox LXC 206). Airflow 2.10.5 matches
the version already running on that host.

```
trade_marts_daily  (@daily, max_active_runs=1, retries=2, failure alert)

clickhouse_ready ─► ensure_raw_schema ─► load_raw ─► dbt_build ─► data_quality ─► record_run
   PythonSensor      IF NOT EXISTS DDL    drop+insert   build with     5 gates for      ops.pipeline_runs
   (reschedule)      from dbt repo        day partition  vars {ds}      the logical day  (ReplacingMergeTree)
```

## Design

- **Pinned dbt project.** The image downloads `dbt-clickhouse-marts` at an
  exact commit (`DBT_MARTS_REF` build arg) into `/opt/dbt-project`, and installs
  dbt into its own venv so its pins never fight Airflow's constraints.
- **Date-scoped, idempotent tasks.** `load_raw` replaces exactly one day
  partition; `dbt_build` passes `start_date/end_date = ds` so `fact_trades`
  rebuilds only that window; reruns change nothing.
- **Quality gate beyond dbt tests.** A green dbt build on an empty source
  still passes uniqueness tests. `data_quality` checks that the day actually
  loaded, the fact holds every distinct raw trade of the day, no duplicates
  exist, and PnL covers the day. Failures raise `AirflowFailException`: bad data
  does not improve on retry.
- **Alerts.** `on_failure_callback` posts to a Discord webhook (the homelab's
  Hermes channel) when the Airflow Variable `alert_webhook_url` is set;
  otherwise it only logs.
- **Secrets.** `make env` generates the Fernet key, webserver secret, admin
  and DB passwords into a gitignored `.env`; compose refuses to start without them.

## Run

```bash
make check                                   # offline unit tests (DAG tests skip without Airflow)
make up                                      # build image, start Airflow + ClickHouse
make backfill START=2026-01-01 END=2026-01-07
make verify                                  # end-to-end proof, writes results/verification.json
make clean
```

| Port | Service |
|---|---|
| 18080 | **Airflow web UI** (user `admin`, password in `.env`) |
| 18125 | ClickHouse HTTP (`dbt` / `dbt`) |

See [VERIFICATION.md](VERIFICATION.md). On the platform, the UI is published
as `airflow.cahyo.tech` behind Cloudflare Access by
[homelab-data-platform](https://github.com/mpandudc/homelab-data-platform).
