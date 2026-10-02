"""Runtime verification for the trade_marts_daily DAG.

1. DAG integrity tests and import-error check inside the Airflow image.
2. Backfill seven logical dates through the LocalExecutor; every task succeeds.
3. Marts hold exactly one row per distinct raw trade; one audit row per date.
4. Re-running a past date (--reset-dagruns) changes nothing.
5. The quality gate fails a date that was never loaded.
"""

from __future__ import annotations

import base64
import json
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CH_URL = "http://localhost:18125/"
CH_AUTH = "Basic " + base64.b64encode(b"dbt:dbt").decode()
SCHED = ("docker", "compose", "exec", "-T", "airflow-scheduler")


def sh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    print("+", " ".join(args[:10]), flush=True)
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True)
    if check and result.returncode != 0:
        print(result.stdout[-4000:], result.stderr[-4000:], sep="\n")
        raise SystemExit(f"command failed: {' '.join(args[:10])}")
    return result


def ch(query: str) -> str:
    req = urllib.request.Request(CH_URL, data=query.encode(), headers={"Authorization": CH_AUTH}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode().strip()


def metadata(sql: str) -> str:
    return sh("docker", "compose", "exec", "-T", "airflow-db", "psql", "-U", "airflow", "-d", "airflow",
              "-At", "-c", sql).stdout.strip()


def expect(condition: bool, message: str) -> None:
    print(("PASS " if condition else "FAIL ") + message, flush=True)
    if not condition:
        raise SystemExit(message)


def snapshot() -> dict:
    return {
        "raw_distinct_trades": int(ch("select uniqExact(trade_id) from raw.trades")),
        "fact_trades": int(ch("select count() from marts.fact_trades")),
        "pnl_rows": int(ch("select count() from marts.fct_daily_pnl")),
        "audit_dates": int(ch("select count() from ops.pipeline_runs final")),
        "max_pnl_date": ch("select max(as_of_date) from marts.fct_daily_pnl"),
    }


def main() -> None:
    evidence: dict = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    sh("docker", "compose", "down", "--volumes", "--remove-orphans")
    sh("docker", "compose", "up", "-d", "--build", "--wait")
    evidence["versions"] = {
        "airflow": sh(*SCHED, "airflow", "version").stdout.strip(),
        "dbt_marts_ref": sh(*SCHED, "cat", "/opt/dbt-project/.ref").stdout.strip(),
        "dbt": sh(*SCHED, "/opt/dbt-venv/bin/dbt", "--version").stdout.split("\n")[1].strip(),
        "clickhouse": ch("select version()"),
    }

    tests = sh(*SCHED, "python", "-m", "unittest", "discover", "-s", "/opt/airflow/tests", "-v", check=False)
    print(tests.stderr[-1500:])
    expect(tests.returncode == 0 and "skipped" not in tests.stderr.split("Ran")[-1], "DAG + unit tests pass in image")
    import_errors = sh(*SCHED, "airflow", "dags", "list-import-errors").stdout
    expect("No data found" in import_errors, "no DAG import errors")

    started = time.monotonic()
    sh(*SCHED, "airflow", "dags", "backfill", "trade_marts_daily", "-s", "2026-01-01", "-e", "2026-01-07")
    evidence["backfill_7_days_seconds"] = round(time.monotonic() - started, 1)
    runs = metadata("select state, count(*) from dag_run where dag_id = 'trade_marts_daily' group by state")
    tis = metadata("select state, count(*) from task_instance where dag_id = 'trade_marts_daily' group by state")
    expect(runs == "success|7", f"7 DAG runs succeeded ({runs})")
    expect(tis == "success|42", f"all 42 task instances succeeded ({tis})")

    base = snapshot()
    evidence["after_backfill"] = base
    expect(base["fact_trades"] == base["raw_distinct_trades"], "fact has one row per distinct raw trade")
    expect(base["audit_dates"] == 7, "one audit row per logical date")
    expect(base["max_pnl_date"] == "2026-01-07", "PnL reaches the last backfilled date")

    sh(*SCHED, "airflow", "dags", "backfill", "trade_marts_daily", "-s", "2026-01-03", "-e", "2026-01-03",
       "--reset-dagruns", "-y")
    expect(snapshot() == base, "re-running 2026-01-03 leaves marts and audit unchanged")

    gate = sh(*SCHED, "airflow", "tasks", "test", "trade_marts_daily", "data_quality", "2026-03-01", check=False)
    output = gate.stdout + gate.stderr
    expect(gate.returncode != 0 and "data quality gate failed" in output, "quality gate fails an unloaded date")
    evidence["gate_failure_message"] = next(
        (line.strip()[-300:] for line in output.splitlines() if "data quality gate failed" in line), ""
    )

    evidence["audit_sample"] = json.loads(
        ch("select ds, metrics from ops.pipeline_runs final order by ds limit 1 format JSONEachRow")
    )
    evidence["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    out = ROOT / "results" / "verification.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"evidence written to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
