"""DAG integrity tests. They need Airflow, so they run inside the image
(`verify_runtime.py` executes them there) and are skipped on a bare host."""

import importlib.util
import unittest
from pathlib import Path

HAS_AIRFLOW = importlib.util.find_spec("airflow") is not None
DAGS = Path(__file__).resolve().parents[1] / "dags"


@unittest.skipUnless(HAS_AIRFLOW, "airflow not installed")
class DagIntegrityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from airflow.models import DagBag

        cls.bag = DagBag(dag_folder=str(DAGS), include_examples=False)

    def test_no_import_errors(self):
        self.assertEqual(self.bag.import_errors, {})

    def test_task_order(self):
        dag = self.bag.get_dag("trade_marts_daily")
        order = [t.task_id for t in dag.topological_sort()]
        expected = ["clickhouse_ready", "ensure_raw_schema", "load_raw", "dbt_build", "data_quality", "record_run"]
        self.assertEqual(order, expected)

    def test_single_active_run_and_retries(self):
        dag = self.bag.get_dag("trade_marts_daily")
        self.assertEqual(dag.max_active_runs, 1)
        self.assertFalse(dag.catchup)
        for task in dag.tasks:
            self.assertGreaterEqual(task.retries, 2, task.task_id)
            self.assertIsNotNone(task.on_failure_callback, task.task_id)

    def test_dbt_build_is_scoped_to_logical_date(self):
        command = self.bag.get_dag("trade_marts_daily").get_task("dbt_build").bash_command
        self.assertIn("start_date", command)
        self.assertIn("{{ ds }}", command)


if __name__ == "__main__":
    unittest.main()
