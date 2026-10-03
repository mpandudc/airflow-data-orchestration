import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from lib.dbt_artifacts import COLUMNS, load_rows, rows_from_run_results, to_json_each_row  # noqa: E402

PAYLOAD = {
    "metadata": {"invocation_id": "inv-1", "generated_at": "2026-10-03T05:00:00.123456Z"},
    "results": [
        {"unique_id": "model.clickhouse_marts.fact_trades", "status": "success", "execution_time": 1.5,
         "adapter_response": {"rows_affected": 42}, "message": "OK"},
        {"unique_id": "test.clickhouse_marts.unique_fact_trades_trade_id.abc", "status": "fail",
         "execution_time": 0.2, "adapter_response": {}, "message": None},
    ],
}


class DbtArtifactsTest(unittest.TestCase):
    def test_rows_carry_run_context_and_outcome(self):
        rows = rows_from_run_results(PAYLOAD, "2026-01-03", "backfill__2026-01-03")
        self.assertEqual([r["resource_type"] for r in rows], ["model", "test"])
        self.assertEqual(rows[0]["rows_affected"], 42)
        self.assertIsNone(rows[1]["rows_affected"])
        self.assertEqual(rows[1]["status"], "fail")
        self.assertEqual(rows[0]["generated_at"], "2026-10-03 05:00:00.123")
        self.assertEqual(set(rows[0]), set(COLUMNS))

    def test_missing_file_yields_no_rows(self):
        self.assertEqual(load_rows(Path("/nonexistent/run_results.json"), "2026-01-03", "r"), [])

    def test_json_each_row_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run_results.json"
            path.write_text(json.dumps(PAYLOAD), encoding="utf-8")
            rows = load_rows(path, "2026-01-03", "r")
        decoded = [json.loads(line) for line in to_json_each_row(rows).splitlines()]
        self.assertEqual(decoded, rows)


if __name__ == "__main__":
    unittest.main()
