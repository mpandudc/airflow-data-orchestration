import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from lib.notify import build_payload  # noqa: E402
from lib.quality import checks_for, evaluate  # noqa: E402

DAY = date(2026, 1, 3)


class QualityTest(unittest.TestCase):
    def setUp(self):
        self.checks = checks_for(DAY)
        self.good = {
            "raw_trades_loaded": 35,
            "fact_complete_for_day": 0,
            "fact_has_no_duplicates": 0,
            "pnl_covers_day": 12,
            "pnl_not_ahead_of_prices": 0,
        }

    def test_check_names_are_unique(self):
        names = [c.name for c in self.checks]
        self.assertEqual(len(names), len(set(names)))

    def test_day_scoped_checks_reference_the_day(self):
        for check in self.checks:
            if check.name in ("raw_trades_loaded", "fact_complete_for_day", "pnl_covers_day"):
                self.assertIn("2026-01-03", check.sql)

    def test_healthy_results_pass(self):
        self.assertEqual(evaluate(self.good, self.checks), [])

    def test_empty_load_fails_even_if_everything_else_is_consistent(self):
        failures = evaluate({**self.good, "raw_trades_loaded": 0, "pnl_covers_day": 0}, self.checks)
        self.assertEqual(len(failures), 2)
        self.assertTrue(failures[0].startswith("raw_trades_loaded"))

    def test_missing_fact_rows_fail(self):
        self.assertTrue(evaluate({**self.good, "fact_complete_for_day": 3}, self.checks))

    def test_missing_result_is_an_error_not_a_pass(self):
        with self.assertRaises(KeyError):
            evaluate({}, self.checks)


class NotifyTest(unittest.TestCase):
    def test_payload_is_bounded_and_names_the_task(self):
        payload = build_payload("trade_marts_daily", "dbt_build", "2026-01-03", 2, "http://x/log", "e" * 1000)
        self.assertIn("trade_marts_daily.dbt_build", payload["content"])
        self.assertLess(len(payload["content"]), 500)


if __name__ == "__main__":
    unittest.main()
