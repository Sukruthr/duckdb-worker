from contextlib import redirect_stdout
import io
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb

import experiment


ROWS = 1_000_000
SORT_SQL = (
    "SELECT i, hash(i) AS sort_key, repeat(CAST(i AS VARCHAR), 8) AS payload "
    "FROM inputs ORDER BY sort_key;\n-- trailing comment\n"
)


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        work = Path.cwd() / "work"
        work.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="concurrency-test-", dir=work)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "shared.duckdb"
        self.spill = self.root / "spill"
        config = {"threads": 1, "preserve_insertion_order": False}
        self.constants = patch.multiple(
            experiment, DATABASE=self.database, TEMP_DIRECTORY=self.spill, CONFIG=config,
        )
        self.constants.start()
        self.addCleanup(self.constants.stop)
        with duckdb.connect(str(self.database)) as con:
            con.execute(f"CREATE VIEW inputs AS SELECT i FROM range({ROWS}) AS items(i)")

    def run_case(self, function, sql, memory, cursor_count):
        with redirect_stdout(io.StringIO()):
            result = function(sql, memory)
        self.assertEqual(result["case"], function.__name__.replace("_", ""))
        self.assertEqual(len(result["requests"]), 3)
        self.assertTrue(result["engine_usable"])
        self.assertGreater(result["peak_rss_bytes"], 0)
        self.assertGreater(result["batch_ms"], 0)
        self.assertEqual(result["settings"]["threads"], 1)
        self.assertFalse(result["settings"]["preserve_insertion_order"])
        self.assertEqual({r["id"] for r in result["requests"]}, {1, 2, 3})
        self.assertEqual({r["cursor"] for r in result["requests"]}, set(range(1, cursor_count + 1)))
        for request in result["requests"]:
            self.assertLessEqual(request["arrived_at"], request["started_at"])
            self.assertLessEqual(request["started_at"], request["finished_at"])
            for key in ("queue_ms", "query_ms", "total_ms"):
                self.assertTrue(math.isfinite(request[key]))
                self.assertGreaterEqual(request[key], 0)
            self.assertAlmostEqual(request["total_ms"], request["queue_ms"] + request["query_ms"], places=5)
        for cursor in range(1, cursor_count + 1):
            requests = sorted(
                (r for r in result["requests"] if r["cursor"] == cursor),
                key=lambda r: r["started_at"],
            )
            for previous, current in zip(requests, requests[1:]):
                self.assertLessEqual(previous["finished_at"], current["started_at"])
        events = sorted(
            event for request in result["requests"]
            for event in ((request["started_at"], 1), (request["finished_at"], -1))
        )
        active = 0
        for _, change in events:
            active += change
            self.assertLessEqual(active, cursor_count)
        self.assertEqual(active, 0)
        if self.spill.exists():
            self.assertEqual(list(self.spill.iterdir()), [])
        self.assertEqual(list(self.root.rglob("*.json")), [])
        # A different configuration can reopen the file only after all case handles close.
        with duckdb.connect(str(self.database), config={"threads": 2}) as con:
            self.assertEqual(con.execute("SELECT 42").fetchone(), (42,))
        return result

    def cases(self):
        return ((experiment.case_1, 1), (experiment.case_2, 2), (experiment.case_3, 2))

    def test_normal_requests_all_layouts(self):
        for function, cursor_count in self.cases():
            with self.subTest(case=function.__name__):
                result = self.run_case(function, "SELECT i FROM inputs LIMIT 1000;\n-- comment\n", "128MB", cursor_count)
                self.assertEqual([r["status"] for r in result["requests"]], ["ok"] * 3)
                self.assertEqual([r["rows_consumed"] for r in result["requests"]], [1000] * 3)
                self.assertTrue(all(r["error"] is None for r in result["requests"]))
                self.assertGreater(result["engine_peak_buffer_bytes"], 0)
                self.assertEqual(result["engine_peak_spill_bytes"], 0)

    def test_spill_requests_all_layouts(self):
        for function, cursor_count in self.cases():
            with self.subTest(case=function.__name__):
                result = self.run_case(function, SORT_SQL, "64MB", cursor_count)
                self.assertEqual([r["status"] for r in result["requests"]], ["ok"] * 3,
                                 [r["error"] for r in result["requests"]])
                self.assertEqual([r["rows_consumed"] for r in result["requests"]], [ROWS] * 3)
                self.assertGreater(result["engine_peak_spill_bytes"], 0)

    def test_handled_oom_all_layouts(self):
        for function, cursor_count in self.cases():
            with self.subTest(case=function.__name__):
                result = self.run_case(function, SORT_SQL, "1MB", cursor_count)
                self.assertEqual([r["status"] for r in result["requests"]], ["duckdb_oom"] * 3)
                self.assertTrue(all(r["error"] for r in result["requests"]))
                self.assertEqual([r["rows_consumed"] for r in result["requests"]], [0] * 3)
                self.assertIsNone(result["engine_peak_buffer_bytes"])
                self.assertIsNone(result["engine_peak_spill_bytes"])

    def test_query_errors_leave_engine_usable(self):
        result = self.run_case(experiment.case_1, "SELECT missing_column FROM inputs", "128MB", 1)
        self.assertEqual([r["status"] for r in result["requests"]], ["query_error"] * 3)
        self.assertTrue(all(r["error"] for r in result["requests"]))
        self.assertIsNone(result["engine_peak_buffer_bytes"])
        self.assertIsNone(result["engine_peak_spill_bytes"])


if __name__ == "__main__":
    unittest.main()
