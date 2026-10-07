import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


SCRIPT = Path(__file__).with_name("run.py")


class WorkerTests(unittest.TestCase):
    def setUp(self):
        scratch = Path.cwd() / "work"
        scratch.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="worker-test-", dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.spill = self.root / "spill"

    def invoke(self, *arguments):
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--temp-directory", str(self.spill), *arguments],
            capture_output=True,
            text=True,
            timeout=60,
        )

    def assert_scratch_clean(self):
        if self.spill.exists():
            self.assertEqual(list(self.spill.iterdir()), [])
        self.assertEqual(list(self.root.glob(".duckdb-output-*")), [])

    def sort_query(self):
        query = self.root / "sort.sql"
        query.write_text(
            "SELECT i, hash(i) AS sort_key FROM range(1000000) AS items(i) ORDER BY sort_key;",
            encoding="utf-8",
        )
        return query

    def test_export_correctness_and_profile(self):
        output = self.root / "customer's results.parquet"
        profile = self.root / "profile.json"
        result = self.invoke("--output", str(output), "--profile", str(profile))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("threads=1", result.stdout)
        with duckdb.connect() as con:
            actual = con.execute(
                "SELECT count(*), sum(row_count), sum(total) FROM read_parquet(?)",
                [str(output)],
            ).fetchone()
        self.assertEqual(actual, (1000, 1000000, 499999500000))
        self.assertIn("system_peak_buffer_memory", json.loads(profile.read_text()))
        self.assert_scratch_clean()

    def test_existing_result_is_preserved(self):
        output = self.root / "result.parquet"
        first = self.invoke("--output", str(output))
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        original = output.read_bytes()
        again = self.invoke("--output", str(output))
        self.assertEqual(again.returncode, 1)
        self.assertIn("Output already exists", again.stdout)
        self.assertEqual(output.read_bytes(), original)
        self.assert_scratch_clean()

    def test_real_duckdb_oom_is_handled(self):
        output = self.root / "result.parquet"
        result = self.invoke("--memory-limit", "1MB", "--output", str(output))
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("DuckDB OOM", result.stdout)
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(output.exists())
        self.assert_scratch_clean()

    def test_larger_than_budget_sort_spills_and_completes(self):
        output = self.root / "sorted.parquet"
        profile = self.root / "sort-profile.json"
        result = self.invoke(
            "--query", str(self.sort_query()), "--memory-limit", "16MB",
            "--output", str(output), "--profile", str(profile),
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertGreater(json.loads(profile.read_text())["system_peak_temp_dir_size"], 0)
        with duckdb.connect() as con:
            actual = con.execute(
                "SELECT count(*), sum(i) FROM read_parquet(?)", [str(output)]
            ).fetchone()
        self.assertEqual(actual, (1000000, 499999500000))
        self.assert_scratch_clean()

    def test_spill_capacity_failure_is_handled(self):
        output = self.root / "sorted.parquet"
        result = self.invoke(
            "--query", str(self.sort_query()), "--memory-limit", "16MB",
            "--max-temp-size", "1MB", "--output", str(output),
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("DuckDB OOM", result.stdout)
        self.assertFalse(output.exists())
        self.assert_scratch_clean()

    def test_multiple_statements_are_rejected_before_execution(self):
        query = self.root / "invalid.sql"
        query.write_text("CREATE TABLE unwanted AS SELECT 1; SELECT 2;", encoding="utf-8")
        output = self.root / "result.parquet"
        result = self.invoke("--query", str(query), "--output", str(output))
        self.assertEqual(result.returncode, 1)
        self.assertIn("exactly one SELECT", result.stdout)
        self.assertFalse(output.exists())
        self.assert_scratch_clean()

    def test_existing_database_opens_read_only(self):
        database = self.root / "input.duckdb"
        with duckdb.connect(str(database)) as con:
            con.execute("CREATE TABLE orders AS SELECT * FROM range(7)")
        query = self.root / "orders.sql"
        query.write_text("WITH summary AS (SELECT count(*) AS n FROM orders) SELECT * FROM summary;", encoding="utf-8")
        output = self.root / "orders.parquet"
        result = self.invoke(
            "--database", str(database), "--query", str(query), "--output", str(output)
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        with duckdb.connect() as con:
            self.assertEqual(con.execute("SELECT n FROM read_parquet(?)", [str(output)]).fetchone(), (7,))
        self.assert_scratch_clean()


if __name__ == "__main__":
    unittest.main()
