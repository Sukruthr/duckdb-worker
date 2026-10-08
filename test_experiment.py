import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import duckdb

from experiment import prepare_database


SCRIPT = Path(__file__).with_name("experiment.py")
ROWS = 1_000_000
QUERY = "SELECT i, hash(i) AS sort_key FROM trips ORDER BY sort_key;\n-- trailing comment\n"
TOPOLOGIES = {"case1": 1, "case2": 2, "case3": 2}


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        work = Path.cwd() / "work"
        work.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="experiment-test-", dir=work)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "local input.parquet"
        self.database = self.root / "input.duckdb"
        with duckdb.connect(config={"threads": 1}) as con:
            con.sql(f"SELECT i FROM range({ROWS}) AS items(i)").write_parquet(str(self.data))
        self.assertEqual(prepare_database(self.database, self.data), ROWS)

    def invoke(self, *arguments):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), *arguments],
            capture_output=True, text=True, timeout=90,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        return result

    def run_worker(self, topology, mode, memory):
        cell = self.root / f"{mode}-{topology}"
        cell.mkdir()
        plan = {
            "database": str(self.database), "query": QUERY, "expected_rows": ROWS,
            "topology": topology, "mode": mode, "memory_limit": memory,
            "requests": 3, "threads": 1, "max_temp_size": "128MB",
        }
        path = cell / "plan.json"
        path.write_text(json.dumps(plan), encoding="utf-8")
        self.invoke("--worker", str(path))
        case = json.loads((cell / "result.json").read_text(encoding="utf-8"))
        self.assertEqual(case["topology"], topology)
        self.assertEqual(case["mode"], mode)
        self.assertTrue(case["engine_survived"])
        self.assertEqual(len(case["requests"]), 3)
        self.assertGreaterEqual(case["max_active_requests"], 1)
        self.assertLessEqual(case["max_active_requests"], TOPOLOGIES[topology])
        self.assertEqual(case["effective_settings"]["threads"], 1)
        self.assertFalse(case["effective_settings"]["preserve_insertion_order"])
        self.assertGreater(case["rss_peak_bytes"], 0)
        self.assertTrue(case["samples"])
        self.assertEqual(list(cell.glob("scratch-*")), [])
        self.assertEqual(list(cell.rglob("*.parquet")), [])
        for cursor in range(1, TOPOLOGIES[topology] + 1):
            requests = sorted(
                (r for r in case["requests"] if r["cursor"] == cursor),
                key=lambda r: r["started_ms"],
            )
            for previous, current in zip(requests, requests[1:]):
                self.assertLessEqual(previous["finished_ms"], current["started_ms"])
        return cell, case

    def assert_successful_exports(self, cell, case):
        for request in case["requests"]:
            self.assertEqual(request["status"], "ok", request["error"])
            self.assertEqual(request["rows"], ROWS)
            self.assertIsNone(request["error"])
            profile = json.loads((cell / request["profile"]).read_text(encoding="utf-8"))
            nodes, pending = [], list(profile["children"])
            while pending:
                node = pending.pop()
                nodes.append(node)
                pending.extend(node.get("children", []))
            operators = {node["operator_name"] for node in nodes}
            self.assertTrue({"COPY_TO_FILE", "ORDER_BY", "READ_PARQUET"} <= operators)
            scan, = [node for node in nodes if node["operator_name"] == "READ_PARQUET"]
            self.assertEqual(scan["operator_rows_scanned"], ROWS)
            self.assertIn(str(self.data), scan["extra_info"]["Filename(s)"])
            self.assertGreater(profile["system_peak_buffer_memory"], 0)
            self.assertGreaterEqual(profile["system_peak_temp_dir_size"], 0)
        self.assertEqual(len(list(cell.glob("request-*-profile.json"))), 3)

    def test_normal_exports_all_topologies(self):
        for topology in TOPOLOGIES:
            with self.subTest(topology=topology):
                cell, case = self.run_worker(topology, "normal", "128MB")
                self.assert_successful_exports(cell, case)

    def test_spill_exports_all_topologies(self):
        for topology in TOPOLOGIES:
            with self.subTest(topology=topology):
                memory = "16MB" if topology == "case1" else "32MB"
                cell, case = self.run_worker(topology, "spill", memory)
                self.assert_successful_exports(cell, case)
                self.assertGreater(case["profile_spill_peak_bytes"], 0)

    def test_handled_oom_preserves_all_topologies(self):
        for topology in TOPOLOGIES:
            with self.subTest(topology=topology):
                cell, case = self.run_worker(topology, "oom", "1MB")
                for request in case["requests"]:
                    self.assertEqual(request["status"], "oom", request["error"])
                    self.assertTrue(request["error"])
                    self.assertIsNone(request["rows"])
                    self.assertIsNone(request["profile"])
                self.assertEqual(list(cell.glob("request-*-profile.json")), [])

    def test_custom_query_and_local_source_metadata(self):
        work = self.root / "cli"
        work.mkdir()
        cached = work / "yellow_tripdata_2025-01.parquet"
        shutil.copyfile(self.data, cached)
        query = self.root / "custom.sql"
        sql = "SELECT i FROM trips ORDER BY i LIMIT 7;\n-- trailing comment\n"
        query.write_text(sql, encoding="utf-8")
        result = self.invoke(
            "--work-directory", str(work), "--query", str(query),
            "--topology", "case1", "--mode", "normal", "--normal-memory", "128MB",
        )
        self.assertIn("Using cached data", result.stdout)
        report_path, = work.glob("*/results.json")
        report = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(report["data_sha256"], hashlib.sha256(cached.read_bytes()).hexdigest())
        self.assertEqual(report["data_bytes"], cached.stat().st_size)
        self.assertEqual(report["row_count"], ROWS)
        self.assertEqual(report["query"], sql)
        self.assertEqual([r["rows"] for r in report["cases"][0]["requests"]], [7, 7, 7])
        self.assertEqual([r["status"] for r in report["cases"][0]["requests"]], ["ok"] * 3)
        self.assertTrue(report_path.with_name("report.html").exists())


if __name__ == "__main__":
    unittest.main()
