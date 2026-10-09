r"""See how DuckDB query threads change spilling under one fixed memory budget.

Run from the workspace in PowerShell (the uv environment is already installed):
    uv run --no-project --python .\work\.venv\Scripts\python.exe .\outputs\thread_memory_spill_demo.py

One workload connection and one full sort run at a time. Only threads changes.
128 MB is the TOTAL buffer-manager budget, not 128 MB for each thread.
More threads can need more live buffers, so spilling does not guarantee success.

The table omits system_peak_buffer_memory: DuckDB 1.5.6 overstates that profiler
metric. It measures neither reliable peak buffer memory nor total Python RAM.
Peak spill storage is still usable. N/A means a failed query has no final profile.

Guidance: https://duckdb.org/docs/current/guides/performance/environment#memory
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DATABASE = ROOT / "work" / "taxi.duckdb"
OUTPUTS = ROOT / "outputs"

# Edit these constants to explore; keep memory and spill fixed across the sweep.
THREAD_COUNTS = [1, 2, 4, 8]
MEMORY_LIMIT = "128MB"
SPILL_LIMIT = "1GB"
EXPECTED_ROWS = 2_964_624
SORT_SQL = """
    SELECT * FROM trips
    ORDER BY total_amount DESC, tpep_pickup_datetime
"""


def check_database():
    if not DATABASE.is_file():
        raise RuntimeError("Taxi database missing. Run duckdb_memory_demo.py to prepare it.")
    con = duckdb.connect(str(DATABASE), read_only=True)
    try:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'trips'"
        ).fetchone()[0]
        if not exists:
            raise RuntimeError("The trips table is missing. Run duckdb_memory_demo.py first.")
        rows = con.execute("SELECT count(*) FROM trips").fetchone()[0]
        if rows != EXPECTED_ROWS:
            raise RuntimeError(f"Expected {EXPECTED_ROWS:,} trips; found {rows:,}.")
        return rows
    finally:
        con.close()


def profile_nodes(node):
    yield node
    for child in node.get("children", []):
        yield from profile_nodes(child)


def run_case(threads):
    spill_dir = ROOT / "work" / "thread_memory_spill" / f"threads_{threads}"
    spill_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "requested_threads": threads,
        "memory_limit": MEMORY_LIMIT,
        "spill_limit": SPILL_LIMIT,
        "status": "SUCCESS",
        "peak_spill_bytes": None,
        "sort_rows": None,
        "exception_type": None,
        "error": None,
    }

    con = duckdb.connect(str(DATABASE), read_only=True)
    try:
        con.execute("SET threads = ?", [threads])
        con.execute("SET memory_limit = ?", [MEMORY_LIMIT])
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET temp_directory = ?", [spill_dir.as_posix()])
        # SET is required here: in 1.5.6 connect(config=...) can report a spill
        # cap without actually applying it to the temporary-file manager.
        con.execute("SET max_temp_directory_size = ?", [SPILL_LIMIT])

        # Read settings back rather than assuming every SET took effect.
        names = ["threads", "memory_limit", "max_temp_directory_size",
                 "preserve_insertion_order", "temp_directory"]
        values = con.execute("""
            SELECT current_setting('threads'), current_setting('memory_limit'),
                   current_setting('max_temp_directory_size'),
                   current_setting('preserve_insertion_order'),
                   current_setting('temp_directory')
        """).fetchone()
        result["settings"] = dict(zip(names, values))
        result["expected_temp_directory"] = spill_dir.as_posix()

        try:
            # DuckDB executes and discards the complete sorted result. Python
            # receives one small JSON document, not millions of result rows.
            profile = json.loads(
                con.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + SORT_SQL).fetchone()[1]
            )
            result["peak_spill_bytes"] = profile["system_peak_temp_dir_size"]
            result["sort_rows"] = next(
                node["operator_cardinality"] for node in profile_nodes(profile)
                if node.get("operator_type") == "ORDER_BY"
            )
        except duckdb.Error as error:
            result["status"] = "OOM" if isinstance(error, duckdb.OutOfMemoryException) else "ERROR"
            result["exception_type"] = type(error).__name__
            result["error"] = str(error)
            profile = {"exception_type": type(error).__name__, "error": str(error)}
    finally:
        # Closing the last connection removes DuckDB's spill files. The profile
        # retains peak usage even when the directory is empty afterwards.
        con.close()

    profile_path = OUTPUTS / "thread_memory_spill_profiles" / f"threads_{threads}.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    result["profile_file"] = str(profile_path)
    return result


def main():
    rows = check_database()
    print(f"DuckDB {duckdb.__version__}; full sort of {rows:,} trips.")
    print("One workload connection and one query at a time; only threads changes.")
    print(f"{MEMORY_LIMIT} is the total shared budget, not a per-thread allowance.")
    print("The unreliable 1.5.6 buffer-peak metric is omitted; spill peaks are in MiB.\n")
    print(f"{'Threads':>7} {'Memory budget':>14} {'Spill allowance':>17} {'Peak spill MiB':>16}  Outcome", flush=True)

    results = []
    for threads in THREAD_COUNTS:
        result = run_case(threads)
        results.append(result)
        peak = result["peak_spill_bytes"]
        peak_text = "N/A" if peak is None else f"{peak / (1024 * 1024):.1f}"
        print(
            f"{threads:>7} {MEMORY_LIMIT:>14} {SPILL_LIMIT:>17} "
            f"{peak_text:>16}  {result['status']}", flush=True,
        )

    baseline = next((r for r in results if r["requested_threads"] == 1), None)
    successful = [r for r in results if r["status"] == "SUCCESS"]
    budgets = {
        (r["settings"]["memory_limit"], r["settings"]["max_temp_directory_size"])
        for r in results
    }
    checks = {
        "requested_threads_applied": all(
            r["settings"]["threads"] == r["requested_threads"] for r in results
        ),
        "same_memory_and_spill_budgets": len(budgets) == 1,
        "insertion_order_disabled_and_spill_enabled": all(
            r["settings"]["preserve_insertion_order"] is False
            and r["settings"]["temp_directory"] == r["expected_temp_directory"]
            for r in results
        ),
        "successful_sorts_process_all_rows": bool(successful) and all(
            r["sort_rows"] == rows for r in successful
        ),
        "one_thread_succeeds_with_spill": (
            baseline is not None and baseline["status"] == "SUCCESS"
            and baseline["peak_spill_bytes"] > 0
        ),
        "no_unexpected_errors": all(r["status"] != "ERROR" for r in results),
    }
    summary = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "duckdb_version": duckdb.__version__,
        "rows": rows,
        "sql": SORT_SQL,
        "memory_limit": MEMORY_LIMIT,
        "spill_limit": SPILL_LIMIT,
        "notes": [
            "The memory budget is shared by the query threads, not multiplied by threads.",
            "SQL MB/GB are decimal; displayed MiB = bytes / 1024**2.",
            "Peak spill is maximum temporary storage occupancy, not total disk writes.",
            "DuckDB 1.5.6 overstates system_peak_buffer_memory; it is omitted from this comparison.",
            "OOM is an observed outcome; no monotonic spill pattern or failure threshold is assumed.",
        ],
        "results": results,
        "checks": checks,
    }
    summary_path = OUTPUTS / "thread_memory_spill_results.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    for result in results:
        if result["error"]:
            print(f"\n{result['requested_threads']} threads ({result['exception_type']}):\n{result['error']}")
    print("\nValidation:")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {name.replace('_', ' ')}")
    print("\nMore threads change live buffers and sort-run sizes within the same budget.")
    print("Available spill space does not guarantee that all required buffers fit.")
    print("Observed spill need not rise monotonically with threads; OOM cases have N/A peaks.")
    print("This experiment measures spill storage and outcomes, not actual process-memory peaks.")
    print(f"Results: {summary_path}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
