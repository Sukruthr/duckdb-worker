"""Run a small, repeatable DuckDB memory-limit and disk-spill experiment."""

import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import duckdb


ROOT = Path(__file__).resolve().parent.parent
WORK = ROOT / "work"
OUTPUTS = ROOT / "outputs"
DATA = WORK / "data" / "yellow_tripdata_2024-01.parquet"
DATABASE = WORK / "taxi.duckdb"
DATA_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2024-01.parquet"
)

# Edit these settings to experiment. GB/MB are DuckDB's decimal units.
THREADS = 1
SCENARIOS = [
    # Name, query, memory_limit, max_temp_directory_size (None disables spill)
    ("scan_128mb", "scan", "128MB", "1GB"),
    ("sort_2gb", "sort", "2GB", "1GB"),
    ("sort_256mb", "sort", "256MB", "1GB"),
    ("sort_128mb", "sort", "128MB", "1GB"),
    ("sort_no_spill", "sort", "128MB", None),
    ("sort_spill_cap", "sort", "128MB", "1MB"),
]
QUERIES = {
    "scan": "SELECT count(*) AS trips, sum(trip_distance) AS miles FROM trips",
    "sort": """
        SELECT * FROM trips
        ORDER BY total_amount DESC, tpep_pickup_datetime
    """,
}


def connection(memory_limit, spill_limit, name, *, read_only=True):
    """Give each connection its own spill directory and fixed settings."""
    temp_dir = WORK / "spill" / name
    if spill_limit is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DATABASE), read_only=read_only)
    try:
        con.execute("SET threads = ?", [THREADS])
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET memory_limit = ?", [memory_limit])
        con.execute("SET temp_directory = ?", [temp_dir.as_posix() if spill_limit else ""])
        # Apply with SET: in this version a connect(config=...) cap can be
        # reported by current_setting() without being enforced by the spill manager.
        con.execute("SET max_temp_directory_size = ?", [spill_limit or "1GB"])
    except duckdb.Error:
        con.close()
        raise
    return con


def prepare_data():
    """Download and persist the data once, outside the measured queries."""
    DATA.parent.mkdir(parents=True, exist_ok=True)
    if DATA.exists():
        print(f"Reusing downloaded data: {DATA}", flush=True)
    else:
        print("Downloading January 2024 NYC yellow taxi trips (~50 MB)...", flush=True)
        partial = DATA.with_suffix(".parquet.part")
        with urlopen(DATA_URL, timeout=60) as response, partial.open("wb") as target:
            shutil.copyfileobj(response, target, length=1024 * 1024)
            expected_size = response.headers.get("Content-Length")
        if expected_size and partial.stat().st_size != int(expected_size):
            raise RuntimeError("Incomplete download; rerun to retry.")
        partial.replace(DATA)

    con = connection("2GB", "1GB", "setup", read_only=False)
    try:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'trips'"
        ).fetchone()[0]
        if exists:
            print(f"Reusing local table: {DATABASE}", flush=True)
        else:
            print("Loading all source columns into the local trips table...", flush=True)
            con.execute("CREATE TABLE trips AS SELECT * FROM read_parquet(?)", [str(DATA)])
        rows = con.execute("SELECT count(*) FROM trips").fetchone()[0]
        source_rows = con.execute("SELECT count(*) FROM read_parquet(?)", [str(DATA)]).fetchone()[0]
        if rows != source_rows:
            raise RuntimeError(f"Row count mismatch: table={rows}, source={source_rows}")
        columns = len(con.execute("DESCRIBE trips").fetchall())
        con.execute("CHECKPOINT")
    finally:
        con.close()
    print(f"Verified {rows:,} rows, {columns} columns. Preparation is not timed.\n", flush=True)
    return {
        "url": DATA_URL,
        "rows": rows,
        "columns": columns,
        "download_bytes": DATA.stat().st_size,
    }


def profile_nodes(node):
    yield node
    for child in node.get("children", []):
        yield from profile_nodes(child)


def run_case(name, query, memory_limit, spill_limit):
    result = {
        "name": name,
        "query": query,
        "memory_limit": memory_limit,
        "spill_limit": spill_limit,
        "status": "success",
        # Keep this explicitly labeled: DuckDB 1.5.6 inflates this profiler metric.
        "reported_peak_buffer_bytes": None,
        "peak_spill_bytes": None,
        "sort_rows": None,
        "error": None,
    }
    con = connection(memory_limit, spill_limit, name)
    try:
        start = time.perf_counter()
        try:
            # Execute EVERY result row inside DuckDB, returning just a JSON plan.
            profile = json.loads(
                con.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + QUERIES[query]).fetchone()[1]
            )
            result["elapsed_seconds"] = time.perf_counter() - start
            result["reported_peak_buffer_bytes"] = profile["system_peak_buffer_memory"]
            result["peak_spill_bytes"] = profile["system_peak_temp_dir_size"]
            if query == "sort":
                result["sort_rows"] = next(
                    node["operator_cardinality"] for node in profile_nodes(profile)
                    if node.get("operator_type") == "ORDER_BY"
                )
        except duckdb.Error as error:
            result["elapsed_seconds"] = time.perf_counter() - start
            result["error"] = str(error)
            message = str(error).lower()
            expected = (
                spill_limit is None and (
                    "no temporary directory" in message
                    or "unused blocks cannot be offloaded" in message
                )
            ) or (spill_limit == "1MB" and "max_temp_directory_size" in message)
            result["status"] = "expected_failure" if expected else "unexpected_failure"
            profile = {"error": str(error)}
    finally:
        # DuckDB removes its temporary spill files when the connection closes.
        con.close()
    profile_path = OUTPUTS / "profiles" / f"{name}.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    result["profile_file"] = str(profile_path)
    return result


def mib(value):
    return "N/A" if value is None else f"{value / (1024 * 1024):.1f}"


def main():
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    print(f"DuckDB {duckdb.__version__}; threads={THREADS}; preserve_insertion_order=false\n")
    print("NOTE: DuckDB 1.5.6 overstates the reported buffer peak (*); see README.\n")
    dataset = prepare_data()
    print(
        f"{'Case':<18} {'Memory':>7} {'Spill cap':>10} {'Seconds':>8} "
        f"{'Reported*':>11} {'Spill MiB':>10}  Status", flush=True,
    )
    results = []
    for scenario in SCENARIOS:
        result = run_case(*scenario)
        results.append(result)
        print(
            f"{result['name']:<18} {result['memory_limit']:>7} "
            f"{result['spill_limit'] or 'disabled':>10} {result['elapsed_seconds']:>8.3f} "
            f"{mib(result['reported_peak_buffer_bytes']):>11} {mib(result['peak_spill_bytes']):>10}  "
            f"{result['status']}", flush=True,
        )

    cases = {result["name"]: result for result in results}
    successful_sorts = [r for r in results if r["query"] == "sort" and r["status"] == "success"]
    checks = {
        "scan_succeeds_without_spill": (
            cases["scan_128mb"]["status"] == "success"
            and cases["scan_128mb"]["peak_spill_bytes"] == 0
        ),
        "high_memory_sort_has_no_spill": (
            cases["sort_2gb"]["status"] == "success"
            and cases["sort_2gb"]["peak_spill_bytes"] == 0
        ),
        "constrained_sort_succeeds_with_spill": any(
            r["status"] == "success" and r["peak_spill_bytes"] > 0
            for r in results if r["name"] in ("sort_256mb", "sort_128mb")
        ),
        "successful_sorts_process_all_rows": (
            bool(successful_sorts)
            and all(r["sort_rows"] == dataset["rows"] for r in successful_sorts)
        ),
        "no_spill_fails_for_expected_reason": cases["sort_no_spill"]["status"] == "expected_failure",
        "spill_cap_fails_for_expected_reason": cases["sort_spill_cap"]["status"] == "expected_failure",
        "no_unexpected_failures": all(r["status"] != "unexpected_failure" for r in results),
    }
    summary = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "duckdb_version": duckdb.__version__,
        "threads": THREADS,
        "preserve_insertion_order": False,
        "measurement_notes": {
            "reported_peak_buffer_bytes": (
                "Raw system_peak_buffer_memory from the profiler. DuckDB 1.5.6 "
                "overstates this metric; it is not a reliable actual memory peak."
            ),
            "peak_spill_bytes": "Peak temporary storage occupancy, not cumulative disk writes.",
            "display_units": "MiB = 1024**2 bytes; SQL MB and GB are decimal.",
        },
        "dataset": dataset,
        "queries": QUERIES,
        "results": results,
        "checks": checks,
    }
    summary_path = OUTPUTS / "results.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    for result in results:
        if result["error"]:
            print(f"\n{result['name']}:\n{result['error']}")
    print("\nValidation:")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {name.replace('_', ' ')}")
    print("\n* Reported is the profiler's buffer-memory figure in MiB; 1.5.6 overstates it.")
    print("It is not a reliable actual peak and does not measure total Python RAM.")
    print("Spill MiB is peak temporary storage, not cumulative disk writes.")
    print("Timings include query profiling; OS caching and disk speed affect them.")
    print(f"Results: {summary_path}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
