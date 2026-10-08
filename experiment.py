import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
import time

import duckdb
import psutil


DATABASE = Path("work/concurrency.duckdb")
TEMP_DIRECTORY = Path("work/spill")
CONFIG = {"threads": 1, "preserve_insertion_order": False}
MEMORY_LIMITS = {"normal": "1GB", "spill": "256MB", "oom": "1MB"}


def case_1(sql, memory_limit):
    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    TEMP_DIRECTORY.mkdir(parents=True, exist_ok=True)
    config = {**CONFIG, "memory_limit": memory_limit, "temp_directory": str(TEMP_DIRECTORY)}
    with duckdb.connect(str(DATABASE), config=config) as connection:
        with connection.cursor() as cursor:
            return {"case": "case1", **_run_requests([cursor, cursor, cursor], sql)}


def case_2(sql, memory_limit):
    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    TEMP_DIRECTORY.mkdir(parents=True, exist_ok=True)
    config = {**CONFIG, "memory_limit": memory_limit, "temp_directory": str(TEMP_DIRECTORY)}
    # Both connections must exist before changing live global engine settings.
    with duckdb.connect(str(DATABASE), config=config) as connection_a:
        with duckdb.connect(str(DATABASE), config=config) as connection_b:
            with connection_a.cursor() as cursor_a, connection_b.cursor() as cursor_b:
                return {"case": "case2", **_run_requests([cursor_a, cursor_b, cursor_a], sql)}


def case_3(sql, memory_limit):
    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    TEMP_DIRECTORY.mkdir(parents=True, exist_ok=True)
    config = {**CONFIG, "memory_limit": memory_limit, "temp_directory": str(TEMP_DIRECTORY)}
    with duckdb.connect(str(DATABASE), config=config) as connection:
        with connection.cursor() as cursor_a, connection.cursor() as cursor_b:
            return {"case": "case3", **_run_requests([cursor_a, cursor_b, cursor_a], sql)}


def _run_requests(request_cursors, sql):
    cursors = list(dict.fromkeys(request_cursors))
    cursor_ids = {cursor: index + 1 for index, cursor in enumerate(cursors)}
    locks = {cursor: threading.Lock() for cursor in cursors}
    cursors[0].execute("SET max_temp_directory_size = '2GB'")
    values = cursors[0].execute(
        "SELECT current_setting('memory_limit'), current_setting('threads'), "
        "current_setting('max_temp_directory_size'), "
        "current_setting('preserve_insertion_order'), current_setting('temp_directory')"
    ).fetchone()
    settings = dict(zip(
        ["memory_limit", "threads", "max_temp_directory_size", "preserve_insertion_order", "temp_directory"],
        values,
    ))
    for cursor in cursors:
        cursor.execute("SET enable_profiling = 'no_output'")
    print(f"Settings: {settings}", flush=True)

    barrier = threading.Barrier(4)
    stop = threading.Event()
    process = psutil.Process()
    peak_rss = process.memory_info().rss

    def sample_rss():
        nonlocal peak_rss
        while not stop.wait(0.05):
            peak_rss = max(peak_rss, process.memory_info().rss)

    def request(index, cursor):
        barrier.wait()
        arrived = time.perf_counter()
        cursor_id = cursor_ids[cursor]
        print(f"Request {index} ARRIVED cursor={cursor_id}", flush=True)
        with locks[cursor]:
            started = time.perf_counter()
            print(f"Request {index} START cursor={cursor_id} queue={(started - arrived) * 1000:.1f}ms", flush=True)
            rows, status, error = 0, "ok", None
            buffer_peak, spill_peak = None, None
            try:
                cursor.execute(sql)
                while batch := cursor.fetchmany(2048):
                    rows += len(batch)
            except duckdb.OutOfMemoryException as exc:
                status, error = "duckdb_oom", str(exc)
            except duckdb.Error as exc:
                status, error = "query_error", str(exc)
            finished = time.perf_counter()
            if status == "ok":
                # Read before another request or diagnostic query replaces this profile.
                profile = json.loads(cursor.get_profiling_information())
                buffer_peak = profile.get("system_peak_buffer_memory")
                spill_peak = profile.get("system_peak_temp_dir_size")
            record = {
                "id": index, "cursor": cursor_id, "arrived_at": arrived,
                "started_at": started, "finished_at": finished,
                "queue_ms": (started - arrived) * 1000,
                "query_ms": (finished - started) * 1000,
                "total_ms": (finished - arrived) * 1000,
                "rows_consumed": rows, "status": status, "error": error,
            }
            print(f"Request {index} FINISH cursor={cursor_id} status={status} "
                  f"rows={rows:,} query={record['query_ms']:.1f}ms total={record['total_ms']:.1f}ms"
                  + (f" | {error.splitlines()[0]}" if error else ""), flush=True)
            return record, buffer_peak, spill_peak

    monitor = threading.Thread(target=sample_rss, daemon=True)
    monitor.start()
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(request, index + 1, cursor)
                       for index, cursor in enumerate(request_cursors)]
            barrier.wait()
            outcomes = [future.result() for future in futures]
    finally:
        stop.set()
        monitor.join()
        peak_rss = max(peak_rss, process.memory_info().rss)

    requests = [record for record, _, _ in outcomes]
    buffer_peak = max((value for _, value, _ in outcomes if value is not None), default=None)
    spill_peak = max((value for _, _, value in outcomes if value is not None), default=None)
    usability = []
    for cursor in cursors:
        cursor.disable_profiling()
        try:
            usability.append(cursor.execute("SELECT 42").fetchone() == (42,))
        except duckdb.Error:
            usability.append(False)
    result = {
        "settings": settings, "requests": requests,
        "batch_ms": (max(r["finished_at"] for r in requests) - min(r["arrived_at"] for r in requests)) * 1000,
        "peak_rss_bytes": peak_rss, "engine_peak_buffer_bytes": buffer_peak,
        "engine_peak_spill_bytes": spill_peak, "engine_usable": all(usability),
    }
    counts = {status: sum(r["status"] == status for r in requests)
              for status in ("ok", "duckdb_oom", "query_error")}
    spill = "unavailable" if spill_peak is None else f"{spill_peak / 1048576:.1f}MiB"
    buffer = "unavailable" if buffer_peak is None else f"{buffer_peak / 1048576:.1f}MiB"
    print(f"Summary: {counts} batch={result['batch_ms']:.1f}ms "
          f"rss_peak={peak_rss / 1048576:.1f}MiB engine_buffer_peak={buffer} "
          f"engine_spill_peak={spill} engine_usable={result['engine_usable']}", flush=True)
    return result


if __name__ == "__main__":
    cases = {"case1": case_1, "case2": case_2, "case3": case_3}
    parser = argparse.ArgumentParser(description="Three simultaneous DuckDB requests, one case per process.")
    parser.add_argument("case", choices=cases)
    parser.add_argument("scenario", choices=MEMORY_LIMITS)
    args = parser.parse_args()
    sql = Path(__file__).with_name("experiment.sql").read_text(encoding="utf-8")
    print(f"DuckDB {duckdb.__version__} | {args.case} | {args.scenario}", flush=True)
    result = cases[args.case](sql, MEMORY_LIMITS[args.scenario])
    statuses = [request["status"] for request in result["requests"]]
    spill = result["engine_peak_spill_bytes"]
    if args.scenario == "oom":
        expected = statuses == ["duckdb_oom"] * 3
    else:
        expected = statuses == ["ok"] * 3 and spill is not None
        expected = expected and (spill == 0 if args.scenario == "normal" else spill > 0)
    if not expected or not result["engine_usable"]:
        print("Observed outcomes did not meet this scenario's expectation; limits were not changed.", flush=True)
        raise SystemExit(1)
