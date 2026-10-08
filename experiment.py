import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import time
import urllib.request

import duckdb
import psutil

from experiment_report import write_report


SOURCE_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
TOPOLOGIES = {
    "case1": "1 connection / 1 cursor",
    "case2": "2 connections / 1 cursor each",
    "case3": "1 connection / 2 cursors",
}


def download_data(destination):
    if destination.exists():
        print(f"Using cached data: {destination}", flush=True)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(".part")
    print(f"Downloading NYC TLC January 2025 yellow taxi data: {SOURCE_URL}", flush=True)
    try:
        with urllib.request.urlopen(SOURCE_URL, timeout=120) as response:
            expected = response.headers.get("Content-Length")
            with partial.open("wb") as output:
                shutil.copyfileobj(response, output)
        if expected and partial.stat().st_size != int(expected):
            raise OSError("Incomplete dataset download")
        partial.replace(destination)
    finally:
        partial.unlink(missing_ok=True)


def prepare_database(database, data):
    with duckdb.connect(str(database), config={"threads": 1, "memory_limit": "512MB"}) as con:
        # A persistent view keeps every topology on exactly the same local input.
        source = str(data).replace("'", "''")
        con.execute(f"CREATE VIEW trips AS SELECT * FROM read_parquet('{source}')")
        return con.execute("SELECT count(*) FROM trips").fetchone()[0]


def directory_bytes(directory):
    total = 0
    for path in directory.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            pass  # DuckDB can remove a spill file between enumeration and stat.
    return total


def run_case(plan, root):
    root.mkdir(parents=True, exist_ok=True)
    requests = plan["requests"]
    config = {
        "memory_limit": plan["memory_limit"],
        "threads": plan["threads"],
        "preserve_insertion_order": False,
    }
    results = []
    samples = []
    process = psutil.Process()
    with TemporaryDirectory(prefix="scratch-", dir=root) as scratch:
        scratch = Path(scratch)
        spill = scratch / "spill"
        spill.mkdir()
        config["temp_directory"] = str(spill)
        parents, cursors = [], []
        stop = threading.Event()
        monitor = None
        try:
            count = 2 if plan["topology"] == "case2" else 1
            # Open matching connections before changing any global engine settings.
            for _ in range(count):
                parents.append(duckdb.connect(plan["database"], read_only=True, config=config))
            if plan["topology"] == "case3":
                cursors = [parents[0].cursor(), parents[0].cursor()]
            else:
                cursors = [parent.cursor() for parent in parents]
            parents[0].execute("SET max_temp_directory_size = ?", [plan["max_temp_size"]])
            settings = parents[0].execute(
                "SELECT current_setting('memory_limit'), current_setting('threads'), "
                "current_setting('max_temp_directory_size'), "
                "current_setting('preserve_insertion_order')"
            ).fetchone()
            effective = dict(zip(
                ["memory_limit", "threads", "max_temp_directory_size", "preserve_insertion_order"],
                settings,
            ))
            for con in cursors:
                con.execute("SET enable_profiling = 'json'")
                con.execute("PRAGMA disable_profiling")
                con.execute("SET profiling_coverage = 'ALL'")
            locks = [threading.Lock() for _ in cursors]
            barrier = threading.Barrier(requests + 1)
            state_lock = threading.Lock()
            active, max_active = 0, 0
            origin = time.perf_counter()

            def now_ms():
                return (time.perf_counter() - origin) * 1000

            def sample():
                samples.append({"ms": now_ms(), "rss_bytes": process.memory_info().rss,
                                "spill_bytes": directory_bytes(spill)})

            def watch():
                while not stop.wait(0.05):
                    sample()

            def send_request(index):
                nonlocal active, max_active
                slot = (index - 1) % len(cursors)
                barrier.wait()
                submitted = now_ms()
                with locks[slot]:
                    started = now_ms()
                    with state_lock:
                        active += 1
                        max_active = max(max_active, active)
                    con = cursors[slot]
                    profile = root / f"request-{index}-profile.json"
                    output = scratch / f"request-{index}.parquet"
                    status, error, rows = "ok", None, None
                    print(f"  request {index} START cursor {slot + 1} "
                          f"queue={started - submitted:.1f}ms", flush=True)
                    try:
                        # Change the retained output path while profiling is disabled.
                        con.execute("SET profiling_output = ?", [str(profile)])
                        con.execute("SET enable_profiling = 'json'")
                        con.sql(plan["query"]).write_parquet(
                            str(output), compression="snappy", row_group_size=8192,
                        )
                    except duckdb.OutOfMemoryException as exc:
                        status, error = "oom", str(exc)
                    except duckdb.Error as exc:
                        status, error = "error", str(exc)
                    finally:
                        finished = now_ms()
                        con.execute("PRAGMA disable_profiling")
                        with state_lock:
                            active -= 1
                    record = {
                        "id": index, "cursor": slot + 1, "submitted_ms": submitted,
                        "started_ms": started, "finished_ms": finished,
                        "queue_ms": started - submitted, "query_ms": finished - started,
                        "total_ms": finished - submitted, "status": status, "error": error,
                        "rows": rows, "profile": profile.name if status == "ok" and profile.exists() else None,
                    }
                    print(f"  request {index} {status.upper()} "
                          f"query={record['query_ms']:.1f}ms total={record['total_ms']:.1f}ms"
                          + (f" | {error.splitlines()[0]}" if error else ""), flush=True)
                    if status == "ok":
                        # Validate the full export after disabling profiling, without fetching its rows.
                        actual = con.execute("SELECT count(*) FROM read_parquet(?)", [str(output)]).fetchone()[0]
                        record["rows"] = actual
                        expected = plan.get("expected_rows")
                        if expected is not None and actual != expected:
                            record.update(status="error", error=f"Row count mismatch: {actual}, expected {expected}")
                    if status != "ok":
                        profile.unlink(missing_ok=True)
                    output.unlink(missing_ok=True)
                    return record

            sample()
            monitor = threading.Thread(target=watch, daemon=True)
            monitor.start()
            with ThreadPoolExecutor(max_workers=requests) as pool:
                futures = [pool.submit(send_request, i + 1) for i in range(requests)]
                barrier.wait()
                results = [future.result() for future in futures]
            elapsed = now_ms()
            sample()
            stop.set()
            monitor.join()
            survived = all(con.execute("SELECT 42").fetchone() == (42,) for con in cursors)
        finally:
            stop.set()
            if monitor:
                monitor.join()
            for con in reversed(parents + cursors):
                con.close()

    profiles = [json.loads((root / result["profile"]).read_text(encoding="utf-8"))
                for result in results if result["profile"]]
    case = {
        "topology": plan["topology"], "label": TOPOLOGIES[plan["topology"]],
        "mode": plan["mode"], "memory_limit": plan["memory_limit"],
        "effective_settings": effective, "elapsed_ms": elapsed,
        "rss_peak_bytes": max(s["rss_bytes"] for s in samples),
        "sampled_spill_peak_bytes": max(s["spill_bytes"] for s in samples),
        "profile_spill_peak_bytes": max((p.get("system_peak_temp_dir_size", 0) for p in profiles), default=0),
        "profile_buffer_peak_bytes": max((p.get("system_peak_buffer_memory", 0) for p in profiles), default=0),
        "max_active_requests": max_active, "engine_survived": survived,
        "requests": results, "samples": samples,
    }
    (root / "result.json").write_text(json.dumps(case, indent=2), encoding="utf-8")
    return case


def main():
    parser = argparse.ArgumentParser(description="Three DuckDB connection/cursor cases, with three simultaneous requests each.")
    parser.add_argument("--work-directory", type=Path, default=Path("work/experiment"))
    parser.add_argument("--query", type=Path, default=Path(__file__).with_name("experiment.sql"))
    parser.add_argument("--topology", choices=["all", *TOPOLOGIES], default="all")
    parser.add_argument("--mode", choices=["all", "normal", "spill", "oom"], default="all")
    parser.add_argument("--normal-memory", default="1GB")
    parser.add_argument("--spill-memory", default="256MB")
    parser.add_argument("--oom-memory", default="1MB")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--requests", type=int, default=3)
    parser.add_argument("--max-temp-size", default="2GB")
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        run_case(json.loads(args.worker.read_text(encoding="utf-8")), args.worker.parent)
        return 0
    if args.threads < 1 or not 1 <= args.requests <= 32:
        parser.error("Use positive threads and between 1 and 32 requests")
    work = args.work_directory.resolve()
    work.mkdir(parents=True, exist_ok=True)
    root = work / datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    root.mkdir()
    data = work / "yellow_tripdata_2025-01.parquet"
    download_data(data)
    query = args.query.read_text(encoding="utf-8")
    with duckdb.connect() as con:
        statements = con.extract_statements(query)
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            parser.error("The query file must contain exactly one SELECT using the trips view")
    database = root / "input.duckdb"
    row_count = prepare_database(database, data)
    with data.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    print(f"DuckDB {duckdb.__version__} | {row_count:,} rows | same persistent database", flush=True)
    modes = {"normal": args.normal_memory, "spill": args.spill_memory, "oom": args.oom_memory}
    cases = []
    for mode, budget in modes.items():
        if args.mode not in ("all", mode):
            continue
        for topology, label in TOPOLOGIES.items():
            if args.topology not in ("all", topology):
                continue
            cell = root / f"{mode}-{topology}"
            cell.mkdir()
            expected_rows = row_count if args.query.resolve() == Path(__file__).with_name("experiment.sql").resolve() else None
            plan = {"database": str(database), "expected_rows": expected_rows, "query": query,
                    "topology": topology, "mode": mode, "memory_limit": budget,
                    "requests": args.requests, "threads": args.threads,
                    "max_temp_size": args.max_temp_size}
            plan_path = cell / "plan.json"
            plan_path.write_text(json.dumps(plan, indent=2), encoding="utf-8")
            print(f"\n{mode.upper()} | {label} | memory_limit={budget}", flush=True)
            # A fresh process prevents an earlier case's allocator state contaminating RSS.
            subprocess.run([sys.executable, str(Path(__file__).resolve()), "--worker", str(plan_path)],
                           check=True, timeout=600)
            cases.append(json.loads((cell / "result.json").read_text(encoding="utf-8")))
    report = {
        "duckdb_version": duckdb.__version__, "python_version": platform.python_version(),
        "platform": platform.platform(), "created_at": datetime.now(timezone.utc).isoformat(),
        "source_url": SOURCE_URL, "source_page": SOURCE_PAGE, "data_sha256": digest,
        "data_bytes": data.stat().st_size, "row_count": row_count, "query": query,
        "common_settings": {"threads": args.threads, "max_temp_directory_size": args.max_temp_size,
                            "preserve_insertion_order": False, "requests": args.requests},
        "cases": cases,
    }
    (root / "results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    destination = root / "report.html"
    write_report(report, destination)
    print(f"\nReport: {destination}\nRaw results: {root / 'results.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
