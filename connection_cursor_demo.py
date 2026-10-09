r"""Compare DuckDB connections/cursors while two identical queries compete.

Run from the workspace in PowerShell; activation is unnecessary:
    uv pip install --python .\work\.venv\Scripts\python.exe "psutil==7.2.2"
    uv run --no-project --python .\work\.venv\Scripts\python.exe .\outputs\connection_cursor_demo.py --smoke
    uv run --no-project --python .\work\.venv\Scripts\python.exe .\outputs\connection_cursor_demo.py

Edit SQL and resource settings in connection_cursor_workload.py. This supervisor
opens NO DuckDB connection. Each case gets a fresh child process, with both query
handles inside it; starting Q2 in another process would change the experiment.

Full runs calibrate Q1 alone to 150-210 seconds, then freeze that SQL for all three
comparisons. Expect about 20-30 minutes. --smoke uses eight hash repetitions and
a one-second Q2 delay, without duration calibration. Its reports are separate.

RSS is process RAM, not DuckDB buffer memory. OS threads include idle/Python
threads; CPU core equivalents show activity. One-second samples can miss peaks.
Spill-file sizes are logical file lengths, not cumulative writes or DuckDB's
occupied spill capacity. Overlapping queries share process resources. The buggy
DuckDB 1.5.6 system_peak_buffer_memory metric is retained only in raw profiles.
"""

import argparse
import csv
import html
import json
import multiprocessing as mp
import queue
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import psutil

import connection_cursor_workload as workload


OUTPUT_DIR = workload.ROOT / "outputs" / "connection_cursor"
SPILL_ROOT = workload.ROOT / "work" / "connection_cursor_spill"
SAMPLE_SECONDS = 1.0
IDLE_SECONDS = 3.0
TAIL_SECONDS = 5.0
DELAY_SECONDS = 10.0
CASE_TIMEOUT_SECONDS = 900.0
CANCEL_GRACE_SECONDS = 10.0
TARGET_SECONDS = 180.0
CALIBRATION_BAND = (150.0, 210.0)
MAX_CALIBRATION_ATTEMPTS = 3
MAX_HASH_REPEATS = 512
PAIRED_CASES = ("one_cursor", "two_connections", "two_cursors")
MIB = 1024**2

NOTES = [
    f"{workload.MEMORY_LIMIT} is the shared DuckDB buffer budget, not a hard limit on process RSS. "
    "Process memory, CPU, and thread counts cannot be attributed to individual overlapping queries.",
    f"All OS threads includes Python callers and idle DuckDB workers. threads={workload.THREADS} is a shared "
    "scheduler setting, not a private pool per connection or a cap on all OS threads. "
    "Query-calling threads can also execute DuckDB tasks, so concurrent callers can use more CPU cores.",
    "CPU core equivalents = change in process CPU seconds / elapsed seconds. 2.0 means "
    "approximately two busy cores. Resource peaks and average CPU are sampled estimates.",
    "Spill-file charts show logical file lengths. Windows can retain freed space in these "
    "files; these lengths are not DuckDB's occupied spill capacity or cumulative disk writes.",
    "Raw profile spill peaks observe the shared database during each query. Never add Q1 "
    "and Q2 peaks. Failed queries have no final profile. Buffer-memory peaks in 1.5.6 are buggy.",
    "Execution includes execute and fetch of the small EXPLAIN JSON result. Application wait "
    "is measured before that call; it is not DuckDB scheduler wait. A running call can wait inside DuckDB.",
    "Each case uses fresh process state, but OS file caching persists. Timings are individual "
    "observations; concurrency need not improve speed or increase memory and spilling monotonically.",
]
SOURCES = [
    ("Cursor sessions", "https://duckdb.org/docs/current/clients/python/overview#about-cursor"),
    ("Memory limits", "https://duckdb.org/docs/current/configuration/pragmas#memory-limit"),
    ("Shared task scheduler", "https://github.com/duckdb/duckdb/blob/v1.5.6/src/parallel/task_scheduler.cpp"),
    ("Public monitoring example", "https://github.com/duckdb/duckdb-tpch/blob/main/benchmark.py"),
    ("1.5.6 profiler bug", "https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/main/query_profiler.hpp#L66-L69"),
    ("Windows spill files", "https://github.com/duckdb/duckdb/blob/v1.5.6/src/storage/temporary_file_manager.cpp"),
]


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def spill_file_bytes(directory):
    total = 0
    for path in directory.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except (FileNotFoundError, PermissionError):
            pass  # DuckDB may delete a file between enumeration and stat.
    return total


def query_state(query, timestamp):
    if query is None or timestamp < query["submitted_at"]:
        return "not submitted"
    if query.get("finished_at") is not None and timestamp >= query["finished_at"]:
        return query["status"].lower()
    if query.get("started_at") is None or timestamp < query["started_at"]:
        return "waiting"
    return "executing"


def run_case(case_key, run_id, repeats, output_dir, delay, timeout):
    """Monitor one child. Only directories created by TemporaryDirectory are removed."""
    print(f"\n{run_id}: {workload.CASES[case_key]['label']} | hash repetitions={repeats}", flush=True)
    SPILL_ROOT.mkdir(parents=True, exist_ok=True)
    profiles = output_dir / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    record = {"run_id": run_id, "case_key": case_key, "hash_repeats": repeats,
              "metadata": None, "queries": [], "events": [], "samples": [],
              "error": None, "stop_reason": None, "forced_stop": False}
    context = mp.get_context("spawn")
    events, cancel = context.Queue(), context.Event()
    live_queries = {}

    def consume(event):
        kind = event["event"]
        if kind == "ready":
            record["metadata"] = event["metadata"]
        elif kind in ("query_submitted", "query_started"):
            name = event["query"]
            live = live_queries.setdefault(name, {"query": name, "status": "RUNNING"})
            field = "submitted_at" if kind == "query_submitted" else "started_at"
            live[field] = event.get(field, event["time"])
        elif kind == "query_finished":
            result = event["result"].copy()
            profile = result.pop("profile", None)
            filename = f"profiles/{run_id}_{result['query']}.json"
            write_json(output_dir / filename, profile if profile is not None else result)
            result["profile_file"] = filename
            live_queries[result["query"]] = result
            record["queries"].append(result)
            event = {**event, "result": result}
            if result.get("error"):
                print(f"\n{result['query']} {result['status']}:\n{result['error']}", flush=True)
        elif kind == "fatal":
            record["error"] = event.get("traceback", event.get("error"))
            print(f"\nSetup/worker error:\n{record['error']}", flush=True)
        record["events"].append(event)

    with tempfile.TemporaryDirectory(prefix=f"{run_id}_", dir=SPILL_ROOT) as owned_spill:
        spill_dir = Path(owned_spill)
        child = context.Process(target=workload.run_workload, name=run_id, args=(
            case_key, repeats, str(spill_dir), delay, IDLE_SECONDS, TAIL_SECONDS, events, cancel))
        child.start()
        record["pid"] = child.pid
        process = psutil.Process(child.pid)
        launched = time.perf_counter()
        next_sample, last_print, previous = launched, -float("inf"), None
        stopped_at = None
        try:
            while True:
                try:
                    try:
                        consume(events.get(timeout=0.1))
                        while True:
                            consume(events.get_nowait())
                    except queue.Empty:
                        pass
                    now = time.perf_counter()
                    if now >= next_sample:
                        try:
                            with process.oneshot():
                                rss = process.memory_info().rss
                                native_threads = process.num_threads()
                                cpu = process.cpu_times()
                            cpu_seconds = cpu.user + cpu.system
                            sample = {"time": now, "rss_bytes": rss, "os_threads": native_threads,
                                      "cpu_seconds": cpu_seconds, "cpu_cores": None,
                                      "spill_file_bytes": spill_file_bytes(spill_dir)}
                            if previous:
                                sample["cpu_cores"] = max(0.0, (cpu_seconds - previous["cpu_seconds"]) /
                                                          (now - previous["time"]))
                            record["samples"].append(sample)
                            previous = sample
                            if sys.stdout.isatty() or now - last_print >= 10:
                                epoch = live_queries.get("Q1", {}).get("started_at", launched)
                                cpu_text = "N/A" if sample["cpu_cores"] is None else f"{sample['cpu_cores']:.2f}"
                                line = (f"t={now-epoch:6.1f}s | Q1 {query_state(live_queries.get('Q1'), now):13} "
                                        f"Q2 {query_state(live_queries.get('Q2'), now):13} | RSS {rss/MIB:7.1f} MiB "
                                        f"| OS threads {native_threads:2} | CPU {cpu_text} cores "
                                        f"| spill files {sample['spill_file_bytes']/MIB:7.1f} MiB")
                                print(("\r" + line.ljust(160)) if sys.stdout.isatty() else line,
                                      end="" if sys.stdout.isatty() else "\n", flush=True)
                                last_print = now
                        except psutil.NoSuchProcess:
                            pass
                        next_sample = now + SAMPLE_SECONDS
                    if now - launched >= timeout and not cancel.is_set():
                        record["stop_reason"] = "TIMEOUT"
                        cancel.set()
                        stopped_at = now
                        print(f"\nTimeout after {timeout:g}s; interrupting workload cursors.", flush=True)
                    if stopped_at is not None and child.is_alive() and now - stopped_at >= CANCEL_GRACE_SECONDS:
                        child.terminate()
                        record["forced_stop"] = True
                    if not child.is_alive():
                        child.join()
                        while True:
                            try:
                                consume(events.get_nowait())
                            except queue.Empty:
                                break
                        break
                except KeyboardInterrupt:
                    if not cancel.is_set():
                        record["stop_reason"] = "CTRL_C"
                        cancel.set()
                        stopped_at = time.perf_counter()
                        print("\nCtrl+C: interrupting workload cursors; saving partial measurements.", flush=True)
        finally:
            if child.is_alive():
                cancel.set()
                child.join(CANCEL_GRACE_SECONDS)
                if child.is_alive():
                    child.terminate()
                    child.join()
                    record["forced_stop"] = True
            events.close()
            events.join_thread()
        record["exit_code"] = child.exitcode
        record["observed_until"] = time.perf_counter()
        # Do not fabricate SQL completion times after a killed/crashed process.
        finished_names = {q["query"] for q in record["queries"]}
        for name, live in live_queries.items():
            if name not in finished_names:
                record["queries"].append({**live, "finished_at": None, "status": "CANCELLED",
                                          "error": "Worker stopped without a final query result.",
                                          "sort_rows": None, "profile_spill_bytes": None, "profile_file": None})
    record["spill_cleaned"] = True
    if sys.stdout.isatty():
        print()
    record["queries"].sort(key=lambda q: q["query"])
    record["events"].sort(key=lambda event: event["time"])
    epoch = next((q.get("started_at") for q in record["queries"] if q["query"] == "Q1"), None)
    for sample in record["samples"]:
        sample["offset_seconds"] = sample["time"] - epoch if epoch is not None else None
        for name in ("Q1", "Q2"):
            query = next((q for q in record["queries"] if q["query"] == name), None)
            sample[f"{name.lower()}_state"] = query_state(query, sample["time"])
    record["summary"] = case_summary(record)
    print(f"Completed: {record['summary']['outcome']} | workload {number(record['summary']['workload_seconds'])}s", flush=True)
    return record


def case_summary(record):
    queries, samples = record["queries"], record["samples"]
    starts = [q["started_at"] for q in queries if q.get("started_at") is not None]
    finishes = [q["finished_at"] for q in queries if q.get("finished_at") is not None]
    start = min(starts) if starts else None
    end = max(finishes) if finishes else None
    complete = bool(queries) and all(q.get("finished_at") is not None for q in queries)
    window_end = end if complete else record["observed_until"]
    active = [s for s in samples if start is not None and start <= s["time"] <= window_end]
    # Clip interval estimates to the workload window, excluding idle/tail time.
    cpu_work = 0.0
    if start is not None:
        for left, right in zip(samples, samples[1:]):
            overlap = max(0.0, min(right["time"], window_end) - max(left["time"], start))
            cpu_work += overlap * max(0.0, right["cpu_seconds"] - left["cpu_seconds"]) / (right["time"] - left["time"])
    statuses = {q["status"] for q in queries}
    outcome = (record["stop_reason"] or ("ERROR" if record["error"] or record["exit_code"] else
               "ERROR" if "ERROR" in statuses else "RESOURCE_ERROR" if "RESOURCE_ERROR" in statuses else
               "CANCELLED" if "CANCELLED" in statuses else "SUCCESS" if queries else "ERROR"))
    return {"outcome": outcome, "workload_seconds": end-start if complete and start is not None else None,
            "sample_count": len(samples), "workload_sample_count": len(active),
            "peak_sampled_rss_bytes": max((s["rss_bytes"] for s in active), default=None),
            "peak_os_threads": max((s["os_threads"] for s in active), default=None),
            "average_cpu_cores": cpu_work/(window_end-start) if start is not None and window_end > start and active else None,
            "peak_sampled_spill_file_bytes": max((s["spill_file_bytes"] for s in active), default=None)}


def query_seconds(query):
    if query.get("started_at") is None or query.get("finished_at") is None:
        return None
    return query["finished_at"] - query["started_at"]


def validate(runs, comparison_ids, smoke, delay):
    checks = []

    def check(name, passed, detail=""):
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    comparison = [r for r in runs if r["run_id"] in comparison_ids]
    check("Baseline and all three comparison cases ran", len(comparison) == 4)
    frozen = {r["hash_repeats"] for r in comparison}
    check("Identical SQL scale across comparison cases", len(frozen) == 1)
    for record in runs:
        prefix = record["run_id"]
        metadata = record["metadata"] or {}
        check(f"{prefix}: pinned DuckDB and source rows", metadata.get("version") == "1.5.6" and
              metadata.get("source_rows") == workload.EXPECTED_SOURCE_ROWS)
        check(f"{prefix}: shared settings verified on every handle", metadata.get("settings_verified"))
        check(f"{prefix}: worker completed without unexpected error", not record["error"] and
              record["exit_code"] == 0 and not record["stop_reason"] and
              all(q["status"] in ("SUCCESS", "RESOURCE_ERROR") for q in record["queries"]))
        expected_queries = 1 if record["case_key"] == "baseline" else 2
        check(f"{prefix}: all query outcomes recorded", len(record["queries"]) == expected_queries)
        check(f"{prefix}: process samples captured", record["summary"]["workload_sample_count"] > 0)
        for query in record["queries"]:
            if query["status"] == "SUCCESS":
                check(f"{prefix}/{query['query']}: complete ORDER_BY", query["sort_rows"] == workload.EXPECTED_SORT_ROWS,
                      f"{query['sort_rows']} rows")
        if record["case_key"] != "baseline" and len(record["queries"]) == 2:
            q1, q2 = record["queries"]
            actual_delay = q2["submitted_at"] - q1["started_at"] if q1.get("started_at") is not None else None
            check(f"{prefix}: Q2 submission delay", actual_delay is not None and abs(actual_delay-delay) <= 1.0,
                  f"requested={delay}s, observed={actual_delay}s")
            if record["case_key"] == "one_cursor":
                check(f"{prefix}: shared cursor serialized", q1.get("finished_at") is not None and
                      q2.get("started_at") is not None and q2["started_at"] >= q1["finished_at"])
            else:
                check(f"{prefix}: separate cursors attempted overlap", q2.get("started_at") is not None and
                      q1.get("finished_at") is not None and q2["started_at"] < q1["finished_at"])
    baseline = comparison[0] if comparison and comparison[0]["case_key"] == "baseline" else None
    q1 = baseline["queries"][0] if baseline and baseline["queries"] else {}
    check("Accepted baseline succeeded with measurable profile spill", q1.get("status") == "SUCCESS" and
          (q1.get("profile_spill_bytes") or 0) > 0)
    if not smoke:
        duration = query_seconds(q1)
        check("Accepted baseline is 150-210 seconds", duration is not None and CALIBRATION_BAND[0] <= duration <= CALIBRATION_BAND[1])
    return checks


def number(value, digits=1):
    if value is None:
        return "N/A"
    # Q1 is submitted a fraction of a second before its execution-entry zero.
    # Keep the raw timestamps, but avoid confusing '-0.0' in rounded tables.
    return f"{0.0 if abs(value) < 0.5*10**(-digits) else value:.{digits}f}"


def comparison_tables(records):
    baseline = next((r for r in records if r["case_key"] == "baseline"), None)
    denominator = query_seconds(baseline["queries"][0]) if baseline and baseline["queries"] else None
    case_rows, query_rows = [], []
    for record in records:
        summary = record["summary"]
        label = workload.CASES[record["case_key"]]["label"]
        case_rows.append([label, summary["outcome"], number(summary["workload_seconds"]),
                          number(summary["peak_sampled_rss_bytes"]/MIB if summary["peak_sampled_rss_bytes"] is not None else None),
                          str(summary["peak_os_threads"] or "N/A"), number(summary["average_cpu_cores"], 2),
                          number(summary["peak_sampled_spill_file_bytes"]/MIB if summary["peak_sampled_spill_file_bytes"] is not None else None)])
        epoch = next((q.get("started_at") for q in record["queries"] if q["query"] == "Q1"), None)
        for query in record["queries"]:
            start, finish = query.get("started_at"), query.get("finished_at")
            execution = query_seconds(query)
            query_rows.append([label, query["query"], query["status"],
                               number(query["submitted_at"]-epoch if epoch is not None else None),
                               number(start-query["submitted_at"] if start is not None else None), number(execution),
                               number(finish-query["submitted_at"] if finish is not None else None),
                               number(execution/denominator if query["status"] == "SUCCESS" and execution is not None and denominator else None, 2)])
    return ((["Case", "Outcome", "Workload s", "Peak RSS MiB*", "Peak OS threads", "Avg CPU cores*", "Peak spill files MiB*"], case_rows),
            (["Case", "Query", "Outcome", "Submit s", "App wait s", "Execute s", "Latency s", "Exec / baseline"], query_rows))


def table_text(headers, rows):
    widths = [max(len(str(row[i])) for row in [headers, *rows]) for i in range(len(headers))]
    return "\n".join(" | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)) for row in [headers, *rows])


def table_html(headers, rows):
    return "<div class='table'><table><thead><tr>" + "".join(f"<th>{html.escape(str(h))}</th>" for h in headers) + \
        "</tr></thead><tbody>" + "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in row) +
                                        "</tr>" for row in rows) + "</tbody></table></div>"


def svg_chart(samples, field, title, maximum, x_min, x_max, scale=1):
    def x(t):
        return 54 + (t-x_min)/(x_max-x_min)*532

    def y(value):
        return 138 - value/maximum*120

    parts = [f"<svg viewBox='0 0 600 170' role='img' aria-label='{html.escape(title)}'><title>{html.escape(title)}</title>"]
    for fraction in (0, 0.5, 1):
        value = maximum*fraction
        parts.append(f"<line x1='54' y1='{y(value):.1f}' x2='586' y2='{y(value):.1f}' stroke='#e2e8f0'/><text x='48' y='{y(value)+4:.1f}' text-anchor='end'>{value:.1f}</text>")
    for tick in (0, x_max/2, x_max):
        parts.append(f"<text x='{x(tick):.1f}' y='159' text-anchor='middle'>{tick:.0f}s</text>")
    parts.append(f"<line x1='{x(0):.1f}' y1='18' x2='{x(0):.1f}' y2='138' stroke='#94a3b8' stroke-dasharray='3 3'/>")
    points = " ".join(f"{x(s['offset_seconds']):.1f},{y(s[field]/scale):.1f}" for s in samples
                      if s["offset_seconds"] is not None and s[field] is not None)
    parts.append(f"<polyline points='{points}' fill='none' stroke='#2563eb' stroke-width='1.8'/></svg>")
    return f"<div class='chart'><h4>{html.escape(title)}</h4>{''.join(parts)}</div>"


def timeline_html(record, x_min, x_max):
    queries = record["queries"]
    epoch = next((q.get("started_at") for q in queries if q["query"] == "Q1"), None)
    if epoch is None:
        return "<p>No query execution began.</p>"

    def x(timestamp):
        return 54 + ((timestamp-epoch)-x_min)/(x_max-x_min)*532

    parts = ["<svg viewBox='0 0 600 98' role='img' aria-label='Query waiting and execution timeline'>"]
    for i, query in enumerate(queries):
        y = 12 + i*32
        submit, start, finish = query["submitted_at"], query.get("started_at"), query.get("finished_at")
        parts.append(f"<text x='10' y='{y+14}'>{query['query']}</text>")
        end = finish if finish is not None else record["observed_until"]
        wait_end = start if start is not None else end
        parts.append(f"<rect x='{x(submit):.1f}' y='{y}' width='{max(0.3,x(wait_end)-x(submit)):.1f}' height='20' fill='#fbbf24'><title>Application wait</title></rect>")
        if start is not None:
            parts.append(f"<rect x='{x(start):.1f}' y='{y}' width='{max(0.3,x(end)-x(start)):.1f}' height='20' fill='#2563eb'><title>{html.escape(query['status'])}: execution call</title></rect>")
    parts.append("<text x='54' y='94'>Yellow: application wait · Blue: execution call</text></svg>")
    return "".join(parts)


def save_reports(output_dir, document, runs, comparison_ids):
    selected = [r for r in runs if r["run_id"] in comparison_ids]
    document["runs"] = [{k: v for k, v in r.items() if k != "samples"} for r in runs]
    document["comparison_runs"] = comparison_ids
    fields = ["run_id", "case_key", "time", "offset_seconds", "q1_state", "q2_state", "rss_bytes",
              "os_threads", "cpu_seconds", "cpu_cores", "spill_file_bytes"]
    with (output_dir / "samples.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for record in runs:
            writer.writerows({"run_id": record["run_id"], "case_key": record["case_key"], **s} for s in record["samples"])
    # Both tables and charts use these same event-aligned samples. Read the CSV
    # back before rendering so its consistency check appears in every report.
    with (output_dir / "samples.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    expected = [{"run_id": r["run_id"], "case_key": r["case_key"], **s} for r in runs for s in r["samples"]]
    matching = len(rows) == len(expected) and all(
        all(row[field] == ("" if item[field] is None else str(item[field])) for field in fields)
        for row, item in zip(rows, expected))
    document["validation"].append({"check": "CSV matches event-derived samples used by JSON summaries and HTML",
                                   "passed": matching, "detail": f"{len(rows)} samples"})
    if not matching and document["status"] != "CANCELLED":
        document["status"] = "FAILED"
    tables = comparison_tables(selected)
    body = [f"<p class='status'>{document['status']} · {document['mode']} · DuckDB 1.5.6</p>",
            "<h1>Connections, cursors, and shared resources</h1>",
            f"<p>Shared {workload.MEMORY_LIMIT} buffer budget · {workload.THREADS} DuckDB threads · "
            f"{workload.SPILL_LIMIT} spill allowance · {workload.EXPECTED_SORT_ROWS:,} rows per full sort.</p>",
            "<p>Saved data: <a href='results.json'>JSON results and events</a> · "
            "<a href='samples.csv'>CSV resource samples</a></p>",
            "<h2>Case comparison</h2>", table_html(*tables[0]),
            "<p>*Sampled resource measurements cover Q1 execution entry through the last query completion, excluding idle/setup/tail.</p>",
            "<h2>Query comparison</h2>", table_html(*tables[1]),
            "<p>Submit offsets use Q1 execution entry as zero. Execution ratios compare the identical frozen query with the accepted baseline.</p>"]
    plotted = selected or runs
    points = [s for r in plotted for s in r["samples"] if s["offset_seconds"] is not None]
    x_min = min([-IDLE_SECONDS, *[s["offset_seconds"] for s in points]])
    x_max = max([1.0, *[s["offset_seconds"] for s in points]])
    metrics = [("rss_bytes", "Process RSS (MiB)", MIB), ("cpu_cores", "CPU core equivalents", 1),
               ("os_threads", "All native OS threads", 1), ("spill_file_bytes", "Spill-file logical size (MiB)", MIB)]
    maximums = {field: max([1.0, *[s[field]/scale for s in points if s[field] is not None]])*1.05 for field, _, scale in metrics}
    body.append("<h2>Timelines</h2><p>All charts share time and metric scales across cases. Zero is Q1 execution entry; the dashed line marks it.</p>")
    for record in plotted:
        case = workload.CASES[record["case_key"]]
        body.extend([f"<section><h3>{html.escape(case['label'])} <small>{html.escape(record['run_id'])}</small></h3>",
                     f"<p>{case['parent_connections']} parent connect() call(s), {case['cursors']} cursor() call(s), "
                     f"{case['native_contexts']} retained native contexts. Hash repetitions: {record['hash_repeats']}. "
                     f"Outcome: {record['summary']['outcome']}.</p>",
                     timeline_html(record, x_min, x_max), "<div class='charts'>"])
        for field, title, scale in metrics:
            body.append(svg_chart(record["samples"], field, title, maximums[field], x_min, x_max, scale))
        body.append("</div></section>")
        for query in record["queries"]:
            body.append(f"<p>{query['query']} profile spill peak (shared instance): {number(query.get('profile_spill_bytes')/MIB if query.get('profile_spill_bytes') is not None else None)} MiB</p>")
            if query.get("profile_file"):
                body.append(f"<p><a href='{html.escape(query['profile_file'], quote=True)}'>{query['query']} raw profile or error</a></p>")
            if query.get("error"):
                body.append(f"<details open><summary>{query['query']} full error</summary><pre>{html.escape(query['error'])}</pre></details>")
        if record["error"]:
            body.append(f"<pre>{html.escape(record['error'])}</pre>")
    calibration = [r for r in runs if r["case_key"] == "baseline"]
    body.extend(["<h2>Calibration</h2>", table_html(["Run", "Hash repeats", "Execution s", "Outcome"], [
        [r["run_id"], r["hash_repeats"], number(query_seconds(r["queries"][0]) if r["queries"] else None), r["summary"]["outcome"]] for r in calibration])])
    frozen = selected[0]["hash_repeats"] if selected else runs[-1]["hash_repeats"] if runs else workload.INITIAL_HASH_REPEATS
    body.extend(["<h2>SQL and interpretation</h2>", f"<pre>{html.escape(workload.build_sql(frozen))}</pre>",
                 "<ul>" + "".join(f"<li>{html.escape(note)}</li>" for note in NOTES) + "</ul>",
                 "<h2>Validation</h2>", table_html(["Check", "Result", "Detail"], [
                     [c["check"], "PASS" if c["passed"] else "FAIL", c["detail"]] for c in document["validation"]]),
                 "<p>Sources: " + " · ".join(f"<a href='{url}'>{html.escape(label)}</a>" for label, url in SOURCES) + "</p>"])
    css = "body{font:15px system-ui,sans-serif;color:#172033;background:#f8fafc;margin:0 auto;padding:32px;max-width:1250px}h1{font-size:32px}h2{margin-top:32px}p,li{line-height:1.6}.status{color:#475569}section{background:white;border:1px solid #e2e8f0;border-radius:10px;padding:20px;margin:18px 0}small{font-size:13px;color:#64748b}.table{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:10px;border-bottom:1px solid #e2e8f0}th{background:#e2e8f0}pre{white-space:pre-wrap;background:#e2e8f0;padding:16px;border-radius:6px}.charts{display:grid;grid-template-columns:1fr 1fr;gap:12px}.chart h4{margin:8px 0}svg{width:100%;max-height:210px}svg text{font:11px system-ui;fill:#475569}li{margin-bottom:8px}a{color:#1d4ed8}@media(max-width:700px){body{padding:16px}.charts{grid-template-columns:1fr}}"
    (output_dir / "report.html").write_text("<!doctype html><html lang='en'><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>DuckDB connections and cursors</title><style>" + css + "</style><body>" + "".join(body) + "</body></html>", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--smoke", action="store_true", help="Short run: repeats=8, Q2 delay=1s; no duration calibration.")
    args = parser.parse_args()
    output_dir = OUTPUT_DIR / "smoke" if args.smoke else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    delay, timeout = (1.0, 120.0) if args.smoke else (DELAY_SECONDS, CASE_TIMEOUT_SECONDS)
    repeats = 8 if args.smoke else workload.INITIAL_HASH_REPEATS
    runs, comparison_ids = [], []
    document = {"created_at": datetime.now(timezone.utc).isoformat(), "mode": "smoke" if args.smoke else "full",
                "status": "RUNNING", "psutil_version": psutil.__version__, "delay_seconds": delay,
                "sample_seconds": SAMPLE_SECONDS, "target_seconds": None if args.smoke else TARGET_SECONDS,
                "validation": []}
    print(f"Shared settings: memory={workload.MEMORY_LIMIT}, threads={workload.THREADS}, spill={workload.SPILL_LIMIT}. "
          f"Full sort rows={workload.EXPECTED_SORT_ROWS:,}. Q2 delay={delay:g}s.\n"
          "Sampled RSS is process memory; OS thread count includes Python and idle threads.\n"
          "Reports: " + str(output_dir), flush=True)
    for attempt in range(1, (1 if args.smoke else MAX_CALIBRATION_ATTEMPTS)+1):
        run_id = "baseline_smoke" if args.smoke else f"calibration_{attempt}"
        baseline = run_case("baseline", run_id, repeats, output_dir, delay, timeout)
        runs.append(baseline)
        query = baseline["queries"][0] if baseline["queries"] else {}
        duration = query_seconds(query)
        if query.get("status") != "SUCCESS" or baseline["summary"]["outcome"] != "SUCCESS" or (query.get("profile_spill_bytes") or 0) <= 0:
            print("A successful spilling baseline is required; stopping comparisons.", flush=True)
            break
        if args.smoke or CALIBRATION_BAND[0] <= duration <= CALIBRATION_BAND[1]:
            comparison_ids.append(run_id)
            print(f"Accepted baseline: {duration:.1f}s; frozen hash repetitions={repeats}.", flush=True)
            break
        adjusted = max(1, min(MAX_HASH_REPEATS, round(repeats*TARGET_SECONDS/duration)))
        print(f"Calibration observed {duration:.1f}s; next hash repetitions={adjusted}.", flush=True)
        if adjusted == repeats:
            break
        repeats = adjusted
    if comparison_ids:
        for case_key in PAIRED_CASES:
            record = run_case(case_key, case_key, repeats, output_dir, delay, timeout)
            runs.append(record)
            comparison_ids.append(case_key)
            if record["stop_reason"] == "CTRL_C":
                break
    document["validation"] = validate(runs, comparison_ids, args.smoke, delay)
    document["status"] = "CANCELLED" if any(r["stop_reason"] == "CTRL_C" for r in runs) else \
        "VALIDATED" if all(c["passed"] for c in document["validation"]) else "FAILED"
    save_reports(output_dir, document, runs, comparison_ids)
    if not all(c["passed"] for c in document["validation"]):
        document["status"] = "CANCELLED" if document["status"] == "CANCELLED" else "FAILED"
    document["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_json(output_dir / "results.json", document)
    selected = [r for r in runs if r["run_id"] in comparison_ids]
    for headers, rows in comparison_tables(selected):
        print("\n" + table_text(headers, rows))
    print("\n*Resource peaks and CPU averages are sampled estimates. See report notes for measurement scope.")
    failures = [c for c in document["validation"] if not c["passed"]]
    print(f"\n{document['status']}: {len(document['validation'])-len(failures)}/{len(document['validation'])} checks passed.")
    for failure in failures:
        print(f"  FAIL: {failure['check']} {failure['detail']}")
    print(f"Report: {output_dir / 'report.html'}")
    return 0 if document["status"] == "VALIDATED" else 130 if document["status"] == "CANCELLED" else 1


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
