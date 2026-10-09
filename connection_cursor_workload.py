"""Child-process workload for the connection/cursor memory and spill experiment.

All handles open the same existing taxi database read-only. Cursors have their
own query context but share the database's memory budget, threads, and spill
directory. The runner receives events and saves profiles; this module writes
only the temporary spill files that DuckDB needs while executing the queries.
"""

import json
import signal
import threading
import time
import traceback
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent.parent
DATABASE = ROOT / "work" / "taxi.duckdb"
THREADS = 4
MEMORY_LIMIT = "512MB"
SPILL_LIMIT = "10GB"
COPY_FACTOR = 3
EXPECTED_SOURCE_ROWS = 2_964_624
EXPECTED_SORT_ROWS = EXPECTED_SOURCE_ROWS * COPY_FACTOR
INITIAL_HASH_REPEATS = 256

CASES = {
    "baseline": {
        "label": "Baseline: Q1 alone",
        "parent_connections": 1, "cursors": 1, "native_contexts": 2,
    },
    "one_cursor": {
        "label": "1 connection / 1 cursor",
        "parent_connections": 1, "cursors": 1, "native_contexts": 2,
    },
    "two_connections": {
        "label": "2 connections / 1 cursor each",
        "parent_connections": 2, "cursors": 2, "native_contexts": 4,
    },
    "two_cursors": {
        "label": "1 connection / 2 cursors",
        "parent_connections": 1, "cursors": 2, "native_contexts": 3,
    },
}


def build_sql(hash_repeats):
    """Expand the same real dataset and give each sort a tunable hashing cost."""
    if not isinstance(hash_repeats, int) or hash_repeats < 1:
        raise ValueError("hash_repeats must be a positive integer")
    return f"""
        SELECT t.*, r.copy_id,
               sha256(repeat(concat_ws('|',
                   CAST(t.tpep_pickup_datetime AS VARCHAR),
                   CAST(t.tpep_dropoff_datetime AS VARCHAR),
                   CAST(t.total_amount AS VARCHAR),
                   CAST(r.copy_id AS VARCHAR)), {hash_repeats})) AS sort_key
        FROM trips AS t
        CROSS JOIN range({COPY_FACTOR}) AS r(copy_id)
        ORDER BY sort_key, r.copy_id
    """


def _emit(events, event, *, event_time=None, **fields):
    events.put({
        "event": event,
        "time": time.perf_counter() if event_time is None else event_time,
        **fields,
    })


def _settings(con):
    names = ["threads", "memory_limit", "max_temp_directory_size",
             "preserve_insertion_order", "temp_directory"]
    values = con.execute("""
        SELECT current_setting('threads'), current_setting('memory_limit'),
               current_setting('max_temp_directory_size'),
               current_setting('preserve_insertion_order'),
               current_setting('temp_directory')
    """).fetchall()[0]
    return dict(zip(names, values))


def _profile_nodes(node):
    yield node
    for child in node.get("children", []):
        yield from _profile_nodes(child)


def _is_resource_error(error):
    message = str(error).lower()
    return isinstance(error, duckdb.OutOfMemoryException) or (
        isinstance(error, duckdb.Error)
        and ("max_temp_directory_size" in message
             or "maximum temporary directory size" in message)
    )


def run_workload(case_key, hash_repeats, spill_dir, delay_seconds, idle_seconds,
                 tail_seconds, events, cancel_event):
    """Spawn-safe entry point; run Q1 and optionally submit Q2 after the delay.

    The controller can cancel SQL while workers are inside DuckDB. Interrupts
    target the actual worker cursors, because interrupting their parents would
    not cancel them. Parents stay open until every worker has stopped.
    """
    # Only the parent handles Ctrl+C; it cancels this child through the Event.
    # Setting this here leaves the runner's own SIGINT handler unchanged.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    parents, cursors, workers = [], [], []
    active = {}
    results = {}
    state_lock = threading.Lock()
    shared_query_lock = threading.Lock()
    q1_started = threading.Event()
    q1_started_at = None

    def interrupt_active():
        with state_lock:
            handles = list(active.values())
        for handle in handles:
            handle.interrupt()

    def query_worker(query, handle, submitted_at):
        nonlocal q1_started_at
        result = {
            "query": query, "submitted_at": submitted_at,
            "started_at": None, "finished_at": None,
            "status": "SUCCESS", "exception_type": None, "error": None,
            "profile": None, "sort_rows": None, "profile_spill_bytes": None,
        }
        # The shared-cursor case needs a lock across BOTH calls: another execute
        # between execute() and fetchone() would replace the first query's result.
        lock = shared_query_lock if case_key == "one_cursor" else threading.Lock()
        raw_profile = None
        try:
            with lock:
                if cancel_event.is_set():
                    result["status"] = "CANCELLED"
                    result["error"] = "Cancelled before execution."
                else:
                    started_at = time.perf_counter()
                    result["started_at"] = started_at
                    with state_lock:
                        active[id(handle)] = handle
                        if query == "Q1":
                            q1_started_at = started_at
                            q1_started.set()
                    _emit(events, "query_started", event_time=started_at,
                          query=query, started_at=started_at)
                    try:
                        raw_profile = handle.execute(
                            "EXPLAIN (ANALYZE, FORMAT JSON) " + sql
                        ).fetchone()[1]
                    finally:
                        # Parsing JSON is intentionally outside the query time.
                        result["finished_at"] = time.perf_counter()
                        with state_lock:
                            active.pop(id(handle), None)

            if raw_profile is not None:
                profile = json.loads(raw_profile)
                sorts = [node for node in _profile_nodes(profile)
                         if node.get("operator_type") == "ORDER_BY"]
                if len(sorts) != 1:
                    raise RuntimeError("Expected one ORDER_BY in the completed profile")
                result["profile"] = profile
                result["sort_rows"] = sorts[0]["operator_cardinality"]
                result["profile_spill_bytes"] = profile["system_peak_temp_dir_size"]
                if result["sort_rows"] != EXPECTED_SORT_ROWS:
                    raise RuntimeError(
                        f"Expected {EXPECTED_SORT_ROWS:,} sorted rows; "
                        f"found {result['sort_rows']:,}"
                    )
        except Exception as error:
            result["exception_type"] = type(error).__name__
            result["error"] = str(error)
            if cancel_event.is_set() and isinstance(error, duckdb.InterruptException):
                result["status"] = "CANCELLED"
            elif _is_resource_error(error):
                result["status"] = "RESOURCE_ERROR"
            else:
                result["status"] = "ERROR"
        finally:
            if result["finished_at"] is None:
                result["finished_at"] = time.perf_counter()
            with state_lock:
                results[query] = result
            _emit(events, "query_finished", event_time=result["finished_at"],
                  query=query, result=result)

    def submit(query, handle):
        submitted_at = time.perf_counter()
        _emit(events, "query_submitted", event_time=submitted_at,
              query=query, submitted_at=submitted_at)
        worker = threading.Thread(target=query_worker,
                                  args=(query, handle, submitted_at), name=query)
        workers.append(worker)
        worker.start()

    try:
        topology = {"case_key": case_key, **CASES[case_key]}
        if duckdb.__version__ != "1.5.6":
            raise RuntimeError(f"Use DuckDB 1.5.6; found {duckdb.__version__}")
        if not DATABASE.is_file():
            raise RuntimeError("Taxi database missing; run duckdb_memory_demo.py first")
        if min(delay_seconds, idle_seconds, tail_seconds) < 0:
            raise ValueError("Delay, idle, and tail durations must be nonnegative")
        sql = build_sql(hash_repeats)
        spill_path = Path(spill_dir).resolve().as_posix()

        parents.append(duckdb.connect(str(DATABASE.resolve()), read_only=True))
        anchor = parents[0]
        source_rows = anchor.execute("SELECT count(*) FROM trips").fetchall()[0][0]
        if source_rows != EXPECTED_SOURCE_ROWS:
            raise RuntimeError(
                f"Expected {EXPECTED_SOURCE_ROWS:,} source trips; found {source_rows:,}"
            )

        # These are GLOBAL settings: set them once for the shared database.
        anchor.execute("SET threads = ?", [THREADS])
        anchor.execute("SET memory_limit = ?", [MEMORY_LIMIT])
        anchor.execute("SET preserve_insertion_order = false")
        anchor.execute("SET temp_directory = ?", [spill_path])
        # In 1.5.6 use SET after opening, so the spill cap is actually enforced.
        anchor.execute("SET max_temp_directory_size = ?", [SPILL_LIMIT])
        if case_key == "two_connections":
            parents.append(duckdb.connect(str(DATABASE.resolve()), read_only=True))
            cursors.extend(parent.cursor() for parent in parents)
        else:
            cursors.extend(anchor.cursor() for _ in range(topology["cursors"]))

        handles = {f"parent_{i + 1}": con for i, con in enumerate(parents)}
        handles.update({f"cursor_{i + 1}": con for i, con in enumerate(cursors)})
        settings = {name: _settings(con) for name, con in handles.items()}
        configured = settings["parent_1"]
        # Explicit SET succeeded; verify every handle sees the same normalized
        # budgets. DuckDB reports decimal MB/GB settings in rounded binary units.
        verified = all(values == configured for values in settings.values()) and (
            configured["threads"] == THREADS
            and configured["preserve_insertion_order"] is False
            and configured["temp_directory"] == spill_path
        )
        if not verified:
            raise RuntimeError("Settings readback did not match the configured database")
        _emit(events, "ready", metadata={
            "version": duckdb.__version__, "source_rows": source_rows,
            "settings_verified": verified, "settings_by_handle": settings,
            "requested_settings": {
                "threads": THREADS, "memory_limit": MEMORY_LIMIT,
                "max_temp_directory_size": SPILL_LIMIT,
                "preserve_insertion_order": False, "temp_directory": spill_path,
            },
            "topology": topology,
        })

        if not cancel_event.wait(idle_seconds):
            submit("Q1", cursors[0])
            q2_submitted = False
            while True:
                cancelled = cancel_event.is_set()
                if cancelled:
                    interrupt_active()
                    for worker in workers:
                        worker.join(timeout=0.02)
                elif case_key != "baseline" and not q2_submitted and q1_started.is_set():
                    if time.perf_counter() >= q1_started_at + delay_seconds:
                        submit("Q2", cursors[0] if case_key == "one_cursor" else cursors[1])
                        q2_submitted = True

                done_submitting = case_key == "baseline" or q2_submitted or cancelled
                if done_submitting and not any(worker.is_alive() for worker in workers):
                    break
                # Check cancellation at least every 0.1 seconds, including while
                # Q2 waits for Q1's shared cursor to become available.
                wait_seconds = 0.05
                if not q2_submitted and q1_started.is_set() and case_key != "baseline":
                    until_q2 = q1_started_at + delay_seconds - time.perf_counter()
                    if until_q2 > 0:
                        wait_seconds = min(wait_seconds, until_q2)
                cancel_event.wait(wait_seconds)

        for worker in workers:
            worker.join()
        cancel_event.wait(tail_seconds)
        _emit(events, "case_finished", case_key=case_key,
              cancelled=cancel_event.is_set(), query_count=len(results),
              query_statuses={query: result["status"] for query, result in results.items()})
    except Exception as error:
        cancel_event.set()
        interrupt_active()
        _emit(events, "fatal", error=str(error), exception_type=type(error).__name__,
              traceback=traceback.format_exc())
    finally:
        # Closing a parent also closes its cursors; never do so during a query.
        while any(worker.is_alive() for worker in workers):
            interrupt_active()
            for worker in workers:
                worker.join(timeout=0.05)
        for con in [*reversed(cursors), *reversed(parents)]:
            con.close()
