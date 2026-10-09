# DuckDB memory limit and spill demo

A single Python script loads January 2024 NYC yellow taxi trips into a local DuckDB database, then compares a streaming scan and a full sort under different memory and spill budgets. There are **2,964,624 rows and 19 columns**. The source download is 49,961,641 bytes (about 50 MB).

## Run with uv

From `C:\Users\sukru\Documents\Codex\2026-10-08\i-x20` in PowerShell:

```powershell
# One-time setup; already completed in this workspace.
uv venv --python 3.11 .\work\.venv
uv pip install --python .\work\.venv\Scripts\python.exe "duckdb==1.5.6"

# Run or rerun the experiment. No activation is needed.
uv run --no-project --python .\work\.venv\Scripts\python.exe .\outputs\duckdb_memory_demo.py
```

The first run downloads and loads the data. Later runs reuse the download and the database. Preparation is excluded from query timing. uv may print a harmless warning that `--no-project` was provided without a project.

## What to watch

The SQL and `SCENARIOS` are near the top of `duckdb_memory_demo.py`. Every case uses one DuckDB thread and a fresh connection, with `preserve_insertion_order=false`.

| Case | Memory budget | Spill cap | What it demonstrates |
|---|---:|---:|---|
| Scan | 128 MB | 1 GB | `COUNT(*)` and `SUM(trip_distance)` stream with little working state. |
| Full sort | 2 GB | 1 GB | The decoded sort intermediates fit in memory. |
| Full sort | 256 MB | 1 GB | DuckDB spills intermediate data to disk. |
| Full sort | 128 MB | 1 GB | Less memory increases pressure on spill storage. |
| Full sort | 128 MB | Disabled | The same query fails when it needs to spill. |
| Full sort | 128 MB | 1 MB | It fails because the allowed temporary storage is exhausted. |

The sort selects **all columns** and orders by `total_amount DESC, tpep_pickup_datetime`, without `LIMIT`. `EXPLAIN (ANALYZE, FORMAT JSON)` executes and consumes the whole result inside DuckDB, then returns its profile. Python never holds millions of result rows. The script checks that the `ORDER_BY` operator processes every source row.

Try changing the memory budgets or spill caps while keeping the case names. Validation describes the original six cases; experimental settings can legitimately make those checks fail. Edit `THREADS` separately to explore its effect without mixing two variables in the initial comparison.

## Settings and measurements

- `memory_limit` controls DuckDB's buffer-manager budget. It is **not a hard cap on the Python process**: some allocations and Python's own memory lie outside it. The 2 GB setting is a ceiling, not a request to allocate 2 GB immediately.
- `temp_directory` chooses the local spill-storage location. An empty string disables spilling. Each case has its own directory under `work/spill/`.
- `max_temp_directory_size` caps temporary spill capacity. The script uses explicit SQL `SET` statements: testing 1.5.6 found that supplying this cap only in `connect(config=...)` reported the setting but did not enforce it.
- `Spill MiB` is the profiler's **peak temporary-storage occupancy**, not total bytes written over the query's lifetime. Zero means the profile observed no spill.
- `Reported*` is the raw profiler buffer-memory figure in MiB. **DuckDB 1.5.6 has a reporting bug that inflates this metric**: its peak updater adds a new high value instead of replacing the previous high. Values above the memory limit in this column do not establish that the buffer manager violated its budget. The [versioned source](https://github.com/duckdb/duckdb/blob/v1.5.6/src/include/duckdb/main/query_profiler.hpp#L66-L69) shows the faulty update. No actual process-memory peak is measured here.
- SQL `MB`/`GB` use decimal units; the display uses binary MiB (`bytes / 1024**2`). For example, 128 MB is about 122.1 MiB.

A 50 MB compressed file can require much more memory to sort its decoded rows and sort keys. Spilling increases I/O and can increase runtime. OS file caching, disk speed, and profiling overhead also affect timings; lower memory need not produce a strictly slower run every time. Fresh connections clear DuckDB's per-instance state, while the OS cache remains.

## Files and validation

`work/data/` holds the download and `work/taxi.duckdb` holds the persistent table. DuckDB removes its spill files when each connection closes, so an empty spill directory afterwards does **not** mean no spilling happened. Peak usage remains recorded in the profiles.

`outputs/results.json` holds settings, measurements, full errors, and validation results. `outputs/profiles/` holds one JSON profile per successful case and an error document for each failed case. These outputs are overwritten on rerun. Failure metrics are `N/A`/JSON `null`, not fabricated zeroes.

Expected failures are part of the experiment. Exit code zero means the scan, full-row sorts, spill observation, and both intended failure reasons passed validation; unexpected query failures or unmet checks produce exit code one.

## Sources

- [NYC TLC dataset publisher](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [DuckDB memory management](https://duckdb.org/2024/07/09/memory-management)
- [Buffer-manager memory limit](https://duckdb.org/docs/current/configuration/pragmas#memory-limit)
- [Profiling metrics and units](https://duckdb.org/docs/current/dev/metrics)
- [Full-result consumption by EXPLAIN ANALYZE](https://github.com/duckdb/duckdb/blob/v1.5.6/src/execution/operator/helper/physical_explain_analyze.cpp)
- [Public sorting experiment with memory limits](https://github.com/spareilleux/learn/blob/main/code/duckdb/timings/07-performance.sql)
- [uv virtual environments](https://docs.astral.sh/uv/pip/environments/)
