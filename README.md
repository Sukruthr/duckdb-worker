# Small DuckDB worker

The worker applies explicit DuckDB settings and exports one query per process directly to Parquet. It is an example to adapt to your workload; it does not require a server, pandas, or a task queue. The separate concurrency experiment adds psutil for process-memory measurements.

## Connection/cursor experiment

```sh
uv sync --locked
uv run --locked experiment.py case1 normal
```

The CLI runs one selected topology and memory mode. It reuses the cached [official NYC TLC January 2025 yellow taxi Parquet](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) at `work/experiment/yellow_tripdata_2025-01.parquet`; there is no downloader. The dataset and previous local results are preserved in ignored `work/`.

| Case | Topology | Three concurrent arrivals |
| --- | --- | --- |
| `case1` | 1 connection, 1 cursor | Assignments `[A, A, A]`; requests queue behind one cursor lock. |
| `case2` | 2 connections, 1 cursor each | Assignments `[A, B, A]`; two requests can overlap. |
| `case3` | 1 connection, 2 cursors | Assignments `[A, B, A]`; two requests can overlap. |

Each function releases three requests together, each running the same supplied SQL. A lock covers execution and complete result consumption on its assigned cursor, so another thread cannot overwrite the result. Queue time is the wait for that lock; query time covers execution and fetching. All connections open `work/concurrency.duckdb`, sharing one engine budget within the process. `cursor()` creates an independent session. [Python API](https://duckdb.org/docs/current/clients/python/overview#about-cursor).

The default `experiment.sql` sorts the cached Parquet directly. The three case functions also accept a supplied trusted SQL string. Results are consumed with `fetchmany(2048)` and counted without accumulating a full Python result list or exporting Parquet.

Modes use `memory_limit=1GB` for `normal`, `256MB` for `spill`, and `1MB` for `oom`. The selected budget is identical across topologies, with `threads=1`, `preserve_insertion_order=false` and a `2GB` spill cap. Spill remains enabled in every mode. These names describe intended tests; the returned statuses and profile metrics establish what actually happened.

```sh
uv run --locked experiment.py case2 spill
uv run --locked experiment.py case3 oom
uv run --locked python -m unittest -v test_run test_experiment
```

The Python APIs are `case_1(sql, memory_limit)`, `case_2(sql, memory_limit)` and `case_3(sql, memory_limit)`. Each creates its topology and returns a plain dictionary. The shared helper `_run_requests(request_cursors, sql)` accepts the three assigned cursor handles, such as `[A, B, A]`, and deduplicates them internally to give repeated handles the same lock. It runs the requests, reads profiles, samples RSS and checks engine recovery.

```python
from experiment import case_3

result = case_3("SELECT i FROM range(1000) t(i)", "1GB")
assert all(request["rows_consumed"] == 1000 for request in result["requests"])
print(result["settings"], result["peak_rss_bytes"], result["engine_usable"])
```

Request records contain `queue_ms`, `query_ms`, `total_ms`, `rows_consumed`, `status` and `error`; statuses are `ok`, `duckdb_oom` or `query_error`. Shared fields include `settings`, `batch_ms`, `peak_rss_bytes`, `engine_peak_buffer_bytes`, `engine_peak_spill_bytes` and `engine_usable`. Settings come from `current_setting`; DuckDB may normalize a requested `GB`/`MB` limit to `GiB`/`MiB`, so compare parsed byte values when validating a limit. Engine peak fields are bytes; `None` means no profile measurement was available, not a measured zero.

Requests call DuckDB directly from Python threads. The CLI prints the observations; it does not run an automatic matrix or create HTML, JSON, profile or query-output files. It exits `0` for the expected scenario outcome and a usable engine, including expected handled OOM, and `1` for an unexpected outcome or unusable engine. Normal expects completion without spill; spill expects completion with positive profile spill; OOM expects three handled `duckdb_oom` results. Run each CLI command independently when comparing cases. Three arrivals are a small demonstration, not a statistical benchmark; OS file caching and allocator state can affect timings.

DuckDB native profiling uses `no_output`; successful profiles are retrieved in memory. Process RSS is sampled every 50 ms, and its peak is shared metadata rather than per-request memory. Sampling can miss brief peaks. Profile buffer/spill peaks describe the shared engine and can include overlapping requests; they are not summed or attributed to a single request. Positive profile temporary-storage usage establishes disk spill. DuckDB may still create temporary spill files while a query runs. [Profiling metrics](https://duckdb.org/docs/current/dev/metrics).

The OOM case handles DuckDB `OutOfMemoryException` and checks cursor recovery. This local Windows experiment covers completion, spill and DuckDB allocation failure; it does not reproduce a Linux kernel kill or Kubernetes `OOMKilled`. `memory_limit` is not a process RSS ceiling. [OOM guidance](https://duckdb.org/docs/current/guides/performance/oom).

## Run locally

Open a terminal in this directory. Python 3.11+ and uv are required.

```sh
uv sync --locked
uv run --locked run.py
```

The example groups one million generated rows into 1,000 output rows. The result is `work/result.parquet`. Repeating the command with that existing output exits with an error; choose a new output filename.

```sh
uv run --locked run.py --output work/second-result.parquet --profile work/profile.json
uv run --locked run.py --help
```

All `work/...` paths are relative to the terminal's current directory. The bundled query path is resolved beside the script.

## Use your own query

Replace `query.sql`, or pass another UTF-8 file containing one SELECT query. WITH queries and a trailing semicolon are supported. Input SQL is trusted application code; DuckDB can access files and extensions, so this is not a sandbox for arbitrary user SQL.

For example:

```sql
SELECT customer_id, count(*) AS orders, sum(amount) AS total
FROM read_parquet('/data/orders/*.parquet')
WHERE order_date >= DATE '2026-01-01'
GROUP BY customer_id;
```

```sh
uv run --locked run.py --query query.sql --output work/orders.parquet
```

To query existing DuckDB tables, also pass `--database /data/analytics.duckdb`. Existing databases are opened read-only. The worker runs queries serially within each process; launching multiple processes still multiplies budgets and needs external admission control.

## Settings

| Option | Local default | Purpose |
| --- | --- | --- |
| `--memory-limit` | `512MB` | Explicit managed-memory budget, leaving room for the runtime. |
| `--threads` | `1` | Restrained parallelism. |
| `--temp-directory` | `work/spill` | Root of a unique spill directory for this run. |
| `--max-temp-size` | `2GB` | Bounds DuckDB temporary-file consumption. |
| `--output` | `work/result.parquet` | New output file, published after successful export. |
| `--profile` | Disabled | Optional JSON query profile at a new file path. |

For one instance in an 8 GiB container, the Kubernetes example sets `memory_limit=4GB`, `threads=2`, and `max_temp_directory_size=50GB`. These are starting values to measure against your real query, not guaranteed sizing. DuckDB `GB` is decimal while Kubernetes `Gi` is binary: `4GB` is approximately 3.73 GiB.

The script sets `preserve_insertion_order=false`. Use explicit SQL `ORDER BY` where output ordering matters. There is no automatic percentage detection or tuning logic to replicate.

## Practices used

1. Pin DuckDB and commit `uv.lock` for reproducible installation.
2. Apply limits before running queries. Set the spill cap immediately after connection creation so it reaches the initialized buffer manager.
3. Run one query per process and keep thread count low initially.
4. Export inside DuckDB instead of creating a full Python result list/DataFrame.
5. Give each run its own spill directory. Close DuckDB before removing that directory.
6. Write to a temporary sibling directory, then publish the output after success. Ordinary failures leave no partial result at the destination and preserve existing output.
7. Log effective settings. Optionally record the query profile for engine memory and spill measurements.
8. Handle DuckDB allocation errors with exit code 1 and a readable message. Avoid automatic identical retries.

`memory_limit` is not a process/container RSS cap. A non-spillable aggregate or application/native allocation can still exhaust container memory. A SIGKILL cannot be caught or cleaned up by Python, and can leave scratch directories behind. Each new run uses a separate directory; Kubernetes removes emptyDir contents when its Pod is deleted. This example does not delete directories from other runs. [DuckDB OOM guidance](https://duckdb.org/docs/current/guides/performance/oom).

Profiling reports engine-accounted memory and temporary storage, not total RSS or cgroup usage. A kernel kill may prevent the final profile from being written. Check container telemetry and termination evidence too.

The spill-cap test caught a startup detail in the tested DuckDB 1.5.6 Windows build: passing `max_temp_directory_size` only in the connection config reported the intended value but did not enforce it. The worker uses an explicit `SET max_temp_directory_size` after opening the connection, and the test verifies that exceeding the cap produces a handled failure. The released source distinguishes the startup config from the live buffer-manager setter. [Setting implementation](https://github.com/duckdb/duckdb/blob/v1.5.6/src/main/settings/custom_settings.cpp#L1326), [live spill-limit setter](https://github.com/duckdb/duckdb/blob/v1.5.6/src/storage/standard_buffer_manager.cpp#L427).

## Kubernetes

Build and push an image from this directory, and replace the image name in `job.yaml`:

```sh
docker build -t your-registry/duckdb-worker:example .
docker push your-registry/duckdb-worker:example
kubectl apply -f job.yaml
kubectl logs job/duckdb-worker-example
```

The Job runs the bundled example once. It uses disk-backed emptyDir spill storage because `medium: Memory` would charge spill files to the writing container's RAM limit. The spill size leaves space within the ephemeral-storage budget for logs and writable layers. Node capacity and storage enforcement still depend on your cluster. `sizeLimit` is not a disk reservation. [Kubernetes emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir).

There are no automatic job retries (`backoffLimit: 0`, `restartPolicy: Never`). Results and profiles are stored on the `duckdb-worker-results` PVC and survive Job/Pod deletion. The manifest requests a 5 GiB claim using your cluster's default storage class; set `storageClassName` if your cluster requires it, or use an existing claim instead. Read the output through a running Pod/application that mounts that claim. `kubectl cp` cannot read from an already completed worker container.

Mount your input data/database and SQL file, or include the query in the image. For another run, choose new result/profile filenames and a new Job name. Size resources and PVC capacity for your workload, and manage old output files explicitly. Multiple Jobs or replicas still multiply memory and disk use.

If the container is killed, inspect the Pod description, termination reason, and node/cgroup evidence. An engine allocation error is handled by this script; `OOMKilled` and node-pressure eviction require container/node investigation.

```sh
kubectl describe pod POD_NAME
kubectl get pod POD_NAME -o yaml
```

## Verification

```sh
uv run --locked python -m unittest -v test_run test_experiment
```

Local verification covers Parquet correctness, actual disk spilling, handled memory/spill-capacity failures, cleanup, and preservation of existing output. The spill test sorts one million rows with a 16 MB configured engine budget and checks that the profile records temporary storage. The bundled default query is small and does not need to spill. Neither test proves a process-RSS ceiling or Kubernetes kernel OOM behavior. Docker and Kubernetes execution require those tools and a cluster.

Verified locally on October 6, 2026 with Windows, Python 3.11.6, uv 0.10.4, and DuckDB 1.5.6: all seven tests passed. The Kubernetes YAML was parsed and its volume/claim relationships checked. Docker image builds and live Kubernetes execution were not run because Docker and kubectl are unavailable in this environment.
