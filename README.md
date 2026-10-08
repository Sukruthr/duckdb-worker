# Small DuckDB worker

The worker applies explicit DuckDB settings and exports one query per process directly to Parquet. It is an example to adapt to your workload; it does not require a server, pandas, or a task queue. The separate concurrency experiment adds psutil for process-memory measurements.

## Connection/cursor experiment

```sh
uv sync --locked
uv run --locked experiment.py
```

This downloads the [official NYC TLC January 2025 yellow taxi Parquet](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) once and runs **27 real requests**: three simultaneous requests for each of three topologies, in each of three memory modes.

| Topology | Query cursors | Request behavior |
| --- | --- | --- |
| 1 connection, 1 cursor | 1 | Requests queue behind one cursor lock. |
| 2 connections, 1 cursor each | 2 | Two requests can overlap; the third waits for a cursor. |
| 1 connection, 2 cursors | 2 | Two requests can overlap; the third waits for a cursor. |

Both connections open **the same database file**. All three cases therefore share one DuckDB engine budget within their process. `cursor()` creates an independent session, not a separate engine. A lock covers each cursor's entire request so results cannot be overwritten by another thread. [Python API](https://duckdb.org/docs/current/clients/python/overview#about-cursor), [instance-cache implementation](https://github.com/duckdb/duckdb-python/blob/v1.5.6/src/duckdb_py/pyconnection.cpp#L2200).

The same full-sort SQL in `experiment.sql` and the same local input are used throughout. Only `memory_limit` changes between modes: normal `1GB`, spill `256MB`, OOM `1MB`. Within each mode the settings are identical across topologies: `threads=1`, `preserve_insertion_order=false`, a `2GB` spill cap, and identical Parquet export settings. Spill remains enabled even in OOM mode. These budgets are experiment inputs, not production recommendations; the report shows actual outcomes rather than assuming them.

Watch START/completion/error events in the terminal. Open the printed **report.html** path to compare every request's queue and query latency, overlap timeline, RSS history, spill evidence, and full errors. `results.json` and successful query profiles are saved beside it. Large query outputs and spill scratch are removed after validating row counts. Input data is cached in `work/experiment/`; each run has its own directory.

```sh
uv run --locked experiment.py --topology case3 --mode spill
uv run --locked experiment.py --mode spill --spill-memory 64MB
uv run --locked experiment.py --mode spill --spill-memory 128MB
uv run --locked experiment.py --query my-query.sql
uv run --locked python -m unittest -v test_run test_experiment
```

Your query file must contain one trusted SELECT using the `trips` view. Requests call DuckDB directly from Python threads, not through HTTP, with round-robin cursor assignment. Each topology/mode runs in a fresh subprocess; download and connection setup are outside request latency. Query time includes profiling setup and Parquet export; post-export row-count validation is outside query time but still holds the cursor lock. There is no retry. Three requests are a small demonstration, not a statistical benchmark; OS file caching and case order can affect timings.

RSS is sampled every 50 ms and may miss brief peaks. Profile buffer/spill peaks describe the **shared engine**, not memory attributable to an individual request; they are not summed. Disk spill is established by positive profile temporary-storage usage, with filesystem sampling as additional evidence. [Profiling metrics](https://duckdb.org/docs/current/dev/metrics).

The OOM case catches an actual DuckDB `OutOfMemoryException`, then checks each cursor still answers `SELECT 42`. It deliberately does **not** trigger a Windows/Linux kernel kill or Kubernetes `OOMKilled`. `memory_limit` is not a process RSS ceiling. [OOM guidance](https://duckdb.org/docs/current/guides/performance/oom).

Downloaded data and machine-specific results stay in ignored `work/`, not Git.

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
uv run --locked python -m unittest -v test_run
```

Local verification covers Parquet correctness, actual disk spilling, handled memory/spill-capacity failures, cleanup, and preservation of existing output. The spill test sorts one million rows with a 16 MB configured engine budget and checks that the profile records temporary storage. The bundled default query is small and does not need to spill. Neither test proves a process-RSS ceiling or Kubernetes kernel OOM behavior. Docker and Kubernetes execution require those tools and a cluster.

Verified locally on October 6, 2026 with Windows, Python 3.11.6, uv 0.10.4, and DuckDB 1.5.6: all seven tests passed. The Kubernetes YAML was parsed and its volume/claim relationships checked. Docker image builds and live Kubernetes execution were not run because Docker and kubectl are unavailable in this environment.
