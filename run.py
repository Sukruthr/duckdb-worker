import argparse
import logging
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb


def run_query(args):
    query = args.query.read_text(encoding="utf-8")
    output = args.output.resolve()
    spill_root = args.temp_directory.resolve()
    if output.exists():
        raise FileExistsError(f"Output already exists: {output}. Choose a new path.")
    if args.profile:
        args.profile = args.profile.resolve()
        if args.profile == output or args.profile.exists():
            raise ValueError("Choose a new profile path different from the output path.")
        args.profile.parent.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    spill_root.mkdir(parents=True, exist_ok=True)

    # Each run owns its scratch directories; cleanup happens after DuckDB closes.
    with TemporaryDirectory(prefix="query-", dir=spill_root) as spill:
        with TemporaryDirectory(prefix=".duckdb-output-", dir=output.parent) as staging:
            config = {
                "memory_limit": args.memory_limit,
                "threads": args.threads,
                "temp_directory": spill,
                "preserve_insertion_order": False,
            }
            with duckdb.connect(
                args.database, read_only=args.database != ":memory:", config=config
            ) as con:
                # Apply the spill cap after the buffer manager has initialized.
                con.execute("SET max_temp_directory_size = ?", [args.max_temp_size])
                statements = con.extract_statements(query)
                if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
                    raise ValueError("The query file must contain exactly one SELECT query.")
                settings = con.execute(
                    "SELECT version(), current_setting('memory_limit'), "
                    "current_setting('threads'), current_setting('max_temp_directory_size')"
                ).fetchone()
                logging.info(
                    "DuckDB %s | memory=%s | threads=%s | max_spill=%s | spill=%s",
                    *settings, spill,
                )
                if args.profile:
                    con.execute("SET enable_profiling = 'json'")
                    con.execute("SET profiling_output = ?", [str(args.profile)])
                    con.execute("SET profiling_coverage = 'ALL'")

                # The relational API exports in DuckDB, without a Python result list.
                result = Path(staging) / "result.parquet"
                con.sql(query).write_parquet(str(result), compression="zstd")

            result.replace(output)
    logging.info("Wrote %s (%s bytes)", output, output.stat().st_size)


def main():
    parser = argparse.ArgumentParser(
        description="Run one SELECT query and export it directly to Parquet.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--query", type=Path, default=Path(__file__).with_name("query.sql"),
        help="UTF-8 file containing one SELECT query.",
    )
    parser.add_argument(
        "--output", type=Path, default=Path("work/result.parquet"), help="New Parquet output path.",
    )
    parser.add_argument("--database", default=":memory:", help="Existing databases open read-only.")
    parser.add_argument("--memory-limit", default="512MB", help="DuckDB managed-memory budget.")
    parser.add_argument("--threads", type=int, default=1, help="DuckDB query threads.")
    parser.add_argument(
        "--temp-directory", type=Path, default=Path("work/spill"), help="Disk-backed spill root.",
    )
    parser.add_argument("--max-temp-size", default="2GB", help="Maximum DuckDB spill size.")
    parser.add_argument("--profile", type=Path, help="Optional JSON query profile at a new path.")
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("--threads must be at least 1")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout)
    try:
        run_query(args)
    except duckdb.OutOfMemoryException as exc:
        logging.error("DuckDB OOM: %s", exc)
        logging.error("Reduce threads or query size; check spill space. This job is not retried.")
        return 1
    except (duckdb.Error, OSError, ValueError) as exc:
        logging.error("Job failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
