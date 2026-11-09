"""TPC-DS in the Spark dialects vs DuckDB's default dialect, on the same database.

The 99 queries in bench/tpcds/ are the text of the tpcds extension's tpcds_queries() (the TPC templates with their
default parameters; they are standard SQL and run in all dialects unchanged). For each query: the result under every
dialect is compared with the native result (multiset compare, informational: tie-ordering under LIMIT may differ),
then timed. Queries that fail to parse / bind / run in a dialect are reported as errors (these are findings).

Data: CALL dsdgen(sf=...) from DuckDB's tpcds extension, stored in bench/out/tpcds_sf<sf>.duckdb and reused.
The extension is installed from extensions.duckdb.org, or loaded from $TPCDS_EXTENSION (a locally built
tpcds.duckdb_extension) when that is set.

usage: python bench/tpcds_bench.py [--sf 1] [--runs 3] [--queries 1,2,...] [--threads N] [--timeout S]
"""
import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import harness  # noqa: E402
from harness import normalize, results_equal  # noqa: E402

harness.FLOAT_REL = 1e-6

QUERIES = Path(__file__).resolve().parent / "tpcds"


def ensure_data(sf):
    common.OUT_DIR.mkdir(exist_ok=True)
    path = common.OUT_DIR / f"tpcds_sf{sf}.duckdb"
    if path.exists():
        return path
    con = common.connect(str(path), threads=os.cpu_count() or 2, load=False)
    ext = os.environ.get("TPCDS_EXTENSION")
    if ext:
        con.execute(f"LOAD '{ext}'")
    else:
        con.execute("INSTALL tpcds")
        con.execute("LOAD tpcds")
    print(f"generating TPC-DS sf={sf} into {path}", flush=True)
    con.execute(f"CALL dsdgen(sf={sf})")
    con.close()
    return path


def normalized(rows):
    return [[normalize(v) for v in row] for row in rows]


def first_line(e):
    return str(e).split("\n")[0][:200]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", default="1")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--queries", default=None)
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 2)
    ap.add_argument("--dialects", default="spark,spark_strict")
    ap.add_argument("--slow", type=float, default=20.0, help="queries whose first run takes longer (s) are timed once")
    args = ap.parse_args()
    db = ensure_data(args.sf)
    con = common.connect(str(db), threads=args.threads, read_only=True)
    nums = [int(q) for q in args.queries.split(",")] if args.queries else range(1, 100)
    dialects = args.dialects.split(",")
    results = []
    errors = 0
    for q in nums:
        sql = (QUERIES / f"q{q:02d}.sql").read_text().strip().rstrip(";")
        row = {"query": q}
        common.set_dialect(con, None)
        try:
            t = time.perf_counter()
            expected = normalized(con.execute(sql).fetchall())
            first = time.perf_counter() - t
            runs = 1 if first > args.slow else args.runs
            row["native"] = common.timeit(lambda: con.execute(sql).fetchall(), runs=runs)
        except Exception as e:  # noqa: BLE001
            row["native_error"] = first_line(e)
            results.append(row)
            print(f"Q{q:02d} native error: {row['native_error']}", flush=True)
            continue
        for d in dialects:
            common.set_dialect(con, d)
            try:
                got = normalized(con.execute(sql).fetchall())
                row[f"{d}_same_result"] = results_equal(expected, got, ordered=False)
                row[d] = common.timeit(lambda: con.execute(sql).fetchall(), runs=runs)
                row[f"{d}_ratio"] = row[d]["median"] / row["native"]["median"]
            except Exception as e:  # noqa: BLE001
                row[f"{d}_error"] = first_line(e)
                errors += 1
            finally:
                common.set_dialect(con, None)
        results.append(row)
        cells = "  ".join(f"{d} {row[d + '_ratio']:.2f}x" + ("" if row.get(d + "_same_result") else " (RESULT DIFFERS)")
                          if d + "_ratio" in row else f"{d} ERROR {row.get(d + '_error')}" for d in dialects)
        print(f"Q{q:02d} native {row['native']['median'] * 1000:8.1f} ms  {cells}", flush=True)
    for d in dialects:
        g = common.geomean([r.get(f"{d}_ratio") for r in results])
        print(f"geometric mean ratio {d}: {g:.3f}" if g else f"{d}: no results")
    con.close()
    common.write_results(f"tpcds_sf{args.sf}", results)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    with common.BenchLock():
        main()
