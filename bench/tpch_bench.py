"""TPC-H in the Spark dialects vs DuckDB's default dialect, on the same database.

The 22 queries in bench/tpch/ are written in Spark SQL (they are also valid DuckDB SQL, so the native
run uses the same text in the default dialect). Results of every dialect are checked against the native
results before timing.

Data: CALL dbgen(sf=...) from DuckDB's tpch extension, stored in bench/out/tpch_sf<sf>.duckdb and reused.
The extension is installed from extensions.duckdb.org, or loaded from $TPCH_EXTENSION (a locally built
tpch.duckdb_extension) when that is set.

usage: python bench/tpch_bench.py [--sf 1] [--runs 5] [--queries 1,6,...] [--threads N]
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import harness  # noqa: E402
from harness import normalize, results_equal  # noqa: E402

# native DuckDB computes avg and decimal division in DOUBLE, the Spark dialects in DECIMAL with Spark's scale
# rules (checked against Spark by test/cases/tpch_small.sql): compare with a looser tolerance here
harness.FLOAT_REL = 1e-6

QUERIES = Path(__file__).resolve().parent / "tpch"


def ensure_data(sf):
    common.OUT_DIR.mkdir(exist_ok=True)
    path = common.OUT_DIR / f"tpch_sf{sf}.duckdb"
    if path.exists():
        return path
    con = common.connect(str(path), threads=os.cpu_count() or 2, load=False)
    tpch = os.environ.get("TPCH_EXTENSION")
    if tpch:
        con.execute(f"LOAD '{tpch}'")
    else:
        con.execute("INSTALL tpch")
        con.execute("LOAD tpch")
    print(f"generating TPC-H sf={sf} into {path}", flush=True)
    con.execute(f"CALL dbgen(sf={sf})")
    con.close()
    return path


def normalized(rows):
    return [[normalize(v) for v in row] for row in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", default="1")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--queries", default=None)
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 2)
    ap.add_argument("--dialects", default="spark,spark_strict")
    args = ap.parse_args()
    db = ensure_data(args.sf)
    con = common.connect(str(db), threads=args.threads, read_only=True)
    nums = [int(q) for q in args.queries.split(",")] if args.queries else range(1, 23)
    dialects = args.dialects.split(",")
    results = []
    failures = 0
    for q in nums:
        sql = (QUERIES / f"q{q:02d}.sql").read_text().strip().rstrip(";")
        row = {"query": q}
        common.set_dialect(con, None)
        expected = normalized(con.execute(sql).fetchall())
        row["native"] = common.timeit(lambda: con.execute(sql).fetchall(), runs=args.runs)
        for d in dialects:
            common.set_dialect(con, d)
            try:
                got = normalized(con.execute(sql).fetchall())
                # informational: native DuckDB's DOUBLE avg / division differ from Spark's DECIMAL results by
                # design (checked against Spark by test/cases/tpch_small.sql); only errors fail the run
                row[f"{d}_same_result"] = results_equal(expected, got, ordered=True)
                row[d] = common.timeit(lambda: con.execute(sql).fetchall(), runs=args.runs)
                row[f"{d}_ratio"] = row[d]["median"] / row["native"]["median"]
            except Exception as e:  # noqa: BLE001
                row[f"{d}_error"] = str(e).split("\n")[0][:200]
                failures += 1
            finally:
                common.set_dialect(con, None)
        results.append(row)
        cells = "  ".join(f"{d} {row[d + '_ratio']:.2f}x" + ("" if row.get(d + "_same_result") else " (RESULT DIFFERS)")
                          if d + "_ratio" in row else f"{d} {row.get(d + '_error')}" for d in dialects)
        print(f"Q{q:02d} native {row['native']['median'] * 1000:8.1f} ms  {cells}", flush=True)
    for d in dialects:
        g = common.geomean([r.get(f"{d}_ratio") for r in results])
        print(f"geometric mean ratio {d}: {g:.3f}" if g else f"{d}: no results")
    con.close()
    common.write_results(f"tpch_sf{args.sf}", results)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    with common.BenchLock():
        main()
