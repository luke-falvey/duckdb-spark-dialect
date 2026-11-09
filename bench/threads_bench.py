"""Thread scaling, ANSI-mode overhead and memory-limited execution of TPC-H queries.

  threads   queries at 1, 2, 4 threads: time per dialect, ratio to native, and the speed-up from 1 thread
  ansi      spark_ansi_enabled = true vs false (same dialect): the cost of the checked arithmetic / casts
  memory    memory_limit = 256MB (and 1GB): the query must finish, with the same result digest as unlimited

usage: python bench/threads_bench.py [--sf 1] [--queries 1,3,5,9,13,18,21] [--runs 5]
"""
import argparse
import hashlib
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import tpch_bench  # noqa: E402
from harness import normalize  # noqa: E402

QUERIES = tpch_bench.QUERIES


def digest(rows):
    return hashlib.sha1(repr([[normalize(v) for v in r] for r in rows]).encode()).hexdigest()[:10]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", default="1")
    ap.add_argument("--queries", default="1,3,5,9,13,18,21")
    ap.add_argument("--runs", type=int, default=5)
    args = ap.parse_args()
    db = tpch_bench.ensure_data(args.sf)
    nums = [int(q) for q in args.queries.split(",")]
    results = {"threads": [], "ansi": [], "memory": []}
    cores = os.cpu_count() or 2
    for t in sorted({1, 2, min(4, cores)}):
        con = common.connect(str(db), threads=t, read_only=True)
        for q in nums:
            sql = (QUERIES / f"q{q:02d}.sql").read_text().strip().rstrip(";")
            common.set_dialect(con, None)
            native = common.timeit(lambda: con.execute(sql).fetchall(), runs=args.runs)["median"]
            common.set_dialect(con, "spark")
            spark = common.timeit(lambda: con.execute(sql).fetchall(), runs=args.runs)["median"]
            common.set_dialect(con, None)
            results["threads"].append({"threads": t, "query": q, "native_s": native, "spark_s": spark, "ratio": spark / native})
        con.close()
    print("threads scaling (spark/native ratio; speed-up of the Spark dialect from 1 thread)")
    for q in nums:
        rows = {r["threads"]: r for r in results["threads"] if r["query"] == q}
        base = rows[1]["spark_s"]
        print(f"Q{q:02d} " + "  ".join(f"{t}t {r['ratio']:.2f}x (spark speed-up {base / r['spark_s']:.2f}x, native {rows[1]['native_s'] / r['native_s']:.2f}x)"
                                         for t, r in sorted(rows.items())))
    con = common.connect(str(db), threads=min(4, cores), read_only=True)
    common.set_dialect(con, "spark")
    print("ANSI mode overhead (spark dialect, ansi on / off)")
    for q in nums:
        sql = (QUERIES / f"q{q:02d}.sql").read_text().strip().rstrip(";")
        con.execute("SET spark_ansi_enabled = false")
        off = common.timeit(lambda: con.execute(sql).fetchall(), runs=args.runs)["median"]
        con.execute("SET spark_ansi_enabled = true")
        try:
            on = common.timeit(lambda: con.execute(sql).fetchall(), runs=args.runs)["median"]
        except Exception as e:  # noqa: BLE001
            print(f"Q{q:02d} ansi error: {str(e).splitlines()[0][:100]}")
            on = None
        con.execute("SET spark_ansi_enabled = false")
        if on:
            results["ansi"].append({"query": q, "off_s": off, "on_s": on, "ratio": on / off})
            print(f"Q{q:02d} ansi on/off {on / off:.2f}x ({off * 1000:.1f} -> {on * 1000:.1f} ms)")
    if results["ansi"]:
        print(f"geometric mean ansi on/off: {common.geomean([r['ratio'] for r in results['ansi']]):.3f}")
    common.set_dialect(con, None)
    con.close()
    print("memory-limited execution (result digest must equal the unlimited run)")
    for limit in ("1GB", "256MB"):
        for q in nums:
            sql = (QUERIES / f"q{q:02d}.sql").read_text().strip().rstrip(";")
            con = common.connect(str(db), threads=min(4, cores), read_only=True)
            common.set_dialect(con, "spark")
            expected = digest(con.execute(sql).fetchall())
            con.execute(f"SET memory_limit = '{limit}'")
            try:
                got = digest(con.execute(sql).fetchall())
                status = "ok" if got == expected else "RESULT DIFFERS"
            except Exception as e:  # noqa: BLE001
                status = "error: " + str(e).splitlines()[0][:100]
            results["memory"].append({"limit": limit, "query": q, "status": status})
            print(f"Q{q:02d} memory_limit {limit}: {status}")
            con.close()
    common.write_results("threads", results)


if __name__ == "__main__":
    with common.BenchLock():
        main()
