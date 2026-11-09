"""Fixed costs of the Spark dialects: extension load, first statement, and the latency of tiny statements.

  load      LOAD of the extension in a fresh connection (median of N)
  first     first SELECT 1 after LOAD + SET current_dialect (catalog / grammar initialisation)
  tiny      per-statement latency (parse + bind + execute + fetch) of small statements, native vs dialects,
            on a warm connection: where parse overhead and bind-time rewrites are visible

usage: python bench/latency_bench.py [--repeat 300] [--loads 15]
"""
import argparse
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
from engines import extension_path  # noqa: E402

TINY = {
    "select_1": "SELECT 1",
    "arith": "SELECT 1 + 2 * 3",
    "cast_string": "SELECT CAST('12' AS INT) + 1",
    "string_fn": "SELECT upper(concat('a', 'b')), length('hello'), substr('abcdef', 2, 3)",
    "date_fn": "SELECT date_add(DATE '2024-01-31', 1), year(DATE '2024-01-31'), date_format(TIMESTAMP '2024-01-31 10:00:00', 'yyyy-MM-dd')",
    "case_when": "SELECT CASE WHEN 1 > 2 THEN 'a' WHEN 2 > 1 THEN 'b' END",
    "table_small": "SELECT g, sum(v), avg(v) FROM VALUES ('a', 1), ('a', 2), ('b', 3) AS t(g, v) GROUP BY g ORDER BY g",
    "array_fn": "SELECT size(array(1, 2, 3)), array_contains(array(1, 2, 3), 2), sort_array(array(3, 1, 2))",
    "like": "SELECT 'abc' LIKE 'a%', 'abc' RLIKE '^a'",
    "join_small": "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS x(k, v) JOIN VALUES (1, 'c') AS y(k, w) ON x.k = y.k",
}


def stmt_time(con, sql, repeat):
    for _ in range(5):
        con.execute(sql).fetchall()
    samples = []
    for _ in range(repeat):
        t = time.perf_counter_ns()
        con.execute(sql).fetchall()
        samples.append(time.perf_counter_ns() - t)
    return statistics.median(samples) / 1e3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeat", type=int, default=300)
    ap.add_argument("--loads", type=int, default=15)
    args = ap.parse_args()
    results = {}
    loads, firsts = [], []
    for _ in range(args.loads):
        con = common.connect(load=False)
        t = time.perf_counter()
        con.execute(f"LOAD '{extension_path()}'")
        loads.append(time.perf_counter() - t)
        t = time.perf_counter()
        con.execute("SET current_dialect = 'spark'")
        con.execute("SELECT 1").fetchall()
        firsts.append(time.perf_counter() - t)
        con.close()
    results["load_ms"] = statistics.median(loads) * 1e3
    results["first_statement_ms"] = statistics.median(firsts) * 1e3
    print(f"LOAD {results['load_ms']:.1f} ms   first statement after SET current_dialect {results['first_statement_ms']:.1f} ms")
    con = common.connect()
    rows = []
    for name, sql in TINY.items():
        common.set_dialect(con, None)
        try:
            native = stmt_time(con, sql, args.repeat)
        except Exception:  # noqa: BLE001
            native = None
        row = {"name": name, "native_us": native}
        for d in ("spark", "spark_strict"):
            common.set_dialect(con, d)
            try:
                row[f"{d}_us"] = stmt_time(con, sql, args.repeat)
            except Exception as e:  # noqa: BLE001
                row[f"{d}_error"] = str(e).split("\n")[0][:100]
        common.set_dialect(con, None)
        rows.append(row)
        def cell(d):
            if d + "_us" not in row:
                return f"{d} {row.get(d + '_error', 'n/a')}"
            ratio = f" ({row[d + '_us'] / native:.2f}x)" if native else " (no native equivalent)"
            return f"{d} {row[d + '_us']:7.0f} us{ratio}"
        print(f"{name:14} native {native if native else float('nan'):7.0f} us  {cell('spark')}  {cell('spark_strict')}")
    results["tiny"] = rows
    ratios = [r["spark_us"] / r["native_us"] for r in rows if r.get("native_us") and "spark_us" in r]
    print(f"geometric mean tiny-statement ratio spark: {common.geomean(ratios):.2f}")
    common.write_results("latency", results)


if __name__ == "__main__":
    with common.BenchLock():
        main()
