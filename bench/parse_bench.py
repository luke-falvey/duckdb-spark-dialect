"""Parse (tokenize + rewrite + PEG parse) overhead of the Spark dialects vs DuckDB's default dialect.

Parse time is measured with `connection.extract_statements(sql)`, which parses with the connection's
current dialect without binding or executing.

  corpus   every statement in test/cases/*.sql, 200 parses each, per dialect
  scaling  statements with 10 .. 10,000 select items / IN items / CASE branches / UNION ALL branches /
           nested parentheses; time per parse against size (superlinear growth is a finding)

usage: python bench/parse_bench.py [--repeat N] [--sizes 10,100,1000,10000] [--smoke]
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
from harness import load_cases  # noqa: E402

SHAPES = {
    "select_items": lambda n: "SELECT " + ", ".join(f"c{i % 7} + {i} AS a{i}" for i in range(n)) + " FROM t",
    "in_list": lambda n: "SELECT * FROM t WHERE c1 IN (" + ", ".join(str(i) for i in range(n)) + ")",
    "case_branches": lambda n: "SELECT CASE " + " ".join(f"WHEN c1 = {i} THEN '{i}'" for i in range(n)) + " END FROM t",
    "union_all": lambda n: " UNION ALL ".join(f"SELECT {i} AS a, c1 FROM t" for i in range(n)),
    "nested_parens": lambda n: "SELECT " + "(" * n + "c1" + ")" * n + " FROM t",
    "and_chain": lambda n: "SELECT * FROM t WHERE " + " AND ".join(f"c{i % 7} > {i}" for i in range(n)),
    "function_calls": lambda n: "SELECT " + ", ".join(f"upper(concat(s, '{i}'))" for i in range(n)) + " FROM t",
}


def parse_time(con, sql, repeat):
    t = time.perf_counter_ns()
    for _ in range(repeat):
        con.extract_statements(sql)
    return (time.perf_counter_ns() - t) / 1e9 / repeat


def parses(con, sql):
    try:
        con.extract_statements(sql)
        return True
    except Exception:  # noqa: BLE001
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeat", type=int, default=200)
    ap.add_argument("--sizes", default="10,100,1000,10000")
    ap.add_argument("--smoke", action="store_true")
    args = ap.parse_args()
    con = common.connect()
    dialects = ["duckdb", "spark", "spark_strict"]
    results = {"corpus": {}, "scaling": {}}

    # corpus: statements that parse in all three dialects, so the comparison is like for like
    cases = [c.sql for c in load_cases(sorted((common.PROJECT / "test" / "cases").glob("*.sql")))]
    if args.smoke:
        cases = cases[::10]
    both = []
    for sql in cases:
        ok = True
        for d in dialects:
            common.set_dialect(con, d)
            ok = ok and parses(con, sql)
        if ok:
            both.append(sql)
    spark_only = [sql for sql in cases if sql not in both]
    for d in dialects:
        common.set_dialect(con, d)
        total = sum(parse_time(con, sql, args.repeat) for sql in both)
        results["corpus"][d] = {"statements": len(both), "total_s": total, "per_statement_us": total / len(both) * 1e6}
    common.set_dialect(con, "spark")
    total = sum(parse_time(con, sql, args.repeat) for sql in spark_only if parses(con, sql))
    results["corpus"]["spark_only_statements"] = {"statements": len(spark_only),
                                                  "per_statement_us": total / max(1, len(spark_only)) * 1e6}
    base = results["corpus"]["duckdb"]["per_statement_us"]
    for d in dialects:
        r = results["corpus"][d]
        r["ratio"] = r["per_statement_us"] / base
        print(f"corpus {d:13} {r['per_statement_us']:8.1f} us/statement  ratio {r['ratio']:.2f}x ({len(both)} statements)")

    sizes = [int(s) for s in args.sizes.split(",")]
    if args.smoke:
        sizes = [s for s in sizes if s <= 1000] + [10000] if 10000 in sizes else sizes
    for shape, make in SHAPES.items():
        results["scaling"][shape] = {}
        for d in dialects:
            common.set_dialect(con, d)
            row = {}
            for n in sizes:
                if shape == "nested_parens" and n > 1000:
                    continue  # DuckDB's own parser limits nesting depth
                sql = make(n)
                try:
                    reps = max(1, min(args.repeat, int(2000 / n)))
                    row[n] = parse_time(con, sql, reps)
                except Exception as e:  # noqa: BLE001
                    row[n] = f"error: {str(e).splitlines()[0][:80]}"
            results["scaling"][shape][d] = row
            cells = "  ".join(f"{n}:{v * 1000:.2f}ms" if isinstance(v, float) else f"{n}:{v}" for n, v in row.items())
            print(f"scaling {shape:15} {d:13} {cells}")
    common.set_dialect(con, None)
    common.write_results("parse_smoke" if args.smoke else "parse", results)


if __name__ == "__main__":
    with common.BenchLock():
        main()
