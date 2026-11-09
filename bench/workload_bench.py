"""Spark-shaped workloads (event-log analytics: funnels, string cleaning, windows, joins, date bucketing, arrays,
JSON, casts) on a generated table, Spark dialects vs DuckDB's default dialect.

Each bench/workloads/*.sql has a `-- spark` section and, when the default dialect needs different function names, a
`-- native` section (otherwise the Spark text is run natively too). The result of every dialect is checked against
the native one (informational; type differences between Spark and DuckDB defaults are expected) before timing.

usage: python bench/workload_bench.py [--rows 2000000] [--runs 5] [--only 01,03] [--threads N]
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
import harness  # noqa: E402
from harness import normalize, results_equal  # noqa: E402

harness.FLOAT_REL = 1e-6
WORKLOADS = Path(__file__).resolve().parent / "workloads"


def ensure_data(rows):
    common.OUT_DIR.mkdir(exist_ok=True)
    path = common.OUT_DIR / f"workload_{rows}.duckdb"
    if path.exists():
        return path
    con = common.connect(str(path), threads=os.cpu_count() or 2, load=False)
    con.execute(f"""CREATE TABLE events AS SELECT
        r AS id,
        CAST(r * 2654435761 % 100003 AS BIGINT) AS user_id,
        TIMESTAMP '2024-01-01 00:00:00' + to_seconds((r * 37) % 7000000) AS ts,
        CAST(((r * 7919) % 1000000) / 100.0 AS DECIMAL(12,2)) AS amount,
        (['us', 'de', 'fr', 'jp', ' BR', 'In ', 'gb', 'ca', 'au', 'mx'])[1 + r % 10] AS country,
        (['ok', 'ok', 'ok', 'refund', 'error'])[1 + r % 5] AS status,
        'http' || (CASE WHEN r % 3 = 0 THEN 's' ELSE '' END) || '://host' || CAST(r % 200 AS VARCHAR) || '.example.com/p/' || CAST(r % 99991 AS VARCHAR) AS url,
        '{{"kind": "k' || CAST(r % 11 AS VARCHAR) || '", "qty": ' || CAST(r % 9 AS VARCHAR) || '}}' AS payload,
        list_transform(range(CAST(r % 6 AS BIGINT)), lambda x: 't' || CAST((x * 7 + r) % 40 AS VARCHAR)) AS tags,
        CASE WHEN r % 17 = 0 THEN NULL ELSE CAST(r % 4 AS INT) END AS retries,
        CASE WHEN r % 10 = 0 THEN 'x' || CAST(r % 100 AS VARCHAR) ELSE CAST(r % 100000 AS VARCHAR) END AS code
        FROM range({rows}) t(r)""")
    con.execute("""CREATE TABLE dim AS SELECT * FROM (VALUES ('us', 'AMER', 0.10), ('de', 'EMEA', 0.20), ('fr', 'EMEA', 0.20),
        ('jp', 'APAC', 0.05), (' BR', 'AMER', 0.15), ('In ', 'APAC', 0.00), ('gb', 'EMEA', 0.20), ('ca', 'AMER', 0.10),
        ('au', 'APAC', 0.05), ('mx', 'AMER', 0.10)) t(country, region, discount)""")
    con.close()
    return path


def sections(text):
    spark, native, cur = [], [], None
    for line in text.splitlines():
        if line.strip() == "-- spark":
            cur = spark
        elif line.strip() == "-- native":
            cur = native
        elif cur is not None:
            cur.append(line)
    s = "\n".join(spark).strip().rstrip(";")
    n = "\n".join(native).strip().rstrip(";") or s
    return s, n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2000000)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--only", default=None)
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 2)
    ap.add_argument("--dialects", default="spark,spark_strict")
    args = ap.parse_args()
    db = ensure_data(args.rows)
    con = common.connect(str(db), threads=args.threads, read_only=True)
    files = sorted(WORKLOADS.glob("*.sql"))
    if args.only:
        keep = args.only.split(",")
        files = [f for f in files if any(f.name.startswith(k) for k in keep)]
    dialects = args.dialects.split(",")
    results, errors = [], 0
    for f in files:
        spark_sql, native_sql = sections(f.read_text())
        row = {"workload": f.stem}
        common.set_dialect(con, None)
        try:
            expected = [[normalize(v) for v in r] for r in con.execute(native_sql).fetchall()]
            row["native"] = common.timeit(lambda: con.execute(native_sql).fetchall(), runs=args.runs)
        except Exception as e:  # noqa: BLE001
            row["native_error"] = str(e).split("\n")[0][:200]
            results.append(row)
            print(f"{f.stem} native error: {row['native_error']}", flush=True)
            errors += 1
            continue
        for d in dialects:
            common.set_dialect(con, d)
            try:
                got = [[normalize(v) for v in r] for r in con.execute(spark_sql).fetchall()]
                row[f"{d}_same_result"] = results_equal(expected, got, ordered=True)
                row[d] = common.timeit(lambda: con.execute(spark_sql).fetchall(), runs=args.runs)
                row[f"{d}_ratio"] = row[d]["median"] / row["native"]["median"]
            except Exception as e:  # noqa: BLE001
                row[f"{d}_error"] = str(e).split("\n")[0][:200]
                errors += 1
            finally:
                common.set_dialect(con, None)
        results.append(row)
        cells = "  ".join(f"{d} {row[d + '_ratio']:.2f}x" + ("" if row.get(d + "_same_result") else " (RESULT DIFFERS)")
                          if d + "_ratio" in row else f"{d} ERROR {row.get(d + '_error')}" for d in dialects)
        print(f"{f.stem:22} native {row['native']['median'] * 1000:8.1f} ms  {cells}", flush=True)
    for d in dialects:
        g = common.geomean([r.get(f"{d}_ratio") for r in results])
        print(f"geometric mean ratio {d}: {g:.3f}" if g else f"{d}: no results")
    con.close()
    common.write_results("workloads", results)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    with common.BenchLock():
        main()
