"""Function and operator overhead: each pair in func_pairs.toml, Spark expression in the `spark` dialect vs
the native DuckDB expression in the default dialect, on the same tables, single-threaded.

usage: python bench/func_bench.py [--rows N] [--runs N] [--only name,name] [--smoke] [--check]
  --check   also compare the results of the two sides (count / value) and print differences
"""
import argparse
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

PAIRS = Path(__file__).resolve().parent / "func_pairs.toml"


def null10(expr, n="r"):
    """NULL for 10% of rows."""
    return f"CASE WHEN {n} % 10 = 3 THEN NULL ELSE {expr} END"


def create_tables(con, rows):
    common.set_dialect(con, None)
    con.execute(f"""CREATE OR REPLACE TABLE ints AS SELECT
        {null10('CAST((r * 7919) % 100003 - 50000 AS INT)')} AS i,
        CAST(r % 97 + 1 AS INT) AS j,
        {null10('r * 2654435761 % 1000000007')} AS b
        FROM range({rows}) t(r)""")
    con.execute(f"""CREATE OR REPLACE TABLE doubles AS SELECT
        {null10('((r * 7919) % 100003) / 7.0 - 7000')} AS x,
        {null10('((r * 104729) % 1000003) / 13.0')} AS y
        FROM range({rows}) t(r)""")
    con.execute(f"""CREATE OR REPLACE TABLE decs AS SELECT
        {null10('CAST(((r * 7919) % 100003) / 100.0 AS DECIMAL(18,2))')} AS d,
        {null10('CAST(((r * 104729) % 1000003) / 1000.0 AS DECIMAL(38,10))')} AS e
        FROM range({rows}) t(r)""")
    con.execute(f"""CREATE OR REPLACE TABLE strs AS SELECT
        {null10("'ab' || CAST(r % 1000 AS VARCHAR) || 'c' || chr(97 + CAST(r % 26 AS INT))")} AS s,
        {null10("chr(65 + CAST(r % 26 AS INT)) || 'xy'")} AS s2,
        {null10("repeat('lorem' || CAST(r % 97 AS VARCHAR) || ' ipsum abc', 12)")} AS l,
        {null10("'Ünïcødé ' || CAST(r % 1000 AS VARCHAR) || ' 日本'")} AS u,
        {null10("'a' || CAST(r % 10 AS VARCHAR) || ',b;c,' || CAST(r % 7 AS VARCHAR) || ';d,e'")} AS csv,
        {null10("CAST(r % 100000 - 50000 AS VARCHAR)")} AS sn,
        {null10("strftime(DATE '2000-01-01' + CAST(r % 9000 AS INT), '%Y-%m-%d')")} AS sd,
        {null10("strftime(TIMESTAMP '2000-01-01' + to_seconds(r * 37), '%Y-%m-%d %H:%M:%S')")} AS sts,
        {null10("'{\"a\": ' || CAST(r % 1000 AS VARCHAR) || ', \"b\": \"x' || CAST(r % 13 AS VARCHAR) || '\"}'")} AS js,
        CAST(r % 97 + 1 AS INT) AS j
        FROM range({rows}) t(r)""")
    con.execute(f"""CREATE OR REPLACE TABLE dates AS SELECT
        {null10('DATE \'2000-01-01\' + CAST(r % 9000 AS INT)')} AS dt,
        {null10("TIMESTAMP '2000-01-01' + to_seconds(r * 37)")} AS ts
        FROM range({rows}) t(r)""")
    con.execute(f"""CREATE OR REPLACE TABLE arrs AS SELECT
        {null10('list_transform(range(CAST(r % 21 AS BIGINT)), lambda x: CAST((x * 7 + r) % 50 AS INT))')} AS a,
        {null10("list_transform(range(CAST(r % 5 AS BIGINT)), lambda x: 'v' || CAST(x AS VARCHAR))")} AS sa
        FROM range({rows}) t(r)""")
    con.execute(f"""CREATE OR REPLACE TABLE maps AS SELECT
        {null10("map_from_entries(list_transform(range(CAST(r % 6 AS BIGINT)), lambda x: {'k': 'k' || CAST(x AS VARCHAR), 'v': CAST(x * r % 100 AS INT)}))")} AS m
        FROM range({rows}) t(r)""")


def wrap(pair, side):
    expr = pair[side]
    kind = pair.get("kind", "scalar")
    table = pair["table"]
    if kind == "scalar":
        return f"SELECT count(v) FROM (SELECT {expr} AS v FROM {table})"
    if kind == "agg":
        return f"SELECT {expr} FROM {table}"
    if kind == "group":
        return f"SELECT count(*) FROM (SELECT j, {expr} AS v FROM {table} GROUP BY j)"
    return f"SELECT count(*) FROM ({expr})"


def limit_rows(pair, sql, rows):
    n = pair.get("n")
    if n and n < rows:
        return sql.replace(f"FROM {pair['table']}", f"FROM (SELECT * FROM {pair['table']} LIMIT {n})")
    return sql


DB_PATH = common.OUT_DIR / "func_tables.duckdb"


def make_db(rows):
    """The benchmark tables once, in bench/out/func_tables.duckdb, so every tool (func_bench, ab, profile) times
    identical data and opens it read-only instead of regenerating it."""
    common.OUT_DIR.mkdir(exist_ok=True)
    DB_PATH.unlink(missing_ok=True)
    con = common.connect(str(DB_PATH), threads=2, load=False)
    create_tables(con, rows)
    con.execute("CREATE TABLE bench_meta AS SELECT %d AS rows" % rows)
    con.close()
    print(f"wrote {DB_PATH} ({rows} rows per table)")


def run(args):
    pairs = tomllib.loads(PAIRS.read_text())["pair"]
    if args.only:
        wanted = set(args.only.split(","))
        pairs = [p for p in pairs if p["name"] in wanted]
    if args.smoke:
        pairs = [p for p in pairs if p.get("smoke")]
    if args.db:
        con = common.connect(str(DB_PATH), threads=1, read_only=True)
        args.rows = con.execute("SELECT rows FROM bench_meta").fetchone()[0]
    else:
        con = common.connect(threads=1)
        create_tables(con, args.rows)
    results = []
    for p in pairs:
        spark_sql = limit_rows(p, wrap(p, "spark"), args.rows)
        native_sql = limit_rows(p, wrap(p, "native"), args.rows) if p.get("native") else None
        row = {"name": p["name"], "kind": p.get("kind", "scalar"), "rows": min(args.rows, p.get("n", args.rows))}
        try:
            common.set_dialect(con, "spark")
            spark_val = con.execute(spark_sql).fetchall()
            row["spark"] = common.timeit(lambda: con.execute(spark_sql).fetchall(), runs=args.runs)
        except Exception as e:  # noqa: BLE001
            row["spark_error"] = str(e).split("\n")[0][:200]
            spark_val = None
        finally:
            common.set_dialect(con, None)
        if native_sql:
            try:
                native_val = con.execute(native_sql).fetchall()
                row["native"] = common.timeit(lambda: con.execute(native_sql).fetchall(), runs=args.runs)
                if "spark" in row:
                    row["ratio"] = row["spark"]["median"] / row["native"]["median"]
                if args.check and spark_val is not None and spark_val != native_val:
                    row["check"] = f"results differ: spark={spark_val} native={native_val}"
            except Exception as e:  # noqa: BLE001
                row["native_error"] = str(e).split("\n")[0][:200]
        if "spark" in row:
            row["spark_rows_per_s"] = row["rows"] / row["spark"]["median"]
        results.append(row)
        ratio = f"{row['ratio']:6.2f}x" if "ratio" in row else "   -   "
        t = f"{row['spark']['median'] * 1000:8.1f} ms" if "spark" in row else row.get("spark_error", "")
        print(f"{p['name']:28} {ratio}  spark {t}  {row.get('check', '')}{row.get('native_error', '')}", flush=True)
    con.close()
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--only")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--name", default=None)
    ap.add_argument("--make-db", action="store_true", help="write bench/out/func_tables.duckdb and exit")
    ap.add_argument("--db", action="store_true", help="use bench/out/func_tables.duckdb (read-only) instead of tables in memory")
    args = ap.parse_args()
    if args.make_db:
        make_db(args.rows)
        return
    results = run(args)
    common.write_results(args.name or ("func_smoke" if args.smoke else "func"), results)


if __name__ == "__main__":
    with common.BenchLock():
        main()
