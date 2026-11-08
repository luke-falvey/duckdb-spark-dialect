"""Run every case in DuckDB (spark dialect) and compare with the Spark 3.5 golden results.

usage: python test/run_duckdb.py [-v] [-k substring] [--dialect spark|spark_strict] [case files...]

Outcome per case:
  PASS      same rows (or both engines raise an error, or nondeterministic and both succeed)
  LENIENT   Spark raises an error, DuckDB returns a result (DuckDB accepts more than Spark)
  FAIL      DuckDB raises an error where Spark succeeds, or the rows differ
"""
import argparse
import json
import os
import pickle
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

os.environ["TZ"] = "UTC"
time.tzset()

import duckdb  # noqa: E402

from harness import ROOT, load_cases, normalize_typed, results_equal  # noqa: E402

PROJECT = ROOT.parent
EXTENSION = Path(os.environ.get(
    "SPARK_EXTENSION", PROJECT / "build" / "spark_dialect.duckdb_extension"))


COMMANDS = ("INSERT", "CREATE", "DROP", "ALTER", "TRUNCATE", "USE", "CACHE", "UNCACHE", "REFRESH", "DELETE",
            "UPDATE", "MERGE", "CLEAR", "ADD", "MSCK", "ANALYZE")


def is_command(sql):
    words = sql.lstrip("( ").split(None, 1)
    return bool(words) and words[0].upper() in COMMANDS


def connect(dialect="spark"):
    # the extension is built from the v2.0.0 sources in a checkout without tags (version "v2.0.0-dev1")
    con = duckdb.connect(config={"allow_unsigned_extensions": "true", "allow_extensions_metadata_mismatch": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET current_dialect = '{dialect}'")
    return con


def run_file(path, verbose, keyword, dialect="spark"):
    golden_path = ROOT / "golden" / (path.name + ".pkl")
    golden = pickle.loads(golden_path.read_bytes())
    con = connect(dialect)
    results = []
    for case in load_cases([path]):
        expected = golden.get(case.index)
        if expected is None or expected["sql"] != case.sql:
            raise SystemExit(f"golden file out of date for {case.id}; run make_golden.py")
        if keyword and keyword not in case.sql and not case.has("setup"):
            continue
        if case.has("skip"):
            results.append((case, "SKIP", case.directives["skip"]))
            continue
        try:
            try:
                rel = con.sql(case.sql)
                rows = rel.fetchall() if rel is not None else []
                types = rel.types if rel is not None else []
            except duckdb.InvalidInputException as e:
                if "Expected exactly one SQL statement" not in str(e):
                    raise
                # rewrites such as INSERT OVERWRITE produce several statements
                rows = con.execute(case.sql).fetchall()
                types = []
            actual = [[normalize_typed(v, types[i] if i < len(types) else None) for i, v in enumerate(row)]
                      for row in rows]
            duck_error = None
        except Exception as e:  # noqa: BLE001
            actual = None
            duck_error = str(e).split("\n")[0][:300]
        if case.has("setup"):
            if duck_error:
                results.append((case, "FAIL", f"setup failed: {duck_error}"))
            continue
        if duck_error and "INTERNAL" in duck_error:
            # an internal error is never acceptable, even where Spark also fails
            outcome = ("FAIL", f"duckdb internal error: {duck_error}")
        elif not expected["ok"]:
            if duck_error:
                outcome = ("PASS", "both error")
            elif dialect == "spark_strict":
                # the strict dialect must reject everything Spark rejects
                outcome = ("FAIL", f"LENIENT in strict mode; spark error: {expected['error'][:120]}")
            else:
                outcome = ("LENIENT", f"spark error: {expected['error'][:120]}")
        elif duck_error:
            outcome = ("FAIL", f"duckdb error: {duck_error}")
        elif case.has("nondeterministic"):
            outcome = ("PASS", "")
        elif is_command(case.sql) and expected["rows"] == []:
            # DDL / DML: Spark returns no rows, DuckDB a row count; the data is checked by later queries
            outcome = ("PASS", "")
        elif results_equal(expected["rows"], actual, case.ordered):
            outcome = ("PASS", "")
        else:
            outcome = ("FAIL", f"rows differ\n      spark:  {expected['rows'][:5]}\n      duckdb: {actual[:5]}")
        results.append((case, *outcome))
        if verbose and outcome[0] != "PASS":
            print(f"{outcome[0]} {case.id}: {case.sql[:200]}\n      {outcome[1]}")
    con.close()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-k", "--keyword")
    parser.add_argument("--dialect", choices=["spark", "spark_strict"], default="spark")
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    files = [Path(f) for f in args.files] or sorted((ROOT / "cases").glob("*.sql"))

    total = Counter()
    by_function = defaultdict(Counter)
    failures = []
    summary_lines = []
    for path in files:
        results = run_file(path, args.verbose, args.keyword, args.dialect)
        counts = Counter(r[1] for r in results)
        total.update(counts)
        line = f"{path.name:28} " + "  ".join(f"{k}={counts[k]}" for k in ("PASS", "LENIENT", "FAIL", "SKIP"))
        print(line)
        summary_lines.append(line)
        for case, outcome, detail in results:
            if "function" in case.directives:
                by_function[case.directives["function"]][outcome] += 1
            if outcome in ("FAIL", "LENIENT", "SKIP"):
                failures.append(f"{outcome} {case.id}\n  sql: {case.sql}\n  {detail}\n")
    print(f"{'TOTAL':28} " + "  ".join(f"{k}={total[k]}" for k in ("PASS", "LENIENT", "FAIL", "SKIP")))
    summary_lines.append(f"{'TOTAL':28} " + "  ".join(f"{k}={total[k]}" for k in ("PASS", "LENIENT", "FAIL", "SKIP")))
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    suffix = "" if args.dialect == "spark" else "_" + args.dialect
    (out / f"failures{suffix}.txt").write_text("\n".join(failures))
    if not args.files and not args.keyword:
        (out / f"summary{suffix}.txt").write_text("\n".join(summary_lines) + "\n")
    if by_function:
        (out / "functions.json").write_text(json.dumps(by_function, indent=1, sort_keys=True))
        bad = sorted(f for f, c in by_function.items() if c["FAIL"])
        print(f"functions with a failing example: {len(bad)} of {len(by_function)}")
    sys.exit(1 if total["FAIL"] else 0)


if __name__ == "__main__":
    main()
