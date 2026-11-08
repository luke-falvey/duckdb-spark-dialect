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
from collections import Counter, defaultdict
from pathlib import Path

from engines import DuckRunner, extension_path, scratch_dir, use_utc  # noqa: E402
from harness import ROOT, file_config, load_cases, results_equal  # noqa: E402

EXTENSION = extension_path()
CASE_TIMEOUT = 60.0


COMMANDS = ("INSERT", "CREATE", "DROP", "ALTER", "TRUNCATE", "USE", "CACHE", "UNCACHE", "REFRESH", "DELETE",
            "UPDATE", "MERGE", "CLEAR", "ADD", "MSCK", "ANALYZE")


def is_command(sql):
    words = sql.lstrip("( ").split(None, 1)
    return bool(words) and words[0].upper() in COMMANDS


def run_file(path, verbose, keyword, dialect="spark"):
    golden_path = ROOT / "golden" / (path.name + ".pkl")
    golden = pickle.loads(golden_path.read_bytes())
    duck = DuckRunner(dialect=dialect, timeout=CASE_TIMEOUT, extension=EXTENSION)
    # file-level `-- @config key=value`: the same setting in the dialect (Spark's SET) as in the golden run
    duck.setup([f"SET {key} = {value}" for key, value in file_config(path)])
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
        r = duck.run(case.sql)
        actual = r.rows if r.ok else None
        duck_error = None if r.ok else r.error
        if r.status in ("crash", "timeout"):
            # counted like an internal error: never acceptable
            duck_error = f"INTERNAL ({r.status}) {r.error}"
        elif r.ok and (case.has("setup") or is_command(case.sql)):
            duck.replay.append(case.sql)  # restored if the worker has to restart
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
    duck.close()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-k", "--keyword")
    parser.add_argument("--dialect", choices=["spark", "spark_strict"], default="spark")
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    files = [Path(f).resolve() for f in args.files] or sorted((ROOT / "cases").glob("*.sql"))
    use_utc()
    # statements that write files (COPY, EXPORT DATABASE, ...) must not write into the repository
    os.chdir(scratch_dir("duck-cases-"))

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
