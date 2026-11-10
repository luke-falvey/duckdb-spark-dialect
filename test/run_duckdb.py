"""Run every case in DuckDB (spark dialect) and compare with the Spark 3.5 golden results.

usage: python test/run_duckdb.py [-v] [-k substring] [--dialect spark|spark_strict] [--config key=value]
                                 [--duck-set key=value] [files...]

Outcome per case:
  PASS      same rows, column types and names (or both engines raise an error in the same phase, or
            nondeterministic and both succeed)
  LENIENT   Spark raises an error, DuckDB returns a result (DuckDB accepts more than Spark)
  PHASE     both raise an error, but at different phases (Spark analysis vs DuckDB execution or the reverse);
            counted, not failing, in `spark`; FAIL in `spark_strict`
  FAIL      DuckDB raises an error where Spark succeeds, the rows / column types / names differ, or an error
            phase / gated error class differs in `spark_strict`
  XFAIL     a FAIL that is expected: `-- @xfail <id> <reason>` or an entry of test/xfail.toml
  XPASS     an expected failure that passed: fails the run, remove the marker
  SKIP      `-- @skip`

--config key=value   run every file under an extra Spark setting (golden files in test/golden/<slug>/); repeatable.
--duck-set key=value set a DuckDB setting in the dialect process only (the Spark golden is unchanged); used to check
                     that DuckDB's own settings do not leak into the Spark dialects; repeatable.
Files with `-- @matrix` lines run once per combination of the values.
"""
import argparse
import fnmatch
import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

from engines import DuckRunner, extension_path, scratch_dir, use_utc  # noqa: E402
import harness  # noqa: E402
from harness import (ROOT, apply_xfail, config_slug, file_directives, is_command, judge, load_cases,  # noqa: E402
                     load_golden, load_xfail, matrix_combinations)

EXTENSION = extension_path()
CASE_TIMEOUT = 60.0
KINDS = ("PASS", "LENIENT", "PHASE", "FAIL", "XFAIL", "XPASS", "SKIP")


def parse_pairs(items):
    pairs = []
    for item in items or []:
        key, _, value = item.partition("=")
        pairs.append((key.strip(), value.strip()))
    return tuple(pairs)


def run_file(path, verbose, keyword, dialect="spark", extra=(), duck_sets=(), xfail=()):
    """Run one case file under one configuration; returns [(case, kind, detail, notes)]."""
    directives = file_directives(path)
    golden = load_golden(path, extra)
    slug = config_slug(extra)
    duck = DuckRunner(dialect=dialect, timeout=CASE_TIMEOUT, extension=EXTENSION)
    # file-level `-- @config key=value`, matrix values and --config: the same setting in the dialect (Spark's SET)
    # as in the golden run
    conf_error = None
    for key, value in [*directives["config"], *extra, *duck_sets]:
        r = duck.run(f"SET {key} = {value}")
        if r.ok:
            duck.replay.append(f"SET {key} = {value}")
        elif conf_error is None:
            # the dialect rejects the setting (plan 9.1): every case of this configuration reports that error, so
            # that test/xfail.toml can expect it (`conf-rejected`)
            conf_error = r
    results = []
    for case in load_cases([path]):
        expected = golden.get(case.index)
        if expected is None or expected["sql"] != case.sql or expected.get("sets", []) != case.sets:
            raise SystemExit(f"golden file out of date for {case.id} {slug}; run make_golden.py")
        if keyword and keyword not in case.sql and not case.has("setup"):
            continue
        if case.has("skip"):
            results.append((case, "SKIP", case.directives["skip"], {}))
            continue
        # per-case settings: `SET key = value` before, `RESET key` after (also tests the dialect's RESET)
        set_error = None
        for key, value in case.sets:
            r = duck.run(f"SET {key} = {value}")
            if not r.ok:
                set_error = r
                break
        if conf_error is not None:
            r = conf_error
            r.extra = dict(r.extra or {}, stage="bind")
        elif set_error is not None:
            r = set_error
            r.extra = dict(r.extra or {}, stage="bind")
        else:
            r = duck.run(case.sql)
        for key, _ in case.sets:
            duck.run(f"RESET {key}")
        if r.ok and (case.has("setup") or is_command(case.sql)) and not case.sets:
            duck.replay.append(case.sql)  # restored if the worker has to restart
        if case.has("setup"):
            if not r.ok and conf_error is None:
                outcome = apply_xfail(case, slug, dialect, harness.Outcome("FAIL", f"setup failed: {r.error}"), xfail)
                results.append((case, outcome.kind, outcome.detail, outcome.notes))
            continue
        zone = None
        for key, value in [*directives["config"], *extra, *case.sets]:
            if key == "spark.sql.session.timeZone":
                zone = value
        outcome = judge(case.sql, case.ordered, case.directives, expected, r, dialect, case.file,
                        exact=directives["exact"], session_zone=zone)
        outcome = apply_xfail(case, slug, dialect, outcome, xfail)
        results.append((case, outcome.kind, outcome.detail, outcome.notes))
        if verbose and outcome.kind not in ("PASS", "LENIENT", "PHASE"):
            print(f"{outcome.kind} {case.id}{'[' + slug + ']' if slug else ''}: {case.sql[:200]}\n      {outcome.detail}")
    duck.close()
    return results


def configurations(path, cli_extra):
    """The configurations a file runs under: its matrix combinations on top of the command line settings."""
    directives = file_directives(path)
    return [combo + cli_extra for combo in matrix_combinations(directives["matrix"])]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-k", "--keyword")
    parser.add_argument("--dialect", choices=["spark", "spark_strict"], default="spark")
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--duck-set", action="append", default=[])
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    files = [Path(f).resolve() for f in args.files] or sorted((ROOT / "cases").rglob("*.sql"))
    cli_extra = parse_pairs(args.config)
    duck_sets = parse_pairs(args.duck_set)
    use_utc()
    # statements that write files (COPY, EXPORT DATABASE, ...) must not write into the repository
    os.chdir(scratch_dir("duck-cases-"))

    xfail = load_xfail()
    total = Counter()
    by_function = defaultdict(Counter)
    failures = []
    summary_lines = []
    records = []
    for path in files:
        for extra in configurations(path, cli_extra):
            results = run_file(path, args.verbose, args.keyword, args.dialect, extra, duck_sets, xfail)
            counts = Counter(r[1] for r in results)
            total.update(counts)
            label = harness.case_file_name(path) + (f"[{config_slug(extra)}]" if extra else "")
            line = f"{label:44} " + "  ".join(f"{k}={counts[k]}" for k in KINDS if k != "SKIP" or counts[k])
            print(line)
            summary_lines.append(line)
            for case, outcome, detail, notes in results:
                if "function" in case.directives:
                    by_function[case.directives["function"]][outcome] += 1
                records.append({"file": case.file, "config": config_slug(extra), "index": case.index,
                                "outcome": outcome, "xfail": notes.get("xfail"), "names": notes.get("names"),
                                "class": notes.get("class")})
                if outcome in ("FAIL", "LENIENT", "SKIP", "XPASS", "PHASE", "XFAIL"):
                    failures.append(f"{outcome} {case.id} {config_slug(extra)}\n  sql: {case.sql}\n  {detail}\n")
    total_line = f"{'TOTAL':44} " + "  ".join(f"{k}={total[k]}" for k in KINDS if k != "SKIP" or total[k])
    print(total_line)
    summary_lines.append(total_line)
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    suffix = "" if args.dialect == "spark" else "_" + args.dialect
    (out / f"failures{suffix}.txt").write_text("\n".join(failures))
    (out / f"cases{suffix}.json").write_text(json.dumps(records))
    if not args.files and not args.keyword and not cli_extra and not duck_sets:
        (out / f"summary{suffix}.txt").write_text("\n".join(summary_lines) + "\n")
    if by_function:
        (out / "functions.json").write_text(json.dumps(by_function, indent=1, sort_keys=True))
        bad = sorted(f for f, c in by_function.items() if c["FAIL"])
        print(f"functions with a failing example: {len(bad)} of {len(by_function)}")
    # an xfail entry that matched no failing case is stale (only judged for files that were run, never with -k)
    stale = []
    run_names = {harness.case_file_name(p) for p in files}
    for entry in xfail:
        if args.keyword or args.dialect not in entry.dialects:
            continue
        if any(fnmatch.fnmatchcase(n, entry.file) for n in run_names) and not getattr(entry, "fails", 0):
            stale.append(entry)
    for entry in stale:
        print(f"STALE xfail entry (matched no failing case): id={entry.id} file={entry.file} reason={entry.reason}")
    sys.exit(1 if total["FAIL"] or total["XPASS"] or stale else 0)


if __name__ == "__main__":
    main()
