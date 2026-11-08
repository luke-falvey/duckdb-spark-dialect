"""Re-run crash / hang / internal-error regressions (and, with --findings, a fuzz run's findings).

Every statement in test/fuzz/crash_regressions.sql must finish in every dialect without a crash,
an INTERNAL error or a timeout. Ordinary errors are fine. Directives (see test/harness.py):
  -- @dialect spark,spark_strict   only these dialects (default: spark, spark_strict, duckdb)
  -- @timeout 30                   seconds (default 10)
  -- @upstream <reason>            a DuckDB bug that also happens without the extension: skipped

With --known, instead check known_differences.toml: the `example` of every entry is run in Spark and in both
dialects and must still differ from Spark; an entry whose example now matches is stale and fails.

usage: python test/fuzz/replay.py [--findings test/fuzz/out/<run>] [--shell asan/duckdb] [--known]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus  # noqa: E402,F401
from engines import DuckRunner, ShellRunner, use_utc  # noqa: E402
from harness import load_cases  # noqa: E402
import compare  # noqa: E402
import triage  # noqa: E402

REGRESSIONS = Path(__file__).resolve().parent / "crash_regressions.sql"
ALL = ("spark", "spark_strict", "duckdb")


def check_known():
    from engines import SparkOracle
    spark = SparkOracle()
    lenient = DuckRunner(dialect="spark", timeout=30.0)
    strict = DuckRunner(dialect="spark_strict", timeout=30.0)
    stale = 0
    entries = compare.load_known()
    for entry in entries:
        sql = entry.get("example")
        if not sql:
            stale += 1
            print(f"FAIL entry without an example: {entry['signature']} {entry['reason'][:60]!r}")
            continue
        findings = compare.classify(spark.run(sql), lenient.run(sql), strict.run(sql), ordered=False)
        if not findings:
            stale += 1
            print(f"STALE example no longer differs from Spark: {sql}")
    for r in (lenient, strict):
        r.close()
    spark.stop()
    print(f"known differences: {len(entries)} entries, {stale} stale")
    return 1 if stale else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--findings", help="a fuzz output directory: replay its crash/internal/timeout findings")
    ap.add_argument("--shell", help="also replay through this (sanitizer) duckdb shell")
    ap.add_argument("--shell-extension", help="the extension built with the same sanitizer flags as --shell")
    ap.add_argument("--known", action="store_true", help="check that known_differences.toml has no stale entry")
    args = ap.parse_args()
    use_utc()
    if args.known:
        sys.exit(check_known())

    items = []  # (label, sql, dialects, timeout)
    for case in load_cases([REGRESSIONS]):
        if case.has("upstream"):
            continue
        dialects = tuple(case.directives.get("dialect", ",".join(ALL)).replace(" ", "").split(","))
        items.append((case.id, case.sql, dialects, float(case.directives.get("timeout", 10))))
    if args.findings:
        for i, f in enumerate(triage.load_findings(args.findings)):
            if f["kind"] in ("crash", "internal", "timeout"):
                items.append((f"finding {i} [{f['signature']}]", f["sql"], (f["dialect"],), 10.0))

    runners = {}
    failed = 0
    for label, sql, dialects, timeout in items:
        for d in dialects:
            if d not in runners:
                runners[d] = DuckRunner(dialect=None if d == "duckdb" else d, timeout=timeout)
            r = runners[d].run(sql, timeout=timeout, want_rows=False)
            bad = r.status in ("crash", "internal", "timeout")
            if bad:
                failed += 1
                print(f"FAIL {label} [{d}] {r.status}: {r.error}\n  {sql[:300]}")
            if args.shell:
                s, _, detail = ShellRunner(args.shell, dialect=None if d == "duckdb" else d,
                                           timeout=timeout * 10, extension=args.shell_extension).run_batch([sql])
                if s != "ok":
                    failed += 1
                    print(f"FAIL {label} [{d}, shell] {s}: {(detail or '')[:500]}\n  {sql[:300]}")
    for r in runners.values():
        r.close()
    print(f"replay: {len(items)} statements, {failed} failures")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
