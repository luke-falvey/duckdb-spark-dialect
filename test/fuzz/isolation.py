"""Dialect isolation: DuckDB SQL must behave the same with the extension loaded (after a Spark dialect was
selected and reset) as without the extension.

Statements come from DuckDB's own sqllogictest files (duckdb/test/sql/**/*.test in a DuckDB checkout).
Each file runs, statement by statement, in two fresh connections:
  plain     DuckDB without the extension
  loaded    DuckDB with the extension, after SET current_dialect = 'spark'; RESET current_dialect
Results must be identical (status ok/error and rows). Nondeterministic statements are skipped.

usage: python test/fuzz/isolation.py [--duckdb-src /path/to/duckdb] [--budget FILES] [--seed N]
"""
import argparse
import json
import os
import random
import re
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus  # noqa: E402
import triage  # noqa: E402
from engines import DuckRunner, scratch_dir, use_utc  # noqa: E402

NONDETERMINISTIC = re.compile(
    r"\b(random|rand|setseed|uuid|gen_random_uuid|now|current_\w+|get_current\w*|today|epoch_ms\(\s*\)|"
    r"txid_current|version|pragma_version|duckdb_\w+|pragma|which_secret|getenv|sqlsmith|enable_profiling|"
    r"memory_limit|threads|temp_directory|checkpoint|database_size|pragma_database_size|duckdb_memory)\b",
    re.I)
UNSUPPORTED = re.compile(r"\b(EXPLAIN|INSTALL|LOAD|ATTACH|DETACH|COPY|EXPORT|IMPORT|read_\w+|glob|parquet_\w+|"
                         r"\w+_scan|__TEST_DIR__|\$\{|\{[a-z_]+\})", re.I)

# functions the extension defines in its own schema: DuckDB's versions must win in the default dialect
SHADOW_CHECKS = ["SELECT add(1, 2)", "SELECT concat('a', NULL)", "SELECT length('é日本')", "SELECT size([1, 2])",
                 "SELECT cast_value(1, 2)", "SELECT typeof(1::HUGEINT)", "SELECT array_sort([3, 1, 2])",
                 "SELECT split('a,b', ',')", "SELECT substr('abc', 0, 1)", "SELECT '1' + 1", "SELECT 7 // 2",
                 "SELECT 1 / 0", "SELECT [1, 2][1]", "SELECT {'a': 1}.a", "SELECT 2147483647::INT + 1::INT",
                 "SELECT 'a' || NULL", "SELECT hash('a')", "SELECT format('{}', 1)", "SELECT 1 <=> NULL"]


def parse_test_file(path):
    """Yield SQL statements (query / statement ok|error blocks) from a sqllogictest file."""
    lines = path.read_text(errors="replace").splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith(("require ", "loop ", "foreach ", "concurrentloop", "mode skip", "load ")):
            if line.startswith(("require ", "load ")):
                return  # needs an extension or a database file
            if line.startswith(("loop", "foreach", "concurrentloop")):
                return  # templated statements
        if line.startswith(("statement ", "query ")):
            i += 1
            sql = []
            while i < len(lines) and lines[i].strip() and lines[i].strip() != "----":
                sql.append(lines[i])
                i += 1
            # skip expected results
            if i < len(lines) and lines[i].strip() == "----":
                while i < len(lines) and lines[i].strip():
                    i += 1
            text = "\n".join(sql).strip()
            if text:
                yield text
        i += 1


def same(a, b):
    if a.status != b.status:
        return False
    if a.status != "ok":
        return True
    return a.rows == b.rows or sorted(map(repr, a.rows)) == sorted(map(repr, b.rows))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb-src", default=os.environ.get("DUCKDB_SRC", "/home/user/duckdb"))
    ap.add_argument("--budget", type=int, default=300, help="number of test files")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    use_utc()
    out_dir = Path(args.out or (corpus.FUZZ_DIR / "out" / (time.strftime("%Y%m%d-%H%M%S") + f"-isolation-{args.seed}"))).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    duckdb_src = Path(args.duckdb_src).resolve()
    os.chdir(scratch_dir("isolation-"))

    files = sorted((duckdb_src / "test" / "sql").rglob("*.test"))
    rng = random.Random(args.seed)
    rng.shuffle(files)
    findings, counts = [], Counter()

    def check(plain, loaded, sql, label):
        a = plain.run(sql, timeout=10, want_rows=True)
        b = loaded.run(sql, timeout=10, want_rows=True)
        counts["statements"] += 1
        if a.status in ("crash", "internal", "timeout") or b.status in ("crash", "internal", "timeout"):
            counts["skipped_unstable"] += 1 if a.status == b.status else 0
        if not same(a, b):
            rec = {"kind": "isolation", "dialect": "duckdb", "sql": sql, "file": label, "seed": args.seed,
                   "plain": {"status": a.status, "rows": repr(a.rows)[:300], "error": a.error},
                   "duck": {"status": b.status, "rows": repr(b.rows)[:300], "error": b.error}}
            rec["signature"] = f"isolation|{a.status}/{b.status}|{triage.mask_message(b.error or a.error)[:80]}"
            findings.append(rec)
            with open(out_dir / "findings.jsonl", "a") as f:
                f.write(json.dumps(rec) + "\n")

    def connections():
        plain = DuckRunner(dialect=None, load_extension=False, timeout=10)
        loaded = DuckRunner(dialect=None, load_extension=True, timeout=10)
        for sql in ("SET threads = 1",):
            plain.setup([sql])
            loaded.setup([sql])
        loaded.setup(["SET current_dialect = 'spark'", "RESET current_dialect"])
        return plain, loaded

    plain, loaded = connections()
    for sql in SHADOW_CHECKS:
        check(plain, loaded, sql, "shadow-checks")
    plain.close()
    loaded.close()

    used = 0
    for path in files:
        if used >= args.budget:
            break
        stmts = [s for s in parse_test_file(path) if not NONDETERMINISTIC.search(s) and not UNSUPPORTED.search(s)]
        if not stmts:
            continue
        used += 1
        counts["files"] += 1
        plain, loaded = connections()
        label = str(path.relative_to(duckdb_src))
        for sql in stmts[:200]:
            check(plain, loaded, sql, label)
        plain.close()
        loaded.close()
    summary = triage.summarize(findings, counts, title=f"isolation seed={args.seed} files={used}")
    (out_dir / "summary.md").write_text(summary)
    print("\n".join(summary.splitlines()[:50]))
    sys.exit(1 if findings else 0)


if __name__ == "__main__":
    main()
