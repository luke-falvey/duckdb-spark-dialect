"""Spark's own SQL test inputs (sql/core/src/test/resources/sql-tests/inputs at v3.5.9) as a finding source.

  fetch    sparse-checkout the inputs into test/fuzz/out/spark-sql-tests (not committed)
  seeds    write all statements to a file usable as `mutate.py --extra-seeds`
  run      run every input file differentially: each file in a fresh Spark session and fresh DuckDB workers
           (the files create their own tables), statements in order; findings are grouped by file

This corpus covers Spark's edge cases but also features out of scope (ANSI mode, Hive, UDFs, config
variants), so it is a finding source, not an acceptance gate. Directories and files listed in SKIP are not run.

usage: python test/fuzz/spark_corpus.py fetch
       python test/fuzz/spark_corpus.py seeds --out seeds.sql
       python test/fuzz/spark_corpus.py run [--files N] [--match substring]
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus  # noqa: E402
import compare  # noqa: E402
import triage  # noqa: E402
from engines import DuckRunner, SparkOracle, scratch_dir, use_utc  # noqa: E402

TAG = "v3.5.9"
CACHE = corpus.FUZZ_DIR / "out" / "spark-sql-tests"
INPUTS = CACHE / "sql" / "core" / "src" / "test" / "resources" / "sql-tests" / "inputs"
# out of scope for a non-ANSI, non-Hive dialect
SKIP = ("ansi/", "udf/", "udaf/", "typeCoercion/", "postgreSQL/", "udtf", "hive", "thriftserver", "timestampNTZ/",
        "nonansi/", "subquery/in-subquery/", "pipe", "collation", "variant", "sql-session-variables",
        "identifier-clause", "show-", "describe", "change-column", "charvarchar", "cte-legacy", "cte-nonlegacy",
        "explain", "mode-", "try_arithmetic", "datetime-legacy", "parse-schema-string", "transform")


def fetch():
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    if not (CACHE / ".git").exists():
        subprocess.run(["git", "clone", "--depth", "1", "--branch", TAG, "--filter=blob:none", "--sparse",
                        "https://github.com/apache/spark.git", str(CACHE)], check=True)
    subprocess.run(["git", "-C", str(CACHE), "sparse-checkout", "set", "sql/core/src/test/resources/sql-tests/inputs"],
                   check=True)
    sha = subprocess.run(["git", "-C", str(CACHE), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    print(f"Spark {TAG} ({sha}): {sum(1 for _ in INPUTS.rglob('*.sql'))} input files in {INPUTS}")


def statements(path):
    """Split a Spark SQL test file the way SQLQueryTestSuite does: ';' at the end of a line ends a statement;
    '--' comment lines (including --IMPORT / --SET / --CONFIG_DIM directives) are dropped."""
    text = path.read_text(errors="replace")
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    out, cur = [], []
    for ln in lines:
        cur.append(ln)
        if ln.rstrip().endswith(";"):
            stmt = "\n".join(cur).strip().rstrip(";").strip()
            if stmt:
                out.append(stmt)
            cur = []
    rest = "\n".join(cur).strip()
    if rest:
        out.append(rest)
    return out


def files(match=None):
    out = []
    for p in sorted(INPUTS.rglob("*.sql")):
        rel = str(p.relative_to(INPUTS))
        if any(s in rel for s in SKIP) or (match and match not in rel):
            continue
        out.append(p)
    return out


def run(args):
    use_utc()
    out_dir = Path(args.out or (corpus.FUZZ_DIR / "out" / (time.strftime("%Y%m%d-%H%M%S") + "-spark-corpus"))).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(scratch_dir("spark-corpus-"))
    known = compare.load_known()
    spark = SparkOracle()
    findings, counts = [], Counter()
    paths = files(args.match)[: args.files]
    for n, path in enumerate(paths):
        rel = str(path.relative_to(INPUTS))
        spark.reset()
        lenient = DuckRunner(dialect="spark", timeout=args.timeout)
        strict = DuckRunner(dialect="spark_strict", timeout=args.timeout)
        for sql in statements(path):
            if re.match(r"\s*(SET|RESET)\b", sql, re.I):
                continue  # configuration variants (ANSI, legacy flags) are out of scope
            s, d, t = spark.run(sql), lenient.run(sql), strict.run(sql)
            counts["statements"] += 1
            counts["spark_ok" if s.ok else "spark_error"] += 1
            from harness import Case
            for kind, dialect, diff in compare.classify(s, d, t, Case("x", 0, sql, {}).ordered):
                duck = d if dialect == "spark" else t
                rec = {"kind": kind, "dialect": dialect, "sql": sql, "file": rel, "difference": diff,
                       "spark": {"ok": s.ok, "rows": repr(s.rows)[:500], "error": s.error, "phase": s.phase},
                       "duck": {"status": duck.status, "rows": repr(duck.rows)[:500], "error": duck.error}}
                rec["signature"] = triage.signature(rec)
                rec["known"] = bool(compare.is_known(rec["signature"], known, sql))
                counts[("known:" if rec["known"] else "") + kind] += 1
                findings.append(rec)
                with open(out_dir / "findings.jsonl", "a") as f:
                    f.write(json.dumps(rec) + "\n")
        lenient.close()
        strict.close()
        print(f"  [{n + 1}/{len(paths)}] {rel}: findings so far {len(findings)}", flush=True)
    by_file = Counter(f["file"] for f in findings if f["kind"] in compare.FAILING and not f["known"])
    summary = triage.summarize(findings, counts, title=f"Spark {TAG} sql-tests corpus ({len(paths)} files)")
    summary += "\n## Failing findings by input file\n\n" + "\n".join(f"- {k}: {v}" for k, v in by_file.most_common()) + "\n"
    (out_dir / "summary.md").write_text(summary)
    print("\n".join(summary.splitlines()[:40]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["fetch", "seeds", "run"])
    ap.add_argument("--out")
    ap.add_argument("--files", type=int, default=None)
    ap.add_argument("--match")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()
    if args.command == "fetch":
        fetch()
    elif args.command == "seeds":
        stmts = [s for p in files() for s in statements(p)]
        Path(args.out).write_text(";\n".join(stmts) + ";\n")
        print(f"wrote {len(stmts)} statements to {args.out}")
    else:
        run(args)


if __name__ == "__main__":
    main()
