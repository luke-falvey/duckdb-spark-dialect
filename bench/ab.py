"""A/B comparison of two builds of the extension: the tool that decides whether a change helps.

usage: python bench/ab.py --a /home/user/ext-ab/before --b /home/user/ext-ab/after --suite func --only sum_decimal
       python bench/ab.py --a ... --b ... --suite tpch --sf 1 --queries 1,9,10,13

--a / --b are directories holding a build named spark_dialect.duckdb_extension (DuckDB derives the entry point from
the file name, so the two builds need different directories), or the path of such a file.

Both builds run as long-lived worker processes (two builds of one extension cannot share a process) on the same
data, and every query alternates A, B, A, B, ... so machine drift hits both sides equally. Per query it prints the
median of each side, the ratio B/A with a bootstrap 95% interval and a verdict: faster / slower when the interval
excludes 1.0, otherwise no change. Copy the extensions first (cp build/... /home/user/ext-ab/before.duckdb_extension):
a rebuild replaces the file in place.

Suites: func (bench/func_pairs.toml, spark side, over bench/out/func_tables.duckdb), tpch (bench/tpch/*.sql).
More suites register in SUITES (bench/tpcds_bench.py and bench/workload_bench.py add theirs).
"""
import argparse
import json
import statistics
import subprocess
import sys
import time
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

BENCH = Path(__file__).resolve().parent


def extension_file(path):
    path = Path(path)
    return path / "spark_dialect.duckdb_extension" if path.is_dir() else path


class Worker:
    def __init__(self, ext, db):
        ext = extension_file(ext)
        self.proc = subprocess.Popen([sys.executable, str(BENCH / "_ab_worker.py"), "--ext", str(ext), "--db", str(db)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        ready = self.proc.stdout.readline()
        if not ready:
            raise SystemExit(f"worker for {ext} did not start")

    def run(self, **request):
        self.proc.stdin.write(json.dumps(request) + "\n")
        self.proc.stdin.flush()
        return json.loads(self.proc.stdout.readline())

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=30)


# ---- suites: each returns (database path, threads, [(name, sql, dialect)]) -------------------------------

def suite_func(args):
    import func_bench
    pairs = tomllib.loads(func_bench.PAIRS.read_text())["pair"]
    if args.only:
        wanted = set(args.only.split(","))
        pairs = [p for p in pairs if p["name"] in wanted]
    if not func_bench.DB_PATH.exists():
        raise SystemExit("run `python bench/func_bench.py --make-db` first")
    import duckdb
    con = duckdb.connect(str(func_bench.DB_PATH), read_only=True)
    rows = con.execute("SELECT rows FROM bench_meta").fetchone()[0]
    con.close()
    queries = [(p["name"], func_bench.limit_rows(p, func_bench.wrap(p, "spark"), rows), args.dialect) for p in pairs]
    return func_bench.DB_PATH, args.threads or 1, queries


def suite_tpch(args):
    import tpch_bench
    db = tpch_bench.ensure_data(args.sf)
    nums = [int(q) for q in args.queries.split(",")] if args.queries else range(1, 23)
    queries = [(f"Q{q:02d}", (tpch_bench.QUERIES / f"q{q:02d}.sql").read_text().strip().rstrip(";"), args.dialect)
               for q in nums]
    return db, args.threads or common.os.cpu_count(), queries


SUITES = {"func": suite_func, "tpch": suite_tpch}


def compare(a_ext, b_ext, db, threads, queries, runs, warmup=1, out=print):
    a, b = Worker(a_ext, db), Worker(b_ext, db)
    rows = []
    try:
        for name, sql, dialect in queries:
            request = {"sql": sql, "dialect": dialect, "threads": threads}
            for _ in range(warmup):
                ra, rb = a.run(**request), b.run(**request)
            if ra["error"] or rb["error"]:
                rows.append({"name": name, "error": ra["error"] or rb["error"]})
                out(f"{name:30} error: {ra['error'] or rb['error']}")
                continue
            ta, tb = [], []
            for _ in range(runs):
                ta.append(a.run(**request)["seconds"])
                tb.append(b.run(**request)["seconds"])
            # a noisy item gets more runs, as in common.timeit
            while len(ta) < 3 * runs and max(common.cv(ta), common.cv(tb)) > 0.05:
                ta.append(a.run(**request)["seconds"])
                tb.append(b.run(**request)["seconds"])
            ratio, lo, hi = common.ratio_interval(ta, tb)
            verdict = "faster" if hi < 1.0 else "slower" if lo > 1.0 else "no change"
            row = {"name": name, "a_ms": statistics.median(ta) * 1000, "b_ms": statistics.median(tb) * 1000,
                   "ratio": ratio, "lo": lo, "hi": hi, "verdict": verdict, "runs": len(ta),
                   "cv_a": common.cv(ta), "cv_b": common.cv(tb), "same_result": ra["digest"] == rb["digest"]}
            rows.append(row)
            out(f"{name:30} A {row['a_ms']:9.2f} ms  B {row['b_ms']:9.2f} ms  B/A {ratio:5.3f} [{lo:5.3f}, {hi:5.3f}]"
                f"  {verdict:9}  n={len(ta)} cv={max(row['cv_a'], row['cv_b']) * 100:.1f}%"
                + ("" if row["same_result"] else "  RESULT DIFFERS"))
    finally:
        a.close()
        b.close()
    return rows


def markdown(rows):
    lines = ["| item | A ms | B ms | B/A | 95% interval | verdict |", "|---|---:|---:|---:|---|---|"]
    for r in rows:
        if "error" in r:
            lines.append(f"| {r['name']} | | | | | error: {r['error']} |")
        else:
            lines.append(f"| {r['name']} | {r['a_ms']:.2f} | {r['b_ms']:.2f} | {r['ratio']:.3f} | "
                         f"[{r['lo']:.3f}, {r['hi']:.3f}] | {r['verdict']}{'' if r['same_result'] else ' (RESULT DIFFERS)'} |")
    ok = [r["ratio"] for r in rows if "ratio" in r]
    if ok:
        lines.append(f"\ngeometric mean B/A: {common.geomean(ok):.3f} over {len(ok)} items")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="extension build A (before)")
    ap.add_argument("--b", required=True, help="extension build B (after)")
    ap.add_argument("--suite", default="func", choices=sorted(SUITES))
    ap.add_argument("--only", help="func: comma-separated pair names")
    ap.add_argument("--queries", help="tpch: comma-separated query numbers")
    ap.add_argument("--sf", default="1")
    ap.add_argument("--runs", type=int, default=9)
    ap.add_argument("--threads", type=int)
    ap.add_argument("--dialect", default="spark")
    ap.add_argument("--md", help="also write the table as Markdown to this file")
    args = ap.parse_args()
    with common.BenchLock():
        db, threads, queries = SUITES[args.suite](args)
        rows = compare(args.a, args.b, db, threads, queries, args.runs)
    print()
    table = markdown(rows)
    print(table)
    if args.md:
        Path(args.md).write_text(table + "\n")
    common.OUT_DIR.mkdir(exist_ok=True)
    (common.OUT_DIR / f"ab-{time.strftime('%Y%m%d-%H%M%S')}.json").write_text(json.dumps(rows, indent=1))
    sys.exit(1 if any("error" in r for r in rows) else 0)


if __name__ == "__main__":
    main()
