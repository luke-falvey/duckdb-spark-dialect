"""Differential fuzzer: random Spark SQL (gen.py) in Spark 3.5, `spark` and `spark_strict`.

usage: python test/fuzz/differential.py [--seed N] [--budget N] [--depth D] [--shapes a,b] [--no-reduce]
       python test/fuzz/differential.py --sql-file queries.sql   (run given statements instead)

Exit status 1 if there is a failing finding (see compare.FAILING) that is not in known_differences.toml.
"""
import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus  # noqa: E402
import compare  # noqa: E402
import gen  # noqa: E402
import reduce as reducer  # noqa: E402
import triage  # noqa: E402
from engines import DuckRunner, SparkOracle, scratch_dir, use_utc  # noqa: E402
from harness import Case  # noqa: E402


def is_ordered(sql):
    return Case("gen", 0, sql, {}).ordered


class Differential:
    def __init__(self, timeout=20.0, strict=True, setup=None):
        self.spark = SparkOracle()
        self.lenient = DuckRunner(dialect="spark", timeout=timeout)
        self.strict = DuckRunner(dialect="spark_strict", timeout=timeout) if strict else None
        self.setup_sql = setup if setup is not None else gen.setup_statements()
        self.spark.setup(self.setup_sql)
        self.lenient.setup(self.setup_sql)
        if self.strict:
            self.strict.setup(self.setup_sql)

    def run(self, sql):
        s = self.spark.run(sql)
        d = self.lenient.run(sql)
        t = self.strict.run(sql) if self.strict else None
        return s, d, t, compare.classify(s, d, t, is_ordered(sql))

    def close(self):
        self.lenient.close()
        if self.strict:
            self.strict.close()

    def record(self, kind, dialect, diff, sql, s, d, t, **extra):
        duck = d if dialect == "spark" else t
        rec = {"kind": kind, "dialect": dialect, "sql": sql, "difference": diff, "setup": "gen.setup_statements()",
               "spark": {"ok": s.ok, "rows": repr(s.rows)[:800], "error": s.error, "phase": s.phase},
               "duck": {"status": duck.status, "rows": repr(duck.rows)[:800], "error": duck.error}, **extra}
        rec["signature"] = triage.signature(rec)
        return rec

    def reduce(self, rec, max_steps=200):
        kind, dialect = rec["kind"], rec["dialect"]
        # the same failure: same kind and dialect, and the same (masked) error or kind of difference
        want_error = triage.mask_message(rec["duck"]["error"]) if kind in ("duck_error", "internal") else None
        want_diff = rec.get("difference")

        def still(cand):
            s, d, t, found = self.run(cand)
            duck = d if dialect == "spark" else t
            for k, dd, diff in found:
                if (k, dd) != (kind, dialect):
                    continue
                if want_error is not None and triage.mask_message(duck.error) != want_error:
                    continue
                if want_diff is not None and diff != want_diff:
                    continue
                return True
            return False
        out, steps = reducer.reduce(rec["sql"], still, max_steps)
        rec["reduced_sql"] = out
        rec["reduce_steps"] = steps
        s, d, t, _ = self.run(out)
        duck = d if dialect == "spark" else t
        rec["reduced"] = {"spark": repr(s.rows)[:300] if s.ok else s.error,
                          "duck": repr(duck.rows)[:300] if duck.ok else duck.error}
        rec["signature"] = triage.signature(rec)
        return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--budget", type=int, default=2000)
    ap.add_argument("--depth", type=int, default=3)
    ap.add_argument("--shapes", default=None)
    ap.add_argument("--no-strict", action="store_true")
    ap.add_argument("--no-reduce", action="store_true")
    ap.add_argument("--reduce-limit", type=int, default=40, help="reduce at most this many new signatures")
    ap.add_argument("--sql-file", help="statements (one per blank-line separated block) to run instead")
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    if args.seed is None:
        args.seed = random.randrange(1 << 30)
    use_utc()
    run_id = time.strftime("%Y%m%d-%H%M%S") + f"-diff-{args.seed}"
    out_dir = Path(args.out or (corpus.FUZZ_DIR / "out" / run_id)).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(scratch_dir("diff-"))
    print(f"differential: seed={args.seed} budget={args.budget} depth={args.depth} out={out_dir}", flush=True)

    if args.sql_file:
        from harness import load_cases
        queries = [("file", c.sql) for c in load_cases([Path(args.sql_file).resolve()]) if not c.has("setup")]
    else:
        shapes = set(args.shapes.split(",")) if args.shapes else None
        queries = list(gen.generate(args.seed, args.budget, args.depth, shapes))

    diff = Differential(args.timeout, strict=not args.no_strict)
    known = compare.load_known()
    counts = Counter()
    findings = []
    reduced_sigs = set()
    start = time.time()
    for i, (shape, sql) in enumerate(queries):
        s, d, t, found = diff.run(sql)
        counts["queries"] += 1
        counts["spark_ok" if s.ok else "spark_error"] += 1
        counts[f"shape:{shape}"] += 1
        if not found:
            counts["pass"] += 1
        for kind, dialect, difference in found:
            rec = diff.record(kind, dialect, difference, sql, s, d, t, seed=args.seed, n=i, shape=shape,
                              generator=gen.GENERATOR)
            pre = rec["signature"]
            if (not args.no_reduce and kind in compare.FAILING and kind not in ("crash", "timeout")
                    and pre not in reduced_sigs and len(reduced_sigs) < args.reduce_limit):
                reduced_sigs.add(pre)
                rec = diff.reduce(rec)
            k = compare.is_known(rec["signature"], known, rec["sql"])
            rec["known"] = bool(k)
            counts[("known:" if k else "") + kind] += 1
            findings.append(rec)
            with open(out_dir / "findings.jsonl", "a") as f:
                f.write(json.dumps(rec) + "\n")
        if i % 200 == 199:
            el = time.time() - start
            print(f"  {i + 1}/{len(queries)} ({(i + 1) / el:.1f}/s) spark_ok={counts['spark_ok']} "
                  f"findings={sum(1 for f in findings if f['kind'] in compare.FAILING and not f['known'])}", flush=True)
    diff.close()
    ok_rate = counts["spark_ok"] / max(1, counts["queries"])
    summary = triage.summarize(findings, counts, title=f"differential seed={args.seed} budget={args.budget} "
                               f"(Spark accepted {ok_rate:.0%})")
    (out_dir / "summary.md").write_text(summary)
    print("\n".join(summary.splitlines()[:60]))
    failing = [f for f in findings if f["kind"] in compare.FAILING and not f["known"]]
    print(f"failing findings: {len(failing)} in {len({f['signature'] for f in failing})} signatures; "
          f"Spark accepted {ok_rate:.0%} of generated queries")
    sys.exit(1 if failing else 0)


if __name__ == "__main__":
    main()
