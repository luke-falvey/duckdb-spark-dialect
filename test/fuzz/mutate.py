"""Mutation fuzzer: crash / hang / internal-error oracle, DuckDB only.

Takes every statement in test/cases/*.sql (and optionally extra seed files), mutates it at the token,
character or structure level, and runs it in each dialect. Ordinary errors are expected; a crash,
a timeout, or an INTERNAL error is a finding.

usage: python test/fuzz/mutate.py [--seed N] [--budget N] [--dialects spark,spark_strict,duckdb]
                                  [--jobs N] [--shell path/to/asan/duckdb] [--extra-seeds file.sql ...]
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import re
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import corpus  # noqa: E402
from corpus import lex, unlex  # noqa: E402
from engines import DuckRunner, ShellRunner, use_utc  # noqa: E402
import triage  # noqa: E402

GENERATOR = "mutate@1"

# keep fuzzed statements from reaching outside the process (downloads, files) or reconfiguring it
WORKER_PRELUDE = ["SET autoinstall_known_extensions = false", "SET autoload_known_extensions = false",
                  "SET threads = 1"]


# ---- mutators ------------------------------------------------------------------------------------

class Mutator:
    def __init__(self, rng, seeds):
        self.rng = rng
        self.seeds = seeds
        self.vocab = corpus.vocabulary()

    def random_token(self):
        r = self.rng.random()
        v = self.vocab
        if r < 0.45:
            return ("word", self.rng.choice(v["words"]))
        if r < 0.65:
            return ("word", self.rng.choice(v["functions"]))
        if r < 0.85:
            return ("op", self.rng.choice(v["punct"]))
        return ("literal", self.rng.choice(v["literals"]))

    # token level
    def t_delete(self, toks):
        if toks:
            del toks[self.rng.randrange(len(toks))]
        return toks

    def t_duplicate(self, toks):
        if toks:
            i = self.rng.randrange(len(toks))
            toks.insert(i, toks[i])
        return toks

    def t_swap(self, toks):
        if len(toks) > 1:
            i = self.rng.randrange(len(toks) - 1)
            toks[i], toks[i + 1] = toks[i + 1], toks[i]
        return toks

    def t_insert(self, toks):
        toks.insert(self.rng.randrange(len(toks) + 1), self.random_token())
        return toks

    def t_replace(self, toks):
        if toks:
            toks[self.rng.randrange(len(toks))] = self.random_token()
        return toks

    def t_truncate(self, toks):
        if len(toks) > 1:
            del toks[self.rng.randrange(1, len(toks)):]
        return toks

    def t_literal(self, toks):
        idx = [i for i, (k, _) in enumerate(toks) if k in ("number", "string")]
        if idx:
            toks[self.rng.choice(idx)] = ("literal", self.rng.choice(self.vocab["literals"]))
        return toks

    def t_splice(self, toks):
        other = lex(self.rng.choice(self.seeds))
        if not other:
            return toks
        a = self.rng.randrange(len(other))
        b = min(len(other), a + self.rng.randint(1, 8))
        i = self.rng.randrange(len(toks) + 1)
        j = min(len(toks), i + self.rng.randint(0, 4))
        toks[i:j] = other[a:b]
        return toks

    def t_unbalance(self, toks):
        # drop a closing bracket, or add an opening one
        closers = [i for i, (_, t) in enumerate(toks) if t in (")", "]", ">")]
        if closers and self.rng.random() < 0.5:
            del toks[self.rng.choice(closers)]
        else:
            toks.insert(self.rng.randrange(len(toks) + 1), ("op", self.rng.choice(["(", "[", "<"])))
        return toks

    TOKEN_MUTATORS = [("t_delete", 3), ("t_duplicate", 2), ("t_swap", 2), ("t_insert", 4), ("t_replace", 4),
                      ("t_truncate", 3), ("t_literal", 2), ("t_splice", 2), ("t_unbalance", 2)]

    # character level
    def c_mutate(self, sql):
        rng = self.rng
        choice = rng.randrange(9)
        pos = rng.randrange(len(sql) + 1)
        if choice == 0:
            return sql[:pos] + rng.choice(["'", '"', "`", "\\", "r'", "X'", "X'A", "/*", "--", "$$"]) + sql[pos:]
        if choice == 1:
            return sql[:pos] + rng.choice(["\x00", "\x01", "\x7f", "﻿", " ", "\U0001F600", "́"]) + sql[pos:]
        if choice == 2:
            # invalid UTF-8 (surrogate escapes become raw bytes in the shell runner; Python gets U+FFFD)
            return sql[:pos] + rng.choice(["\udcff", "\udcc0\udc80", "\udced\udca0\udc80"]) + sql[pos:]
        if choice == 3:
            return sql + rng.choice([" '", " \\", " /*", " --", " r'", " `"])
        if choice == 4:
            return sql[:pos] + "'" + "a" * rng.choice([1000, 100000, 1000000]) + "'" + sql[pos:]
        if choice == 5:
            return sql[:pos] + "9" * rng.choice([20, 40, 400, 10000]) + sql[pos:]
        if choice == 6:
            return sql[:pos] + sql[rng.randrange(len(sql) + 1):]
        if choice == 7:
            return sql.upper() if rng.random() < 0.5 else sql.lower()
        return sql[:pos] + rng.choice([";", ";;", "; ;", ";\n"]) + sql[pos:]

    # structure level
    WRAPS = ["(%s)", "-%s", "NOT %s", "CASE WHEN true THEN %s END", "array(%s)", "struct(%s)",
             "named_struct('a', %s)", "coalesce(%s)", "abs(%s)", "concat(%s, 'x')", "%s + 1", "nvl(%s, 1)",
             "transform(array(%s), x -> x)", "if(true, %s, NULL)", "-(%s)", "~%s", "CAST(%s AS STRING)"]

    def s_nest(self, sql):
        depth = self.rng.choice([10, 100, 1000, 3000, 10000])
        wrap = self.rng.choice(self.WRAPS)
        expr = "1"
        for _ in range(depth):
            expr = wrap % expr
        return f"SELECT {expr}", depth

    def s_long_list(self, sql):
        n = self.rng.choice([1000, 10000, 50000])
        kind = self.rng.randrange(6)
        items = ", ".join(str(i) for i in range(n))
        if kind == 0:
            return f"SELECT 1 IN ({items})", n
        if kind == 1:
            return "SELECT * FROM VALUES " + ", ".join(f"({i})" for i in range(n)), n
        if kind == 2:
            return "SELECT " + ", ".join(f"{i} AS c{i}" for i in range(min(n, 20000))), n
        if kind == 3:
            return f"SELECT concat({', '.join(repr(str(i)) for i in range(n))})", n
        if kind == 4:
            return f"SELECT stack({min(n, 5000)}, {', '.join(str(i) for i in range(min(n, 5000)))})", n
        return "SELECT CASE " + " ".join(f"WHEN id = {i} THEN {i}" for i in range(min(n, 10000))) + " END FROM range(3) t(id)", n

    def s_many_statements(self, sql):
        n = self.rng.choice([10, 100, 1000])
        return ";".join([sql] * n), n

    def mutate(self):
        """Returns (sql, mutator names, size class)."""
        seed = self.rng.choice(self.seeds)
        r = self.rng.random()
        if r < 0.04:
            sql, n = self.rng.choice([self.s_nest, self.s_long_list, self.s_many_statements])(seed)
            return sql, ["structural"], n
        if r < 0.14:
            sql = seed
            names = []
            for _ in range(self.rng.randint(1, 2)):
                sql = self.c_mutate(sql)
                names.append("char")
            return sql, names, 0
        toks = lex(seed)
        names = []
        total = sum(w for _, w in self.TOKEN_MUTATORS)
        for _ in range(self.rng.choice([1, 1, 1, 2, 2, 3])):
            pick = self.rng.uniform(0, total)
            for name, w in self.TOKEN_MUTATORS:
                pick -= w
                if pick <= 0:
                    break
            toks = getattr(self, name)(toks)
            names.append(name)
        return unlex(toks), names, 0


# ---- running -------------------------------------------------------------------------------------

def make_runner(dialect, timeout):
    runner = DuckRunner(dialect=None if dialect == "duckdb" else dialect, timeout=timeout)
    for sql in WORKER_PRELUDE:
        # the prelude runs before the dialect switch would be ideal, but DuckDB SET also works in the
        # spark dialects (as a DuckDB setting in `spark`; stored as a Spark conf in `spark_strict`)
        r = runner.run(sql)
        if r.ok:
            runner.replay.append(sql)
    return runner


def load_setups(runner, setups):
    for sql in setups:
        r = runner.run(sql)
        if r.ok:
            runner.replay.append(sql)


def fuzz_worker(job, args, seeds, setups, out_dir):
    use_utc()
    rng = random.Random(args.seed * 1000 + job)
    mut = Mutator(rng, seeds)
    dialects = args.dialects.split(",")
    runners = {d: make_runner(d, args.timeout) for d in dialects}
    for r in runners.values():
        load_setups(r, setups)
    findings = []
    counts = Counter()
    per_job = args.budget // args.jobs + (1 if job < args.budget % args.jobs else 0)
    start = time.time()
    for n in range(per_job):
        sql, names, size = mut.mutate()
        if corpus.UNSAFE.search(sql):
            counts["skipped_unsafe"] += 1
            continue
        timeout = args.timeout * (3 if size >= 1000 else 1)
        for d, runner in runners.items():
            r = runner.run(sql, timeout=timeout, want_rows=False)
            counts[f"{d}:{r.status}"] += 1
            if r.status in ("crash", "timeout", "internal"):
                rec = {"kind": r.status, "dialect": d, "sql": sql, "seed": args.seed, "job": job, "n": n,
                       "generator": GENERATOR, "mutators": names, "duck": {"status": r.status, "error": r.error}}
                rec["signature"] = triage.signature(rec)
                findings.append(rec)
                with open(out_dir / f"findings-{job}.jsonl", "a") as f:
                    f.write(json.dumps(rec) + "\n")
        # fuzzed DDL (DROP TABLE emp, ...) erodes the shared state; start over now and then
        if n % 500 == 499:
            for runner in runners.values():
                runner.restart()
        if job == 0 and n % 1000 == 999:
            rate = (n + 1) / (time.time() - start)
            print(f"  job 0: {n + 1}/{per_job} inputs, {rate:.0f}/s, findings so far {len(findings)}", flush=True)
    for runner in runners.values():
        runner.close()
    return findings, counts


def shell_pass(args, seeds, out_dir):
    """Run mutated inputs in batches through a sanitizer-built shell."""
    rng = random.Random(args.seed)
    mut = Mutator(rng, seeds)
    findings = []
    counts = Counter()
    for dialect in args.dialects.split(","):
        shell = ShellRunner(args.shell, dialect=None if dialect == "duckdb" else dialect, timeout=args.timeout * 30,
                            extension=args.shell_extension)
        done = 0
        while done < args.shell_budget:
            batch = []
            while len(batch) < args.batch:
                sql, names, size = mut.mutate()
                if not corpus.UNSAFE.search(sql) and size < 1000:
                    batch.append(sql)
            done += len(batch)
            status, index, detail = shell.run_batch(batch)
            counts[f"shell-{dialect}:{status}"] += 1
            if status == "ok":
                continue
            # confirm on the single statement
            culprit = batch[index] if index is not None and index < len(batch) else None
            if culprit is not None:
                s2, _, d2 = shell.run_batch([culprit])
                if s2 == "ok":
                    culprit, s2, d2 = None, status, detail  # depends on earlier statements
            rec = {"kind": status if status != "sanitizer" else "crash", "dialect": dialect, "runner": "shell",
                   "sql": culprit or ";\n".join(batch[: (index or 0) + 1]), "seed": args.seed,
                   "generator": GENERATOR, "duck": {"status": status, "error": d2 if culprit else detail}}
            rec["signature"] = triage.signature(rec)
            findings.append(rec)
            with open(out_dir / "findings-shell.jsonl", "a") as f:
                f.write(json.dumps(rec) + "\n")
    return findings, counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--budget", type=int, default=20000, help="inputs (each runs in every dialect)")
    ap.add_argument("--dialects", default="spark,spark_strict,duckdb")
    ap.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--extra-seeds", nargs="*", default=[])
    ap.add_argument("--shell", help="sanitizer-built duckdb shell; runs a batched pass through it")
    ap.add_argument("--shell-extension", help="the extension built with the same sanitizer flags as --shell")
    ap.add_argument("--shell-budget", type=int, default=20000)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    if args.seed is None:
        args.seed = random.randrange(1 << 30)
    run_id = time.strftime("%Y%m%d-%H%M%S") + f"-mutate-{args.seed}"
    out_dir = Path(args.out or (corpus.FUZZ_DIR / "out" / run_id))
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"mutate: seed={args.seed} budget={args.budget} dialects={args.dialects} jobs={args.jobs} out={out_dir}")

    setups, seeds = corpus.seed_statements()
    for f in args.extra_seeds:
        seeds += [s for s in re.split(r";\s*\n", Path(f).read_text()) if s.strip()]

    findings, counts = [], Counter()
    if args.budget:
        if args.jobs == 1:
            results = [fuzz_worker(0, args, seeds, setups, out_dir)]
        else:
            # pool workers start their own DuckDB worker processes, so they must not be daemonic
            from concurrent.futures import ProcessPoolExecutor
            with ProcessPoolExecutor(args.jobs, mp_context=mp.get_context("spawn")) as pool:
                futures = [pool.submit(fuzz_worker, j, args, seeds, setups, out_dir) for j in range(args.jobs)]
                results = [f.result() for f in futures]
        for f, c in results:
            findings += f
            counts.update(c)
    if args.shell:
        f, c = shell_pass(args, seeds, out_dir)
        findings += f
        counts.update(c)

    summary = triage.summarize(findings, counts, title=f"mutate seed={args.seed} budget={args.budget}")
    (out_dir / "summary.md").write_text(summary)
    print(summary)
    serious = [f for f in findings if f["kind"] in ("crash", "internal")]
    sys.exit(1 if serious else 0)


if __name__ == "__main__":
    main()
