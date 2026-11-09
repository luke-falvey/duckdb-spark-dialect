"""Timing helpers shared by the benchmarks.

Method: 1 warm-up run, then N measured runs (default 5); the median is reported, with min and max.
Ratios (spark dialect / native DuckDB) are the primary metric because they are fairly stable across
machines; absolute times are recorded too.
"""
import fcntl
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
PROJECT = BENCH_DIR.parent
sys.path.insert(0, str(PROJECT / "test"))
from engines import extension_path, use_utc  # noqa: E402

OUT_DIR = BENCH_DIR / "out"


def connect(database=":memory:", threads=1, load=True, read_only=False):
    import duckdb
    use_utc()
    con = duckdb.connect(database, read_only=read_only,
                         config={"allow_unsigned_extensions": "true", "allow_extensions_metadata_mismatch": "true"})
    if load:
        con.execute(f"LOAD '{extension_path()}'")
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET threads = {threads}")
    return con


def set_dialect(con, dialect):
    if dialect in (None, "duckdb"):
        con.execute("RESET current_dialect")
    else:
        con.execute(f"SET current_dialect = '{dialect}'")


def timeit(fn, runs=5, warmup=1, max_runs=15, cv_limit=0.05):
    """Median of `runs` timed calls; when the coefficient of variation is above `cv_limit` more runs are added (up to
    `max_runs`) so a noisy item is measured more precisely instead of reported with a wide spread."""
    for _ in range(warmup):
        fn()
    times = []

    def run_once():
        t = time.perf_counter_ns()
        fn()
        times.append((time.perf_counter_ns() - t) / 1e9)

    for _ in range(runs):
        run_once()
    while len(times) < max_runs and cv(times) > cv_limit:
        run_once()
    return {"median": statistics.median(times), "min": min(times), "max": max(times), "runs": len(times),
            "cv": round(cv(times), 4)}


def cv(times):
    """Coefficient of variation (standard deviation / mean)."""
    if len(times) < 2:
        return 0.0
    mean = statistics.fmean(times)
    return statistics.stdev(times) / mean if mean else 0.0


BUSY_PROCESSES = ("ninja", "cmake --build", "java", "fuzz/differential.py", "fuzz/mutate.py", "fuzz/args_sweep.py",
                  "fuzz/spark_corpus.py", "make_golden.py")


def busy_processes():
    """Processes that disturb timings (builds, Spark, fuzzers); the benchmark's own ancestors are ignored."""
    me = {str(os.getpid()), str(os.getppid())}
    out = subprocess.run(["ps", "-eo", "pid,args"], capture_output=True, text=True).stdout.splitlines()[1:]
    found = []
    for line in out:
        pid, _, args = line.strip().partition(" ")
        if pid in me or "ps -eo" in args:
            continue
        name = args.split(" ")[0].rsplit("/", 1)[-1]
        if name in ("ninja", "java") or any(b in args for b in BUSY_PROCESSES[1:] if b != "java"):
            found.append(args[:100])
    return found


class BenchLock:
    """One timed benchmark at a time: holds bench/out/.lock, and refuses to start while a build, a fuzzer or Spark
    runs (set BENCH_ALLOW_BUSY=1 to time anyway, e.g. in CI where nothing else runs)."""

    def __enter__(self):
        OUT_DIR.mkdir(exist_ok=True)
        self.file = open(OUT_DIR / ".lock", "w")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SystemExit("another benchmark is running (bench/out/.lock is held)")
        busy = [] if os.environ.get("BENCH_ALLOW_BUSY") else busy_processes()
        if busy:
            raise SystemExit("not timing while these run (BENCH_ALLOW_BUSY=1 overrides):\n  " + "\n  ".join(busy))
        return self

    def __exit__(self, *exc):
        fcntl.flock(self.file, fcntl.LOCK_UN)
        self.file.close()


def ratio_interval(a_times, b_times, samples=2000, seed=1):
    """Ratio of medians b/a with a bootstrap 95% interval (resampling the runs of each side)."""
    import random
    rng = random.Random(seed)
    ratios = []
    for _ in range(samples):
        ra = statistics.median(rng.choices(a_times, k=len(a_times)))
        rb = statistics.median(rng.choices(b_times, k=len(b_times)))
        if ra > 0:
            ratios.append(rb / ra)
    ratios.sort()
    point = statistics.median(b_times) / statistics.median(a_times)
    return point, ratios[int(0.025 * len(ratios))], ratios[int(0.975 * len(ratios)) - 1]


def machine():
    cpu = "?"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    try:
        sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=PROJECT, capture_output=True,
                             text=True).stdout.strip()
    except OSError:
        sha = "?"
    import duckdb
    return {"cpu": cpu, "nproc": os.cpu_count(), "python": platform.python_version(),
            "duckdb": duckdb.__version__, "extension_sha": sha, "date": time.strftime("%Y-%m-%d %H:%M")}


def current_branch():
    try:
        return subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=PROJECT, capture_output=True,
                              text=True).stdout.strip()
    except OSError:
        return "?"


def write_results(name, results, history=True):
    OUT_DIR.mkdir(exist_ok=True)
    path = OUT_DIR / f"{name}.json"
    mach = machine()
    path.write_text(json.dumps({"machine": mach, "results": results}, indent=1))
    print(f"wrote {path}")
    if history:
        append_history(name, mach, results)
    return path


HISTORY_DIR = BENCH_DIR / "history"


def append_history(suite, mach, results):
    """One JSON line per run in bench/history/<suite>.jsonl: commit, branch, machine, and per-item medians and ratios."""
    HISTORY_DIR.mkdir(exist_ok=True)
    items = {}
    if not isinstance(results, list):
        results = []
    for r in results:
        name = r.get("name") or r.get("query") or r.get("case")
        if isinstance(name, int):
            name = f"q{name:02d}"
        if name is None:
            continue
        item = {}
        for key in ("ratio", "spark_ratio", "strict_ratio"):
            if key in r:
                item[key] = round(r[key], 4)
        for side in ("spark", "native", "strict"):
            if isinstance(r.get(side), dict) and "median" in r[side]:
                item[side + "_ms"] = round(r[side]["median"] * 1000, 3)
        if item:
            items[str(name)] = item
    line = {"suite": suite, "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "commit": mach.get("extension_sha"),
            "branch": current_branch(), "machine": {"cpu": mach.get("cpu"), "nproc": mach.get("nproc")},
            "items": items}
    with open(HISTORY_DIR / f"{suite}.jsonl", "a") as f:
        f.write(json.dumps(line) + "\n")


def geomean(xs):
    xs = [x for x in xs if x and x > 0]
    return statistics.geometric_mean(xs) if xs else None
