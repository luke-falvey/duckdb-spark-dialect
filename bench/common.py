"""Timing helpers shared by the benchmarks.

Method: 1 warm-up run, then N measured runs (default 5); the median is reported, with min and max.
Ratios (spark dialect / native DuckDB) are the primary metric because they are fairly stable across
machines; absolute times are recorded too.
"""
import json
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


def timeit(fn, runs=5, warmup=1):
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(runs):
        t = time.perf_counter_ns()
        fn()
        times.append((time.perf_counter_ns() - t) / 1e9)
    return {"median": statistics.median(times), "min": min(times), "max": max(times), "runs": runs}


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


def write_results(name, results):
    OUT_DIR.mkdir(exist_ok=True)
    path = OUT_DIR / f"{name}.json"
    path.write_text(json.dumps({"machine": machine(), "results": results}, indent=1))
    print(f"wrote {path}")
    return path


def geomean(xs):
    xs = [x for x in xs if x and x > 0]
    return statistics.geometric_mean(xs) if xs else None
