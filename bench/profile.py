"""Sampling profiler for the extension, built on gdb because `perf` is not available in the container.

usage: python bench/profile.py --sql "SELECT count(v) FROM (SELECT hex(i) AS v FROM range(5000000) t(i))" \
           --dialect spark --seconds 40 [--setup file.sql] [--name hex]
       python bench/profile.py --sql ... --native-sql "..." --explain      # EXPLAIN ANALYZE of both sides

It runs the query in a loop in the DuckDB shell built from source (/home/user/build-rel/duckdb, `SET threads = 1`
by default), attaches gdb every ~second for `bt 40` of every thread (about 1 sample per second: attaching costs
0.9 s), drops threads that are idle, and prints the top self frames (where the time is spent) and top inclusive
frames (which callers it belongs to). The summary goes to bench/out/profile-<name>.txt; paste its top lines into
docs/perf-log.md.

The shell needs the extension as a file named spark_dialect.duckdb_extension (SPARK_EXTENSION or build/).
"""
import argparse
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

SHELL = os.environ.get("DUCKDB_SHELL", "/home/user/build-rel/duckdb")
IDLE = ("pthread_cond_wait", "pthread_cond_timedwait", "__futex", "futex_wait", "epoll_wait", "__poll", "poll (",
        "clock_nanosleep", "nanosleep", "sem_wait", "pthread_join", "read (")


def function_name(frame):
    """The function of one gdb frame line, without address, arguments and file."""
    m = re.match(r"#\d+\s+(?:0x[0-9a-f]+ in )?(.*)", frame)
    if not m:
        return None
    text = m.group(1).replace("(anonymous namespace)::", "").replace("duckdb::", "")
    depth = 0
    for i, c in enumerate(text):
        if c in "<[":
            depth += 1
        elif c in ">]":
            depth -= 1
        elif c == "(" and depth == 0 and i > 0:
            text = text[:i]
            break
    return text.strip()[:110]


def parse_stacks(output):
    """Stacks (lists of function names, innermost first) of the threads that are doing work."""
    stacks, current = [], None
    for line in output.splitlines():
        if line.startswith("Thread "):
            current = []
            stacks.append(current)
        elif line.startswith("#") and current is not None:
            name = function_name(line)
            if name:
                current.append(name)
    busy = []
    for stack in stacks:
        top = " ".join(stack[:4])
        if stack and not any(i in top for i in IDLE) and "ExecuteTask" in " ".join(stack) or \
           stack and any("Kernel" in f or "Execute" in f for f in stack[:6]) and not any(i in top for i in IDLE):
            busy.append(stack)
    return busy


def sample(pid):
    r = subprocess.run(["gdb", "-batch", "-p", str(pid), "-iex", "set debuginfod enabled off", "-ex",
                        "set pagination off", "-ex", "thread apply all bt 40"], capture_output=True, text=True)
    return parse_stacks(r.stdout)


def summarize(samples, top=25):
    selfs, inclusive = Counter(), Counter()
    n = len(samples)
    for stack in samples:
        selfs[stack[0]] += 1
        for f in set(stack):
            inclusive[f] += 1
    lines = [f"{n} samples of busy threads"]
    lines.append("\ntop self frames (where the time is spent)")
    lines += [f"{100 * c / n:5.1f}%  {f}" for f, c in selfs.most_common(top)]
    lines.append("\ntop inclusive frames")
    lines += [f"{100 * c / n:5.1f}%  {f}" for f, c in inclusive.most_common(top)]
    return "\n".join(lines)


def profile(sql, dialect, seconds, setup, threads, ext, name):
    ext = Path(ext)
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "profile.sql"
        head = [f"LOAD '{ext}';", f"SET threads = {threads};"]
        if setup:
            head.append(Path(setup).read_text())
        if dialect:
            head.append(f"SET current_dialect = '{dialect}';")
        statement = sql.strip().rstrip(";") + ";"
        script.write_text("\n".join(head) + "\n" + (statement + "\n") * int(os.environ.get("PROFILE_REPEAT", "4000")))
        proc = subprocess.Popen([SHELL, "-unsigned", "-no-agent"], stdin=open(script), stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        time.sleep(3)  # load, setup and a few iterations
        samples = []
        end = time.time() + seconds
        while time.time() < end and proc.poll() is None:
            samples += sample(proc.pid)
        proc.kill()
        proc.wait()
    if not samples:
        raise SystemExit("no samples: the query finished too early or gdb could not attach")
    text = summarize(samples)
    path = common.OUT_DIR / f"profile-{name}.txt"
    common.OUT_DIR.mkdir(exist_ok=True)
    path.write_text(f"query: {sql}\ndialect: {dialect} threads: {threads}\n\n{text}\n")
    print(text)
    print(f"\nwrote {path}")


def explain(sql, native_sql, dialect, setup, ext):
    import duckdb
    for label, query, d in (("spark", sql, dialect), ("native", native_sql, None)):
        if not query:
            continue
        con = common.connect(threads=1)
        if setup:
            con.execute(Path(setup).read_text())
        common.set_dialect(con, d)
        plan = con.execute("EXPLAIN ANALYZE " + query).fetchall()
        print(f"===== {label} =====")
        print(plan[0][1] if plan else "")
        con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", required=True)
    ap.add_argument("--native-sql", help="the equivalent query in DuckDB SQL (for --explain)")
    ap.add_argument("--dialect", default="spark")
    ap.add_argument("--seconds", type=int, default=40)
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--setup", help="a SQL file run before the query (tables)")
    ap.add_argument("--name", default="query")
    ap.add_argument("--explain", action="store_true")
    ap.add_argument("--ext", default=os.environ.get("SPARK_EXTENSION", str(common.PROJECT / "build" /
                                                                          "spark_dialect.duckdb_extension")))
    args = ap.parse_args()
    if args.explain:
        explain(args.sql, args.native_sql, args.dialect, args.setup, args.ext)
        return
    with common.BenchLock():
        profile(args.sql, args.dialect, args.seconds, args.setup, args.threads, args.ext, args.name)


if __name__ == "__main__":
    main()
