"""Large inputs must finish in bounded time and memory, or fail with an error, never crash.

Each case runs in a DuckDB worker process (spark dialect) with memory_limit = 1GB and a timeout; the
outcome, time and the worker's peak RSS are recorded. Anything that crashes or times out is a finding.

usage: python bench/scaling_bench.py [--timeout 60] [--smoke]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
from engines import DuckRunner  # noqa: E402

CASES = [
    # (name, sql, smoke)
    ("sequence_100M", "SELECT size(sequence(1, 100000000))", False),
    ("repeat_1G", "SELECT length(repeat('x', 1000000000))", False),
    ("array_repeat_1G", "SELECT size(array_repeat(1, 1000000000))", False),
    ("explode_1M", "SELECT count(*) FROM (SELECT explode(sequence(1, 1000000)))", True),
    ("posexplode_1Mx100", "SELECT count(*) FROM (SELECT posexplode(sequence(1, 100)) FROM range(1000000))", False),
    ("collect_list_10M", "SELECT size(collect_list(id)) FROM range(10000000)", False),
    ("concat_ws_collect_1M", "SELECT length(concat_ws(',', collect_list(CAST(id AS STRING)))) FROM range(1000000)", True),
    ("from_json_deep_1000", "SELECT from_json('" + '{"a":' * 1000 + "1" + "}" * 1000 + "', 'a INT') IS NULL", True),
    ("get_json_object_deep_10000", "SELECT get_json_object('" + "[" * 10000 + "1" + "]" * 10000 + "', '$[0]') IS NULL", True),
    ("json_huge_string", "SELECT get_json_object(concat('{\"a\":\"', repeat('x', 50000000), '\"}'), '$.a') IS NULL", False),
    ("str_to_map_1M", "SELECT size(str_to_map(concat_ws(',', transform(sequence(1, 1000000), x -> concat('k', x, ':', x)))))", False),
    ("format_string_10000_args", "SELECT length(format_string(repeat('%s', 10000), " + ", ".join(["'a'"] * 10000) + "))", True),
    ("array_sort_comparator_10000", "SELECT size(array_sort(sequence(10000, 1, -1), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END))", True),
    ("array_sort_comparator_100x10000", "SELECT sum(size(array_sort(sequence(100, 1, -1), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END))) FROM range(10000)", True),
    ("split_regex_long_string", "SELECT size(split(repeat('a,b;', 2500000), '[,;]'))", True),
    ("split_backtracking", "SELECT size(split(repeat('ab', 50000), '(a|b)*c'))", True),
    ("regexp_extract_long", "SELECT length(regexp_extract(repeat('ab', 1000000), '(a|b)*', 0))", True),
    ("rlike_long", "SELECT repeat('a', 1000000) RLIKE '(a|aa)*b'", True),
    ("sha2_100MB", "SELECT length(sha2(repeat('x', 100000000), 256))", False),
    ("lpad_1G", "SELECT length(lpad('x', 1000000000, 'ab'))", False),
    ("in_list_100000", "SELECT count(*) FROM range(1000) t(id) WHERE id IN (" + ", ".join(str(i) for i in range(100000)) + ")", False),
    ("window_10M", "SELECT max(r) FROM (SELECT row_number() OVER (ORDER BY id DESC) AS r FROM range(10000000))", False),
    ("group_by_window_rewrite_10M", "SELECT count(*) FROM (SELECT window(ts, '1 hour'), count(*) FROM (SELECT timestamp_seconds(id) AS ts FROM range(10000000)) GROUP BY window(ts, '1 hour'))", False),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout", type=float, default=60)
    ap.add_argument("--smoke", action="store_true")
    args = ap.parse_args()
    results = []
    bad = 0
    for name, sql, smoke in CASES:
        if args.smoke and not smoke:
            continue
        # a fresh worker per case: an allocation failure can leave the process's address space fragmented
        runner = DuckRunner(dialect="spark", timeout=args.timeout, memory_mb=6144)
        runner.setup(["SET memory_limit = '1GB'", "SET threads = 2"])
        r = runner.run(sql, timeout=args.timeout)
        runner.close()
        row = {"name": name, "status": r.status, "seconds": r.seconds, "error": r.error,
               "result": repr(r.rows)[:80] if r.ok else None}
        if r.status in ("crash", "timeout", "internal"):
            bad += 1
        results.append(row)
        detail = row["result"] if r.ok else (r.error or "")[:100]
        print(f"{name:34} {r.status:8} {r.seconds:7.2f}s  {detail}", flush=True)
    common.write_results("scaling_smoke" if args.smoke else "scaling", results)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    with common.BenchLock():
        main()
