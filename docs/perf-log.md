# Performance log

Plan: [plans/performance-testing.md](plans/performance-testing.md); backlog: [perf-backlog.md](perf-backlog.md).
One entry per action, newest last: date, branch and commit, what was measured or changed, numbers before and after
(from `bench/ab.py`), profile summary, decision. Read the last entries when resuming.

## Summary (updated at the end of the work)

(not finished yet)

## Branches pushed

- `perf/tooling` (phase A)

## Entries

### Phase A: tooling (branch perf/tooling, from main bee28f4)

- `bench/common.py`: `timeit` adds runs (up to 15) while the coefficient of variation is above 5%; `BenchLock`
  (`bench/out/.lock`, refuses to start while ninja / java / fuzzers run, `BENCH_ALLOW_BUSY=1` overrides); bootstrap
  `ratio_interval`; every `write_results` appends to `bench/history/<suite>.jsonl`.
- `bench/ab.py` + `bench/_ab_worker.py`: interleaved A/B comparison of two builds (one worker process per build),
  bootstrap 95% interval, verdict, result digest check. An A/A run of 3 functions (sum_int, add_int, format_string)
  gave ratios 0.989 to 1.057 with intervals including 1.0: "no change", as it must.
- `bench/profile.py`: gdb sampling profiler for the shell binary (`perf` is not installed): about 1 sample per second,
  top self / inclusive frames, `--explain` for EXPLAIN ANALYZE of both sides.
- `bench/func_bench.py --make-db` / `--db`: the benchmark tables once in `bench/out/func_tables.duckdb`, read-only.
- `bench/report.py --history SUITE`: trend table from the history files.
