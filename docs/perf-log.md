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

#### Noise measurement (4 vCPU container, nothing else running; perf/tooling 84ca5c4)

`func_bench.py --smoke` x5 (ratio spark / native, spread = max / min over the five runs): most items 1.02x to 1.10x;
noisier: `mul_decimal` 1.32x (4.1 to 5.5), `group_sum_int` 1.83x (one outlier), `array_subscript` 1.26x, `explode`
1.44x, `row_number` 1.20x, `like` 1.18x. `tpch_bench.py --sf 1` x3: per-query spread 1.02x to 1.25x (q03, q05, q07,
q10, q11, q15 are 1.17x to 1.25x), geometric mean of the ratio 1.124, 1.165, 1.127 (spark) and 1.125, 1.163, 1.140
(spark_strict). So the "1.18x" TPC-H figure in `docs/PERFORMANCE_RESULTS.md` is within the noise of the true value
(about 1.13x to 1.15x).

Consequences: a single-run ratio is only good to about +-10% (TPC-H queries +-15%); a change smaller than that has to
be shown with `bench/ab.py` (interleaved, bootstrap interval), never by comparing two separate runs. Thresholds in CI
must be well above these spreads.
