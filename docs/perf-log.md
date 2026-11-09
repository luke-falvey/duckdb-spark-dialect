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

### Phase C: item 1 (sum/avg decimal) and a finding on Q13 (branch perf/opt-decimal-sum, from perf/tooling)

**Profile** (`bench/profile.py`, `SELECT sum(d) FROM decs`, 1 thread): spark_sum 40% in the clustered update loop,
20% bitpacking scan, 14% fastunpack; native `sum(d)` 51% in `ExecuteUnaryClustered<SumState<long>, ...>` (it also
uses an int64 state, chosen from column statistics). The aggregation loops are comparable; the TPC-H queries
that regressed with `spark_sum` are not dominated by this loop.

**Change 1**: `DecimalSumState` / `DecimalAvgState` keep a 64-bit partial sum (`small`, checked add, carry into the
128-bit part only on overflow). A/B (before = perf/tooling build, 11 runs): `sum_decimal` 2.39 -> 2.22 ms
(0.929, [0.899, 0.961], faster), `avg_decimal` 2.11 -> 1.98 (0.939, faster), `sum_int` / `group_sum_int` no change.
TPC-H Q01, Q06, Q09, Q10, Q13, Q18: no change. Correct but small; kept.

**Finding (the real cause of Q13's 1.4x to 1.5x)**: `EXPLAIN` of Q13 shows native DuckDB pushes the `count(o_orderkey)`
aggregate below the left join (pre-aggregating `orders` by `o_custkey`); in the Spark dialect the join becomes
`RIGHT` and no pushdown happens. Cause: the dialect's `NOT LIKE` expanded to `NOT like_escape(s, check_like_pattern(p),
'\\')`. `like_escape` is **fallible** (an invalid escape raises), and DuckDB neither moves a fallible predicate
below an outer join nor applies the aggregate pushdown. Reproduced in the default dialect: `NOT like_escape(...)`,
`NOT (a LIKE b)` and `NOT (a ~~ b)` all block it, `a NOT LIKE b` (the `!~~` function) does not.

**Change 2**: `LIKE` / `ILIKE` / `NOT LIKE` / `NOT ILIKE` without an ESCAPE clause are bound by new dispatchers
(`like_op`, `not_like_op`, `ilike_op`, `not_ilike_op`): a constant pattern without a backslash (Spark's escape never
applies) uses DuckDB's `~~`, `!~~`, `~~*`, `!~~*`; anything else keeps `like_escape` with the pattern check. Tests:
new cases in `test/cases/limitations.sql` (NULLs, backslash patterns, column patterns, ESCAPE, the Q13 shape).
Full suite, ANSI cases, crash replay, `--known`, `args_sweep --functions like,ilike,rlike,regexp_like` (2 findings,
both present before the change: `ESCAPE NULL` is accepted by the lenient dialect, `regexp_like(NULL, <invalid regex>)`
raises), differential smoke (0 failing): all unchanged.

**Result** (A/B, before = perf/tooling build, 7 runs, all 22 TPC-H queries): Q13 97.6 -> 65.2 ms (0.668,
[0.636, 0.685], faster); every other query "no change"; geometric mean B/A 0.989.

**Lesson / new backlog item**: fallible functions cost more than their own time: they disable optimizer rules.
Audit every `RegisterFallible` / `SetFallible` kernel that appears in predicates, join conditions and aggregate
arguments (see backlog item 11).

#### Tried and dropped: letting the optimizer split `spark_sum` / `spark_avg` (partial aggregation below a join)

`bench/plan_diff.py` (new: operator / join / aggregate shape of the Spark plan vs the native plan for every TPC-H query)
shows that Q10 and Q18 lack native's `combine_aggr` partial aggregation. Cause: the states had no `STATE_TYPE`
(layout description), and DuckDB's state export looks an aggregate up again **by name in `system.main`**
(`aggregate_export.cpp`: `GetEntry(... Identifier::DefaultSchema() ...)`), so an aggregate registered only in
`system.spark` cannot be split. Tried: `STATE_TYPE` on all five states, qualified names in the swapping binds, and
registering the sets also in `main` (plan shapes of Q10 / Q18 then matched native). A/B against the previous build
(9 runs): Q10 46.97 -> 59.37 ms (1.264, [1.226, 1.338], **slower**), Q18 no change, others no change. The exported
128-bit states cost more than the pushdown saves. Dropped; nothing of it is kept (it would also have put two
functions into the default dialect's `main` schema). Remaining shape differences: Q08 (`Perfect Hash Group By` on
`year()`: the key's statistics are missing), Q17 (an extra `Filter`), Q22.

### Phase C: item 3, `format_string` / `printf` (branch perf/opt-decimal-sum)

**Profile** (`format_string('%s-%d', s, i)`, 1 thread): spread over `_int_free`, `Value` construction / destruction
(`Vector::GetValue` per argument per row), `AddString`, `memcpy`; no single hot spot: the cost is the per-row
allocation churn (a `Value`, the format re-parsed, strings).

**Change**: the format is parsed once per distinct format string per chunk (`ParseFormat` -> `Piece`s; a constant
format is parsed once per chunk), the arguments are read through `UnifiedVectorFormat`, and the common conversions
(`%s` of a VARCHAR, `%d` of an integer type, with width / `-` / `0` flags only) skip the `Value` path; everything else
(floating point, decimals, dates, flags like `,` `+` `(`) still goes through `FormatOne`. A/B (9 runs): `format_string`
728.8 -> 67.1 ms (0.092x, [0.088, 0.095], faster; native `printf` is about 140 ms, so the ratio to native is now
about 0.5x). Checks: full suite both dialects, crash replay, `--known`, `args_sweep --functions format_string,printf`
(0 findings), differential smoke (0 failing).

### Phase C: item 5, `date_format` (branch perf/opt-decimal-sum)

**Profile** (`date_format(ts, 'yyyy-MM-dd HH:mm')`): 24% `memcpy`, 15% `std::string::_M_append`, 14% `Pad` (which built
a `std::string` per number, padded by prepending), 11% `AddString`.
**Change**: `AppendPadded` writes digits straight into the output (two-digit fast path), the fractional-seconds field
uses `snprintf`, the output string is reused per thread. A/B (9 runs): `date_format` 150.0 -> 80.1 ms (0.534,
[0.521, 0.563], faster; native `strftime` about 43 ms, ratio 3.5x -> 1.9x), `to_timestamp_pattern` 112.1 -> 102.5 ms
(0.914, faster). A two-digit shortcut gave no further gain. Checks: suites, replay, `args_sweep` over the datetime
functions: 4 failing findings, the same 4 with the previous build (`from_unixtime(9223372036854775807L)` raises where
Spark wraps: backlog item 12).

## split_comma 126x in --db mode (investigated, not a regression)
In the in-memory tables the pair is 3.3x. With `--db` (persisted, compressed tables) the `csv` column has ~70
distinct values, so DuckDB stores it dictionary-compressed and native `string_split` runs on the dictionary only
(~1.6 ms for 1M rows). The Spark kernel is evaluated per row (~200 ms). The ratio is a property of low-cardinality
stored data, not of the kernel; the in-memory 3.3x is the honest per-row cost. Treat low-cardinality-column pairs
in `--db` mode as dictionary-biased; pair changed to `size(split(...))` / `len(string_split(...))` so list
construction cannot be elided. Follow-up for backlog item 6 (literal separator fast path).

## split: write parts directly into the list child, memchr/memmem for literal separators
SplitKernel built a vector<vector<string>> per chunk and copied every part twice (substr, then AddString). Now parts
are written straight into the list child (geometric Reserve), the source is read as string_t without copying, and
literal separators use memchr (1 byte) / memmem. A/B (ab.py func, 7 runs): split_comma 206.8 -> 102.2 ms
(0.494 [0.473, 0.508]); split_regex 682.1 -> 588.1 ms (0.862 [0.835, 0.891]). Section 6 checks: run_duckdb both
dialects, replay (+ --known), differential seed 1 budget 150: clean. args_sweep split: one finding, identical before and
after (Spark raises on invalid regexes such as '[]' / '{}', we return a result): pre-existing, backlog item 12.
