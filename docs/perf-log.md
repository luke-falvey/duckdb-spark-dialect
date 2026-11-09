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

## hex: drop redundant upper(); array_contains macro rewrite dropped
DuckDB's hex() already returns upper-case digits, so `upper(hex(CAST(x AS BIGINT)))` did a wasted pass.
A/B hex: 37.5 -> 16.6 ms (0.443 [0.416, 0.577]). Tried `list_contains(a, v) OR CASE WHEN list_count(a) < len(a)
THEN NULL ELSE false END` for array_contains: 1.150 [0.797, 1.190], no change, reverted. Remaining array_contains /
size / floor overhead is the cast + coalesce that implement Spark's NULL semantics; closing it needs real kernels.

## Benchmark validity: dictionary-compressed string columns in func_tables.duckdb
Low-cardinality string columns (`l` 97 values, `s` 1000, `csv` 70, `u` 1000) are stored dictionary-compressed in the
persisted tables, so native DuckDB evaluates on the dictionary only while the Spark kernels run per row. This made
substr read 1.9 ms and split_comma 126x. Cardinalities raised to ~100k in bench/func_bench.py (tables rebuilt);
in-memory `func_bench.py` numbers from before this commit are not comparable for the string pairs.

## substr: fast path for positive start and bounded length
Scans only the prefix up to the end position (was the whole string to count continuation bytes) and uses int64
arithmetic (was 128-bit). The general path remains for negative / huge arguments. A/B (high-cardinality `l`):
121.7 -> 84.2 ms (0.692 [0.637, 0.773]). Checks: run_duckdb both dialects, replay (+known), differential seed 1,
args_sweep substr/substring/left/right/overlay: 3 findings identical before and after (encode(...) with
bad charset arguments; pre-existing, item 12).

## to_json: append into one buffer, vector-native path for structs of strings / booleans / integers
Profile (gdb, 33 samples): allocation-bound (Value construction per row, std::string temporaries, malloc/free).
Step 1: JsonEscape/ToJson return strings -> AppendJson into one buffer, run-copying escape, formats read once
per chunk: 1034.8 -> 751.8 ms (0.727 [0.691, 0.791]). Step 2: structs whose fields are all VARCHAR / BOOLEAN /
TINYINT..BIGINT are serialised straight from the child vectors (UnifiedVectorFormat, pre-escaped `"name":`
prefixes); everything else (maps, lists, dates, timestamps, decimals, nested) keeps the Value path:
743.1 -> 71.2 ms (0.096 [0.091, 0.104]); to_json pair ~20x -> ~1.4x vs native. New golden cases in
limitations.sql (escapes, control chars, unicode, NULL fields, NULL struct, all integer widths, boolean) from Spark.
Checks: run_duckdb both dialects, replay (+known), differential seed 1, args_sweep to_json: 2 findings identical
before and after (empty field name; item 12).

## decimal multiply: direct integer product when the exact result fits (p1 + p2 + 1 <= 38)
Before, `d * d` cast the left operand to DECIMAL(wide) and went through DuckDB's `*` plus the rescale kernel
(SparkDecimalResult). When Spark's result precision is <= 38 the product of the stored integers is already exact
(no rounding, no overflow), so a kernel `spark_decimal_multiply_exact<A, B, OUT>` multiplies the physical integers
(64x64 -> 128 bit when neither operand is 128-bit) and writes the result type directly; also skips the ANSI null
check wrapper (it cannot trigger). A/B mul_decimal: 9.20 -> 3.77 ms (0.409 [0.388, 0.421]); TPC-H Q01
85.9 -> 77.5 ms (0.903 [0.799, 0.980]), Q14 0.906; Q09/Q10/Q19 no change. Seven new golden cases in
limitations.sql (precision/scale mixes, int16..int128 storage, integer operands, NULLs, negatives, max values).
Checks: run_duckdb both dialects (ANSI cases included), replay (+known), differential seed 1, isolation clean.

## decimal + / -: direct integer arithmetic when the exact result fits (precision <= 38)
Profile of Q01 (gdb, 1 thread): the third multiplication went through the wide kernel, and `1 - l_discount` /
`1 + l_tax` were cast to DECIMAL(38) (hugeint), subtracted, then narrowed by DecimalRescaleKernel. New
`spark_decimal_{add,subtract}_exact` kernels align the scales with powers of ten and add the stored integers in
`__int128` (operands of any physical width, any result width); no overflow or rounding is possible when Spark's
result precision is <= 38. A/B TPC-H (before = multiply fix only): Q01 64.5 -> 53.6 ms (0.831 [0.769, 0.887]),
Q03/Q05/Q10/Q14 no change. Six golden cases in limitations.sql (scale alignment, all storage widths, integer
operands, NULLs, Q01 expression shape). Checks: run_duckdb both dialects (1684 / 1748 pass), replay (+known),
differential seed 1, isolation clean.
Full TPC-H SF1 afterwards (5 runs, 22 queries): geomean spark 1.070x, spark_strict 1.050x (target <= 1.10x);
Q01 2.03x / 1.92x (target <= 2.0x), Q10 1.23x, Q11 1.12x, none above 1.5x.

## Parsing (backlog item 9): investigated, no change committed
Corpus parse cost is 3.1x - 3.9x DuckDB's default dialect (run-to-run spread of the ratio is +-0.3x on this machine, so
experiments were interleaved, 4 rounds). A gdb sample of a parse loop shows 95% inside the PEG matcher
(`Matcher::MatchParseResult`, `MatchStack::ExecuteFrame` 38% self), i.e. the cost is the number of rule steps, not
the tokenizer / rewrite or our transforms. Scaling is linear in all shapes (10 .. 10,000 items), per-item cost ~35 us
for `cN + N AS aN`. Experiments (grammar rebuilt each time, all reverted):
- dropping SpPredicate (9 alternatives per operand): -10..14% parse time (so factoring the NOT? prefix of the
  predicate alternatives could gain at most that);
- dropping the SpLambda alternative (tried first for every expression): corpus 3.17x -> 2.66x, select_items -27%;
- SpLambdaParams `ColId` -> `Identifier`: no change (identifier matching is not the cost; the cost is the failed
  rule entry and its discarded result).
The remaining lever with real gain is parsing `x -> body` as `SpOr ('->' SpExpression)?` and converting a bare
column / parenthesised column list into lambda parameters in the transform (saves the failed SpLambda attempt per
expression, ~20%); that is a grammar change with its own error-message risk and would still leave the ratio near 2.6x,
so the <= 2.0x target is not reachable with grammar reordering alone. Absolute cost is ~100 us per statement.

## Phase B: TPC-DS SF1 (bench/tpcds_bench.py, queries in bench/tpcds/ from the tpcds extension)
All 99 queries run in both dialects (0 errors). Geomean (3 runs, 4 threads): spark 1.118x, spark_strict 1.128x.
One flagged result difference (Q78): the intended type difference (Spark `/` on decimals is DECIMAL, native is DOUBLE;
the sum of a narrow decimal widens to DECIMAL(38)), not a defect. Worst queries: Q22 3.35x, Q36 2.40x, Q74 2.12x,
Q04 2.09x, Q02 1.82x, Q67 1.65x, Q11 1.57x, Q44 1.43x; everything else below 1.35x.
Cause (EXPLAIN ANALYZE of Q22, source of the optimizers in duckdb/src/optimizer): the Spark aggregates `spark_sum` /
`spark_avg` have no state-export layout (`SetStructStateExport`), so DuckDB (a) cannot cascade ROLLUP / CUBE grouping
sets from the finest level with `combine_aggr` (Q22, Q36, Q67 compute every grouping set from the input CTE: 4
consumers vs 1) and (b) cannot push the aggregate below the join (`partial_aggregate_pushdown`, needs
`HasGetStateTypeCallback`: Q74 / Q04 / Q11 year_total shape). Both need `BindExportedAggregate`, which re-binds the
aggregate by name in `system.main`; ours live in schema `spark`. A first attempt (earlier in this effort) was reverted:
it threw a CatalogException for the unqualified lookup and Q10 got slower. Recorded as backlog item 13 (largest
remaining plan-shape item; needs state layouts for the decimal / integer / double sum and avg states plus
main-schema registration under a hidden name, and the isolation check).
