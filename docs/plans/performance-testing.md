# Plan: performance testing and optimization (long-running, autonomous)

This plan is written for an agent session that works for many hours without a human: it builds better
performance tooling, widens what is measured, and then repeatedly finds and fixes the largest costs of the Spark
dialects, without ever trading correctness for speed. Read all of it before starting, and re-read sections 3, 4
and 13 whenever you resume after a break or a context reset.

## 1. Where things stand (main at bee28f4)

The repository is a DuckDB v2.0.0 extension with two dialects, `spark` and `spark_strict` (and an ANSI mode,
`SET spark.sql.ansi.enabled = true`). Correctness is checked against Spark 3.5 (`test/`, `test/fuzz/`);
performance against equivalent native DuckDB SQL (`bench/`, `docs/PERFORMANCE.md`,
`docs/PERFORMANCE_RESULTS.md`). Read the README, `docs/PERFORMANCE.md` and `test/fuzz/README.md` first.

Current numbers (4 vCPU container; `docs/PERFORMANCE_RESULTS.md`):

| measure | now | earlier best | target |
|---|---|---|---|
| TPC-H SF1 geometric mean, spark / native | 1.18x (`spark_strict` 1.16x) | 1.12x | ≤ 1.10x |
| TPC-H worst queries | Q01 2.57x, Q13 1.66x, Q09 1.35x, Q10 1.26x | | ≤ 1.5x except Q01 ≤ 2.0x |
| parsing, µs per statement | 134 µs, 3.35x DuckDB | | ≤ 2.0x |
| 104 function / operator pairs, geometric mean | 1.30x | | ≤ 1.15x |
| worst pairs | `to_json` 18x, `format_string` 5.3x, `mul_decimal` 4.0x, `date_format` 3.5x, `split_comma` 3.4x, `substr` 2.3x, `array_contains` 2.3x, `cast_double_to_string` 2.2x, `hex` 2.2x, `to_timestamp_pattern` 2.2x | | each ≤ 1.5x, or explained |

Known causes, from `docs/PERFORMANCE.md`: Spark's decimal result types are 128-bit where DuckDB's are 64-bit
(Q01, `mul_decimal`, `sum(decimal)` at 1.6x since `spark_sum` keeps an exact 128-bit state, which moved Q09 /
Q10 / Q13); the `java.util.Formatter`-compatible `format_string` formats row by row; parsing time is in
DuckDB's PEG matcher working through the larger Spark grammar.

Not measured at all yet: TPC-DS; ANSI mode's overhead; `spark_strict` separately from `spark` outside TPC-H;
short-query latency (parse + bind dominate); prepared statements; extension load and first-use costs (grammar
compilation, `CHAR(n)` cast registration on first use); the large-stack thread used for deeply nested statements;
multi-threaded scaling of the dialect's kernels (several keep `thread_local` caches); memory use and spilling of
the dialect's own aggregates (`spark_sum`, `spark_avg`); Spark-shaped workloads (`LATERAL VIEW explode`,
higher-order functions, `PIVOT`, `GROUPING SETS`, JSON, datetime formatting at scale).

## 2. What success looks like

The session runs in phases (section 5). It is finished when every phase is done or its time box is spent,
and:

1. **Tooling** (phase A): an A/B comparison tool, a profiler that works in this container, a benchmark history,
   and noise-aware thresholds exist and are documented.
2. **Coverage** (phase B): TPC-DS, Spark-shaped workloads, latency, ANSI/strict overheads, threads, memory and
   start-up costs are measured, with results in `docs/PERFORMANCE_RESULTS.md`.
3. **Optimization** (phase C): the targets in section 1's table are met, or each miss is explained in
   `docs/PERFORMANCE.md` with what was tried and what limits it.
4. **Guardrails** (phase D): CI and the nightly workflow run the new benchmarks with thresholds that do not
   flake, and `bench/baseline.json` is current.
5. **Correctness never regressed**: the full suite passes in both dialects (and the ANSI cases), the crash
   replay and `replay.py --known` pass, the fuzz smoke is green, and the isolation check is clean, after every
   commit.
6. `docs/perf-log.md` (section 4) records every change with before / after numbers.

## 3. Ground rules (read again whenever you resume)

- **Correctness first.** A faster wrong answer is a regression. Every optimization is followed by the checks in
  section 6 before it is committed. Spark is the oracle for results; native DuckDB is only the yardstick for
  speed.
- **Measure, then change.** Never optimize on a hunch: profile (section 7) and record the profile's top frames
  in the log before writing code. After the change, an A/B comparison (7.2) must show the gain beyond noise.
- **Small commits on a branch.** Work on one branch per phase (`perf/tooling`, `perf/coverage`,
  `perf/opt-<topic>`, `perf/ci`), branched from `main`, one commit per optimization with its numbers in the
  message. Push after every commit (`git push -u origin <branch>`; retry with backoff on network errors). Do not
  merge into `main`, do not force-push, do not open or merge pull requests unless the human asked for them: leave
  the branches pushed and list them in the log.
- **Commands that test and commit are chained with `&&`**, never `;`, so a failing check stops the commit.
- **Time boxes.** Each backlog item (section 9) gets at most about two hours of attempts. If an item is not
  done by then, write what you learned in the log, revert any unfinished change, and move to the next item. Do
  not loop on one problem.
- **Don't change the default DuckDB dialect's behaviour or speed.** The isolation check and the default-dialect
  benchmarks must not move.
- **Don't widen the tolerance of a correctness check to make a change pass**, and don't relax a benchmark
  threshold to hide a regression; a threshold change needs its own commit and a reason.
- **When something in the environment breaks** (disk full, a build dies, the network refuses a host), follow
  section 13 rather than improvising.

## 4. Keeping state across a long session

Context resets and container restarts will happen. Keep the session's state in files, not in memory:

- **`docs/perf-log.md`** (create it in phase A, commit it with each change). One entry per action:
  date/time, branch and commit, what was measured or changed, the numbers before and after (from 7.2), the
  profile summary, and the decision. Start each working session by reading its last entries.
- **`docs/perf-backlog.md`**: the prioritized list of section 9, with a status per item (`todo`, `in progress`,
  `done`, `dropped: <reason>`). Update it whenever an item changes state.
- **`bench/history/`**: the machine-readable results (7.3), appended by every benchmark run.
- Before stopping (or every two hours), commit and push the log, backlog and history.

When resuming: read `docs/perf-log.md` (last entries) and `docs/perf-backlog.md`, check `git status` and the
current branch, rebuild if the build directory is missing (section 13), and continue with the first item that is
`in progress` or `todo`.

## 5. Phases

| phase | content | time box |
|---|---|---|
| A | tooling: A/B comparison, profiler, history, noise | 4 h |
| B | coverage: TPC-DS, Spark workloads, latency, ANSI/strict, threads, memory, start-up | 8 h |
| C | optimization: the backlog of section 9, largest end-to-end impact first | until the backlog is done |
| D | guardrails: CI, nightly, baselines, final report | 2 h |

Do A first (C depends on it). B and C can interleave: when B finds something large, add it to the backlog and
carry on with B. Finish with D.

## 6. Correctness checks after every change

Run these, in this order, before committing a change to `src/`:

```bash
python test/run_duckdb.py && python test/run_duckdb.py --dialect spark_strict
python test/run_duckdb.py test/cases/ansi/*.sql && python test/run_duckdb.py --dialect spark_strict test/cases/ansi/*.sql
python test/fuzz/replay.py && python test/fuzz/replay.py --known
python test/fuzz/args_sweep.py --functions <the functions you changed> --seed 1
python test/fuzz/differential.py --seed 1 --budget 150 --reduce-limit 5
```

- `run_duckdb.py` prints a summary per file; any `FAIL` blocks the commit. Check the `TOTAL` line's exit code,
  not just the printed text.
- For a change to a kernel used by many functions (casts, decimal arithmetic, `Builder`, the comparison or
  aggregate dispatchers), also run the whole sweep (`args_sweep.py --seed 1`, ~10 minutes) and the isolation
  check (`isolation.py --duckdb-src /home/user/duckdb --budget 150`).
- For a change that touches parallel execution or thread-local caches, run the thread checks of 8.6.
- If a check fails, fix the change or revert it. Never update a golden file to match the new output.

## 7. Phase A: tooling

### 7.1 Noise

Measure the noise before trusting any ratio: run `bench/func_bench.py --smoke` five times in a row and
`bench/tpch_bench.py --sf 1` three times, and record each ratio's spread (max / min) in the log. Then:

- extend `bench/common.timeit` to report the coefficient of variation and to take more runs automatically when
  it is above 5% (up to 15 runs);
- pin benchmarks to one process at a time: nothing else (fuzzers, builds, Spark) may run during a timed run.
  Use a lock file (`bench/out/.lock`) that every benchmark script takes, and refuse to start if a build or a fuzz
  run is active (check for `ninja`, `cmake --build`, `java` processes).

### 7.2 A/B comparison: `bench/ab.py`

The core tool for deciding whether a change helps. It takes two extension files (before and after) and runs the
same benchmark queries against both, **interleaved** (A, B, A, B, ...) on the same machine and data, so drift
affects both equally:

```bash
python bench/ab.py --a /home/user/ext-ab/before.duckdb_extension --b /home/user/ext-ab/after.duckdb_extension \
    --suite func --only sum_decimal,mul_decimal --runs 15
python bench/ab.py --a ... --b ... --suite tpch --sf 1 --queries 1,9,10,13
```

Output per query: median A, median B, ratio B/A, a bootstrap 95% confidence interval of the ratio, and a verdict
(`faster` / `slower` / `no change` when the interval contains 1.0). Record the table in the log. Workflow for
every optimization: copy the current extension to `before.duckdb_extension`, build the change, copy it to
`after.duckdb_extension`, run `ab.py` (copies, so a rebuild never changes a file under a running benchmark).

Each extension must run in its own process (two different builds of the same extension cannot be loaded into
one process): run each side as a short-lived subprocess per query batch, or keep one long-lived worker per side
(`test/engines.py` `DuckRunner` already runs DuckDB in a worker process and accepts an `extension` path).

### 7.3 History: `bench/history/`

Every benchmark script appends one JSON line per run to `bench/history/<suite>.jsonl`: commit, branch, machine,
time, and the per-item medians and ratios. `bench/report.py --history` prints a trend table (last N runs per
item). Commit the history files (they are small). This is how later sessions and CI see trends instead of one
noisy number.

### 7.4 Profiler: `bench/profile.py`

`perf` is not installed in this container and `apt-get` may not work; the Python `duckdb` package is stripped,
so its frames have no names. Profile with the DuckDB **shell binary built from source with symbols**
(`/home/user/build-rel/duckdb`, `RelWithDebInfo`), which loads the extension with `-unsigned` and
`LOAD '<path>'`:

1. Write the query to a file; run it in a loop in the shell (`.timer off`, repeat the statement N times, or a
   recursive CTE that drives repetitions), with `SET threads = 1` for a single profile stack, as a background
   process.
2. Sample stacks with gdb: `gdb -batch -p <pid> -ex "thread apply all bt 40"` every 50 to 200 ms for 20 to 60 s.
3. Aggregate: top self frames and top inclusive frames (as in the poor man's profiler), written to
   `bench/out/profile-<name>.txt` and summarized in the log.

Make it one command: `python bench/profile.py --sql "<query>" --dialect spark --setup bench/sql/func_tables.sql
--seconds 30`. Try `perf` once (`which perf`, `pip install` alternatives do not exist); if it is available, add a
`--perf` mode that produces the same summary. Also add `--explain` to print `EXPLAIN ANALYZE` per operator for
the same query in both dialects (the quickest way to see which operator is slower).

Build the shell once: `cmake --build /home/user/build-rel --target duckdb` (it is part of the release build in
`docs/plans/fuzz-and-performance.md` section 4).

### 7.5 Shared data

Benchmarks create the same tables repeatedly (`func_bench.py` creates them in memory each run). Generate them
once into `bench/out/func_tables.duckdb` (and `tpch_sf*.duckdb`, `tpcds_sf*.duckdb`) and open them read-only,
so runs are faster and every tool (ab, profile, func) uses identical data.

## 8. Phase B: wider coverage

Each new suite: a script in `bench/`, results in JSON + history, a section in `docs/PERFORMANCE_RESULTS.md`
(`report.py`), and correctness checked where possible. Record new costs in the backlog (section 9).

### 8.1 TPC-DS

1. Build DuckDB's tpcds extension from the source tree, as for tpch:
   `cmake -S /home/user/duckdb -B /home/user/build-rel -DBUILD_EXTENSIONS="tpch;tpcds"` and
   `cmake --build /home/user/build-rel --target tpcds_loadable_extension`. Generate SF1 (`CALL dsdgen(sf=1)`)
   into `bench/out/tpcds_sf1.duckdb`.
2. Query text: Spark's own TPC-DS queries ship in the Spark repository at
   `sql/core/src/test/resources/tpcds/` (v1.4, 103 files including q14a/b, q23a/b, q24a/b, q39a/b) and
   `tpcds-v2.7.0/`. Fetch them with the same sparse checkout `test/fuzz/spark_corpus.py fetch` uses (add the two
   directories), into `bench/tpcds/spark/` — or, if the license header requires it, fetch them at run time into
   `bench/out/` instead of committing them (check the file headers; Spark is Apache-2.0).
3. Native comparison: DuckDB's own TPC-DS query text (`/home/user/duckdb/extension/tpcds/dsdgen/queries/`), run in
   the default dialect. The query texts differ slightly; compare per query number.
4. `bench/tpcds_bench.py`: like `tpch_bench.py` (per-query times, ratios, geometric mean), plus a count of queries
   that fail in each dialect (a failure is a correctness finding: fix it or record it in the log and backlog).
5. Correctness: Spark's repository also has `tpcds-query-results/` (Spark's expected answers, for its
   `TPCDSQueryTestSuite`). Find out which data it assumes (scale factor, generator); DuckDB's tpcds extension
   embeds the official `dsdgen`, so the same scale factor should give the same data. If the answers match the
   data, compare the Spark dialects' results with them (as an informational check in `tpcds_bench.py
   --check-results`). If not, check a sample of queries at SF 0.01 against PySpark directly (export the tables
   to Parquet, `CREATE TEMPORARY VIEW ... USING parquet` in Spark). Any wrong result goes to the backlog as a
   correctness item, ahead of performance items.

### 8.2 Spark-shaped workloads: `bench/workloads/*.sql`

Queries the way Spark users write them, each with a native DuckDB equivalent where one exists, run by
`bench/workload_bench.py` over the shared tables (and TPC-DS tables where natural):

- `LATERAL VIEW [OUTER] explode / posexplode` over arrays and maps; `inline` of struct arrays;
- higher-order functions on arrays of 10 to 1,000 elements: `transform`, `filter`, `aggregate`, `exists`,
  `zip_with`, `array_sort` with and without a comparator, `map_filter`, `transform_values`;
- `PIVOT`, `GROUPING SETS` / `ROLLUP` / `CUBE`, `count(DISTINCT)` combinations, `collect_list` / `collect_set`
  per group;
- window functions: running sums with `ROWS` and `RANGE` frames, `lag` / `lead`, `first` / `last` with `IGNORE
  NULLS`, `percent_rank`, over 10M rows;
- JSON: `get_json_object`, `from_json` with a schema, `to_json`, `json_tuple` on 1M documents;
- datetime: `date_format`, `to_timestamp` with patterns, `date_trunc`, `window()` time bucketing on 10M rows;
- strings: `regexp_extract`, `split` + `explode`, `concat_ws` over arrays, `format_string`;
- decimals: financial aggregations over `DECIMAL(15,2)` and `DECIMAL(38,10)` (sums, products, averages), the
  pattern behind TPC-H Q01.

### 8.3 Latency of short queries

Interactive use is dominated by fixed costs. `bench/latency_bench.py`: end-to-end time of ~50 short queries
(point lookups, small aggregates, `SELECT` of expressions without a table) split into parse, bind/plan and
execute (time `extract_statements`, `EXPLAIN` and the full query separately), per dialect; also prepared
statements (`PREPARE` once, `EXECUTE` many times: the parse cost should disappear). Report p50 and p99.

### 8.4 Start-up costs

- `LOAD` time of the extension, and the first `SET current_dialect = 'spark'` (grammar compilation) vs later ones;
- first use of `CHAR(n)` for a new `n` (casts are registered on first use);
- connection creation with the extension loaded vs without.

### 8.5 ANSI mode and strict mode

Run `func_bench.py` and TPC-H with `SET spark.sql.ansi.enabled = true` (checked kernels) and in `spark_strict`
(extra parse-time checks), against the lenient `spark` dialect. Expected: ANSI costs a little on arithmetic and
casts; strict costs only at parse time.

### 8.6 Threads

- TPC-H SF1 and the workloads at `threads` = 1, 2, 4: does the ratio to native stay the same as threads grow?
  A kernel that serializes (a global lock, e.g. `BindSparkKernel`'s registry mutex taken at execution time, or a
  shared cache) shows as a ratio growing with threads.
- Correctness under parallelism: the kernels with `thread_local` caches (regex in `split`, datetime patterns) must
  give identical results with 4 threads and different patterns per row (patterns from a column, rows shuffled).
  Add such cases to `test/cases/fuzz_regressions.sql`.

### 8.7 Memory and spilling

- Peak RSS (`resource.getrusage` in a worker, or `/usr/bin/time -v`) of grouped aggregates with 10M groups:
  `sum` / `avg` of int, double and decimal, `spark` vs native. The dialect's aggregate states (`IntegerSumState`,
  `DecimalSumState`, ... in `src/spark_aggregates.cpp`) should be no larger than needed.
- Spilling: run the same queries with `SET memory_limit = '256MB'` and a `temp_directory`; they must complete
  (DuckDB can only spill aggregates whose states are fixed-size and serializable), and record the time.
- The large-stack helper thread for deep expressions (`RunOnLargeStack`): its cost per statement, and that
  memory is released (run 10,000 deep statements, RSS must not grow).

### 8.8 Scale

TPC-H at SF 10 if disk allows (about 3 GB for the database file; check `df -h` first and delete it afterwards),
to see whether ratios hold when data no longer fits in caches.

## 9. Phase C: the optimization backlog

Start `docs/perf-backlog.md` from this list, add what phase B finds, and order by **end-to-end impact** (TPC-H /
TPC-DS time saved first, then per-function ratios weighted by how common the function is in real Spark SQL).
For each item: profile (7.4), form a hypothesis, change, verify (section 6), A/B (7.2), log.

| item | today | idea to start from | target |
|---|---|---|---|
| `sum(decimal)` / `avg(decimal)` 128-bit state (TPC-H Q09, Q10, Q13) | 1.6x | accumulate in `int64_t` while the running sum fits (checked add) and switch the state to `hugeint_t` on overflow, like DuckDB's `sum_no_overflow`; or choose the state by the input's precision at bind time | ≤ 1.15x, TPC-H geomean ≤ 1.12x |
| Q01 / `mul_decimal`: 128-bit decimal products | 2.57x / 4.0x | 64-bit fast paths exist in the kernels; check `EXPLAIN ANALYZE` for which operator dominates (projection or aggregate); consider keeping 64-bit physical types where Spark's precision ≤ 18 | Q01 ≤ 2.0x |
| `format_string` / `printf` | 5.3x | parse the format string once (it is almost always constant: cache per thread, as `CachedPattern` does for datetime); fast paths for `%s` / `%d` without width or flags | ≤ 1.5x |
| `to_json` | 18x | builds DuckDB `Value`s per row: write JSON directly from the vectors (struct / list / map children), or use DuckDB's `to_json` and fix up the differences (number and timestamp formatting) | ≤ 2x |
| `date_format` / `to_timestamp` with patterns | 3.5x / 2.2x | profile the formatter after the pattern cache; avoid `std::string` allocations per field; format into a stack buffer | ≤ 1.5x |
| `split` with a literal separator | 3.4x | the substring fast path exists; the cost may be the `vector<vector<string>>` row buffers and copies: build the list child vector directly | ≤ 1.3x |
| `substr` | 2.3x | the byte-range kernel still scans for code points even for ASCII: detect ASCII once per string with a word-at-a-time check, or use DuckDB's `substring` kernel when the start is positive | ≤ 1.3x |
| `array_contains`, `size`, `floor`, `hex`, casts string to int / date, `cast_double_to_string` | 1.8x to 2.3x | each a small kernel or macro: profile one by one; macros that wrap arguments in conversions are candidates for kernels | ≤ 1.3x |
| parsing | 3.35x | DuckDB's PEG matcher dominates. Try: ordering `SpAtom` alternatives by frequency; merging the keyword-led alternatives behind one first-set check; checking how many hook frames a plain operand passes through after the collapsible rules; measuring the tokenizer's ~40 rewrite passes separately (`SPARK_DIALECT_DEBUG_TOKENS`) and merging passes that scan for the same keywords | ≤ 2.0x |
| findings of phase B | | | |

Rules for this phase:
- Prefer changes inside the dialect's own code. If a fix needs a change to DuckDB itself, record it in the log
  as an upstream suggestion with a reproducer instead of patching DuckDB.
- Prefer removing work (caching a constant, skipping a conversion, a fast path for the common case) over
  micro-optimizing inner loops.
- After every three optimizations, rerun the full benchmark set (func, TPC-H SF1, parse, workloads) and
  `report.py --write`, and check nothing else got slower (another item's ratio up by more than 10% is a
  regression to fix before going on).

## 10. Phase D: guardrails and report

- CI `bench-smoke`: add the A/B-free smoke subsets of the new suites (workloads, latency) with catastrophic
  thresholds only, after measuring their noise on CI: run the job three times (`workflow_dispatch`) and set each
  threshold well above the observed spread.
- Nightly: add TPC-DS SF1, workloads, latency, threads and memory suites; write the trend (`report.py --history`)
  to the step summary.
- Refresh `bench/baseline.json` from the final smoke run, in its own commit, with the reason.
- Update `docs/PERFORMANCE.md` (method, what changed and why, what is left) and `docs/PERFORMANCE_RESULTS.md`
  (`report.py --write`), and the README's Performance section numbers.
- Write the final summary at the top of `docs/perf-log.md`: targets met / missed, the biggest wins, the branches
  pushed, upstream suggestions, and the next steps.

## 11. Environment setup

The container may be fresh. Follow `docs/plans/fuzz-and-performance.md` section 4 for the DuckDB source checkout
(`/home/user/duckdb`, tag v2.0.0) and the `RelWithDebInfo` build in `/home/user/build-rel` (with `BUILD_SHELL=ON`
and `-DBUILD_EXTENSIONS="tpch;tpcds"`), and `pip install "duckdb==2.0.0" "pyspark==3.5.*" pandas pytz` (Java 17+
for Spark). Then:

```bash
mkdir -p build && ln -sf /home/user/build-rel/extension/spark_dialect/spark_dialect.duckdb_extension build/
mkdir -p /home/user/ext-ab /home/user/ext-stable
```

- The first build takes 30 to 45 minutes on 4 cores; start it in the background and write tooling meanwhile.
  Incremental rebuilds of the extension take about a minute.
- `extensions.duckdb.org` is usually unreachable from the container, so `INSTALL tpch` fails: build the
  extensions from the tree as above, and pass `TPCH_EXTENSION=` / `TPCDS_EXTENSION=` paths (add the latter to
  `tpcds_bench.py`). In CI (GitHub runners) `INSTALL` works.
- There is no sanitizer build needed for this plan; delete one if it exists and space is short.

## 12. Benchmarking discipline

- Copy the extension before every long run (`cp build/... /home/user/ext-stable/`) and point `SPARK_EXTENSION`
  at the copy: rebuilding replaces the file in place, and a worker that restarts mid-run then loads a half-written
  file.
- Never time anything while a build, a fuzzer or Spark is running (7.1's lock).
- Ratios, not absolute times, are comparable across sessions; compare absolute times only within one A/B run.
- Small inputs measure parse and bind, not the kernel: use at least 1M rows for function benchmarks, and check the
  plan with `EXPLAIN` once so the optimizer has not removed the expression (`count(v)` over a projection is the
  pattern `func_bench.py` uses).
- `SET` in the Spark dialects is a Spark `SET` and does not change DuckDB settings (`threads`, `memory_limit`):
  set those in the default dialect (`RESET current_dialect`), then select the dialect again
  (`bench/common.set_dialect` does this).

## 13. When the environment gets in the way

| problem | what to do |
|---|---|
| "No space left on device" | `df -h /`; delete your own large files: old `bench/out/*.duckdb` scale factors you are done with, object files of builds you will not recompile (`find /home/user/build-asan -name '*.o' -delete`), old profile outputs. Do not sweep `/tmp` wholesale (not allowed); the test tools remove their own scratch directories. |
| A build is killed (out of memory) | rebuild with `-j 2`; never build while Spark or a fuzzer runs |
| The extension fails to load ("not a DuckDB extension", truncated file) | a rebuild overwrote it during a run: use the stable copy, rerun the benchmark |
| A benchmark result looks impossible (a 10x jump) | rerun it alone; check `bench/out/.lock`, running processes (`ps aux`), and that the right extension file was loaded (`SELECT extension_version FROM duckdb_extensions()`) |
| Spark dies (connection refused) | `SparkOracle` restarts the JVM itself; if it keeps dying, lower `SPARK_DRIVER_MEMORY` or the query size |
| A network host is refused | read the environment documentation tool before working around it; do not disable TLS checks |
| `git push` fails | retry up to four times with backoff (2, 4, 8, 16 s); if it still fails, keep committing locally and note it in the log |
| The session restarts | section 4: read the log and backlog, check `git status`, rebuild if needed, continue |

## 14. Pitfalls from earlier work

- Any kernel that can throw must be fallible (`RegisterFallible` / `SetFallible`) or DuckDB reports an internal
  error; ANSI-mode kernels throw by design.
- Ad-hoc kernels go through `BindSparkKernel` (`Builder::Kernel`): plans that DuckDB copies by serialization need
  the kernel registry.
- Macros repeat their arguments: nested calls of a macro that uses a parameter twice grow exponentially. Prefer
  kernels or dispatchers for anything that may be nested.
- DuckDB's `TRY_CAST` that lowers the scale of a 38-digit decimal is very slow; use `SparkDecimalResult`.
- `StringUtil::Format("%016llx")` sign-extends; format hex by hand.
- The nesting guard once read `/proc/self/maps` on every statement (9x parse time): anything that runs per
  statement or per row must not do system calls or allocations that can be done once.
- `thread_local` caches are per thread: DuckDB runs one pipeline on many threads, so a cache must be correct when
  every thread fills its own, and cheap to check when the cached key matches.
- Results that tie under `LIMIT` make comparisons flaky: add a total order.
