# Plan: fuzz and performance testing for `spark_dialect`

This plan is a self-contained handoff. Read it fully before starting. It assumes no memory of
earlier sessions.

## 1. Context

This repository is a DuckDB v2.0.0 extension that adds two SQL dialects:

- `spark`: Spark SQL 3.5 semantics. It is lenient and also accepts some DuckDB-only SQL.
- `spark_strict`: the same, but it rejects SQL that Spark 3.5 rejects.

Read `README.md` first. The main code layers are:

| Layer | File | Notes for this work |
|---|---|---|
| Tokenizer and token rewrites | `src/spark_tokenizer.cpp` (about 1700 lines) | Hand-written rewrites over token vectors with index arithmetic. This is the most likely place for out-of-bounds reads and hangs on odd input. |
| Grammar | `src/spark_grammar.cpp` | Replaces DuckDB's `Expression` PEG rule. It holds parse-time rewrites of functions (`RewriteFunction`), and casts, CASE and ORDER BY hooks. Deep nesting stresses it. |
| Functions | `src/spark_functions.cpp`, `spark_library.cpp`, `spark_datetime.cpp`, `spark_formats.cpp`, `spark_extras.cpp`, `spark_macros.cpp` | About 440 Spark functions in schema `system.spark`. Operators compile to dispatcher calls such as `system.spark.add(a, b)`, which pick a kernel at bind time. |

The tests are differential: `test/make_golden.py` runs every case in `test/cases/*.sql` in PySpark
3.5 and pickles the results to `test/golden/`. `test/run_duckdb.py` runs the same SQL in DuckDB
and compares the results using `test/harness.py`. `harness.py` normalizes values, compares numbers
with a 1e-9 relative tolerance, and compares unordered results as multisets. Today, all 1233
existing cases plus the strict-mode corpora pass in CI (`.github/workflows/ci.yml`). CI builds the
extension and runs `make_golden.py` live, then runs a test matrix over the `spark` and
`spark_strict` dialects.

The goal of this work is to find what the hand-written corpus misses: crashes, internal errors and
wrong results on inputs nobody wrote by hand. It should also measure and reduce the cost of the
dialect compared with plain DuckDB.

## 2. Goals and acceptance criteria

The work is done when all of the following hold:

1. **Crash and robustness fuzzing**: there is a reproducible fuzzer, run with seeds, that feeds
   mutated and generated SQL to all three dialects (`duckdb`, `spark`, `spark_strict`), including
   an AddressSanitizer/UBSan build. A run of at least 200,000 inputs per dialect finishes with:
   - no process crash;
   - no `INTERNAL Error`;
   - no sanitizer report;
   - no input taking longer than 10 s unless it is listed as expected-slow (for example a huge
     `sequence`).
2. **Differential fuzzing against Spark**: there is a generator of valid Spark SQL whose results
   are compared with Spark 3.5. A run of at least 20,000 generated queries, plus the
   function-argument sweep (section 6.3), ends with every mismatch either fixed or listed in
   `test/fuzz/known_differences.toml` with a reason. That list should hold at most about 15 entries
   and must not contain open-ended patterns such as "all float formatting".
3. **Strict-mode agreement**: over the same generated queries, if Spark accepts a query then
   `spark_strict` accepts it and returns the same rows. If Spark raises a parse or analysis error,
   `spark_strict` raises an error too.
4. **Dialect isolation**: after `RESET current_dialect`, plain DuckDB SQL returns the same results
   with and without the extension loaded (section 6.5).
5. **Regression capture**: every bug fixed because of fuzzing has a minimal reproducer:
   - in `test/cases/fuzz_regressions.sql` with golden output, for wrong results and spurious
     errors;
   - or in `test/fuzz/crash_regressions.sql`, replayed DuckDB-only, for crashes, hangs and internal
     errors.
6. **Performance**: there is a benchmark suite (section 7) with a written report,
   `docs/PERFORMANCE.md`. The report covers parse overhead, per-function and per-operator overhead
   against native DuckDB equivalents, and TPC-H SF1 in the `spark` dialect against the default
   dialect. The 10 worst ratios are investigated, and each one is either fixed or explained.
7. **CI**:
   - a short, fixed-seed fuzz smoke job runs on every push and PR, in under about 10 minutes;
   - a scheduled nightly workflow runs longer fuzzing with random seeds and the sanitizer build;
   - benchmarks run on `workflow_dispatch` and nightly, with results in the step summary.

   Benchmarks never fail CI except for the catastrophic thresholds in section 7.6.
8. The existing suite still passes in both dialects: `python test/run_duckdb.py -v` and
   `python test/run_duckdb.py -v --dialect spark_strict`.

## 3. Ground rules

- **Spark is the oracle.** When DuckDB and Spark disagree, Spark is right. The only exceptions are
  documented non-goals: ANSI mode, and nondeterministic functions. Never edit golden files by hand,
  and never loosen `harness.py` comparison to make a fuzz mismatch go away. If normalization is
  genuinely wrong (an engine-specific Python type), fix it narrowly and add a unit test.
- **Everything is reproducible from a seed.** Every fuzz run prints its seed, and every failure
  record contains the exact SQL, the dialect, the seed and the generator version. Use
  `random.Random(seed)` instances only, never the global `random` module.
- **Fix bugs in the extension, not in the generator.** Don't stop generating a construct because it
  fails. Only exclude a construct when it is out of scope (ANSI mode, randomness, wall-clock time)
  and add it to the exclusion list with a reason.
- **Priority order for fixing**: crashes and sanitizer reports, then hangs, then `INTERNAL` errors,
  then wrong results, then DuckDB errors where Spark succeeds, then leniency in `spark_strict`.
  Fix all items in the first three classes. For the last three classes, fix them in descending
  frequency of their signature (section 6.6).
- **Keep commits small**, one bug class or tool per commit. Run the full existing suite before each
  commit that touches `src/`.
- **Clean up after side effects.** Fuzzed statements may write files (`COPY`, `EXPORT DATABASE`,
  `INSERT OVERWRITE DIRECTORY`, `CREATE TABLE ... LOCATION`). Run every engine with its working
  directory set to a temp directory, never the repo (see 5.1).

## 4. Environment setup

The container starts fresh. Java may already be present; check with `java -version` and install
`openjdk-17-jdk-headless` if not. The other prerequisites are Python 3.11+, git, cmake, ninja and
ccache.

```bash
sudo apt-get update && sudo apt-get install -y ninja-build ccache cmake linux-tools-generic
pip install "duckdb==2.0.0" "pyspark==3.5.*" pandas pytz hypothesis tomli
git clone --depth 1 --branch v2.0.0 https://github.com/duckdb/duckdb.git /home/user/duckdb
```

You need two builds, in separate directories so they don't fight over the cache.

**Release build with symbols.** Use it for differential fuzzing and benchmarks. This configure and
build is the same as CI's apart from the build type:

```bash
cmake -G Ninja -S /home/user/duckdb -B /home/user/build-rel \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_SHELL=ON -DBUILD_UNITTESTS=OFF \
  -DENABLE_SANITIZER=FALSE -DOVERRIDE_GIT_DESCRIBE=v2.0.0 \
  -DDUCKDB_EXTENSION_CONFIGS=$PWD/extension_config.cmake \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build /home/user/build-rel --target spark_dialect_loadable_extension duckdb
```

With 4 cores and a cold cache this takes about 30 to 45 minutes. Start it in the background
immediately and write tooling while it builds. The loadable extension lands at
`build-rel/extension/spark_dialect/spark_dialect.duckdb_extension`. The `duckdb` shell binary has
the extension statically linked, because `extension_config.cmake` loads it, so the shell can run
`SET current_dialect='spark'` without `LOAD`. Check this.

**Sanitizer build.** Use it for crash fuzzing only. It uses a shell binary, not the Python package,
because ASan cannot be loaded into a non-instrumented Python easily.

```bash
cmake -G Ninja -S /home/user/duckdb -B /home/user/build-asan \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHELL=ON -DBUILD_UNITTESTS=OFF \
  -DENABLE_SANITIZER=TRUE -DENABLE_UBSAN=TRUE -DOVERRIDE_GIT_DESCRIBE=v2.0.0 \
  -DDUCKDB_EXTENSION_CONFIGS=$PWD/extension_config.cmake \
  -DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build /home/user/build-asan --target duckdb
```

Check the actual sanitizer option names in DuckDB v2.0.0's `CMakeLists.txt` before running; they
have changed between releases. If a Debug ASan build is too slow, use `RelWithDebInfo` with
sanitizers.

Set `SPARK_EXTENSION=/home/user/build-rel/extension/spark_dialect/spark_dialect.duckdb_extension`.
Then run the existing suite once, before writing any code, to confirm the setup:

```bash
python test/make_golden.py && python test/run_duckdb.py -v && python test/run_duckdb.py -v --dialect spark_strict
```

## 5. Phase 0: hygiene and shared infrastructure

### 5.1 Remove stray files and isolate side effects

The `spark_strict` merge committed files that the corpus wrote as side effects:

- `dir/` and `out.csv` at the repo root;
- `test/dir/` and `test/out.csv`.

They come from `COPY ... TO 'out.csv'` and `EXPORT DATABASE 'dir'` in `test/cases/non_spark.sql`,
which the lenient dialect and Spark executed relative to the current directory. To fix this:

1. Delete those files with `git rm`.
2. Make `make_golden.py` and `run_duckdb.py` `chdir` into a fresh `tempfile.mkdtemp()` before
   running cases. Paths in case files must stay relative.
3. Add `dir/`, `out.csv` and `test/fuzz/out/` to `.gitignore` as a backstop.
4. Re-run both scripts and confirm that `git status` is clean afterwards.

### 5.2 Refactor the harness into reusable pieces

Fuzzers need to run single queries cheaply. Extract the following from `make_golden.py` and
`run_duckdb.py` into `test/engines.py`, without changing their behaviour:

- `SparkOracle`: one long-lived `SparkSession`, with the same configuration as `spark_session()`
  today. Its `run(sql) -> Result` returns `ok`, `rows` (normalized), `error` (first line) and
  `schema`. Its `reset()` drops all tables and views, like the end of `make_golden.py:main`. It
  also has a `setup(statements)` method.
- `DuckRunner`: runs queries in a **separate worker process**. A segfault or hang must not kill the
  fuzzer, and must be detectable.
  - Use `multiprocessing` with a `spawn` context. The worker owns a duckdb connection with the
    extension loaded and the given dialect, and receives SQL over a pipe.
  - Its `run(sql, timeout=10) -> Result` reports one of `ok`, `error`, `internal`, `crash` (the
    worker died; include the exit signal) or `timeout`.
  - On `timeout`, try `con.interrupt()` from a watchdog thread in the worker first. If the worker
    does not answer within 2 more seconds, kill it. Either way, restart the worker and replay the
    current setup statements.
  - Normalize results with `normalize_typed`, exactly as `run_duckdb.py` does today, including the
    multi-statement fallback for rewrites like `INSERT OVERWRITE`.
- `ShellRunner`: the same interface, backed by the ASan `duckdb` shell binary.
  - Run it with `-batch -bail` (or `.bail on`) and statements on stdin, with
    `ASAN_OPTIONS=detect_leaks=0:abort_on_error=1` and `UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1`.
  - Use it for batches: send 200 statements, each followed by a marker `SELECT 'MARK_<n>';`. On a
    crash, the last marker printed shows which statement crashed. Re-run that one alone to confirm.
  - Capture stderr. Any `ERROR: AddressSanitizer`, `runtime error:` or `INTERNAL Error` counts as a
    finding.

Then port `run_duckdb.py` and `make_golden.py` to use these classes, and confirm that the existing
suite results are unchanged. The case-file rules in `harness.py`'s docstring stay as they are.

### 5.3 Failure records

All fuzzers write findings to `test/fuzz/out/<run-id>/findings.jsonl`, which is gitignored. Each
record has these fields:

```json
{"kind": "crash|timeout|internal|mismatch|duck_error|lenient|strict_reject",
 "dialect": "spark", "sql": "...", "setup": ["..."], "seed": 123, "generator": "gen@1",
 "spark": {"ok": true, "rows": "...", "error": null}, "duck": {"status": "ok", "rows": "...", "error": null},
 "signature": "...", "reduced_sql": "..."}
```

`signature` is a stable key for deduplication; see 6.6. Fuzzers also write a `summary.md` that
counts findings per kind and signature, with one example per signature.

## 6. Phase 1: fuzzing

Put the code under `test/fuzz/` and give every tool a CLI:

```
test/fuzz/
  corpus.py           loads seed SQL: test/cases/*.sql (all files, setup blocks too)
  mutate.py           6.1 mutation fuzzer (DuckDB-only, crash oracle)
  gen/                6.4 typed query generator
    schema.py         fuzz tables and their edge-case data
    types.py          Spark type model
    signatures.py     function signature table
    exprs.py, queries.py
  args_sweep.py       6.3 function-argument sweep (differential)
  differential.py     6.4 / 6.5 runner (Spark vs spark vs spark_strict)
  isolation.py        6.5 default-dialect isolation check
  reduce.py           6.6 reducer
  triage.py           6.6 signature + summary
  replay.py           re-runs crash_regressions.sql / a findings file
  known_differences.toml
  crash_regressions.sql
```

### 6.1 Mutation fuzzer (crash and robustness, DuckDB only)

The goal is to make the tokenizer, the token rewrites and the grammar misbehave. The oracle is
crash, timeout, `INTERNAL` or a sanitizer report. Ordinary errors are fine.

Seeds are every statement in `test/cases/*.sql`, about 1,500, plus the inputs in 6.2. The mutators
are below; pick one to three per input, weighted:

- **Token-level** (the main mutator). Tokenize with a small Python lexer that understands Spark
  strings, backtick identifiers, numbers and operators, then apply one of these:
  - delete a token, duplicate a token, or swap two adjacent tokens;
  - insert a random token from a vocabulary. The vocabulary is all Spark keywords from
    `src/include/spark_keywords.hpp`, every word that `spark_tokenizer.cpp` matches on (grep for
    string comparisons, for example `"LATERAL"`, `"OVERWRITE"`, `"CLUSTER"`, `"TBLPROPERTIES"`), and
    the punctuation `( ) [ ] , . ; :: -> => <=> ! ~ ^ | &`;
  - truncate at a random token. Truncation is important: rewrites look ahead with index arithmetic,
    and truncated input hits the end-of-vector paths;
  - replace a literal with an edge literal from 6.3;
  - splice: replace a token range with a token range from another seed.
- **Character-level** (about 10% of inputs):
  - unbalanced quotes, `\` at the end of a string, `r'` without a closing quote, and `X'` with odd
    hex digits;
  - NUL and control characters, invalid UTF-8 bytes, overlong UTF-8, and BOMs;
  - `/*` without `*/`, and `--` comments at the end;
  - 1 MB string literals and 10,000-digit numbers.
- **Structural**:
  - nesting depth: wrap an expression in N levels of `(`, `-`, `NOT`, `CASE WHEN`, `array(`,
    `struct(` or `named_struct('a',`, with N in {10, 100, 1000, 10000};
  - long lists: 10,000 items in `IN (...)`, `VALUES`, `SELECT` columns, `concat(...)` or
    `stack(...)`;
  - many statements: 1,000 statements in one string, separated by `;`;
  - empty statements: `;;;`.

Run every mutated input against three targets:

- `DuckRunner(dialect='spark')`;
- `DuckRunner(dialect='spark_strict')`;
- `DuckRunner` with no `SET current_dialect`, but with the extension loaded. The extension must
  not destabilize the default dialect.

Run the same inputs in batches through `ShellRunner` (ASan).

Use these timeouts: 10 s per statement normally, and 30 s for structural mutations with
N >= 1000. Deep nesting may legitimately fail with a "max expression depth" or
"stack too deep" error. That is fine; a stack overflow segfault is not.

For any findings, record whether plain DuckDB (no extension) also crashes on the same input. If it
does, it's a DuckDB bug: mark it `upstream` and don't block on it. Still add it to
`crash_regressions.sql` with a `-- @upstream` directive so `replay.py` skips it.

Things to check specifically, from reading the code:

- `src/spark_tokenizer.cpp`: every `tokens[i + k]` access needs a bounds check. Truncation
  mutations near the keywords that the rewrites match (`LATERAL VIEW`, `INSERT OVERWRITE`,
  `WITH ROLLUP`, `TABLESAMPLE BUCKET`, `INTERVAL ... TO ...`, `window(`, `PIVOT`) are the most
  likely to find out-of-bounds reads. ASan will report them; in Release builds they may pass
  silently.
- Loops that scan for a matching `)` or a clause end without checking for end of input can hang;
  see `ClauseEnd` and `RewriteGroupingWindow`.
- `src/spark_library.cpp`: the `split` implementation uses `std::regex`, which is recursive and can
  overflow the stack on long inputs or patterns like `(a|b)*` against 100 KB strings. It also
  compiles the regex once per row. Fuzz `split`, `regexp_extract`, `regexp_replace`, `rlike` and
  `str_to_map` with long strings and pathological patterns.
- Recursive helpers in the grammar's parse-time rewrites, and in `DDLParser` (`spark_formats.cpp`)
  for `from_json` and `from_csv` schemas: try deeply nested schema strings such as
  `array<array<...>>` 10,000 levels deep, and unbalanced `<`.

### 6.2 Grammar-aware SQL from Spark's own test corpus

Spark's repo ships thousands of SQL test queries in `sql/core/src/test/resources/sql-tests/inputs/`
under the `v3.5.x` tag. Fetch that directory with a sparse checkout of `apache/spark` at tag
`v3.5.6` or later. Split it into statements with the same splitter Spark uses: `;` at the end of a
line, ignoring `--` comment lines and `--IMPORT`, `--SET` and `--CONFIG_DIM` directives. Then:

- Use every statement as an extra seed for 6.1.
- Run every statement differentially (6.4's runner) against Spark, in a fresh session per input
  file, because the files create their own tables. Expect a lot of noise here:
  - files that need Hive, ANSI mode (`ansi/` directory), `udf/` or `typeCoercion/` configuration
    variants should be skipped; list the skipped directories in the tool;
  - group the results by input file and triage the top signatures.

This corpus is valuable because it was written to cover Spark's edge cases, but treat it as a
finding source. It is not an acceptance gate: don't require 100% on it.

Don't commit the downloaded files. Fetch them in the tool (cache under `test/fuzz/out/`) and record
the Spark commit hash used.

### 6.3 Function-argument sweep (differential)

This is the highest-value fuzzer for wrong results. For every Spark function, call it with edge
values and compare with Spark.

**Seeds.** Use `test/spark35_function_docs.json`, which holds each function's documented examples
(the same examples the 868 cases come from). Parse each example, find its literal arguments, and
build variants that replace one literal at a time, then pairs, with edge values **of the same
literal kind**:

| Kind | Edge values |
|---|---|
| int | `0`, `1`, `-1`, `2147483647`, `-2147483648`, `NULL`, `127Y`, `-128Y`, `32767S`, `9223372036854775807L`, `-9223372036854775808L` |
| decimal | `0.0`, `-0.0BD`, `1E-38BD` (if valid), `99999999999999999999.99BD`, `CAST(1 AS DECIMAL(38,0))`, `0.1`, `NULL` |
| double | `0.0D`, `-0.0D`, `CAST('NaN' AS DOUBLE)`, `CAST('Infinity' AS DOUBLE)`, `CAST('-Infinity' AS DOUBLE)`, `1e308D`, `4.9e-324D`, `0.1D`, `NULL` |
| string | `''`, `' '`, `'  a  '`, `'NULL'`, `'é'`, `'日本語'`, `'😀'` (4-byte UTF-8), `'a\\b'`, `'%_'`, `'\t\n'`, a 10,000-char string via `repeat('ab', 5000)`, `NULL` |
| date | `DATE '1970-01-01'`, `DATE '0001-01-01'`, `DATE '9999-12-31'`, `DATE '1582-10-15'`, `DATE '2024-02-29'`, `NULL` |
| timestamp | `TIMESTAMP '1970-01-01 00:00:00'`, `TIMESTAMP '1969-12-31 23:59:59.999999'`, `TIMESTAMP '2024-02-29 12:00:00.123456'`, `TIMESTAMP '9999-12-31 23:59:59.999999'`, `NULL` |
| array | `array()`, `array(NULL)`, `array(1, NULL, 1)`, `CAST(NULL AS ARRAY<INT>)`, 1,000 elements via `sequence(1, 1000)` |
| map | `map()`, `map(1, NULL)`, `CAST(NULL AS MAP<INT,INT>)` |
| pattern or format string (date formats, `format_string`, regexes, `to_number`) | empty string, unknown letters, `'yyyy-MM-dd HH:mm:ss.SSSSSS'`, `'G'`, `'E'`, `'''quoted'''`, invalid regex `'('`, `'\\'` |
| count, position or length arguments | `0`, `-1`, `1`, length + 1, `2147483647`, `NULL` |

**Run.**

- Each variant runs in Spark, `spark` and `spark_strict`.
- Spark errors on a variant count as PASS when DuckDB also errors, matching the existing rule.
- Run variants in batches through one Spark session. Expect roughly 50 to 200 queries per second
  from Spark for constant-folded queries; budget accordingly. That's about 440 functions times
  about 30 variants, or roughly 13,000 queries.

**Also cover column inputs.** Constant folding hides bugs that appear only on vectors with mixed
nulls, dictionary vectors or constant vectors. For every variant, also run a column form:
`SELECT f(x) FROM VALUES (v1), (v2), (NULL), (v1) AS t(x)`. This form must use at least three rows,
including NULL and a repeated value. Run it in both engines.

**Gaps in the documentation.** Functions with no documented example are listed in
`test/spark35_functions.txt` but not in the docs JSON. For each, write one seed call by hand in
`test/fuzz/manual_seeds.sql`.

### 6.4 Typed query generator (differential)

Generate random valid Spark SQL over a fixed fuzz schema, then compare Spark, `spark` and
`spark_strict`.

**Schema** (`gen/schema.py`). Create about 5 temporary views with `CREATE TEMPORARY VIEW ... AS
SELECT * FROM VALUES ...`, the same mechanism as the setup blocks in `test/cases/queries.sql`. They
must work identically in both engines. Use columns of every Spark type:

- `tinyint`, `smallint`, `int`, `bigint`, `decimal(10,2)`, `decimal(38,18)`;
- `float`, `double`, `boolean`, `string`, `binary`;
- `date`, `timestamp`;
- `array<int>`, `array<string>`, `map<string,int>`, `struct<a:int,b:string>`;
- `array<struct<...>>`.

Use 8 to 20 rows per view, filled with the edge values from 6.3, NULLs in every column, duplicate
rows and duplicate keys (for joins and grouping). Include `-0.0` and `NaN` in double columns:
Spark's NaN semantics (`NaN = NaN` is true, and NaN sorts greatest) are a known divergence point
from DuckDB.

**Type model and signatures** (`gen/types.py`, `gen/signatures.py`):

- Model Spark types, and the implicit coercions Spark applies, only as far as the generator needs
  to produce mostly-valid SQL.
- Write a signature table for a priority set of about 150 functions:
  - all string, math, date/time, conditional, collection and aggregate functions that appear in
    `COVERAGE.md`;
  - each entry has argument kinds, return kind, and whether the function is aggregate, window,
    generator or nondeterministic.
- Generating invalid SQL sometimes is fine, because Spark's error is the oracle. Aim for at least
  80% of generated queries succeeding in Spark, and print that rate in the summary. A low rate
  means the generator is wasting time.

**Expressions** (`gen/exprs.py`):

- `gen_expr(type, depth)` chooses among the following:
  - a column of that type;
  - an edge literal;
  - a function from the signature table returning that type;
  - an operator (`+ - * / % div & | ^ ~ || <=> = <> < <= > >= AND OR NOT`);
  - `CASE`, `IF`, `COALESCE`, `NVL`, or `CAST`/`TRY_CAST` from another type;
  - a subscript (`arr[i]`, `map['k']`, `struct.a`);
  - a lambda in `transform`, `filter`, `aggregate` or `exists`;
  - a scalar subquery.
- Limit the depth to 4 by default, and to 8 in nightly runs.

**Query shapes** (`gen/queries.py`):

- projection plus `WHERE`;
- `GROUP BY` with aggregates, `HAVING`, `GROUPING SETS`, `ROLLUP` and `CUBE`, with `grouping()` and
  `grouping_id()`;
- window functions with `PARTITION BY`, `ORDER BY` and frames (`ROWS` and `RANGE` with numeric and
  interval offsets);
- joins: inner, left, right, full, `LEFT SEMI`, `LEFT ANTI` and cross, with `USING` and `ON`;
- `UNION`, `INTERSECT` and `EXCEPT`, with and without `ALL`, plus `MINUS`;
- `LATERAL VIEW [OUTER] explode` and `posexplode`, and `inline`;
- `PIVOT`;
- CTEs, and correlated and uncorrelated subqueries (`IN`, `EXISTS`, scalar);
- `ORDER BY` with `NULLS FIRST` and `NULLS LAST`, `SORT BY`, `DISTRIBUTE BY` and `CLUSTER BY`;
- `LIMIT` and `OFFSET`;
- `DISTINCT`, and `count(DISTINCT ...)`;
- `FILTER (WHERE ...)` on aggregates.

**Determinism rules.** These avoid false positives:

- If there is `LIMIT` or `OFFSET`, `ORDER BY` must give a total order over the projected
  columns (append all projected columns as tie-breakers). Otherwise, compare as a multiset; the
  harness already does this when there is no top-level `ORDER BY`.
- Window functions whose result depends on the order of tied rows (`row_number`, `first`, `last`,
  `lag`/`lead`, `collect_list` in a window) must have a total order in the window's `ORDER BY`.
- `collect_list`, `array_agg` and `string_agg`/`concat_ws` over `collect_list` are order-dependent
  without a total order. Wrap them in `array_sort` or `sort_array`, or only use them in windows with
  a total order.
- `first` and `last` as plain aggregates are nondeterministic in Spark; generate them only with
  `ignore nulls` over a single-row group, or skip them.
- Floating-point aggregates (`sum`, `avg`, `stddev` and friends over double) may differ in the last
  bits depending on summation order. The harness's 1e-9 relative tolerance usually covers this. If
  a signature shows up only with catastrophic cancellation (a sum close to 0 from large values),
  remove the large values from the double columns that feed aggregates, instead of raising the
  tolerance.
- Never generate `rand`, `randn`, `uuid`, `shuffle`, `current_*`, `now`, `unix_timestamp()` with no
  arguments, `monotonically_increasing_id`, `spark_partition_id`, `input_file_name`, or random-IV
  `aes_encrypt`. The existing case files mark these with `-- @nondeterministic`; collect the full
  list from there.

**Runner** (`differential.py`):

- For each query, run Spark, then `spark` and `spark_strict`, and classify the result:
  - `PASS`;
  - `mismatch` (rows differ);
  - `duck_error` (DuckDB errors where Spark succeeds);
  - `lenient` (Spark errors, `spark` succeeds; record it but don't fail);
  - `strict_reject` (Spark succeeds, `spark_strict` errors);
  - `strict_lenient` (Spark errors with a parse or analysis error, `spark_strict` succeeds);
  - `internal`, `crash`, `timeout`.
- Spark runtime errors are not parse or analysis errors. For example, `java.lang.ArithmeticException`
  doesn't occur in non-ANSI mode, but casts with `spark.sql.ansi.enabled=false` should return
  NULL. If Spark raises a runtime exception and DuckDB returns rows, record it as
  `lenient_runtime` for review. Only `[PARSE_SYNTAX_ERROR]` and analysis errors (`AnalysisException`,
  `[UNRESOLVED_...]`, `[DATATYPE_MISMATCH...]`, and so on) are strict-mode failures. Match on
  Spark's error class in brackets at the start of the message.
- Compare `spark_strict` rows with Spark too, not only `spark` rows. They should be identical, and
  any difference between the two DuckDB dialects is a bug by itself.
- Use the `--budget N`, `--seed S`, `--depth D`, `--shapes a,b,c` and `--functions f1,f2` flags.

### 6.5 Dialect isolation

The extension promises that DuckDB SQL is unaffected unless a Spark dialect is selected. To test
this, generate DuckDB-native queries, or take them from DuckDB's own test corpus. A good source is
the `.test` files under `duckdb/test/sql/` in the v2.0.0 checkout: extract the `query` and
`statement ok` blocks, and skip anything that needs other extensions.

Run each query in two connections:

- plain `duckdb` with no extension;
- `duckdb` with the extension loaded, after `SET current_dialect='spark'` and then
  `RESET current_dialect`.

Results must be identical, compared exactly rather than with tolerance, including error vs. no
error.

Also check that, in the default dialect after loading, functions in `system.spark` don't shadow any
DuckDB function. `SELECT add(1, 2)` and `SELECT concat('a', NULL)` must behave like plain DuckDB.

### 6.6 Triage and reduction

**Signature** (`triage.py`). Group findings so that 5,000 mismatches don't mean 5,000 tickets:

- **crash** and **timeout**: the top 3 frames of the ASan stack, or the first token-rewrite
  function name in the stack. Without a stack, use the mutator name and the first keyword of the
  statement.
- **internal** and **duck_error**: the error message with literals, numbers and identifiers masked.
- **mismatch**: the set of function and operator names in the reduced query, plus the Spark result
  type and the kind of difference (null vs. value, numeric, string, row count).

**Reducer** (`reduce.py`). Reduce each finding while the predicate "still yields the same
signature" holds:

- For generated queries, work on the generator's AST:
  - replace a subexpression with a child of the same type, or with a literal of that type;
  - drop `WHERE`, `GROUP BY` items, joins, CTEs, `ORDER BY` items and projections, while keeping
    the result deterministic;
  - shrink tables to the rows that matter by adding a `WHERE` on a row id column. Give every fuzz
    view an `id` column for this.
- For mutated input with no AST, use token-level delta debugging (ddmin).
- Reduction runs both engines on every step, so cap it at about 300 steps per finding.

**Recording the fix.**

1. Reduce the finding.
2. Fix the bug in `src/`.
3. Add the reduced query to `test/cases/fuzz_regressions.sql`, with its `-- @setup` views. Put
   crash, hang and internal-error findings in `test/fuzz/crash_regressions.sql` instead.
4. Run `python test/make_golden.py test/cases/fuzz_regressions.sql` and commit the new golden
   pickle.
5. Run the full suite in both dialects.

**Known differences** (`known_differences.toml`). Use one entry per signature:

```toml
[[difference]]
signature = "format_number|double|string"
reason = "Java DecimalFormat HALF_EVEN on binary doubles; not reproducible without Java's double->decimal expansion"
example = "SELECT format_number(0.125D, 2)"
```

`differential.py` loads this list and reports matches as `KNOWN`, not as failures. Treat it as a
last resort: try to fix first, and only add an entry if a fix would need a disproportionate amount
of new code. Each entry needs an example query that still reproduces it, and `replay.py` checks
that the example still differs. If it doesn't, the entry is stale and must be removed.

## 7. Phase 2: performance

The question is how much running a query through the `spark` dialect costs compared with the
equivalent native DuckDB SQL. Measure that ratio, find where the cost goes, and remove it where
possible without changing semantics.

Put the benchmarks under `bench/`, outside `test/`:

```
bench/
  common.py         timing helpers, connection setup, result JSON writer
  parse_bench.py    7.1
  func_bench.py     7.2 / 7.3 (reads bench/func_pairs.toml)
  func_pairs.toml   Spark expression <-> native DuckDB expression pairs
  tpch_bench.py     7.4
  tpch/             the 22 TPC-H queries in Spark SQL
  scaling_bench.py  7.5
  report.py         writes docs/PERFORMANCE.md from results JSON
```

**Methodology** (put it in `common.py` and state it in the report):

- Use the RelWithDebInfo build and the loadable extension in the `duckdb==2.0.0` Python package.
  Use `SET threads = 1` for microbenchmarks, so results are stable and ratios are meaningful, and
  `SET threads = <nproc>` for TPC-H.
- Do 1 warm-up run, then 5 measured runs. Report the median and the min/max. Use
  `time.perf_counter_ns`. Fetch results with `fetchall()` only when the result is small; otherwise
  wrap the query as `SELECT count(*) FROM (<q>)`, or use `fetch_arrow_table()` so Python object
  creation doesn't dominate.
- Watch for the `count(*)` wrapper letting the optimizer remove the projection. To avoid it, check
  the plan with `EXPLAIN` once, or wrap as `SELECT sum(hash(<expr>))` for scalar benchmarks.
- Record the machine (`nproc`, CPU model from `/proc/cpuinfo`), the DuckDB version and the
  extension git SHA in the results JSON.
- Ratios (spark time / native time) are the primary metric because they are fairly stable across
  machines. Absolute times are secondary.

### 7.1 Parse and rewrite overhead

Every statement passes through the Spark tokenizer (pre-scan plus about 40 rewrite passes) and a
larger grammar. To measure the cost:

- For each statement in `test/cases/*.sql`, time 1,000 parses in each dialect.
  - Parse without executing. Check whether `con.extract_statements(sql)` in the Python API uses the
    connection's current dialect. If it does, use it; if not, time `PREPARE` plus `DEALLOCATE`, or
    `EXPLAIN`, and note in the report what is included.
  - Compare against the default dialect for statements that are valid in both. Among other cases,
    a large share of `queries.sql` is plain SQL.
- Measure scaling: generate statements with 10, 100, 1,000 and 10,000 select items, `IN` list
  entries, `CASE` branches, `UNION ALL` branches and nested parentheses, and plot parse time
  against size.
  - Any superlinear curve in the Spark dialect that the default dialect doesn't show is a finding:
    most likely a rewrite pass that rescans the token vector, or `vector::insert` in a loop. Each
    rewrite in `spark_tokenizer.cpp` that inserts or erases tokens inside a loop is O(n²).
  - Fix it by building a new vector in one pass.
- Target: parse overhead under 2x the default dialect for ordinary statements, and linear scaling.

### 7.2 Function overhead against native equivalents

`bench/func_pairs.toml` lists pairs of a Spark expression and the closest native DuckDB expression
with the same result on the benchmark data:

```toml
[[pair]]
name = "upper"
spark = "upper(s)"
native = "upper(s)"
data = "strings"

[[pair]]
name = "split_regex"
spark = "split(s, '[,;]')"
native = "regexp_split_to_array(s, '[,;]')"
data = "csv_strings"
```

**Data sets.** Generate these in the default dialect, as `CREATE TABLE ... AS SELECT ... FROM
range(N)`, with `N` = 10,000,000 for cheap functions and 1,000,000 for expensive ones:

- `ints`: int and bigint, with 10% NULLs;
- `doubles`;
- `decimals`: `decimal(18,2)` and `decimal(38,10)`;
- `strings`: short ASCII, long ASCII and non-ASCII;
- `csv_strings`;
- `dates` and `timestamps`;
- `arrays`: lengths 0 to 20;
- `maps`;
- `json_strings`.

**Coverage.** Include at least:

- every operator: `+ - * / % div`, comparisons, `<=>`, `||`, bitwise, unary minus;
- `CAST` between all common pairs, including string to int, string to date, double to string and
  decimal to decimal;
- `CASE`, `if`, `coalesce`, `nvl`;
- subscripts `arr[i]` and `map[k]`;
- aggregates: `sum`, `avg`, `count`, `min`, `max` and `count(DISTINCT)`, over int, bigint, double
  and decimal;
- the 60 most commonly used Spark functions. Use your judgment, but include string, date-format,
  regex, JSON, array and map functions, and hash functions;
- every function implemented as a macro in `src/spark_macros.cpp`, because macros are expanded
  inline and can multiply work. For example, a macro that references its argument three times
  evaluates it three times unless DuckDB's common-subexpression elimination applies.

**Running.** For each pair, run `SELECT sum(hash(<expr>)) FROM <data>` in the `spark` dialect for
the Spark expression and in the default dialect for the native one. The native expression must
fully match the semantics on that data. Check once that both produce the same `sum(hash(...))`, or
explain why not, for example different NULL handling on purpose.

Functions without a native equivalent (Murmur3 `hash`, `xxhash64`, AES, Java date patterns, and
so on) get an absolute throughput number, rows per second, with no ratio.

### 7.3 Known suspects to measure first

These come from reading the code. Confirm each one with a number before changing anything.

1. **Operators compile to dispatcher calls.** `a + b` becomes `system.spark.add(a, b)`, which binds
   to a kernel. Check that the kernel for int + int is a tight loop using
   `BinaryExecutor::Execute`, and doesn't do per-row `Value` work. Expect a ratio near 1x; anything
   over 1.5x for simple arithmetic is a finding. Also check that DuckDB's optimizer can still
   constant-fold, filter-push and use statistics through these calls:
   - compare `EXPLAIN` for `WHERE a + 1 > 5` in both dialects;
   - check that a `WHERE` filter on a Parquet or table column still pushes down when written in
     Spark style, for example `WHERE id = 5` with an int literal, and `WHERE d = DATE '2024-01-01'`;
   - check zone-map and min/max filter pushdown on a 100M-row table with `id BETWEEN ...`.

   If comparisons are rewritten into function calls, pushdown may be lost. That is a large
   end-to-end cost, likely larger than any kernel cost. If so, consider leaving comparisons between
   same-typed operands as native DuckDB comparisons where the semantics are identical (for example
   int vs. int, or date vs. date).
2. **`sum` and `avg` carry an extra `any_value(x)` aggregate** to compute Spark's result type
   (README, Limitations; `spark_grammar.cpp` around line 872). Measure `sum(int)`, `sum(decimal)`
   and `avg(double)` with `GROUP BY` and high cardinality. Possible fix: make `sum_typed` a real
   aggregate, or compute the result type at bind time from the argument type, without an extra
   aggregate. The bind step of a dispatcher can see the argument type.
3. **`split` uses `std::regex` and compiles the pattern per row** (`spark_library.cpp` around line
   195). Also check `parse_url` (around line 688) and any other per-row `std::regex`. Possible fixes:
   - cache the compiled pattern when the pattern argument is constant, using
     `FunctionLocalState` or bind data;
   - switch to RE2, which DuckDB already bundles (`third_party/re2`), which also removes the
     stack-overflow risk from 6.1.

   With RE2, Java regex semantics must be preserved as far as they are today. Check against the
   existing `split` cases and the 6.3 sweep for `split`.
4. **Macros wrapping arguments in `system.spark.as_string(...)`** and similar conversion helpers,
   for example `rlike`/`regexp` in `spark_macros.cpp` line 30. Check that the helper is a no-op
   cast on `VARCHAR` input, and doesn't copy strings.
5. **`case_when` dispatcher** (`spark_functions.cpp`). Spark `CASE` compiles to a function call
   instead of a native `CASE`. Native `CASE` evaluates branches lazily for selected rows only; a
   function call evaluates every branch for every row. That is both slower and a semantic risk:
   an error in an unselected branch. In non-ANSI mode most branches don't error, but `raise_error`,
   `assert_true` and division by zero on decimals can. Measure, and test
   `CASE WHEN x = 0 THEN 0 ELSE 1 / x END` style queries for spurious errors. The dispatcher may already bind to a
   native `BoundCaseExpression` with casts; check the code, and make it do so if it doesn't.
6. **Java datetime pattern formatting and parsing** (`spark_datetime.cpp`). Check that the pattern
   is parsed once per chunk or bind (constant pattern), not per row.
7. **`window` and `session_window` grouping rewrite** (README limitation): the `FROM` source
   becomes a subquery. Measure it with a 10M-row table.
8. **`array_sort` with a comparator** is O(n²) (README). Measure it at array sizes 10, 100, 1,000
   and 10,000. If the comparator is the common shape, `(l, r) -> CASE WHEN l < r THEN -1 ...` or
   `-> l - r`, consider recognizing it and lowering it to native `list_sort`. Otherwise, a merge
   sort that calls the comparator O(n log n) times works if the comparator can be evaluated on
   pairs.
9. **Token rewrites that insert or erase inside loops** (see 7.1).
10. **`CHAR(n)` padding cast** over large tables.

### 7.4 End-to-end: TPC-H SF1 (and SF10 nightly if time allows)

- Generate the data with DuckDB's `tpch` extension in the default dialect: `INSTALL tpch; LOAD
  tpch; CALL dbgen(sf=1);`. The `tpch` extension is downloaded from extensions.duckdb.org; if the
  network blocks it, build it from source by adding `duckdb_extension_load(tpch)` to a local copy
  of the extension config. Save the data to a DuckDB database file and reuse it.
- Write the 22 queries in Spark SQL dialect in `bench/tpch/q01.sql` to `q22.sql`. Start from the
  standard TPC-H text, and use Spark syntax where it differs: `date '1998-12-01' - interval 90 day`,
  `substring`, `extract`, `interval '3' month`. Make sure each one runs in Spark too: run each at
  SF0.01 in Spark and DuckDB, compare results with the harness, and add them to a test case file
  `test/cases/tpch_small.sql` so correctness is checked in CI. The SF0.01 data for Spark can be
  exported from DuckDB to Parquet and read with `CREATE TEMPORARY VIEW ... USING parquet OPTIONS
  (path ...)`. Check that the `spark` dialect accepts that form, or create the views from `VALUES`.
- Benchmark each query three ways:
  - the native DuckDB TPC-H query (`PRAGMA tpch(n)`, default dialect);
  - the Spark text in the `spark` dialect;
  - the Spark text in `spark_strict`.

  Report each query's time and the ratio to native. Target: geometric-mean ratio at most 1.15, and
  no single query over 1.5x without an explanation in the report.
- For any query over 1.3x, compare `EXPLAIN ANALYZE` plans side by side and record the cause:
  - a lost filter pushdown;
  - a dispatcher call;
  - a lost join reorder because of a function in the join condition;
  - the `any_value` extra aggregate;
  - something else.

### 7.5 Scaling and resource limits

Some inputs are slow because of their size, not because of a bug in the query. They need bounded
time and memory, and a proper error instead of a crash. This doubles as robustness testing:

- `sequence(1, 100000000)`, `repeat('x', 1000000000)` and `array_repeat(1, 1000000000)` should give
  an error or run within memory limits, not crash or take down the machine. Run them under
  `SET memory_limit='1GB'`.
- `explode` of a 1M-element array; `posexplode` over 1M rows of 100-element arrays.
- `collect_list` into one huge list; `concat_ws` over it.
- Deep JSON (`from_json` and `get_json_object` with 10,000 levels of nesting); huge JSON strings.
- `str_to_map` with 1M pairs.
- `format_string` with 10,000 arguments.

For each, record the time, the peak RSS (from `/usr/bin/time -v` or `resource.getrusage` in the
worker) and the outcome. Put anything that crashes or exceeds 60 s into the 6.1 findings.

### 7.6 CI thresholds (catastrophic only)

CI machines are noisy, so hard gates are limited to large regressions:

- For the TPC-H SF0.1 ratio, run in CI: fail if the geometric mean exceeds 1.5x or any query
  exceeds 3x native.
- For microbenchmark ratios in the smoke subset: fail if any ratio exceeds 3x its value in the
  committed baseline `bench/baseline.json`. Update the baseline deliberately, in its own commit,
  with an explanation.
- Parse scaling: fail if 10,000 select items take more than 20x the time of 1,000. That catches an
  O(n²) regression.

Everything else is reported in the step summary, not gated.

### 7.7 Optimization loop

For each suspect that the numbers confirm:

1. Profile it, for example with
   `perf record -g python bench/func_bench.py --only <name>` followed by
   `perf report --no-children`, or with DuckDB's `PRAGMA enable_profiling='json'`.
2. Write the fix.
3. Re-run the full differential suite in both dialects, plus the 6.3 sweep for the affected
   functions. Performance fixes must not change results.
4. Re-run the benchmark and record the before and after numbers in `docs/PERFORMANCE.md`.

Don't spend more than about a day of effort on any single suspect; record what was learned and
move on.

## 8. CI integration

Add jobs to `.github/workflows/ci.yml`, reusing the existing `build` and `golden` artifacts:

- **`fuzz-smoke`** (every push and PR; needs `build`):
  - `python test/fuzz/replay.py`: all crash regressions must pass;
  - `python test/fuzz/mutate.py --seed 1 --budget 20000 --dialects spark,spark_strict,duckdb`;
  - `python test/fuzz/differential.py --seed 1 --budget 500`. This needs Spark: install Java and
    PySpark as the `golden` job does;
  - `python test/fuzz/isolation.py --budget 500`.

  Fail on any crash, internal error, timeout or new mismatch signature not in
  `known_differences.toml`. Upload `test/fuzz/out/` as an artifact.
- **`bench-smoke`** (every push and PR, or only on `main` if it is too slow): parse scaling, about
  20 microbenchmark pairs at reduced N, and TPC-H SF0.1, with the thresholds from 7.6.
- **`.github/workflows/nightly.yml`** (`schedule: cron` daily, plus `workflow_dispatch`):
  - build the ASan shell (cache it with ccache like the release build);
  - run `mutate.py` with a random seed, printed and recorded, for 45 minutes per dialect;
  - run the full `args_sweep.py`;
  - run `differential.py --budget 20000 --depth 6`;
  - run the 6.2 Spark-corpus run;
  - run the full benchmarks, including TPC-H SF1.

  Upload the findings and summary, and write the summary to `$GITHUB_STEP_SUMMARY`. The nightly
  run fails only on crashes and internal errors, so wrong-result findings don't turn it red
  forever. Report them in the summary instead.

Keep the existing `test` matrix unchanged.

## 9. Pitfalls

- **Spark session state leaks between queries.** Temp views created by setup blocks persist. Use
  `SparkOracle.reset()` between independent runs and recreate the fuzz schema. Spark's catalog
  calls are slow, so don't reset per query.
- **Timezones.** Both engines must run in UTC: `TZ=UTC` and `time.tzset()` in every process,
  including worker processes, `spark.sql.session.timeZone=UTC`, and `SET TimeZone='UTC'` in
  DuckDB. The worker processes created by `spawn` start fresh, so set this in the worker entry
  point.
- **PySpark result conversion.** `collect()` returns Python objects that `harness.normalize`
  already handles: Rows, Decimals, datetimes, and dicts for maps. Year-month intervals come back as
  ints; see `values_equal`. Don't call `toPandas()`, because it changes types.
- **Spark result-size limits.** Keep generated results small (at most 1,000 rows); add
  `LIMIT 1000` with a total `ORDER BY` when needed. `collect()` of large results is slow and
  memory-hungry.
- **Spark accepts some invalid things lazily.** Analysis happens in `spark.sql()`, execution in
  `collect()`. The `SparkOracle` must catch exceptions from both, and record whether the error was
  analysis-time or run-time, which 6.4 uses.
- **Grammar caching.** Each dialect's grammar is compiled once per database and cached. A worker
  process that switches dialects is fine, but tests should use one connection per dialect to keep
  results attributable.
- **`spark_strict` deliberately lets `CREATE TABLE tbl (dummy ...)` without `USING` through** (see
  README). Add that to the known-differences list with the README's reason, rather than
  re-discovering it.
- **The PEG parser's errors** can mention rewritten text (README, Limitations). Error messages are
  not compared, only error vs. no error, so this is fine. When signatures are built from DuckDB
  error messages, mask the rewritten function names (`system.spark.*`, `__spark_*`) as well as
  literals.
- **Floats in `string` output.** Spark's `CAST(double AS STRING)` uses Java's `Double.toString`,
  which the extension emulates. The 6.3 edge doubles (`4.9e-324`, `1e308`, `0.1`, and values like
  `1.0E7`, `1.0E-3` where Java switches to scientific notation) are where this breaks. Add
  `1.0E7D`, `9999999.0D`, `0.001D` and `0.0001D` to the double edge list.
- **Decimal results.** Spark returns `Decimal` with fixed scale. `harness.normalize` normalizes
  decimals, so `1.00` and `1` compare equal. This hides scale differences in string casts; casting
  a decimal to string compares strings exactly, so you still catch them there.
- **Don't run Spark and the ASan build at the same time on a 4-core box.** Spark needs about 2 GB
  of heap and the ASan shell is memory-hungry. Schedule the phases one after another.
- **The extension's `INTERNAL` errors abort the database in DuckDB v2.** The connection is
  invalidated after an internal error. `DuckRunner` must restart the worker after any `internal`
  status, as well as after `crash` and `timeout`.

## 10. Suggested order and milestones

1. Phase 0: set up the environment and both builds, clean up stray files, refactor the harness
   (5.1 to 5.3), and confirm the existing suite still passes. Commit.
2. Write the mutation fuzzer (6.1) with the Release build, run it, fix crashes, then move to the
   ASan build. Commit each fix with a crash regression. Then add the `fuzz-smoke` CI job (mutation
   and replay parts only).
3. Write the argument sweep (6.3), triage it and fix bugs. Commit fixes with regression cases.
4. Write the typed generator and differential runner (6.4), the strict-mode classifications, the
   reducer and triage (6.6). Fix bugs. Extend `fuzz-smoke`.
5. Add the isolation check (6.5), then the Spark corpus run (6.2).
6. Benchmarks 7.1 to 7.5, then the first `docs/PERFORMANCE.md`.
7. The optimization loop (7.7) for confirmed suspects, in order of end-to-end impact (TPC-H
   first).
8. The CI benchmark job, the nightly workflow, and the README updates: a "Fuzzing" and a
   "Performance" section, with commands and current numbers. Remove any README limitation that
   was fixed, such as the `array_sort` O(n²) note.

Open a pull request when milestone 4 is reached, and push to it as work continues, so CI runs on
the new jobs early.

## 11. Final deliverables

- `test/engines.py` (or equivalent), and the ported `make_golden.py` and `run_duckdb.py`.
- `test/fuzz/` tools with CLIs and a `test/fuzz/README.md` covering usage, seeds, how to reproduce a
  finding, and how to add a known difference.
- `test/cases/fuzz_regressions.sql` with golden output, `test/fuzz/crash_regressions.sql` and
  `test/fuzz/known_differences.toml`.
- `bench/` tools, `bench/baseline.json` and `docs/PERFORMANCE.md`. The report covers methodology,
  machine, parse overhead, the function ratio table, TPC-H per-query table and geometric mean,
  scaling results, the suspects list with outcomes, and remaining work.
- CI jobs `fuzz-smoke` and `bench-smoke`, and `.github/workflows/nightly.yml`.
- A final summary in the PR description:
  - inputs run per fuzzer and per dialect;
  - bugs found and fixed, by class;
  - remaining known differences;
  - performance before and after, for TPC-H geometric mean and the worst 10 function ratios.
