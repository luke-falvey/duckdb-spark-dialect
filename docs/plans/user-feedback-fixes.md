# Plan: fix the gaps a user project found (and the class of bug behind each)

Self-contained handoff. Read `README.md`, the status section of `docs/plans/spark-parity-testing.md` and this file
before starting. Every item below was reproduced on `main` at `1b051cf` and compared with a live Spark.

## 0. Context

A user runs a 77-model dbt-style project (174 build steps) through the extension and compares with Apache Spark
**3.5.0**. After installing the build from PR #7 they reported: one gap fixed (`trim(decimal)`), four still open, and
**two regressions** that forced workarounds in their models. They also noted that our own suite did not contain most
of these cases. The reported items, all confirmed:

| # | Report | Status on `main` | Kind |
|---|---|---|---|
| 1 | `substring` / `left` over `coalesce` / `ifnull` fails to bind | **regression**, lenient dialect | bug |
| 2 | a user macro (`rld_hash`) over `coalesce` / `ifnull` fails to bind | **regression**, same cause as 1 | bug |
| 3 | window function nested inside a window definition: parse error | open | missing feature |
| 4 | `sum` / `avg(string)` with a non-number errors (Spark: NULL for that row) | open | bug |
| 5 | `PIVOT ... IN (1 AS x)` on a DECIMAL column: every pivoted column NULL | open | bug |
| 6 | `to_date(timestamp, 'dd/MM/yyyy')` is NULL (Spark ignores the format) | open | bug |
| 7 | `spark_strict` accepts `CAST(x AS VARCHAR)`; Spark rejects it | open | strictness |

Local reference is Spark 3.5.9 (`pip install pyspark==3.5.*`; see the environment notes in section 6). The user is on
3.5.0. For each fix, also run the reproducer once under `pyspark==3.5.0` (a second virtualenv) and note any
difference between 3.5.0 and 3.5.9 in the commit message; none is expected for these items.

## 1. Rules

The rules of `docs/plans/spark-parity-testing.md` section 3 apply (Spark is the only reference, never edit a golden
file by hand, never weaken a test, both dialects after every change, benchmarks after bind changes). In addition:

1. **No regression is acceptable.** Before regenerating `test/xfail.toml`, run every configuration against the
   *unchanged* `xfail.toml` of `main` and require `FAIL=0` in all eight runs. Use
   `scripts/dev/run_all_configs.sh <absolute dir>` (about 50 minutes on 4 cores), then
   `scripts/dev/regen_xfail.sh <same dir>`, then run again and require `FAIL=0 XPASS=0 stale=0`.
2. Each fix lands with its reproducer in `test/cases/` (golden from Spark) **and** with the generalised cases of
   section 3. A fix without the generalised cases is not done.
3. One commit per item. Open one PR at the end (the user asked for a PR only on completion).

## 2. The fixes

Order: 2.1 first (it is a regression users hit today), then 2.2 to 2.6 in any order, 2.7 last.

### 2.1 Aliased rewrite breaks every macro call (items 1 and 2)

```sql
SELECT substring(coalesce(x, 'abc'), 1, 2), left(ifnull(x, 'abc'), 2) FROM VALUES ('hello'), (NULL) t(x);
-- Binder Error: Macro "substr"() has positional argument following named argument
CREATE MACRO my_hash(a) AS md5(CAST(a AS VARCHAR));
SELECT my_hash(coalesce(x, 'abc')) FROM VALUES ('hello') t(x);   -- does not support the supplied arguments
SELECT struct(coalesce(x, 'abc')) FROM VALUES ('hello') t(x);    -- field is named "coalesce(x, 'abc')"; Spark: col1
SELECT date_add(coalesce(d, DATE'2024-01-01'), 1) FROM VALUES (DATE'2024-02-02') t(d);  -- same error
```

**Cause.** `FinalizeFunctionHookResult` in `src/spark_grammar.cpp` rewrites `COALESCE(a, b)` to
`system.spark.coalesce_value(a, b)` and calls `call->SetAlias("coalesce(a, b)")` to keep Spark's column name
(introduced in `02f85ae`). DuckDB reads an aliased function argument as a **named argument**, so any macro (the
extension's ~300 SQL macros and the user's own) that receives it fails, and `struct()` uses the alias as a field name.
It did not show in `spark_strict`, or for `upper` / `lpad`, only because those arguments happen to be wrapped by
`check_arg` / `as_string`, which hides the alias.

**Fix.** An alias must only exist on a select-list item. Options, in order of preference:
1. Do not set the alias in the function hook. Give the column its Spark name where select-list items are finalized
   (the place that already derives names for unaliased expressions, see `DefaultFieldName` and the result-target
   handling in `spark_grammar.cpp`): if the top-level expression of a select item is the `coalesce_value` call and
   has no alias, name it there.
2. If there is no select-item hook that sees it, keep `SetAlias` but strip aliases from the arguments of every
   function / macro / operator / `CASE` / cast as they are built (one helper called from `RewriteFunction` and the
   `SparkCall` builders). This is more invasive and easier to miss a path; prefer 1.

Check the other `SetAlias` calls in `spark_grammar.cpp` (lines near 950, 1690, 1929) for the same hazard and say in
the commit what each is for.

**Verify.** The four statements above match Spark in both dialects; `SELECT coalesce(x, 'a')` still returns the
column name `coalesce(x, a)` behaviour it has today (names are gated only for explicit aliases, so compare with the
current output, not with Spark's name).

### 2.2 Window function inside a window definition (item 3)

```sql
SELECT x, rank() OVER (PARTITION BY g ORDER BY sum(x) OVER (PARTITION BY g)) FROM VALUES (1, 1), (2, 1) t(x, g);
SELECT x, row_number() OVER (PARTITION BY rank() OVER (ORDER BY x) ORDER BY x) FROM VALUES (1), (2) t(x);
SELECT x, sum(x) OVER w FROM VALUES (1, 1) t(x, g) WINDOW w AS (PARTITION BY g ORDER BY rank() OVER (ORDER BY x));
-- Parser Error: window functions are not allowed in window definitions      (Spark accepts all three)
```

**Cause.** DuckDB's PEG transformer rejects it (`transformer.in_window_definition`, in
`duckdb/src/parser/peg/transformer/transform_expression.cpp` at the two `throw ParserException` sites), and DuckDB's
binder cannot nest window functions either. Spark plans the inner window first, then the outer one.

**Fix.** Rewrite to what Spark does: compute inner windows in a derived table. The repository already has this kind
of rewrite in the tokenizer for `window()` / `session_window()` grouping columns ("the FROM source of that query block
becomes a subquery computing the column", `src/spark_tokenizer.cpp` around line 1700): reuse its machinery for
finding a query block's FROM source and select list.
- For a query block whose `OVER (...)` or named `WINDOW` definition contains a window call: replace each inner window
  call with a generated column `__spark_w<n>`, and wrap the block's source as
  `(SELECT *, <inner call> AS __spark_w<n> FROM <source> [WHERE ...]) `. The outer block keeps its select list.
- Blocks with `GROUP BY` / `HAVING` / aggregates: the inner window is evaluated *after* aggregation in Spark. Handle
  the non-aggregated case first; for aggregated blocks raise a clear `[UNSUPPORTED_FEATURE]` error (not a parse
  error) unless the general rewrite falls out naturally, and list it in the README limitations.
- Generated columns must not leak into `SELECT *` of the outer block: project them away (`* EXCLUDE (...)` in the
  outer select list rewritten by the tokenizer, as the existing rewrite does for its helper columns).
- Nesting deeper than one level: apply the rewrite repeatedly, innermost first.

**Verify.** The three statements, plus: inner window in `PARTITION BY` only, in `ORDER BY` only, in both; inner
`lag` / `lead` / `row_number` / `sum` / `count`; a named window referenced twice; the inner window using a different
partition than the outer; `SELECT *` next to it; a CTE and a subquery containing it; `QUALIFY`-free filter on the
result through an outer query.

### 2.3 Aggregates over strings (item 4)

```sql
SELECT sum(x), avg(x) FROM VALUES ('1'), ('abc'), ('2.5') t(x);          -- Spark: 3.5, 1.75. Here: Conversion Error
SELECT stddev(x), variance(x), var_pop(x), stddev_pop(x) FROM VALUES ('1'), ('abc'), ('3') t(x);   -- no overload
SELECT corr(x, y), covar_pop(x, y), skewness(x), kurtosis(x), try_sum(x), try_avg(x) FROM ...;      -- no overload
SELECT sum(x) OVER (ORDER BY g), sum(DISTINCT x), sum(x) FILTER (WHERE x <> 'abc'), ... GROUP BY g  -- same error
```

**Cause.** `BindCastToDouble` in `src/spark_aggregates.cpp` adds DuckDB's strict cast
(`BoundCastExpression::AddCastToType(..., DOUBLE)`), which raises on `'abc'` and `''`. Spark's implicit cast gives
NULL for a string that is not a number (non-ANSI) and trims whitespace. The statistical aggregates are SQL macros
over DuckDB's aggregates with no string handling at all.

**Fix.**
- `sum` / `avg`: cast with Spark's string-to-double semantics. `SparkCast` lives in `spark_functions.cpp`; expose a
  small function (declared in `spark_dialect.hpp`) that returns the Spark cast of an expression to DOUBLE, or bind
  `system.spark.as_double(arg)` and use that as the argument. Under ANSI the malformed string must raise
  `CAST_INVALID_INPUT` when evaluated (see `SparkImplicitCast`).
- `stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`,
  `skewness`, `kurtosis`, `try_sum`, `try_avg`, `mean`, `regr_*`: route the argument(s) through
  `system.spark.as_double_param(x)` (already used by `skewness` / `kurtosis`; it also rejects booleans, dates and
  nested types like Spark). `median` / `percentile` already work.
- Do **not** change `min` / `max` / `count` / `first` / `collect_*` (strings stay strings there).

**Verify.** All of the above in plain, `GROUP BY`, `OVER`, `DISTINCT` and `FILTER` forms; strings `'1'`, `' 4 '`,
`''`, `'abc'`, `'1e3'`, `'NaN'`, `'Infinity'`, NULL; a column of only malformed strings (Spark: NULL); the ANSI
configuration. `sum(true)` and `avg(DATE ...)` stay analysis errors as in Spark.

### 2.4 PIVOT values are compared as text (item 5)

```sql
SELECT * FROM (SELECT CAST(k AS DECIMAL(10,2)) k, v FROM VALUES (1, 10), (2, 20) t(k, v))
PIVOT (sum(v) FOR k IN (1 AS a, 2 AS b));           -- Spark: 10, 20.   Here: NULL, NULL
-- also NULL here: k DOUBLE with IN (1, 2); k DECIMAL(10,2) with IN (1.0, '2').
-- works here: DECIMAL(10,0), BIGINT, STRING, DATE with a string value.
```

**Cause.** DuckDB's PIVOT matches the `IN` values against the pivot column by their text (`1.00` is not `1`). Spark
casts each `IN` value to the pivot column's type and compares values.

**Fix.** `RewritePivotAggregates` in `src/spark_tokenizer.cpp` already rewrites the PIVOT clause. Options:
1. Rewrite the pivot to conditional aggregation, which is what Spark does semantically:
   `agg(v) FILTER (WHERE k <=> CAST(<value> AS <typeof k>))` per value and aggregate, grouped by the remaining
   columns. The type of `k` is not known in the tokenizer, so emit a dispatcher call such as
   `system.spark.pivot_match(k, <value>)` that binds to Spark's equality with implicit casts (`cmp_nsafe` after
   casting the literal to the column type). This also fixes the known `types-pivot` difference (PIVOT aggregates are
   HUGEINT in DuckDB) as a side effect: check whether those xfail entries then pass.
2. Keep DuckDB's PIVOT but pivot on `system.spark.pivot_key(k)` and the values on `pivot_key(<value cast>)` with a
   canonical text form. Less work, but the cast target type is still unknown at tokenizer time; only choose this if
   1 turns out too large.

Multi-column pivots (`FOR (a, b) IN ((1, 'x') AS p, ...)`), several aggregates with aliases, and the implicit
group-by columns must keep working: the existing cases in `queries.sql` / `regressions.sql` cover them.

**Verify.** Pivot column types TINYINT, INT, BIGINT, DECIMAL(10,0), DECIMAL(10,2), FLOAT, DOUBLE, STRING, DATE,
TIMESTAMP, BOOLEAN, each with integer, decimal and string `IN` values; a value that matches no row; NULL in the
column; aliases and no aliases; one and two aggregates.

### 2.5 `to_date` / `to_timestamp` with a format and a non-string value (item 6)

```sql
SELECT to_date(TIMESTAMP'2024-03-05 10:11:12', 'dd/MM/yyyy'), to_date(DATE'2024-03-05', 'dd/MM/yyyy');
SELECT to_timestamp(TIMESTAMP'2024-03-05 10:11:12', 'dd/MM/yyyy'), to_timestamp(DATE'2024-03-05', 'dd/MM/yyyy');
SELECT try_to_timestamp(ts, 'dd'), to_timestamp_ntz(ts, 'dd'), to_timestamp_ltz(d, 'dd') ...   -- all NULL here
```

**Cause.** The two-argument macros in `src/spark_macros.cpp` (lines 166 to 170) are
`parse_datetime(as_string(s), f)`: a date or timestamp is rendered as text and then fails to parse with the format.
In Spark, `ParseToDate(left, format)` / `ParseToTimestamp` only parse when `left` is a string; a DATE, TIMESTAMP or
TIMESTAMP_NTZ input is cast and the format is ignored. (`unix_timestamp` / `to_unix_timestamp` already do this.)

**Fix.** A dispatcher (bind-time, like `as_timestamp`) for the two-argument forms: if the first argument is a
date/timestamp type, return the cast; if it is a string (or NULL), parse with the format; other types follow Spark
(check with Spark what `to_date(20240305, 'yyyyMMdd')` does: it works there, through a cast to string). Use it in
`to_date`, `to_timestamp`, `to_timestamp_ltz`, `to_timestamp_ntz`, `try_to_timestamp`.

**Verify.** Each of the five functions with DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING, INT and NULL first arguments,
with a format that does not match the rendered value; under the `America/Los_Angeles` configuration too.

### 2.6 `spark_strict`: `VARCHAR` / `CHAR` without a length (item 7)

```sql
SELECT CAST(1 AS VARCHAR);   -- Spark: [DATATYPE_MISSING_SIZE]. spark_strict accepts it.
SELECT CAST(1 AS CHAR);      -- same
```

**Fix.** In `spark_strict` only, reject `VARCHAR` / `CHAR` / `CHARACTER` without `(n)` in type positions (casts,
`CREATE TABLE` columns, nested types) with `[DATATYPE_MISSING_SIZE]`. See `SpCharType` in `src/spark_grammar.cpp`
(line ~1968). The lenient dialect keeps accepting it. Add the statements to `test/cases/non_spark.sql` (the corpus
of things strict must reject) and check `test/cases/strict_valid.sql` still passes.

### 2.7 Housekeeping

- `README.md`: remove fixed items from "Limitations", add what 2.2 leaves unsupported.
- Status section of `docs/plans/spark-parity-testing.md`: one paragraph for this pass with the counts.
- `COVERAGE.md` and `docs/plans/spark-parity/known_differences.md`: regenerate
  (`python scripts/coverage_report.py`, `python scripts/xfail_summary.py`) after the final default run.

## 3. Generalise: the class behind each bug

The user is right that our suite missed these. The matrices test one function or one operator at a time, with
literal arguments. Real models **compose**: a rewritten expression inside a macro, inside a user macro, inside a
window. Add these generators; each writes a case file under `test/cases/` whose expectations come from Spark, and
each must be added to a CI group in `scripts/case_group.py`.

### 3.1 Composition matrix (would have caught items 1 and 2)

New generator `scripts/gen_composition_cases.py`, output `test/cases/types/c15_composition.sql`:
- **Outer functions:** every Spark function that is implemented as a SQL macro (`SPARK_MACROS` in
  `spark_macros.cpp`) or a dispatcher, taking the documented example from `test/spark35_function_docs.json` (the C6
  generator `test/fuzz/args_sweep.py` already finds argument slots: reuse `slots()`).
- **Inner expressions**, substituted into each slot when the type fits: every form the grammar rewrites.
  `coalesce(v, d)`, `ifnull`, `nvl`, `nvl2`, `nullif`, `CASE WHEN`, `if()`, `CAST(v AS T)`, typed literals (`1L`, `1S`, `1Y`, `1.5BD`, `1D`), `v + 0`, `v || ''`, `concat(v, '')`, a scalar subquery,
  `greatest` / `least`, `element_at` / `arr[0]`, a struct field, a lambda-bearing call (`transform(arr, x -> x)[0]`),
  an aggregate (in a grouped query), a window function (in a windowed query).
- Budget: one example per function, at most 2 slots, about 15 inner forms: roughly 8,000 statements, 10 minutes of
  Spark. Columns as well as literals for the inner expression (`FROM VALUES ... t(v)`).
- A second small file, written by hand, for **user-defined macros** (`CREATE MACRO` is DuckDB syntax, so the file
  runs in the lenient dialect only and its expectations are the same query with the macro body inlined, recorded
  from Spark): scalar macro, macro with two arguments, macro calling a Spark function, table macro; each called with
  the inner forms above.

Also add to `test/fuzz/gen.py` (the differential fuzzer's generator) the inner forms it does not produce yet, so the
nightly run keeps composing them.

### 3.2 Rewrites must not leave a trace (the mechanism of item 1)

A unit-level check, no Spark needed: for every expression rewrite in `spark_grammar.cpp` /`spark_tokenizer.cpp`,
the result used as (a) a positional macro argument, (b) a `struct()` field, (c) a `GROUP BY` key that is also
selected, (d) an `ORDER BY` key, (e) a `PIVOT` / `IN` value, binds and gives the same value as the bare
expression. Put it in a new `test/cases/rewrite_positions.sql` (Spark-compared) and grep the source for other
side channels a rewrite can leave: `SetAlias`, `SetQueryLocation`-dependent messages, generated column names.

### 3.3 Implicit casts of aggregate arguments (the class of item 4)

Extend C12 (`scripts/gen_type_cases.py`, aggregates and windows matrix): it already crosses aggregates with types,
but its string values are all numeric-looking or all not. Add mixed columns (`'1'`, `'abc'`, `''`, `' 2 '`, NULL)
and the `DISTINCT`, `FILTER`, `OVER`, and `GROUP BY` forms for every aggregate in `test/spark35_functions.txt` that
Spark marks as an aggregate. The C12 xfail id `types-open-c12` has about 1,300 entries today, 526 of them in
`spark_strict` alone for "aggregate over a type Spark rejects": after 2.3, route every numeric aggregate through
`as_double_param` or an equivalent typed bind and re-measure.

### 3.4 Value matching by text (the class of item 5)

Search for other places where DuckDB compares or groups by a text rendering where Spark compares values:
`PIVOT` (2.4), `UNPIVOT` column lists, `IN` over mixed types in a **subquery** (already listed open), map key
lookups with a key of another numeric type (`map(1.0, 'a')[1]`, `element_at(map(1L, 'a'), 1)`), `array_contains`
/ `array_position` with a value of another numeric type, `find_in_set`, `str_to_map` keys, JOIN `USING` keys
(open). Write the cases first (a hand-written file `test/cases/value_matching.sql`, about 60 statements), record
from Spark, fix what is local, xfail the rest with one id each.

### 3.5 "Format argument is ignored / used" by input type (the class of item 6)

For every function with a format or unit parameter (`to_date`, `to_timestamp*`, `try_to_timestamp`,
`unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `date_format`, `to_char`, `to_varchar`, `to_number`,
`try_to_number`, `trunc`, `date_trunc`, `to_binary`, `try_to_binary`, `format_number`, `next_day`), cross the value
argument with DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING (matching and not matching the format), INT, DECIMAL, NULL.
C6 covers part of this with one example per function; the gap is that its swept value always replaces the
documented string. Add the cross to `gen_type_cases.py` as matrix C16 (about 600 statements).

### 3.6 Nested analytic constructs (the class of item 3)

Hand-written `test/cases/nested_constructs.sql`, recorded from Spark: window inside window (2.2); window inside an
aggregate argument (Spark rejects: check the error class); aggregate inside a window partition / order
(`rank() OVER (ORDER BY sum(x))` with `GROUP BY`: works in Spark and must work here); window in `WHERE` / `HAVING`
(rejected by Spark); window inside a lambda; window over a `LATERAL VIEW`; window inside `CASE` and `coalesce`;
generators inside windows. Fix or xfail each.

### 3.7 Strict type-name audit (the class of item 7)

`test/cases/non_spark.sql` lists what strict must reject. Generate the type-name part instead of hand-listing it:
every DuckDB type name and alias (from `duckdb_types()`), with and without modifiers, in `CAST`, `CREATE TABLE`,
`ARRAY<...>` and function-style casts; record Spark's verdict; strict must agree. Expect new rejections for
`VARCHAR`, `CHAR`, `TEXT`, `INT1/2/4/8`, `FLOAT4/8`, `BPCHAR` and similar names.

## 4. Acceptance

1. The seven items match Spark in the dialect(s) they apply to; the user's workarounds
   (`substr(coalesce(..))`, `rld_hash(nvl(a, b))`, `rank()` to `row_number()`, `sum(cast(.. as double))`, casting PIVOT
   values, dropping the `to_date` format) are no longer needed. State this in the PR description as a table with
   the same rows as the user's.
2. `scripts/dev/run_all_configs.sh` against `main`'s `xfail.toml`: `FAIL=0` in all eight runs. After
   `regen_xfail.sh`: `FAIL=0 XPASS=0 stale=0`.
3. The new generators of section 3 are in CI (`unit` job: generated files up to date; a case group each), their
   failures fixed or carrying an xfail id with a reason. No new `unclassified` id.
4. Fuzzers: `replay.py`, `replay.py --known`, `isolation.py`, `mutate.py --seed 1 --budget 4000`,
   `differential.py --seed 1 --budget 150` report no findings. Smoke benchmarks within `bench/baseline.json`
   (`bench/report.py --check`); 2.1 and 2.3 touch hot bind paths, so run them.
5. Harness unit tests, `test/settings/check_inventory.py`, `scripts/gen_arg_type_rules.py --check` pass.

## 5. What not to do

- Do not special-case `substring` / `left` / the user's macro name: item 1 is an alias leak, fix the leak.
- Do not make aggregates lenient about types Spark rejects (`sum(true)`): only strings and untyped NULL convert.
- Do not turn the nested-window parse error into a silent wrong result: unsupported shapes must raise.
- Do not regenerate `xfail.toml` before the no-regression run of rule 1.1.

## 6. Environment notes (cloud sessions)

- Build: `git clone --depth 1 --branch v2.0.0 https://github.com/duckdb/duckdb.git`, configure as in
  `.github/workflows/ci.yml`, target `spark_dialect_loadable_extension` (first build about 35 minutes on 4 cores,
  incremental builds about 1 minute). Copy the result to `build/spark_dialect.duckdb_extension`.
- `pip install pyspark` fails to build the wheel on this image. Download the sdist
  (`pip download pyspark==3.5.* --no-deps`), unpack it, copy `deps/{jars,bin,sbin,data,licenses,examples}` into
  `pyspark/`, copy `pyspark/` into site-packages, `chmod +x pyspark/bin/*`, `pip install py4j==0.10.9.7`. Java 21
  works with Spark 3.5.
- `extensions.duckdb.org` (TPC-H / TPC-DS data extensions) and GitHub's artifact storage are blocked by the network
  policy: read CI job logs with `gh api repos/<owner>/<repo>/actions/jobs/<id>/logs` instead of artifacts.
- A full default-configuration run takes about 25 minutes per dialect; run the four configurations two at a time
  per dialect as `run_all_configs.sh` does. All commands share one memory limit: do not run Spark recording and
  the eight comparison runs together.
